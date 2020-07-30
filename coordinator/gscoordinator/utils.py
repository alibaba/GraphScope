#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import copy
import glob
import json
import logging
import numbers
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from io import BytesIO
from queue import Empty as EmptyQueue
from queue import Queue
from string import Template

import yaml
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

from gscoordinator.io_utils import PipeWatcher

logger = logging.getLogger("graphscope")

try:
    import gscoordinator

    COORDINATOR_HOME = os.path.abspath(os.path.join(gscoordinator.__file__, "..", ".."))
except ModuleNotFoundError:
    COORDINATOR_HOME = os.path.abspath(os.path.join(__file__, "..", ".."))

GRAPHSCOPE_HOME = os.path.join(COORDINATOR_HOME, "..")

WORKSPACE = "/tmp/gs"
DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"
ANALYTICAL_ENGINE_HOME = os.path.join(GRAPHSCOPE_HOME, "analytical_engine")
ANALYTICAL_ENGINE_PATH = os.path.join(ANALYTICAL_ENGINE_HOME, "build", "grape_engine")

if not os.path.isfile(ANALYTICAL_ENGINE_PATH):
    ANALYTICAL_ENGINE_HOME = "/usr/local/bin"
    ANALYTICAL_ENGINE_PATH = "/usr/local/bin/grape_engine"
TEMPLATE_DIR = os.path.join(COORDINATOR_HOME, "gscoordinator", "template")
BUILTIN_APP_RESOURCE_PATH = os.path.join(
    COORDINATOR_HOME, "gscoordinator", "builtin/app/builtin_app.gar"
)


def is_port_in_use(host, port):
    """Check whether a port is in use.

    Args:
        port (int): A port.

    Returns:
        bool: True if the port in use.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0


def get_lib_path(app_dir, app_name):
    lib_path = ""
    if sys.platform == "linux" or sys.platform == "linux2":
        lib_path = os.path.join(app_dir, "lib%s.so" % app_name)
    elif sys.platform == "darwin":
        lib_path = os.path.join(app_dir, "lib%s.dylib" % app_name)
    else:
        raise RuntimeError("Unsupported platform.")
    assert os.path.isfile(lib_path), "Error occurs when building the frame library."
    return lib_path


def compile_app(workspace: str, app_name: str, attr, engine_config: dict):
    """Compile an application.

    Args:
        workspace (str): working dir.
        app_name (str): target app_name.
        attr (`AttrValue`): All information needed to compile an app.

    Returns:
        str: Path of the built library.
    """

    app_dir = os.path.join(workspace, app_name)
    os.makedirs(app_dir, exist_ok=True)

    # extract gar content
    _extract_gar(app_dir, attr)

    # codegen app and graph info
    # vd_type and md_type is None in cpp_pie
    (
        app_type,
        app_header,
        app_class,
        vd_type,
        md_type,
        pregel_combine,
    ) = _codegen_app_info(app_dir, DEFAULT_GS_CONFIG_FILE, attr)
    logger.info(
        "Codegened application type: %s, app header: %s, app_class: %s, vd_type: %s, md_type: %s, pregel_combine: %s",
        app_type,
        app_header,
        app_class,
        str(vd_type),
        str(md_type),
        str(pregel_combine),
    )

    graph_header, graph_type = _codegen_graph_info(attr)
    logger.info("Codegened graph type: %s, Graph header: %s", graph_type, graph_header)

    os.chdir(app_dir)

    module_name = ""
    cmake_commands = [
        "cmake",
        ".",
        "-DEXPERIMENTAL_ON=" + engine_config["experimental"],
    ]
    if app_type != "cpp_pie":
        if app_type == "cython_pregel":
            pxd_name = "pregel"
            cmake_commands += ["-DCYTHON_PREGEL_APP=True"]
            if pregel_combine:
                cmake_commands += ["-DENABLE_PREGEL_COMBINE=True"]
        else:
            pxd_name = "pie"
            cmake_commands += ["-DCYTHON_PIE_APP=True"]

        # Copy pxd file and generate cc file from pyx
        shutil.copyfile(
            os.path.join(TEMPLATE_DIR, "{}.pxd.template".format(pxd_name)),
            os.path.join(app_dir, "{}.pxd".format(pxd_name)),
        )
        # Assume the gar will have and only have one .pyx file
        for pyx_file in glob.glob(app_dir + "/*.pyx"):
            module_name = os.path.splitext(os.path.basename(pyx_file))[0]
            cc_file = os.path.join(app_dir, module_name + ".cc")
            subprocess.check_call(["cython", "-3", "--cplus", "-o", cc_file, pyx_file])
        app_header = "{}.h".format(module_name)

    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(app_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=app_name,
            _vd_type=vd_type,
            _md_type=md_type,
            _graph_type=graph_type,
            _graph_header=graph_header,
            _module_name=module_name,
            _app_type=app_class,
            _app_header=app_header,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)

    # compile
    cmake_process = subprocess.Popen(
        cmake_commands,
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    cmake_stdout_watcher = PipeWatcher(cmake_process.stdout, sys.stdout)
    setattr(cmake_process, "stdout_watcher", cmake_stdout_watcher)
    cmake_process.wait()

    make_process = subprocess.Popen(
        ["make", "-j4"],
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    make_stdout_watcher = PipeWatcher(make_process.stdout, sys.stdout)
    setattr(make_process, "stdout_watcher", make_stdout_watcher)
    make_process.wait()

    return get_lib_path(app_dir, app_name)


def generate_graph_type_sig(attr: dict):
    graph_type = attr[types_pb2.GRAPH_TYPE].graph_type

    if graph_type == types_pb2.ARROW_PROPERTY:
        oid_type = attr[types_pb2.OID_TYPE].s.decode("utf-8")
        vid_type = attr[types_pb2.VID_TYPE].s.decode("utf-8")
        graph_signature = "vineyard::ArrowFragment<{},{}>".format(oid_type, vid_type)
    elif graph_type == types_pb2.ARROW_PROJECTED:
        oid_type = attr[types_pb2.OID_TYPE].s.decode("utf-8")
        vid_type = attr[types_pb2.VID_TYPE].s.decode("utf-8")
        vdata_type = attr[types_pb2.V_DATA_TYPE].s.decode("utf-8")
        edata_type = attr[types_pb2.E_DATA_TYPE].s.decode("utf-8")
        graph_signature = "gs::ArrowProjectedFragment<{},{},{},{}>".format(
            oid_type, vid_type, vdata_type, edata_type
        )
    elif graph_type == types_pb2.DYNAMIC_PROJECTED:
        vdata_type = attr[types_pb2.V_DATA_TYPE].s.decode("utf-8")
        edata_type = attr[types_pb2.E_DATA_TYPE].s.decode("utf-8")
        graph_signature = "gs::DynamicProjectedFragment<{},{}>".format(
            vdata_type, edata_type
        )
    else:
        raise ValueError("Unsupported graph type: {}".format(graph_type))
    return graph_signature


def compile_graph_frame(
    workspace: str, frame_name: str, attr: dict, engine_config: dict
):
    """Compile an application.

    Args:
        workspace (str): Working dir.
        frame_name (str): Target app_name.
        attr (`AttrValue`): All information needed to compile a graph library.

    Raises:
        ValueError: When graph_type is not supported.

    Returns:
        str: Path of the built graph library.
    """

    frame_dir = os.path.join(workspace, frame_name)
    os.makedirs(frame_dir, exist_ok=True)

    graph_signature = generate_graph_type_sig(attr)

    logger.info("Codegened graph frame type: %s", graph_signature)

    os.chdir(frame_dir)

    graph_type = attr[types_pb2.GRAPH_TYPE].graph_type

    cmake_commands = [
        "cmake",
        ".",
        "-DEXPERIMENTAL_ON=" + engine_config["experimental"],
    ]
    if graph_type == types_pb2.ARROW_PROPERTY:
        cmake_commands += ["-DPROPERTY_GRAPH_FRAME=True"]
    elif (
        graph_type == types_pb2.ARROW_PROJECTED
        or graph_type == types_pb2.DYNAMIC_PROJECTED
    ):
        cmake_commands += ["-DPROJECT_FRAME=True"]
    else:
        raise ValueError("Illegal graph type: {}".format(graph_type))
    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(frame_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=frame_name,
            _graph_type=graph_signature,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)

    # compile
    cmake_process = subprocess.Popen(
        cmake_commands,
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    cmake_stdout_watcher = PipeWatcher(cmake_process.stdout, sys.stdout)
    setattr(cmake_process, "stdout_watcher", cmake_stdout_watcher)
    cmake_process.wait()

    make_process = subprocess.Popen(
        ["make", "-j4"],
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    make_stdout_watcher = PipeWatcher(make_process.stdout, sys.stdout)
    setattr(make_process, "stdout_watcher", make_stdout_watcher)
    make_process.wait()

    return get_lib_path(frame_dir, frame_name)


def _extract_gar(workspace: str, attr):
    """Extract gar to workspace

    Args:
        workspace (str): Working directory
        attr (`AttrValue`): Optionally it can contains the bytes of gar.
    """
    fp = BUILTIN_APP_RESOURCE_PATH  # default is builtin app resources.
    if types_pb2.GAR in attr:
        # if gar sent via bytecode in attr, overwrite.
        fp = BytesIO(attr[types_pb2.GAR].s)
    with zipfile.ZipFile(fp, "r") as zip_ref:
        zip_ref.extractall(workspace)


def _codegen_app_info(workspace: str, meta_file: str, attr):
    """Codegen application by instanize the template specialization.

    Args:
        workspace (str): Working directory
        meta_file (str): A yaml file that contains metas of all builtin app.
        attr (`AttrValue`): For get algorithm name of app.

    Raises:
        KeyError: If the algorithm name doesn't exist in the `meta_file`

    Returns:
        type: app_type
        app class: for fulfilling the CMakelists.
    """
    algo = attr[types_pb2.APP_ALGO].s.decode("utf-8")

    with open(os.path.join(workspace, meta_file), "r") as f:
        config_yaml = yaml.safe_load(f)

    for app in config_yaml["app"]:
        if app["algo"] == algo:
            app_type = app["type"]  # cpp_pie or cython_pregel or cython_pie
            if app_type == "cpp_pie":
                return (
                    app_type,
                    app["src"],
                    "{}<_GRAPH_TYPE>".format(app["class_name"]),
                    None,
                    None,
                    None,
                )
            if app_type in ("cython_pregel", "cython_pie"):
                # cython app doesn't have c-header file
                return (
                    app_type,
                    "",
                    "",
                    app["vd_type"],
                    app["md_type"],
                    app["pregel_combine"],
                )

    raise KeyError("Algorithm does not exist in the gar resource.")


# a mapping for classname to header file.
GRAPH_HEADER_MAP = {
    types_pb2.IMMUTABLE_EDGECUT: (
        "grape::ImmutableEdgecutFragment",
        "grape/fragment/immutable_edgecut_fragment.h",
    ),
    types_pb2.DYNAMIC_PROJECTED: (
        "gs::DynamicProjectedFragment",
        "core/fragment/dynamic_projected_fragment.h",
    ),
    types_pb2.ARROW_PROPERTY: (
        "vineyard::ArrowFragment",
        "vineyard/graph/fragment/arrow_fragment.h",
    ),
    types_pb2.ARROW_PROJECTED: (
        "gs::ArrowProjectedFragment",
        "core/fragment/arrow_projected_fragment.h",
    ),
    types_pb2.DYNAMIC_PROPERTY: (
        "gs::DynamicFragment",
        "core/fragment/dynamic_fragment.h",
    ),
}


def _codegen_graph_info(attr):
    graph_type = attr[types_pb2.GRAPH_TYPE].graph_type
    graph_class, graph_header = GRAPH_HEADER_MAP[graph_type]
    # graph_type is a literal of graph template in c++ side
    if graph_class == "vineyard::ArrowFragment":
        # in a format of full qualified name, e.g. vineyard::ArrowFragment<double, double>
        graph_fqn = "{}<{},{}>".format(
            graph_class,
            attr[types_pb2.OID_TYPE].s.decode("utf-8"),
            attr[types_pb2.VID_TYPE].s.decode("utf-8"),
        )
    elif graph_class in (
        "gs::ArrowProjectedFragment",
        "grape::ImmutableEdgecutFragment",
    ):
        # in a format of gs::ArrowProjectedFragment<int64_t, uint32_t, double, double>
        # or grape::ImmutableEdgecutFragment<int64_t, uint32_t, double, double>
        graph_fqn = "{}<{},{},{},{}>".format(
            graph_class,
            attr[types_pb2.OID_TYPE].s.decode("utf-8"),
            attr[types_pb2.VID_TYPE].s.decode("utf-8"),
            attr[types_pb2.V_DATA_TYPE].s.decode("utf-8"),
            attr[types_pb2.E_DATA_TYPE].s.decode("utf-8"),
        )
    else:
        # gs::DynamicProjectedFragment<double, double>
        graph_fqn = "{}<{},{}>".format(
            graph_class,
            attr[types_pb2.V_DATA_TYPE].s.decode("utf-8"),
            attr[types_pb2.E_DATA_TYPE].s.decode("utf-8"),
        )
    return graph_header, graph_fqn


def distribute_lib_on_k8s(hosts: str, lib_path: str):
    dir = os.path.dirname(lib_path)
    for pod in hosts.split(","):
        subprocess.check_call(
            [
                "kubectl",
                "exec",
                pod,
                "-c",
                "engine",
                "--",
                "mkdir",
                "-p",
                dir,
            ]
        )
        subprocess.check_call(
            [
                "kubectl",
                "cp",
                lib_path,
                "{}:{}".format(pod, lib_path),
                "-c",
                "engine",
            ]
        )


def distribute_lib_via_hosts(hosts: str, lib_path: str):
    dir = os.path.dirname(lib_path)
    for host in hosts.split(","):
        if host not in ("localhost", "127.0.0.1"):
            subprocess.check_call(["ssh", host, "mkdir -p {}".format(dir)])
            subprocess.check_call(["scp", lib_path, "{}:{}".format(host, lib_path)])


def create_single_op_dag(op_type, config=None):
    op_def = op_def_pb2.OpDef(op=op_type, key=uuid.uuid4().hex)
    if config:
        for k, v in config.items():
            op_def.attr[k].CopyFrom(v)

    dag = op_def_pb2.DagDef()
    dag.op.extend([op_def])
    return dag


def dump_as_json(schema, path):
    out = {}
    items = []
    idx = 0
    for i in range(len(schema.vertex_labels)):
        vertex = {"id": idx, "label": schema.vertex_labels[i], "type": "VERTEX"}
        vertex["propertyDefList"] = []
        for j in range(len(schema.vertex_property_names[i].s)):
            names = schema.vertex_property_names[i]
            types = schema.vertex_property_types[i]
            vertex["propertyDefList"].append(
                {"id": j, "name": names.s[j], "data_type": types.s[j].upper()}
            )
        vertex["indexes"] = []
        vertex["indexes"].append({"propertyNames": [names.s[0]]})
        items.append(vertex)
        idx += 1

    for i in range(len(schema.edge_labels)):
        edge = {"id": idx, "label": schema.edge_labels[i], "type": "EDGE"}
        edge["propertyDefList"] = []
        for j in range(len(schema.edge_property_names[i].s)):
            names = schema.edge_property_names[i]
            types = schema.edge_property_types[i]
            edge["propertyDefList"].append(
                {"id": j, "name": names.s[j], "data_type": types.s[j].upper()}
            )
        edge["rawRelationShips"] = []
        edge["rawRelationShips"].append(
            {"srcVertexLabel": "xx", "dstVertexLabel": "xx"}
        )
        idx += 1
        items.append(edge)
    out["types"] = items
    out["partitionNum"] = 4
    with open(path, "w") as fp:
        json.dump(out, fp)


def dump_string(schema_string, path):
    with open(path, "w") as fp:
        fp.write(schema_string)


def parse_readable_memory(value):
    value = str(value).strip()
    num = value[:-2]
    suffix = value[-2:]
    try:
        float(num)
    except ValueError as e:
        raise ValueError(
            "Argument cannot be interpreted as a number: %s" % value
        ) from e
    if suffix not in ["Ki", "Mi", "Gi"]:
        raise ValueError("Memory suffix must be one of 'Ki', 'Mi' and 'Gi': %s" % value)
    return value


def parse_as_glog_level(log_level):
    # log level in glog: INFO=1, DEBUG=10
    # log level in python: DEBUG=10, INFO=20
    if isinstance(log_level, str):
        if log_level == "silent" or log_level == "SILENT":
            log_level = -1
        else:
            log_level = getattr(logging, log_level.upper())
    python_to_glog = {10: 10, 20: 1}
    return python_to_glog.get(log_level, 1)


def str2bool(s):
    if isinstance(s, bool):
        return s
    if s.lower() in ("yes", "true", "t", "y", "1"):
        return True
    return False


class ResolveMPICmdPrefix(object):
    """
    Class for resolving prefix of mpi command.

    Examples:

    .. code:: ipython

        >>> # openmpi found
        >>> rmcp = ResolveMPICmdPrefix()
        >>> (cmd, env) = rmcp.resolve(4, 'h1, h2, h3')
        >>> cmd
        ['mpirun', '--allow-run-as-root',
         '-n', '4', '-host', 'h1:2,h2:1,h3:1']

        >>> env
        {'OMPI_MCA_plm_rsh_agent': '/usr/bin/kube_ssh', # if /usr/bin/kube_ssh in $PATH
         'OMPI_MCA_btl_vader_single_copy_mechanism': 'none',
         'OMPI_MCA_orte_allowed_exit_without_sync': '1'}

        >>> # if openmpi not found, change to mpich
        >>> rmcp = ResolveMPICmdPrefix()
        >>> (cmd, env) = rmcp.resolve(4, 'h1, h2, h3')
        >>> cmd
        ['mpirun', '-n', '4', '-host', 'h1:2,h2:1,h3:1']
        >>> env
        {} # always empty
    """

    _OPENMPI_RSH_AGENT = "OMPI_MCA_plm_rsh_agent"
    _KUBE_SSH_EXEC = "kube_ssh"

    def __init__(self, rsh_agent=False):
        self._rsh_agent = rsh_agent

    @staticmethod
    def openmpi():
        try:
            subprocess.check_call(["ompi_info"], stdout=subprocess.DEVNULL)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def alloc(num_workers, hosts):
        host_list = hosts.split(",")
        host_list_len = len(host_list)
        assert host_list_len != 0

        host_to_proc_num = {}
        if num_workers >= host_list_len:
            quotient = num_workers / host_list_len
            residue = num_workers % host_list_len
            for host in host_list:
                if residue > 0:
                    host_to_proc_num[host] = quotient + 1
                    residue -= 1
                else:
                    host_to_proc_num[host] = quotient
        else:
            raise RuntimeError("The number of hosts less then num_workers")

        for i in range(host_list_len):
            host_list[i] = "{}:{}".format(
                host_list[i], int(host_to_proc_num[host_list[i]])
            )

        return ",".join(host_list)

    def resolve(self, num_workers, hosts):
        cmd = []
        env = {}

        if self.openmpi():
            env["OMPI_MCA_btl_vader_single_copy_mechanism"] = "none"
            env["OMPI_MCA_orte_allowed_exit_without_sync"] = "1"
            # OMPI sends SIGCONT -> SIGTERM -> SIGKILL to the worker process,
            # set the following MCA parameter to zero will emilinates the chances
            # where the process dies before receiving the SIGTERM and do cleanup.
            env["OMPI_MCA_odls_base_sigkill_timeout"] = "0"

            if os.environ.get(self._OPENMPI_RSH_AGENT) is None:
                rsh_agent_path = shutil.which(self._KUBE_SSH_EXEC)
                if self._rsh_agent and rsh_agent_path is not None:
                    env[self._OPENMPI_RSH_AGENT] = rsh_agent_path
            cmd.extend(
                [
                    "mpirun",
                    "--allow-run-as-root",
                ]
            )
        else:
            # ssh agent supported only
            cmd.extend(["mpirun"])
        cmd.extend(["-n", str(num_workers)])
        cmd.extend(["-host", self.alloc(num_workers, hosts)])

        logger.debug("Resolve mpi cmd prefix: {}".format(cmd))
        logger.debug("Resolve mpi env: {}".format(env))
        return (cmd, env)


# In Analytical engine, assume label ids of vertex entries are continuous
# from zero, and property ids of each label is also continuous from zero.
# When transform schema to Maxgraph style, we gather all property names and
# unique them, assign each name a id (index of the vector), then preserve a
# vector<int> for each label, stores mappings from original id to transformed
# id.
def to_maxgraph_schema(gsa_schema_json):
    gsa_schema = json.loads(gsa_schema_json)
    prop_set = set()
    vertex_label_num = 0
    for item in gsa_schema["types"]:
        item["id"] = int(item["id"])
        if item["type"] == "VERTEX":
            vertex_label_num += 1
        for prop in item["propertyDefList"]:
            prop["id"] = int(prop["id"])
            prop_set.add(prop["name"])

    prop_list = sorted(list(prop_set))
    mg_schema = copy.deepcopy(gsa_schema)
    for item in mg_schema["types"]:
        if item["propertyDefList"] == "":
            item["propertyDefList"] = []
        if item["type"] == "VERTEX":
            for prop in item["propertyDefList"]:
                prop["id"] = 1 + prop_list.index(prop["name"])
        elif item["type"] == "EDGE":
            item["id"] = vertex_label_num + item["id"]
            for prop in item["propertyDefList"]:
                prop["id"] = 1 + prop_list.index(prop["name"])
    return json.dumps(mg_schema)
