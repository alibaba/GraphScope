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


import base64
import copy
import datetime
import glob
import hashlib
import inspect
import json
import logging
import numbers
import os
import pickle
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from queue import Empty as EmptyQueue
from queue import Queue
from string import Template

import yaml
from google.protobuf.any_pb2 import Any
from graphscope.framework import utils
from graphscope.framework.errors import CompilationError
from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.utils import PipeWatcher
from graphscope.proto import attr_value_pb2
from graphscope.proto import data_types_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

logger = logging.getLogger("graphscope")


# runtime workspace
try:
    WORKSPACE = os.environ["GRAPHSCOPE_RUNTIME"]
except KeyError:
    WORKSPACE = "/tmp/gs"

# COORDINATOR_HOME
#   1) get from gscoordinator python module, if failed,
#   2) infer from current directory
try:
    import gscoordinator

    COORDINATOR_HOME = os.path.abspath(os.path.join(gscoordinator.__file__, "..", ".."))
except ModuleNotFoundError:
    COORDINATOR_HOME = os.path.abspath(os.path.join(__file__, "..", ".."))

# template directory for codegen
TEMPLATE_DIR = os.path.join(COORDINATOR_HOME, "gscoordinator", "template")

# builtin app resource
BUILTIN_APP_RESOURCE_PATH = os.path.join(
    COORDINATOR_HOME, "gscoordinator", "builtin/app/builtin_app.gar"
)
# default config file in gar resource
DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"
DEFAULT_GRAPHSCOPE_HOME = "/opt/graphscope"

# GRAPHSCOPE_HOME
#   1) get from environment variable `GRAPHSCOPE_HOME`, if not exist,
#   2) infer from COORDINATOR_HOME
GRAPHSCOPE_HOME = os.environ.get("GRAPHSCOPE_HOME", None)

# resolve from pip installed package
if GRAPHSCOPE_HOME is None:
    if os.path.isdir(os.path.join(COORDINATOR_HOME, "graphscope.runtime")):
        GRAPHSCOPE_HOME = os.path.join(COORDINATOR_HOME, "graphscope.runtime")

# find from DEFAULT_GRAPHSCOPE_HOME
if GRAPHSCOPE_HOME is None:
    if os.path.isdir(DEFAULT_GRAPHSCOPE_HOME):
        GRAPHSCOPE_HOME = DEFAULT_GRAPHSCOPE_HOME

# resolve from develop source tree
if GRAPHSCOPE_HOME is None:
    GRAPHSCOPE_HOME = os.path.join(COORDINATOR_HOME, "..")

# ANALYTICAL_ENGINE_HOME
#   1) infer from GRAPHSCOPE_HOME
ANALYTICAL_ENGINE_HOME = os.path.join(GRAPHSCOPE_HOME)
ANALYTICAL_ENGINE_PATH = os.path.join(ANALYTICAL_ENGINE_HOME, "bin", "grape_engine")
if not os.path.isfile(ANALYTICAL_ENGINE_PATH):
    # try get analytical engine from build dir
    ANALYTICAL_ENGINE_HOME = os.path.join(GRAPHSCOPE_HOME, "analytical_engine")
    ANALYTICAL_ENGINE_PATH = os.path.join(
        ANALYTICAL_ENGINE_HOME, "build", "grape_engine"
    )

# INTERACTIVE_ENGINE_SCRIPT
INTERAVTIVE_INSTANCE_TIMEOUT_SECONDS = 600  # 10 mins
INTERACTIVE_ENGINE_SCRIPT = os.path.join(GRAPHSCOPE_HOME, "bin", "giectl")
if not os.path.isfile(INTERACTIVE_ENGINE_SCRIPT):
    INTERACTIVE_ENGINE_SCRIPT = os.path.join(
        GRAPHSCOPE_HOME, "interactive_engine", "bin", "giectl"
    )

# JAVA SDK related CONSTANTS
LLVM4JNI_HOME = os.environ.get("LLVM4JNI_HOME", None)
LLVM4JNI_USER_OUT_DIR_BASE = "user-llvm4jni-output"
PROCESSOR_MAIN_CLASS = "com.alibaba.graphscope.annotation.Main"
JAVA_CODEGNE_OUTPUT_PREFIX = "gs-ffi"
GRAPE_PROCESSOR_JAR = os.path.join(
    GRAPHSCOPE_HOME, "lib", "grape-runtime-0.1-shaded.jar"
)


def get_timestamp():
    now = datetime.datetime.now()
    return datetime.datetime.timestamp(now)


def get_lib_path(app_dir, app_name):
    lib_path = ""
    if sys.platform == "linux" or sys.platform == "linux2":
        lib_path = os.path.join(app_dir, "lib%s.so" % app_name)
    elif sys.platform == "darwin":
        lib_path = os.path.join(app_dir, "lib%s.dylib" % app_name)
    else:
        raise RuntimeError(f"Unsupported platform {sys.platform}")
    return lib_path


def get_app_sha256(attr):
    (
        app_type,
        app_header,
        app_class,
        vd_type,
        md_type,
        pregel_combine,
        java_jar_path,
        java_app_class,
    ) = _codegen_app_info(attr, DEFAULT_GS_CONFIG_FILE)
    graph_header, graph_type = _codegen_graph_info(attr)
    logger.info("Codegened graph type: %s, Graph header: %s", graph_type, graph_header)
    if app_type == "cpp_pie":
        return hashlib.sha256(
            f"{app_type}.{app_class}.{graph_type}".encode("utf-8")
        ).hexdigest()
    elif app_type == "java_pie":
        s = hashlib.sha256()
        # CAUTION!!!!!
        # We believe jar_path.java_app_class can uniquely define one java app
        s.update(f"{app_type}.{java_jar_path}.{java_app_class}".encode("utf-8"))
        if types_pb2.GAR in attr:
            s.update(attr[types_pb2.GAR].s)
        return s.hexdigest()
    else:
        s = hashlib.sha256()
        s.update(f"{app_type}.{app_class}.{graph_type}".encode("utf-8"))
        if types_pb2.GAR in attr:
            s.update(attr[types_pb2.GAR].s)
        return s.hexdigest()


def get_graph_sha256(attr):
    _, graph_class = _codegen_graph_info(attr)
    return hashlib.sha256(graph_class.encode("utf-8")).hexdigest()


def compile_app(workspace: str, library_name, attr, engine_config: dict):
    """Compile an application.

    Args:
        workspace (str): working dir.
        library_name (str): name of library
        attr (`AttrValue`): All information needed to compile an app.
        engine_config (dict): for options of NETWORKX

    Returns:
        str: Path of the built library.
        str: Java jar path. For c++/python app, return None.
        str: Directory containing generated java and jni code. For c++/python app, return None.
        str: App type.
    """
    app_dir = os.path.join(workspace, library_name)
    os.makedirs(app_dir, exist_ok=True)

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
        java_jar_path,
        java_app_class,
    ) = _codegen_app_info(attr, DEFAULT_GS_CONFIG_FILE)
    logger.info(
        "Codegened application type: %s, app header: %s, app_class: %s, vd_type: %s, md_type: %s, pregel_combine: %s, \
            java_jar_path: %s, java_app_class: %s",
        app_type,
        app_header,
        app_class,
        str(vd_type),
        str(md_type),
        str(pregel_combine),
        str(java_jar_path),
        str(java_app_class),
    )

    graph_header, graph_type = _codegen_graph_info(attr)
    logger.info("Codegened graph type: %s, Graph header: %s", graph_type, graph_header)

    os.chdir(app_dir)

    module_name = ""
    # Output directory for java codegen
    java_codegen_out_dir = ""
    cmake_commands = [
        "cmake",
        ".",
        f"-DNETWORKX={engine_config['networkx']}",
        f"-DCMAKE_PREFIX_PATH={GRAPHSCOPE_HOME}",
    ]
    if app_type == "java_pie":
        if not os.path.isfile(GRAPE_PROCESSOR_JAR):
            raise RuntimeError("Grape runtime jar not found")
        # for java need to run preprocess
        java_codegen_out_dir = os.path.join(
            workspace, "{}-{}".format(JAVA_CODEGNE_OUTPUT_PREFIX, library_name)
        )
        cmake_commands += [
            "-DENABLE_JAVA_SDK=ON",
            "-DJAVA_PIE_APP=ON",
            "-DPRE_CP={}:{}".format(GRAPE_PROCESSOR_JAR, java_jar_path),
            "-DPROCESSOR_MAIN_CLASS={}".format(PROCESSOR_MAIN_CLASS),
            "-DJAR_PATH={}".format(java_jar_path),
            "-DOUTPUT_DIR={}".format(java_codegen_out_dir),
        ]
        # if run llvm4jni.sh not found, we just go ahead,since it is optional.
        if LLVM4JNI_HOME and os.path.isfile(os.path.join(LLVM4JNI_HOME, "run.sh")):
            llvm4jni_user_out_dir = os.path.join(
                workspace, "{}-{}".format(LLVM4JNI_USER_OUT_DIR_BASE, library_name)
            )
            cmake_commands += [
                "-DRUN_LLVM4JNI_SH={}".format(os.path.join(LLVM4JNI_HOME, "run.sh")),
                "-DLLVM4JNI_OUTPUT={}".format(llvm4jni_user_out_dir),
                "-DLIB_PATH={}".format(get_lib_path(app_dir, library_name)),
            ]
        else:
            logger.info(
                "Skip running llvm4jni since env var LLVM4JNI_HOME not found or run.sh not found under LLVM4JNI_HOME"
            )
        logger.info(" ".join(cmake_commands))
    elif app_type != "cpp_pie":
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
            os.path.join(TEMPLATE_DIR, f"{pxd_name}.pxd.template"),
            os.path.join(app_dir, f"{pxd_name}.pxd"),
        )
        # Assume the gar will have and only have one .pyx file
        for pyx_file in glob.glob(app_dir + "/*.pyx"):
            module_name = os.path.splitext(os.path.basename(pyx_file))[0]
            cc_file = os.path.join(app_dir, module_name + ".cc")
            subprocess.check_call(["cython", "-3", "--cplus", "-o", cc_file, pyx_file])
        app_header = f"{module_name}.h"

    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(app_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=library_name,
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
    logger.info("Building app ...")
    cmake_process = subprocess.Popen(
        cmake_commands,
        env=os.environ.copy(),
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )
    cmake_stderr_watcher = PipeWatcher(cmake_process.stderr, sys.stdout)
    setattr(cmake_process, "stderr_watcher", cmake_stderr_watcher)
    cmake_process.wait()

    make_process = subprocess.Popen(
        [shutil.which("make"), "-j4"],
        env=os.environ.copy(),
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )
    make_stderr_watcher = PipeWatcher(make_process.stderr, sys.stdout)
    setattr(make_process, "stderr_watcher", make_stderr_watcher)
    make_process.wait()
    lib_path = get_lib_path(app_dir, library_name)
    if not os.path.isfile(lib_path):
        raise CompilationError(f"Failed to compile app {app_class}")
    return lib_path, java_jar_path, java_codegen_out_dir, app_type


def compile_graph_frame(workspace: str, library_name, attr: dict, engine_config: dict):
    """Compile an application.

    Args:
        workspace (str): Working dir.
        library_name (str): name of library
        attr (`AttrValue`): All information needed to compile a graph library.
        engine_config (dict): for options of NETWORKX

    Raises:
        ValueError: When graph_type is not supported.

    Returns:
        str: Path of the built graph library.
        None: For consistency with compiler_app.
        None: For consistency with compile_app.
        None: for consistency with compile_app.
    """

    _, graph_class = _codegen_graph_info(attr)

    logger.info("Codegened graph frame type: %s", graph_class)

    library_dir = os.path.join(workspace, library_name)
    os.makedirs(library_dir, exist_ok=True)

    os.chdir(library_dir)

    graph_type = attr[types_pb2.GRAPH_TYPE].graph_type

    cmake_commands = [
        "cmake",
        ".",
        f"-DNETWORKX={engine_config['networkx']}",
        f"-DCMAKE_PREFIX_PATH={GRAPHSCOPE_HOME}",
    ]
    if graph_type == graph_def_pb2.ARROW_PROPERTY:
        cmake_commands += ["-DPROPERTY_GRAPH_FRAME=True"]
    elif graph_type in (
        graph_def_pb2.ARROW_PROJECTED,
        graph_def_pb2.DYNAMIC_PROJECTED,
        graph_def_pb2.ARROW_FLATTENED,
    ):
        cmake_commands += ["-DPROJECT_FRAME=True"]
    else:
        raise ValueError(f"Illegal graph type: {graph_type}")
    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(library_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=library_name,
            _graph_type=graph_class,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)

    # compile
    logger.info("Building graph library ...")
    cmake_process = subprocess.Popen(
        cmake_commands,
        env=os.environ.copy(),
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )
    cmake_stderr_watcher = PipeWatcher(cmake_process.stderr, sys.stdout)
    setattr(cmake_process, "stderr_watcher", cmake_stderr_watcher)
    cmake_process.wait()

    make_process = subprocess.Popen(
        [shutil.which("make"), "-j4"],
        env=os.environ.copy(),
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )
    make_stderr_watcher = PipeWatcher(make_process.stderr, sys.stdout)
    setattr(make_process, "stderr_watcher", make_stderr_watcher)
    make_process.wait()
    lib_path = get_lib_path(library_dir, library_name)
    if not os.path.isfile(lib_path):
        raise CompilationError(f"Failed to compile graph {graph_class}")
    return lib_path, None, None, None


def op_pre_process(op, op_result_pool, key_to_op, **kwargs):  # noqa: C901
    if op.op == types_pb2.REPORT_GRAPH:
        return
    if op.op == types_pb2.CREATE_GRAPH:
        _pre_process_for_create_graph_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.ADD_LABELS:
        _pre_process_for_add_labels_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.RUN_APP:
        _pre_process_for_run_app_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.BIND_APP:
        _pre_process_for_bind_app_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.PROJECT_GRAPH:
        _pre_process_for_project_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.PROJECT_TO_SIMPLE:
        _pre_process_for_project_to_simple_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.ADD_COLUMN:
        _pre_process_for_add_column_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.UNLOAD_GRAPH:
        _pre_process_for_unload_graph_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op in (
        types_pb2.CONTEXT_TO_NUMPY,
        types_pb2.CONTEXT_TO_DATAFRAME,
        types_pb2.TO_VINEYARD_TENSOR,
        types_pb2.TO_VINEYARD_DATAFRAME,
    ):
        _pre_process_for_context_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op in (types_pb2.GRAPH_TO_NUMPY, types_pb2.GRAPH_TO_DATAFRAME):
        _pre_process_for_output_graph_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.UNLOAD_APP:
        _pre_process_for_unload_app_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.UNLOAD_CONTEXT:
        _pre_process_for_unload_context_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.CREATE_INTERACTIVE_QUERY:
        _pre_process_for_create_interactive_query_op(
            op, op_result_pool, key_to_op, **kwargs
        )
    if op.op == types_pb2.GREMLIN_QUERY:
        _pre_process_for_gremlin_query_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.FETCH_GREMLIN_RESULT:
        _pre_process_for_fetch_gremlin_result(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.CLOSE_INTERACTIVE_QUERY:
        _pre_process_for_close_interactive_query_op(
            op, op_result_pool, key_to_op, **kwargs
        )
    if op.op == types_pb2.SUBGRAPH:
        _pre_process_for_gremlin_to_subgraph_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.CREATE_LEARNING_INSTANCE:
        _pre_process_for_create_learning_graph_op(
            op, op_result_pool, key_to_op, **kwargs
        )
    if op.op == types_pb2.CLOSE_LEARNING_INSTANCE:
        _pre_process_for_close_learning_instance_op(
            op, op_result_pool, key_to_op, **kwargs
        )
    if op.op == types_pb2.OUTPUT:
        _pre_process_for_output_op(op, op_result_pool, key_to_op, **kwargs)


def _pre_process_for_create_graph_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) <= 1
    if len(op.parents) == 1:
        key_of_parent_op = op.parents[0]
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.DATA_SOURCE:
            for key, value in parent_op.attr.items():
                op.attr[key].CopyFrom(value)


def _pre_process_for_add_labels_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 2
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.DATA_SOURCE:
            for key, value in parent_op.attr.items():
                op.attr[key].CopyFrom(value)
        else:
            result = op_result_pool[key_of_parent_op]
            op.attr[types_pb2.GRAPH_NAME].CopyFrom(
                utils.s_to_attr(result.graph_def.key)
            )


def _pre_process_for_close_interactive_query_op(
    op, op_result_pool, key_to_op, **kwargs
):
    assert len(op.parents) == 1
    assert op.parents[0] in op_result_pool


def _pre_process_for_gremlin_to_subgraph_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    assert op.parents[0] in op_result_pool


def _pre_process_for_gremlin_query_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    assert op.parents[0] in op_result_pool


def _pre_process_for_fetch_gremlin_result(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    assert op.parents[0] in op_result_pool


def _pre_process_for_create_interactive_query_op(
    op, op_result_pool, key_to_op, **kwargs
):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    assert result.graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR)
    vy_info = graph_def_pb2.VineyardInfoPb()
    result.graph_def.extension.Unpack(vy_info)
    op.attr[types_pb2.VINEYARD_ID].CopyFrom(utils.i_to_attr(vy_info.vineyard_id))
    op.attr[types_pb2.SCHEMA_PATH].CopyFrom(utils.s_to_attr(vy_info.schema_path))


def _pre_process_for_close_learning_instance_op(
    op, op_result_pool, key_to_op, **kwargs
):
    assert len(op.parents) == 1
    assert op.parents[0] in op_result_pool


def _pre_process_for_create_learning_graph_op(op, op_result_pool, key_to_op, **kwargs):
    from graphscope.learning.graph import Graph as LearningGraph

    nodes = pickle.loads(op.attr[types_pb2.NODES].s)
    edges = pickle.loads(op.attr[types_pb2.EDGES].s)
    gen_labels = pickle.loads(op.attr[types_pb2.GLE_GEN_LABELS].s)
    # get graph schema
    op, op_result_pool, key_to_op
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    assert result.graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR)
    schema = GraphSchema()
    schema.from_graph_def(result.graph_def)
    # get graph vineyard id
    vy_info = graph_def_pb2.VineyardInfoPb()
    result.graph_def.extension.Unpack(vy_info)
    vineyard_id = vy_info.vineyard_id
    # gle handle
    engine_hosts = kwargs.pop("engine_hosts")
    engine_config = kwargs.pop("engine_config")
    handle = get_gl_handle(schema, vineyard_id, engine_hosts, engine_config)
    config = LearningGraph.preprocess_args(handle, nodes, edges, gen_labels)
    config = base64.b64encode(json.dumps(config).encode("utf-8")).decode("utf-8")
    op.attr[types_pb2.VINEYARD_ID].CopyFrom(utils.i_to_attr(vineyard_id))
    op.attr[types_pb2.GLE_HANDLE].CopyFrom(utils.s_to_attr(handle))
    op.attr[types_pb2.GLE_CONFIG].CopyFrom(utils.s_to_attr(config))


# get `bind_app` runtime informarion in lazy mode
def _pre_process_for_bind_app_op(op, op_result_pool, key_to_op, **kwargs):
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.CREATE_APP:
            # app assets
            op.attr[types_pb2.APP_ALGO].CopyFrom(parent_op.attr[types_pb2.APP_ALGO])
            if types_pb2.GAR in parent_op.attr:
                op.attr[types_pb2.GAR].CopyFrom(parent_op.attr[types_pb2.GAR])
        else:
            # get graph runtime information from results
            result = op_result_pool[key_of_parent_op]
            assert result.graph_def.extension.Is(
                graph_def_pb2.VineyardInfoPb.DESCRIPTOR
            )
            vy_info = graph_def_pb2.VineyardInfoPb()
            result.graph_def.extension.Unpack(vy_info)
            op.attr[types_pb2.GRAPH_NAME].CopyFrom(
                attr_value_pb2.AttrValue(s=result.graph_def.key.encode("utf-8"))
            )
            op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
                attr_value_pb2.AttrValue(graph_type=result.graph_def.graph_type)
            )
            op.attr[types_pb2.OID_TYPE].CopyFrom(
                utils.s_to_attr(
                    utils.normalize_data_type_str(
                        utils.data_type_to_cpp(vy_info.oid_type)
                    )
                )
            )
            op.attr[types_pb2.VID_TYPE].CopyFrom(
                utils.s_to_attr(utils.data_type_to_cpp(vy_info.vid_type))
            )
            op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
                utils.s_to_attr(utils.data_type_to_cpp(vy_info.vdata_type))
            )
            op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
                utils.s_to_attr(utils.data_type_to_cpp(vy_info.edata_type))
            )


# get `run_app` runtime informarion in lazy mode
def _pre_process_for_run_app_op(op, op_result_pool, key_to_op, **kwargs):
    # run_app op has only one parent
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    parent_op = key_to_op[key_of_parent_op]
    assert parent_op.op == types_pb2.BIND_APP
    # set graph key
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(parent_op.attr[types_pb2.GRAPH_NAME])
    result = op_result_pool[key_of_parent_op]
    # set app key
    op.attr[types_pb2.APP_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=result.result.decode("utf-8").encode("utf-8"))
    )

    app_type = parent_op.attr[types_pb2.APP_ALGO].s.decode("utf-8")
    if app_type == "java_app":
        # For java app, we need lib path as an explicit arg.
        param = Any()
        lib_path = parent_op.attr[types_pb2.APP_LIBRARY_PATH].s.decode("utf-8")
        param.Pack(data_types_pb2.StringValue(value=lib_path))
        op.query_args.args.extend([param])
        logger.info("Lib path {}".format(lib_path))


def _pre_process_for_unload_graph_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    assert result.graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR)
    vy_info = graph_def_pb2.VineyardInfoPb()
    result.graph_def.extension.Unpack(vy_info)
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(result.graph_def.key))
    op.attr[types_pb2.VINEYARD_ID].CopyFrom(utils.i_to_attr(vy_info.vineyard_id))


def _pre_process_for_unload_app_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    op.attr[types_pb2.APP_NAME].CopyFrom(utils.s_to_attr(result.result.decode("utf-8")))


def _pre_process_for_unload_context_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    parent_op_result = json.loads(result.result.decode("utf-8"))
    context_key = parent_op_result["context_key"]
    op.attr[types_pb2.CONTEXT_KEY].CopyFrom(
        attr_value_pb2.AttrValue(s=context_key.encode("utf-8"))
    )


def _pre_process_for_add_column_op(op, op_result_pool, key_to_op, **kwargs):
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op != types_pb2.RUN_APP:
            # get graph information
            r = op_result_pool[key_of_parent_op]
            graph_name = r.graph_def.key
            graph_type = r.graph_def.graph_type
            schema = GraphSchema()
            schema.from_graph_def(r.graph_def)
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.RUN_APP:
            selector = op.attr[types_pb2.SELECTOR].s.decode("utf-8")
            r = op_result_pool[key_of_parent_op]
            parent_op_result = json.loads(r.result.decode("utf-8"))
            context_key = parent_op_result["context_key"]
            context_type = parent_op_result["context_type"]
            selector = _tranform_dataframe_selector(context_type, schema, selector)
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(graph_name))
    op.attr[types_pb2.GRAPH_TYPE].CopyFrom(utils.graph_type_to_attr(graph_type))
    op.attr[types_pb2.CONTEXT_KEY].CopyFrom(utils.s_to_attr(context_key))
    op.attr[types_pb2.SELECTOR].CopyFrom(utils.s_to_attr(selector))


def _pre_process_for_context_op(op, op_result_pool, key_to_op, **kwargs):
    def __backtrack_key_of_graph_op(key):
        bfs_queue = Queue()
        bfs_queue.put(key)
        while not bfs_queue.empty():
            next_op_key = bfs_queue.get()
            if next_op_key in key_to_op:
                next_op = key_to_op[next_op_key]
                if next_op.op in (
                    types_pb2.CREATE_GRAPH,
                    types_pb2.ADD_COLUMN,
                    types_pb2.ADD_LABELS,
                    types_pb2.TRANSFORM_GRAPH,
                    types_pb2.PROJECT_GRAPH,
                    types_pb2.PROJECT_TO_SIMPLE,
                ):
                    return next_op
                for parent_key in next_op.parents:
                    bfs_queue.put(parent_key)
        return None

    assert len(op.parents) == 1
    schema = None
    key_of_parent_op = op.parents[0]
    graph_op = __backtrack_key_of_graph_op(key_of_parent_op)
    r = op_result_pool[key_of_parent_op]
    # set context key
    parent_op_result = json.loads(r.result.decode("utf-8"))
    context_key = parent_op_result["context_key"]
    context_type = parent_op_result["context_type"]
    op.attr[types_pb2.CONTEXT_KEY].CopyFrom(
        attr_value_pb2.AttrValue(s=context_key.encode("utf-8"))
    )
    r = op_result_pool[graph_op.key]
    # transform selector
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    selector = op.attr[types_pb2.SELECTOR].s.decode("utf-8")
    if op.op in (types_pb2.CONTEXT_TO_DATAFRAME, types_pb2.TO_VINEYARD_DATAFRAME):
        selector = _tranform_dataframe_selector(context_type, schema, selector)
    else:
        # to numpy
        selector = _tranform_numpy_selector(context_type, schema, selector)
    if selector is not None:
        op.attr[types_pb2.SELECTOR].CopyFrom(
            attr_value_pb2.AttrValue(s=selector.encode("utf-8"))
        )


def _pre_process_for_output_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    parent_op = key_to_op[key_of_parent_op]
    result = op_result_pool[key_of_parent_op]
    if parent_op.output_type in (
        types_pb2.VINEYARD_TENSOR,
        types_pb2.VINEYARD_DATAFRAME,
    ):
        # dependent to to_vineyard_dataframe
        r = json.loads(result.result.decode("utf-8"))["object_id"]
        op.attr[types_pb2.VINEYARD_ID].CopyFrom(utils.s_to_attr(r))


def _pre_process_for_output_graph_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    r = op_result_pool[key_of_parent_op]
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    graph_name = r.graph_def.key
    selector = op.attr[types_pb2.SELECTOR].s.decode("utf-8")
    if op.op == types_pb2.GRAPH_TO_DATAFRAME:
        selector = _tranform_dataframe_selector(
            "labeled_vertex_property", schema, selector
        )
    else:
        # to numpy
        selector = _tranform_numpy_selector("labeled_vertex_property", schema, selector)
    if selector is not None:
        op.attr[types_pb2.SELECTOR].CopyFrom(
            attr_value_pb2.AttrValue(s=selector.encode("utf-8"))
        )
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )


def _pre_process_for_project_to_simple_op(op, op_result_pool, key_to_op, **kwargs):
    # for nx graph
    if op.attr[types_pb2.GRAPH_TYPE].graph_type in (
        graph_def_pb2.DYNAMIC_PROJECTED,
        graph_def_pb2.ARROW_FLATTENED,
    ):
        return
    assert len(op.parents) == 1
    # get parent graph schema
    key_of_parent_op = op.parents[0]
    r = op_result_pool[key_of_parent_op]
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    graph_name = r.graph_def.key
    check_argument(
        schema.vertex_label_num == 1,
        "Cannot project to simple, vertex label number is not one.",
    )
    check_argument(
        schema.edge_label_num == 1,
        "Cannot project to simple, edge label number is not one.",
    )
    v_label = schema.vertex_labels[0]
    e_label = schema.edge_labels[0]
    relation = (v_label, v_label)
    check_argument(
        relation in schema.get_relationships(e_label),
        f"Cannot project to simple, Graph doesn't contain such relationship: {v_label} -> {e_label} <- {v_label}.",
    )
    v_props = schema.get_vertex_properties(v_label)
    e_props = schema.get_edge_properties(e_label)
    check_argument(len(v_props) <= 1)
    check_argument(len(e_props) <= 1)
    v_label_id = schema.get_vertex_label_id(v_label)
    e_label_id = schema.get_edge_label_id(e_label)
    v_prop_id, vdata_type = (v_props[0].id, v_props[0].type) if v_props else (-1, None)
    e_prop_id, edata_type = (e_props[0].id, e_props[0].type) if e_props else (-1, None)
    oid_type = schema.oid_type
    vid_type = schema.vid_type
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )
    op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
        utils.graph_type_to_attr(graph_def_pb2.ARROW_PROJECTED)
    )
    op.attr[types_pb2.V_LABEL_ID].CopyFrom(utils.i_to_attr(v_label_id))
    op.attr[types_pb2.V_PROP_ID].CopyFrom(utils.i_to_attr(v_prop_id))
    op.attr[types_pb2.E_LABEL_ID].CopyFrom(utils.i_to_attr(e_label_id))
    op.attr[types_pb2.E_PROP_ID].CopyFrom(utils.i_to_attr(e_prop_id))
    op.attr[types_pb2.OID_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(oid_type))
    )
    op.attr[types_pb2.VID_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(vid_type))
    )
    op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(vdata_type))
    )
    op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(edata_type))
    )


def _pre_process_for_project_op(op, op_result_pool, key_to_op, **kwargs):
    def _get_all_v_props_id(schema, label):
        props = schema.get_vertex_properties(label)
        return [schema.get_vertex_property_id(label, prop.name) for prop in props]

    def _get_all_e_props_id(schema, label):
        props = schema.get_edge_properties(label)
        return [schema.get_edge_property_id(label, prop.name) for prop in props]

    assert len(op.parents) == 1
    # get parent graph schema
    key_of_parent_op = op.parents[0]
    r = op_result_pool[key_of_parent_op]
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    graph_name = r.graph_def.key
    vertices = json.loads(op.attr[types_pb2.VERTEX_COLLECTIONS].s.decode("utf-8"))
    edges = json.loads(op.attr[types_pb2.EDGE_COLLECTIONS].s.decode("utf-8"))
    vertex_collections = {}
    edge_collections = {}
    for label, props in vertices.items():
        label_id = schema.get_vertex_label_id(label)
        if props is None:
            vertex_collections[label_id] = _get_all_v_props_id(schema, label)
        else:
            vertex_collections[label_id] = sorted(
                [schema.get_vertex_property_id(label, prop) for prop in props]
            )
    for label, props in edges.items():
        relations = schema.get_relationships(label)
        valid = False
        for src, dst in relations:
            if src in vertices and dst in vertices:
                valid = True
                break
        if not valid:
            raise ValueError("Cannot find a valid relation in given vertices and edges")
        label_id = schema.get_edge_label_id(label)
        if props is None:
            edge_collections[label_id] = _get_all_e_props_id(schema, label)
        else:
            edge_collections[label_id] = sorted(
                [schema.get_edge_property_id(label, prop) for prop in props]
            )
    vertex_collections = dict(sorted(vertex_collections.items()))
    edge_collections = dict(sorted(edge_collections.items()))

    # construct op attr
    attr = attr_value_pb2.AttrValue()
    v_attr = attr_value_pb2.NameAttrList()
    e_attr = attr_value_pb2.NameAttrList()
    for label, props in vertex_collections.items():
        v_attr.attr[label].CopyFrom(utils.list_i_to_attr(props))
    for label, props in edge_collections.items():
        e_attr.attr[label].CopyFrom(utils.list_i_to_attr(props))
    attr.list.func.extend([v_attr, e_attr])
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )
    op.attr[types_pb2.ARROW_PROPERTY_DEFINITION].CopyFrom(attr)
    del op.attr[types_pb2.VERTEX_COLLECTIONS]
    del op.attr[types_pb2.EDGE_COLLECTIONS]


def _tranform_numpy_selector(context_type, schema, selector):
    if context_type == "tensor":
        selector = None
    if context_type == "vertex_data":
        selector = transform_vertex_data_selector(selector)
    if context_type == "labeled_vertex_data":
        selector = transform_labeled_vertex_data_selector(schema, selector)
    if context_type == "vertex_property":
        selector = transform_vertex_property_data_selector(selector)
    if context_type == "labeled_vertex_property":
        selector = transform_labeled_vertex_property_data_selector(schema, selector)
    return selector


def _tranform_dataframe_selector(context_type, schema, selector):
    selector = json.loads(selector)
    if context_type == "tensor":
        selector = {key: None for key, value in selector.items()}
    if context_type == "vertex_data":
        selector = {
            key: transform_vertex_data_selector(value)
            for key, value in selector.items()
        }
    if context_type == "labeled_vertex_data":
        selector = {
            key: transform_labeled_vertex_data_selector(schema, value)
            for key, value in selector.items()
        }
    if context_type == "vertex_property":
        selector = {
            key: transform_vertex_property_data_selector(value)
            for key, value in selector.items()
        }
    if context_type == "labeled_vertex_property":
        selector = {
            key: transform_labeled_vertex_property_data_selector(schema, value)
            for key, value in selector.items()
        }
    return json.dumps(selector)


def _transform_vertex_data_v(selector):
    if selector not in ("v.id", "v.data"):
        raise SyntaxError("selector of v must be 'id' or 'data'")
    return selector


def _transform_vertex_data_e(selector):
    if selector not in ("e.src", "e.dst", "e.data"):
        raise SyntaxError("selector of e must be 'src', 'dst' or 'data'")
    return selector


def _transform_vertex_data_r(selector):
    if selector != "r":
        raise SyntaxError("selector of r must be 'r'")
    return selector


def _transform_vertex_property_data_r(selector):
    # The second part of selector or r is user defined name.
    # So we will allow any str
    return selector


def _transform_labeled_vertex_data_v(schema, label, prop):
    label_id = schema.get_vertex_label_id(label)
    if prop == "id":
        return f"label{label_id}.{prop}"
    else:
        prop_id = schema.get_vertex_property_id(label, prop)
        return f"label{label_id}.property{prop_id}"


def _transform_labeled_vertex_data_e(schema, label, prop):
    label_id = schema.get_edge_label_id(label)
    if prop in ("src", "dst"):
        return f"label{label_id}.{prop}"
    else:
        prop_id = schema.get_vertex_property_id(label, prop)
        return f"label{label_id}.property{prop_id}"


def _transform_labeled_vertex_data_r(schema, label):
    label_id = schema.get_vertex_label_id(label)
    return f"label{label_id}"


def _transform_labeled_vertex_property_data_r(schema, label, prop):
    label_id = schema.get_vertex_label_id(label)
    return f"label{label_id}.{prop}"


def transform_vertex_data_selector(selector):
    """Optional values:
    vertex selector: 'v.id', 'v.data'
    edge selector: 'e.src', 'e.dst', 'e.data'
    result selector: 'r'
    """
    if selector is None:
        raise RuntimeError("selector cannot be None")
    segments = selector.split(".")
    if len(segments) > 2:
        raise SyntaxError("Invalid selector: %s." % selector)
    if segments[0] == "v":
        selector = _transform_vertex_data_v(selector)
    elif segments[0] == "e":
        selector = _transform_vertex_data_e(selector)
    elif segments[0] == "r":
        selector = _transform_vertex_data_r(selector)
    else:
        raise SyntaxError(f"Invalid selector: {selector}, choose from v / e / r.")
    return selector


def transform_vertex_property_data_selector(selector):
    """Optional values:
    vertex selector: 'v.id', 'v.data'
    edge selector: 'e.src', 'e.dst', 'e.data'
    result selector format: 'r.y', y  denotes property name.
    """
    if selector is None:
        raise RuntimeError("selector cannot be None")
    segments = selector.split(".")
    if len(segments) != 2:
        raise SyntaxError(f"Invalid selector: {selector}")
    if segments[0] == "v":
        selector = _transform_vertex_data_v(selector)
    elif segments[0] == "e":
        selector = _transform_vertex_data_e(selector)
    elif segments[0] == "r":
        selector = _transform_vertex_property_data_r(selector)
    else:
        raise SyntaxError(f"Invalid selector: {selector}, choose from v / e / r.")
    return selector


def transform_labeled_vertex_data_selector(schema, selector):
    """Formats: 'v:x.y/id', 'e:x.y/src/dst', 'r:label',
                x denotes label name, y denotes property name.
    Returned selector will change label name to 'label{id}', where id is x's id in labels.
    And change property name to 'property{id}', where id is y's id in properties.
    """
    if selector is None:
        raise RuntimeError("selector cannot be None")

    ret_type, segments = selector.split(":")
    if ret_type not in ("v", "e", "r"):
        raise SyntaxError(f"Invalid selector: {selector}")
    segments = segments.split(".")
    ret = ""
    if ret_type == "v":
        ret = _transform_labeled_vertex_data_v(schema, *segments)
    elif ret_type == "e":
        ret = _transform_labeled_vertex_data_e(schema, *segments)
    elif ret_type == "r":
        ret = _transform_labeled_vertex_data_r(schema, *segments)
    return "{}:{}".format(ret_type, ret)


def transform_labeled_vertex_property_data_selector(schema, selector):
    """Formats: 'v:x.y/id', 'e:x.y/src/dst', 'r:x.y',
                x denotes label name, y denotes property name.
    Returned selector will change label name to 'label{id}', where id is x's id in labels.
    And change property name to 'property{id}', where id is y's id in properties.
    """
    if selector is None:
        raise RuntimeError("selector cannot be None")
    ret_type, segments = selector.split(":")
    if ret_type not in ("v", "e", "r"):
        raise SyntaxError(f"Invalid selector: {selector}")
    segments = segments.split(".")
    ret = ""
    if ret_type == "v":
        ret = _transform_labeled_vertex_data_v(schema, *segments)
    elif ret_type == "e":
        ret = _transform_labeled_vertex_data_e(schema, *segments)
    elif ret_type == "r":
        ret = _transform_labeled_vertex_property_data_r(schema, *segments)
    return f"{ret_type}:{ret}"


def _extract_gar(app_dir: str, attr):
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
        zip_ref.extractall(app_dir)


def _codegen_app_info(attr, meta_file: str):
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
    fp = BUILTIN_APP_RESOURCE_PATH  # default is builtin app resources.
    if types_pb2.GAR in attr:
        # if gar sent via bytecode in attr, overwrite.
        fp = BytesIO(attr[types_pb2.GAR].s)
    with zipfile.ZipFile(fp, "r") as zip_ref:
        with zip_ref.open(meta_file, "r") as f:
            config_yaml = yaml.safe_load(f)

    algo = attr[types_pb2.APP_ALGO].s.decode("utf-8")
    for app in config_yaml["app"]:
        if app["algo"] == algo:
            app_type = app["type"]  # cpp_pie or cython_pregel or cython_pie, java_pie
            if app_type == "cpp_pie":
                return (
                    app_type,
                    app["src"],
                    f"{app['class_name']}<_GRAPH_TYPE>",
                    None,
                    None,
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
                    None,
                    None,
                )
            if app_type == "java_pie":
                return (
                    app_type,
                    app["driver_header"],  # cxx header
                    "{}<_GRAPH_TYPE>".format(app["class_name"]),  # cxx class name
                    None,  # vd_type,
                    None,  # md_type
                    None,  # pregel combine
                    app["java_jar_path"],
                    app["java_app_class"],  # the running java app class
                )

    raise KeyError("Algorithm does not exist in the gar resource.")


# a mapping for classname to header file.
GRAPH_HEADER_MAP = {
    graph_def_pb2.IMMUTABLE_EDGECUT: (
        "grape::ImmutableEdgecutFragment",
        "grape/fragment/immutable_edgecut_fragment.h",
    ),
    graph_def_pb2.DYNAMIC_PROJECTED: (
        "gs::DynamicProjectedFragment",
        "core/fragment/dynamic_projected_fragment.h",
    ),
    graph_def_pb2.ARROW_PROPERTY: (
        "vineyard::ArrowFragment",
        "vineyard/graph/fragment/arrow_fragment.h",
    ),
    graph_def_pb2.ARROW_PROJECTED: (
        "gs::ArrowProjectedFragment",
        "core/fragment/arrow_projected_fragment.h",
    ),
    graph_def_pb2.DYNAMIC_PROPERTY: (
        "gs::DynamicFragment",
        "core/fragment/dynamic_fragment.h",
    ),
    graph_def_pb2.ARROW_FLATTENED: (
        "gs::ArrowFlattenedFragment",
        "core/fragment/arrow_flattened_fragment.h",
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
    elif graph_class == "gs::ArrowFlattenedFragment":
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
        raise ValueError(f"Argument cannot be interpreted as a number: {value}") from e
    if suffix not in ["Ki", "Mi", "Gi"]:
        raise ValueError(f"Memory suffix must be one of 'Ki', 'Mi' and 'Gi': {value}")
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

        >>> # run without mpi on localhost when setting `num_workers` to 1
        >>> rmcp = ResolveMPICmdPrefix()
        >>> (cmd, env) = rmcp.resolve(1, 'localhost')
        >>> cmd
        []
        >>> env
        {}
    """

    _OPENMPI_RSH_AGENT = "OMPI_MCA_plm_rsh_agent"
    _KUBE_SSH_EXEC = "kube_ssh"

    def __init__(self, rsh_agent=False):
        self._rsh_agent = rsh_agent

    @staticmethod
    def openmpi():
        ompi_info = ""
        if "OPAL_PREFIX" in os.environ:
            ompi_info = os.path.expandvars("$OPAL_PREFIX/bin/ompi_info")
        if not ompi_info:
            if "OPAL_BINDIR" in os.environ:
                ompi_info = os.path.expandvars("$OPAL_BINDIR/ompi_info")
        if not ompi_info:
            ompi_info = "ompi_info"
        try:
            subprocess.check_call([ompi_info], stdout=subprocess.DEVNULL)
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
            host_list[i] = f"{host_list[i]}:{host_to_proc_num[host_list[i]]}"

        return ",".join(host_list)

    @staticmethod
    def find_mpi():
        mpi = ""
        if ResolveMPICmdPrefix.openmpi():
            if "OPAL_PREFIX" in os.environ:
                mpi = os.path.expandvars("$OPAL_PREFIX/bin/mpirun")
            if not mpi:
                if "OPAL_BINDIR" in os.environ:
                    mpi = os.path.expandvars("$OPAL_BINDIR/mpirun")
        if not mpi:
            mpi = shutil.which("mpirun")
        if not mpi:
            raise RuntimeError("mpirun command not found.")
        return mpi

    def resolve(self, num_workers, hosts):
        cmd = []
        env = {}

        if num_workers == 1 and (hosts == "localhost" or hosts == "127.0.0.1"):
            # run without mpi on localhost if workers num is 1
            if shutil.which("ssh") is None:
                # also need a fake ssh agent
                env[self._OPENMPI_RSH_AGENT] = sys.executable
            return cmd, env

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
                    self.find_mpi(),
                    "--allow-run-as-root",
                ]
            )
        else:
            # ssh agent supported only
            cmd.extend([self.find_mpi()])
        cmd.extend(["-n", str(num_workers)])
        cmd.extend(["-host", self.alloc(num_workers, hosts)])

        logger.debug("Resolve mpi cmd prefix: %s", " ".join(cmd))
        logger.debug("Resolve mpi env: %s", json.dumps(env))
        return cmd, env


def get_gl_handle(schema, vineyard_id, engine_hosts, engine_config):
    """Dump a handler for GraphLearn for interaction.

    Fields in :code:`schema` are:

    + the name of node type or edge type
    + whether the graph is weighted graph
    + whether the graph is labeled graph
    + the number of int attributes
    + the number of float attributes
    + the number of string attributes

    An example of the graph handle:

    .. code:: python

        {
            "server": "127.0.0.1:8888,127.0.0.1:8889",
            "client_count": 1,
            "vineyard_socket": "/var/run/vineyard.sock",
            "vineyard_id": 13278328736,
            "node_schema": [
                "user:false:false:10:0:0",
                "item:true:false:0:0:5"
            ],
            "edge_schema": [
                "user:click:item:true:false:0:0:0",
                "user:buy:item:true:true:0:0:0",
                "item:similar:item:false:false:10:0:0"
            ],
            "node_attribute_types": {
                "person": {
                    "age": "i",
                    "name": "s",
                },
            },
            "edge_attribute_types": {
                "knows": {
                    "weight": "f",
                },
            },
        }

    The handle can be decoded using:

    .. code:: python

       base64.b64decode(handle.encode('ascii')).decode('ascii')

    Note that the ports are selected from a range :code:`(8000, 9000)`.

    Args:
        schema: The graph schema.
        vineyard_id: The object id of graph stored in vineyard.
        engine_hosts: A list of hosts for GraphScope engine workers.
        engine_config: dict of config for GAE engine.

    Returns:
        str: Base64 encoded handle

    """

    def group_property_types(props):
        weighted, labeled, i, f, s, attr_types = "false", "false", 0, 0, 0, {}
        for prop in props:
            if prop.type in [graph_def_pb2.STRING]:
                s += 1
                attr_types[prop.name] = "s"
            elif prop.type in (graph_def_pb2.FLOAT, graph_def_pb2.DOUBLE):
                f += 1
                attr_types[prop.name] = "f"
            else:
                i += 1
                attr_types[prop.name] = "i"
            if prop.name == "weight":
                weighted = "true"
            elif prop.name == "label":
                labeled = "true"
        return weighted, labeled, i, f, s, attr_types

    node_schema, node_attribute_types = [], dict()
    for label in schema.vertex_labels:
        weighted, labeled, i, f, s, attr_types = group_property_types(
            schema.get_vertex_properties(label)
        )
        node_schema.append(
            "{}:{}:{}:{}:{}:{}".format(label, weighted, labeled, i, f, s)
        )
        node_attribute_types[label] = attr_types

    edge_schema, edge_attribute_types = [], dict()
    for label in schema.edge_labels:
        weighted, labeled, i, f, s, attr_types = group_property_types(
            schema.get_edge_properties(label)
        )
        for rel in schema.get_relationships(label):
            edge_schema.append(
                "{}:{}:{}:{}:{}:{}:{}:{}".format(
                    rel[0], label, rel[1], weighted, labeled, i, f, s
                )
            )
        edge_attribute_types[label] = attr_types

    handle = {
        "hosts": engine_hosts,
        "client_count": 1,
        "vineyard_id": vineyard_id,
        "vineyard_socket": engine_config["vineyard_socket"],
        "node_schema": node_schema,
        "edge_schema": edge_schema,
        "node_attribute_types": node_attribute_types,
        "edge_attribute_types": edge_attribute_types,
    }
    handle_json_string = json.dumps(handle)
    return base64.b64encode(handle_json_string.encode("utf-8")).decode("utf-8")


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


def check_argument(condition, message=None):
    if not condition:
        if message is None:
            message = "in '%s'" % inspect.stack()[1].code_context[0]
        raise ValueError(f"Check failed: {message}")


def find_java():
    java_exec = ""
    if "JAVA_HOME" in os.environ:
        java_exec = os.path.expandvars("$JAVA_HOME/bin/java")
    if not java_exec:
        java_exec = shutil.which("java")
    if not java_exec:
        raise RuntimeError("java command not found.")
    return java_exec


def get_java_version():
    java_exec = find_java()
    pattern = r'"(\d+\.\d+\.\d+).*"'
    version = subprocess.check_output([java_exec, "-version"], stderr=subprocess.STDOUT)
    return re.search(pattern, version.decode("utf-8")).groups()[0]


def check_gremlin_server_ready(endpoint):
    def _check_task(endpoint):
        from gremlin_python.driver.client import Client

        if "MY_POD_NAME" in os.environ:
            # inner kubernetes env
            if endpoint == "localhost" or endpoint == "127.0.0.1":
                # now, used in mac os with docker-desktop kubernetes cluster,
                # which external ip is 'localhost' when service type is 'LoadBalancer'
                return True

        try:
            client = Client(f"ws://{endpoint}/gremlin", "g")
            client.submit("g.V().limit(1)").all().result()
            try:
                client.close()
            except:  # noqa: E722
                pass
        except Exception as e:
            try:
                client.close()
            except:  # noqa: E722
                pass
            raise RuntimeError(str(e))

        return True

    executor = ThreadPoolExecutor(max_workers=20)

    begin_time = time.time()
    error_message = ""
    while True:
        t = executor.submit(_check_task, endpoint)
        try:
            rlt = t.result(timeout=30)
        except Exception as e:
            t.cancel()
            error_message = str(e)
        else:
            return rlt
        time.sleep(3)
        if time.time() - begin_time > INTERAVTIVE_INSTANCE_TIMEOUT_SECONDS:
            executor.shutdown(wait=False, cancel_futures=True)
            raise TimeoutError(f"Gremlin check query failed: {error_message}")
