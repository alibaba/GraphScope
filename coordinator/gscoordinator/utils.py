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
import datetime
import glob
import hashlib
import inspect
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from queue import Queue
from string import Template

import yaml
from google.protobuf.any_pb2 import Any
from graphscope.framework import utils
from graphscope.framework.errors import CompilationError
from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.utils import PipeWatcher
from graphscope.framework.utils import find_java
from graphscope.framework.utils import get_platform_info
from graphscope.framework.utils import get_tempdir
from graphscope.proto import attr_value_pb2
from graphscope.proto import data_types_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")

RESOURCE_DIR_NAME = "resource"

# runtime workspace
try:
    WORKSPACE = os.environ["GRAPHSCOPE_RUNTIME"]
except KeyError:
    WORKSPACE = os.path.join(get_tempdir(), "gs")

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
DEFAULT_GRAPHSCOPE_HOME = "/usr/local"

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
# Here the GRAPHSCOPE_HOME has been set to the root of the source tree,
# So the engine location doesn't need to check again,
# just rely on GRAPHSCOPE_HOME.
if GRAPHSCOPE_HOME is None:
    GRAPHSCOPE_HOME = os.path.join(COORDINATOR_HOME, "..")

# ANALYTICAL_ENGINE_HOME
#   1) infer from GRAPHSCOPE_HOME
ANALYTICAL_ENGINE_HOME = GRAPHSCOPE_HOME
ANALYTICAL_ENGINE_PATH = os.path.join(ANALYTICAL_ENGINE_HOME, "bin", "grape_engine")

ANALYTICAL_BUILTIN_SPACE = os.path.join(GRAPHSCOPE_HOME, "precompiled", "builtin")

# ANALYTICAL_ENGINE_JAVA_HOME
ANALYTICAL_ENGINE_JAVA_HOME = ANALYTICAL_ENGINE_HOME

ANALYTICAL_ENGINE_JAVA_RUNTIME_JAR = os.path.join(
    ANALYTICAL_ENGINE_JAVA_HOME,
    "lib",
    f"grape-runtime-{__version__}-shaded.jar",
)
ANALYTICAL_ENGINE_JAVA_INIT_CLASS_PATH = ANALYTICAL_ENGINE_JAVA_RUNTIME_JAR

ANALYTICAL_ENGINE_JAVA_JVM_OPTS = f"-Djava.library.path={GRAPHSCOPE_HOME}/lib"
ANALYTICAL_ENGINE_JAVA_JVM_OPTS += (
    f" -Djava.class.path={ANALYTICAL_ENGINE_JAVA_INIT_CLASS_PATH}"
)


# INTERACTIVE_ENGINE_SCRIPT
INTERACTIVE_INSTANCE_TIMEOUT_SECONDS = 120  # 2 mins
INTERACTIVE_ENGINE_SCRIPT = os.path.join(GRAPHSCOPE_HOME, "bin", "giectl")

# default threads per worker configuration for GIE/GAIA
INTERACTIVE_ENGINE_THREADS_PER_WORKER = 2

# JAVA SDK related CONSTANTS
LLVM4JNI_HOME = os.environ.get("LLVM4JNI_HOME", None)
LLVM4JNI_USER_OUT_DIR_BASE = "user-llvm4jni-output"
PROCESSOR_MAIN_CLASS = "com.alibaba.graphscope.annotation.Main"
JAVA_CODEGEN_OUTPUT_PREFIX = "gs-ffi"
GRAPE_PROCESSOR_JAR = os.path.join(
    GRAPHSCOPE_HOME, "lib", f"grape-runtime-{__version__}-shaded.jar"
)

GIRAPH_DRIVER_CLASS = "com.alibaba.graphscope.app.GiraphComputationAdaptor"

# 2 GB
GS_GRPC_MAX_MESSAGE_LENGTH = 2 * 1024 * 1024 * 1024 - 1


def get_timestamp() -> float:
    return datetime.datetime.timestamp(datetime.datetime.now())


def get_lib_path(app_dir: str, app_name: str) -> str:
    if sys.platform == "linux" or sys.platform == "linux2":
        return os.path.join(app_dir, "lib%s.so" % app_name)
    elif sys.platform == "darwin":
        return os.path.join(app_dir, "lib%s.dylib" % app_name)
    else:
        raise RuntimeError(f"Unsupported platform {sys.platform}")


def get_app_sha256(attr, java_class_path: str):
    (
        app_type,
        _,
        app_class,
        vd_type,
        _,
        _,
        java_jar_path,
        java_app_class,
    ) = _codegen_app_info(attr, DEFAULT_GS_CONFIG_FILE, java_class_path)
    graph_header, graph_type, _ = _codegen_graph_info(attr)

    if app_type == "cpp_pie":
        app_sha256 = hashlib.sha256(
            f"{app_type}.{app_class}.{graph_type}".encode("utf-8")
        ).hexdigest()
    elif app_type == "java_pie":
        s = hashlib.sha256()
        s.update(f"{graph_type}.{vd_type}".encode("utf-8"))
        app_sha256 = s.hexdigest()
        logger.info(
            "app sha256 for app {} with graph {}:{}, is {}".format(
                java_app_class, app_type, java_app_class, app_sha256
            )
        )
    else:
        s = hashlib.sha256()
        s.update(f"{app_type}.{app_class}.{graph_type}".encode("utf-8"))
        if types_pb2.GAR in attr:
            s.update(attr[types_pb2.GAR].s)
        app_sha256 = s.hexdigest()
    return app_sha256


def get_graph_sha256(attr):
    _, graph_class, _ = _codegen_graph_info(attr)
    return hashlib.sha256(graph_class.encode("utf-8")).hexdigest()


def check_java_app_graph_consistency(
    app_class, cpp_graph_type, java_class_template_str
):
    splited = cpp_graph_type.split("<")
    java_app_type_params = java_class_template_str[:-1].split("<")[-1].split(",")
    if splited[0] == "vineyard::ArrowFragment":
        if app_class.find("Property") == -1:
            raise RuntimeError(
                "Expected property app, inconsistent app and graph {}, {}".format(
                    app_class, cpp_graph_type
                )
            )
        if len(java_app_type_params) != 1:
            raise RuntimeError("Expected 4 type params in java app")

    if splited[0] == "gs::ArrowProjectedFragment":
        if app_class.find("Projected") == -1:
            raise RuntimeError(
                "Expected Projected app, inconsistent app and graph {}, {}".format(
                    app_class, cpp_graph_type
                )
            )
        if len(java_app_type_params) != 4:
            raise RuntimeError("Expected 4 type params in java app")

    graph_actual_type_params = splited[1][:-1].split(",")
    for i in range(0, len(java_app_type_params)):
        graph_actual_type_param = graph_actual_type_params[i]
        java_app_type_param = java_app_type_params[i]
        if not _type_param_consistent(graph_actual_type_param, java_app_type_param):
            raise RuntimeError(
                "Error in check app and graph consistency, type params index {}, cpp: {}, java: {}".format(
                    i, graph_actual_type_param, java_app_type_param
                )
            )
    return True


def run_command(args: str, cwd=None):
    logger.info("Running command: %s, cwd: %s", args, cwd)
    cp = subprocess.run(shlex.split(args), capture_output=True, cwd=cwd)
    if cp.returncode != 0:
        err = cp.stderr.decode("ascii")
        logger.error(
            "Failed to run command: %s, error message is: %s",
            args,
            err,
        )
        raise RuntimeError(f"Failed to run command: {args}, err: {err}")
    return cp.stdout.decode("ascii")


def delegate_command_to_pod(args: str, pod: str, container: str):
    """Delegate a command to a pod.

    Args:
        command (str): Command to be delegated.
        pod_name (str): Pod name.
        namespace (str): Namespace of the pod.

    Returns:
        str: Output of the command.
    """
    # logger.info("Delegate command to pod: %s, %s, %s", args, pod, container)
    args = f'kubectl exec -c {container} {pod} -- bash -c "{args}"'
    return run_command(args)


def compile_library(commands, workdir, output_name, launcher):
    if launcher.type() == types_pb2.K8S:
        return _compile_on_kubernetes(
            commands,
            workdir,
            output_name,
            launcher.hosts_list[0],
            launcher._engine_cluster.analytical_container_name,
        )
    elif launcher.type() == types_pb2.HOSTS:
        return _compile_on_local(commands, workdir, output_name)
    else:
        raise RuntimeError(f"Unsupported launcher type: {launcher.type()}")


def _compile_on_kubernetes(commands, workdir, output_name, pod, container):
    logger.info(
        "compile on kubernetes, %s, %s, %s, %s, %s",
        commands,
        workdir,
        output_name,
        pod,
        container,
    )
    try:
        full_path = get_lib_path(workdir, output_name)
        try:
            # The library may exists in the analytical pod.
            test_cmd = f"test -f {full_path}"
            logger.debug(delegate_command_to_pod(test_cmd, pod, container))
            logger.info("Library exists, skip compilation")
            cp = f"kubectl cp {pod}:{full_path} {full_path} -c {container}"
            logger.debug(run_command(cp))
            return full_path
        except RuntimeError:
            pass
        parent_dir = os.path.dirname(workdir)
        mkdir = f"mkdir -p {parent_dir}"
        logger.debug(delegate_command_to_pod(mkdir, pod, container))
        cp = f"kubectl cp {workdir} {pod}:{workdir} -c {container}"
        logger.debug(run_command(cp))
        prepend = "source scl_source enable devtoolset-10 rh-python38 &&"
        for command in commands:
            command = f"{prepend} cd {workdir} && {command}"
            logger.debug(delegate_command_to_pod(command, pod, container))
        cp = f"kubectl cp {pod}:{full_path} {full_path} -c {container}"
        logger.debug(run_command(cp))
        if not os.path.isfile(full_path):
            logger.error("Could not find desired library, found files are:")
            logger.error(os.listdir(workdir))
            raise FileNotFoundError(full_path)
    except Exception as e:
        raise CompilationError(f"Failed to compile {output_name} on kubernetes") from e
    return full_path


def _compile_on_local(commands, workdir, output_name):
    logger.info("compile on local, %s, %s, %s", commands, workdir, output_name)
    try:
        for command in commands:
            logger.debug(run_command(command, cwd=workdir))
        full_path = get_lib_path(workdir, output_name)
        if not os.path.isfile(full_path):
            logger.error("Could not find desired library")
            logger.info(os.listdir(workdir))
            raise FileNotFoundError(full_path)
    except Exception as e:
        raise CompilationError(
            f"Failed to compile {output_name} on platform {get_platform_info()}"
        ) from e
    return full_path


def compile_app(
    workspace: str,
    library_name: str,
    attr: dict,
    engine_config: dict,
    launcher,
    java_class_path: str,
):
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
    logger.info("Building app library...")
    library_dir = os.path.join(workspace, library_name)
    os.makedirs(library_dir, exist_ok=True)

    _extract_gar(library_dir, attr)
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
    ) = _codegen_app_info(attr, DEFAULT_GS_CONFIG_FILE, java_class_path)
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

    graph_header, graph_type, graph_oid_type = _codegen_graph_info(attr)
    if app_type == "java_pie":
        logger.info(
            "Check consistent between java app {} and graph {}".format(
                java_app_class, graph_type
            )
        )
        check_java_app_graph_consistency(app_class, graph_type, java_app_class)

    os.chdir(library_dir)

    module_name = ""
    # Output directory for java codegen
    java_codegen_out_dir = ""
    # set OPAL_PREFIX in CMAKE_PREFIX_PATH
    OPAL_PREFIX = os.environ.get("OPAL_PREFIX", "")
    cmake_commands = [
        "cmake",
        ".",
        f"-DNETWORKX={engine_config['networkx']}",
        f"-DCMAKE_PREFIX_PATH='{GRAPHSCOPE_HOME};{OPAL_PREFIX}'",
    ]
    if types_pb2.CMAKE_EXTRA_OPTIONS in attr:
        extra_options = attr[types_pb2.CMAKE_EXTRA_OPTIONS].s.decode("utf-8").split(" ")
        cmake_commands.extend(extra_options)

    if os.environ.get("GRAPHSCOPE_ANALYTICAL_DEBUG", "") == "1":
        cmake_commands.append("-DCMAKE_BUILD_TYPE=Debug")
    if app_type == "java_pie":
        # for java need to run preprocess, and the generated files can be reused,
        # if the fragment & vd type is same.
        java_codegen_out_dir = os.path.join(
            workspace, f"{JAVA_CODEGEN_OUTPUT_PREFIX}-{library_name}"
        )
        # TODO(zhanglei): Could this codegen caching happends on engine side?
        if os.path.isdir(java_codegen_out_dir):
            logger.info(
                "Found existing java codegen directory: %s, skipped codegen",
                java_codegen_out_dir,
            )
            cmake_commands += ["-DJAVA_APP_CODEGEN=OFF"]
        else:
            cmake_commands += ["-DJAVA_APP_CODEGEN=ON"]
        cmake_commands += [
            "-DENABLE_JAVA_SDK=ON",
            "-DJAVA_PIE_APP=ON",
            f"-DPRE_CP={GRAPE_PROCESSOR_JAR}:{java_jar_path}",
            f"-DPROCESSOR_MAIN_CLASS={PROCESSOR_MAIN_CLASS}",
            f"-DJAR_PATH={java_jar_path}",
            f"-DOUTPUT_DIR={java_codegen_out_dir}",
        ]
        # if run llvm4jni.sh not found, we just go ahead,since it is optional.
        # The go ahead part moves to `gscoordinator/template/CMakeLists.template`
        if LLVM4JNI_HOME:
            llvm4jni_user_out_dir = os.path.join(
                workspace, f"{LLVM4JNI_USER_OUT_DIR_BASE}-{library_name}"
            )
            cmake_commands += [
                f"-DRUN_LLVM4JNI_SH={os.path.join(LLVM4JNI_HOME, 'run.sh')}",
                f"-DLLVM4JNI_OUTPUT={llvm4jni_user_out_dir}",
                f"-DLIB_PATH={get_lib_path(library_dir, library_name)}",
            ]
        else:
            logger.info(
                "Skip running llvm4jni since env var LLVM4JNI_HOME not found or run.sh not found under LLVM4JNI_HOME"
            )
    elif app_type not in ("cpp_pie", "cpp_pregel"):
        if app_type == "cython_pregel":
            pxd_name = "pregel"
            cmake_commands += ["-DCYTHON_PREGEL_APP=ON"]
            if pregel_combine:
                cmake_commands += ["-DENABLE_PREGEL_COMBINE=ON"]
        else:
            pxd_name = "pie"
            cmake_commands += ["-DCYTHON_PIE_APP=ON"]

        # Copy pxd file and generate cc file from pyx
        shutil.copyfile(
            os.path.join(TEMPLATE_DIR, f"{pxd_name}.pxd.template"),
            os.path.join(library_dir, f"{pxd_name}.pxd"),
        )
        # Assume the gar will have and only have one .pyx file
        for pyx_file in glob.glob(library_dir + "/*.pyx"):
            module_name = os.path.splitext(os.path.basename(pyx_file))[0]
            cc_file = os.path.join(library_dir, module_name + ".cc")
            subprocess.check_call(["cython", "-3", "--cplus", "-o", cc_file, pyx_file])
        app_header = f"{module_name}.h"

    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(library_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=library_name,
            _oid_type=graph_oid_type,
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
    commands = [" ".join(cmake_commands), "make -j2"]
    lib_path = compile_library(commands, library_dir, library_name, launcher)

    return lib_path, java_jar_path, java_codegen_out_dir, app_type


def compile_graph_frame(
    workspace: str,
    library_name: str,
    attr: dict,
    engine_config: dict,
    launcher,
):
    """Compile a graph.

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
    logger.info("Building graph library ...")
    _, graph_class, _ = _codegen_graph_info(attr)

    library_dir = os.path.join(workspace, library_name)
    os.makedirs(library_dir, exist_ok=True)

    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(library_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r", encoding="utf-8") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=library_name,
            _graph_type=graph_class,
        )
        with open(cmakelists_file, mode="w", encoding="utf-8") as f:
            f.write(content)

    # set OPAL_PREFIX in CMAKE_PREFIX_PATH
    OPAL_PREFIX = os.environ.get("OPAL_PREFIX", "")
    cmake_commands = [
        "cmake",
        ".",
        f"-DNETWORKX={engine_config['networkx']}",
        f"-DENABLE_JAVA_SDK={engine_config['enable_java_sdk']}",
        f"-DCMAKE_PREFIX_PATH='{GRAPHSCOPE_HOME};{OPAL_PREFIX}'",
    ]
    if os.environ.get("GRAPHSCOPE_ANALYTICAL_DEBUG", "") == "1":
        cmake_commands.append("-DCMAKE_BUILD_TYPE=Debug")
    logger.info("Enable java sdk: %s", engine_config["enable_java_sdk"])
    graph_type = attr[types_pb2.GRAPH_TYPE].i
    if graph_type == graph_def_pb2.ARROW_PROPERTY:
        cmake_commands += ["-DPROPERTY_GRAPH_FRAME=ON"]
    elif graph_type in (
        graph_def_pb2.ARROW_PROJECTED,
        graph_def_pb2.DYNAMIC_PROJECTED,
        graph_def_pb2.ARROW_FLATTENED,
    ):
        cmake_commands += ["-DPROJECT_FRAME=ON"]
    else:
        raise ValueError(f"Illegal graph type: {graph_type}")

    # compile
    commands = [" ".join(cmake_commands), "make -j2"]
    lib_path = compile_library(commands, library_dir, library_name, launcher)
    return lib_path, None, None, None


def _type_param_consistent(graph_actucal_type_param, java_app_type_param):
    if java_app_type_param == "java.lang.Long":
        if graph_actucal_type_param in {"uint64_t", "int64_t"}:
            return True
        return False
    if java_app_type_param == "java.lang.Double":
        if graph_actucal_type_param in {"double"}:
            return True
        return False
    if java_app_type_param == "java.lang.Integer":
        if graph_actucal_type_param in {"int32_t", "uint32_t"}:
            return True
        return False
    return False


def op_pre_process(op, op_result_pool, key_to_op, **kwargs):  # noqa: C901
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
        types_pb2.OUTPUT,
    ):
        _pre_process_for_context_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op in (types_pb2.GRAPH_TO_NUMPY, types_pb2.GRAPH_TO_DATAFRAME):
        _pre_process_for_output_graph_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.UNLOAD_APP:
        _pre_process_for_unload_app_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.UNLOAD_CONTEXT:
        _pre_process_for_unload_context_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op == types_pb2.DATA_SINK:
        _pre_process_for_data_sink_op(op, op_result_pool, key_to_op, **kwargs)
    if op.op in (types_pb2.TO_DIRECTED, types_pb2.TO_UNDIRECTED):
        _pre_process_for_transform_op(op, op_result_pool, key_to_op, **kwargs)


def _pre_process_for_create_graph_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) <= 1
    if len(op.parents) == 1:
        key_of_parent_op = op.parents[0]
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.DATA_SOURCE:
            op.large_attr.CopyFrom(parent_op.large_attr)

        # loading graph with giraph format need jvm environ.
        if "engine_java_class_path" in kwargs:
            engine_java_class_path = kwargs.pop("engine_java_class_path")
            op.attr[types_pb2.JAVA_CLASS_PATH].CopyFrom(
                utils.s_to_attr(engine_java_class_path)
            )
        if "engine_jvm_opts" in kwargs:
            engine_jvm_opts = kwargs.pop("engine_jvm_opts")
            op.attr[types_pb2.JVM_OPTS].CopyFrom(utils.s_to_attr(engine_jvm_opts))


def _pre_process_for_add_labels_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 2
    for key_of_parent_op in op.parents:
        parent_op = key_to_op[key_of_parent_op]
        if parent_op.op == types_pb2.DATA_SOURCE:
            op.large_attr.CopyFrom(parent_op.large_attr)
        else:
            result = op_result_pool[key_of_parent_op]
            op.attr[types_pb2.GRAPH_NAME].CopyFrom(
                utils.s_to_attr(result.graph_def.key)
            )


def _pre_process_for_transform_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    result = op_result_pool[op.parents[0]]
    # To compatible with eager evaluation cases where it has the key.
    if types_pb2.GRAPH_NAME not in op.attr:
        op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(result.graph_def.key))


# get `bind_app` runtime information in lazy mode
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
            op.attr[types_pb2.GRAPH_NAME].CopyFrom(
                attr_value_pb2.AttrValue(s=result.graph_def.key.encode("utf-8"))
            )
            op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
                attr_value_pb2.AttrValue(i=result.graph_def.graph_type)
            )

            assert result.graph_def.extension.Is(
                graph_def_pb2.VineyardInfoPb.DESCRIPTOR
            ) or result.graph_def.extension.Is(
                graph_def_pb2.MutableGraphInfoPb.DESCRIPTOR
            )
            if result.graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR):
                vy_info = graph_def_pb2.VineyardInfoPb()
                result.graph_def.extension.Unpack(vy_info)

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
            elif result.graph_def.extension.Is(
                graph_def_pb2.MutableGraphInfoPb.DESCRIPTOR
            ):
                graph_info = graph_def_pb2.MutableGraphInfoPb()
                result.graph_def.extension.Unpack(graph_info)
                op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
                    utils.s_to_attr(utils.data_type_to_cpp(graph_info.vdata_type))
                )
                op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
                    utils.s_to_attr(utils.data_type_to_cpp(graph_info.edata_type))
                )


# get `run_app` runtime information in lazy mode
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

    # loading graph with giraph format need jvm environ.
    if "engine_java_class_path" in kwargs:
        engine_java_class_path = kwargs.pop("engine_java_class_path")
        op.attr[types_pb2.JAVA_CLASS_PATH].CopyFrom(
            utils.s_to_attr(engine_java_class_path)
        )
    if "engine_jvm_opts" in kwargs:
        engine_jvm_opts = kwargs.pop("engine_jvm_opts")
        op.attr[types_pb2.JVM_OPTS].CopyFrom(utils.s_to_attr(engine_jvm_opts))

    app_type = parent_op.attr[types_pb2.APP_ALGO].s.decode("utf-8")

    if app_type.startswith("java_pie:") or app_type.startswith("giraph:"):
        logger.debug("args len: {}".format(len(op.query_args.args)))
        if len(op.query_args.args) == 1:
            original_json_param = data_types_pb2.StringValue()
            op.query_args.args[0].Unpack(original_json_param)
            logger.debug("original user param {}".format(original_json_param))
            user_params = json.loads(original_json_param.value)
            del op.query_args.args[0]
        elif len(op.query_args.args) == 0:
            user_params = {}
        else:
            raise RuntimeError(
                "Unexpected num of params{}".format(len(op.query_args.args))
            )
        # we need extra param in first arg.
        user_params["jar_name"] = engine_java_class_path
        user_params["frag_name"] = "gs::ArrowProjectedFragment<{},{},{},{}>".format(
            parent_op.attr[types_pb2.OID_TYPE].s.decode("utf-8"),
            parent_op.attr[types_pb2.VID_TYPE].s.decode("utf-8"),
            parent_op.attr[types_pb2.V_DATA_TYPE].s.decode("utf-8"),
            parent_op.attr[types_pb2.E_DATA_TYPE].s.decode("utf-8"),
        )

        # for giraph app, we need to add args into orginal query_args, which is a json string
        # first one should be user params, second should be lib_path
        if app_type.startswith("giraph:"):
            user_params["app_class"] = GIRAPH_DRIVER_CLASS
            user_params["user_app_class"] = app_type[7:]
        else:
            user_params["app_class"] = app_type.split(":")[-1]
        logger.debug("user params {}".format(json.dumps(user_params)))
        new_user_param = Any()
        new_user_param.Pack(data_types_pb2.StringValue(value=json.dumps(user_params)))
        op.query_args.args.extend([new_user_param])

        # For java app, we need lib path as an explicit arg.
        lib_param = Any()
        lib_path = parent_op.attr[types_pb2.APP_LIBRARY_PATH].s.decode("utf-8")
        logger.info("Java app: Lib path {}".format(lib_path))
        lib_param.Pack(data_types_pb2.StringValue(value=lib_path))
        op.query_args.args.extend([lib_param])


def _pre_process_for_unload_graph_op(op, op_result_pool, key_to_op, **kwargs):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    result = op_result_pool[key_of_parent_op]
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(utils.s_to_attr(result.graph_def.key))
    if result.graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR):
        vy_info = graph_def_pb2.VineyardInfoPb()
        result.graph_def.extension.Unpack(vy_info)
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
            selector = _transform_dataframe_selector(context_type, schema, selector)
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
                    types_pb2.TO_DIRECTED,
                    types_pb2.TO_UNDIRECTED,
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
    if op.op in (
        types_pb2.CONTEXT_TO_DATAFRAME,
        types_pb2.TO_VINEYARD_DATAFRAME,
        types_pb2.OUTPUT,
    ):
        selector = _transform_dataframe_selector(context_type, schema, selector)
    else:
        # to numpy
        selector = _transform_numpy_selector(context_type, schema, selector)
    if selector is not None:
        op.attr[types_pb2.SELECTOR].CopyFrom(
            attr_value_pb2.AttrValue(s=selector.encode("utf-8"))
        )


def _pre_process_for_data_sink_op(op, op_result_pool, key_to_op, **kwargs):
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
        selector = _transform_dataframe_selector(
            "labeled_vertex_property", schema, selector
        )
    else:
        # to numpy
        selector = _transform_numpy_selector(
            "labeled_vertex_property", schema, selector
        )
    if selector is not None:
        op.attr[types_pb2.SELECTOR].CopyFrom(
            attr_value_pb2.AttrValue(s=selector.encode("utf-8"))
        )
    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )


def _pre_process_for_project_to_simple_op(  # noqa: C901
    op, op_result_pool, key_to_op, **kwargs
):
    assert len(op.parents) == 1
    key_of_parent_op = op.parents[0]
    r = op_result_pool[key_of_parent_op]

    # for nx graph
    if r.graph_def.graph_type == graph_def_pb2.DYNAMIC_PROPERTY:
        graph_info = graph_def_pb2.MutableGraphInfoPb()
        r.graph_def.extension.Unpack(graph_info)
        schema = json.loads(graph_info.property_schema_json)
        graph_name = r.graph_def.key
        v_prop = op.attr[types_pb2.V_PROP_KEY].s.decode("utf-8")
        e_prop = op.attr[types_pb2.E_PROP_KEY].s.decode("utf-8")
        v_prop_type = graph_def_pb2.NULLVALUE
        e_prop_type = graph_def_pb2.NULLVALUE
        if v_prop != "None" and v_prop in schema["vertex"]:
            v_prop_type = schema["vertex"][v_prop]
        if e_prop != "None" and e_prop in schema["edge"]:
            e_prop_type = schema["edge"][e_prop]

        op.attr[types_pb2.GRAPH_NAME].CopyFrom(
            attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
        )
        op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
            utils.graph_type_to_attr(graph_def_pb2.DYNAMIC_PROJECTED)
        )
        op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
            utils.s_to_attr(utils.data_type_to_cpp(v_prop_type))
        )
        op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
            utils.s_to_attr(utils.data_type_to_cpp(e_prop_type))
        )
        return

    # for arrow property graph
    def _check_v_prop_exists_in_all_v_labels(schema, prop):
        exists = True
        for v_label in schema.vertex_labels:
            exists = exists and schema.vertex_property_exists(v_label, prop)
        return exists

    def _check_e_prop_exists_in_all_e_labels(schema, prop):
        exists = True
        for e_label in schema.edge_labels:
            exists = exists and schema.edge_property_exists(e_label, prop)
        return exists

    # get parent graph schema
    schema = GraphSchema()
    schema.from_graph_def(r.graph_def)
    graph_name = r.graph_def.key

    if schema.vertex_label_num == 0:
        raise RuntimeError(
            "Failed to project to simple graph as no vertex exists in this graph."
        )
    if schema.edge_label_num == 0:
        raise RuntimeError(
            "Failed to project to simple graph as no edge exists in this graph."
        )

    need_flatten_graph = False
    if schema.vertex_label_num > 1 or schema.edge_label_num > 1:
        need_flatten_graph = True

    # check and get vertex property
    v_prop = op.attr[types_pb2.V_PROP_KEY].s.decode("utf-8")
    if v_prop == "None":
        v_prop_id = -1
        v_prop_type = graph_def_pb2.NULLVALUE
        if not need_flatten_graph:
            # for projected graph
            # if there is only one property on the label, uses this property
            v_label = schema.vertex_labels[0]
            if schema.vertex_properties_num(v_label) == 1:
                v_prop = schema.get_vertex_properties(v_label)[0]
                v_prop_id = v_prop.id
                v_prop_type = v_prop.type
    else:
        # v_prop should exists in all labels
        if not _check_v_prop_exists_in_all_v_labels(schema, v_prop):
            raise RuntimeError(
                "Property {0} doesn't exists in all vertex labels".format(v_prop)
            )
        # get vertex property id
        v_prop_id = schema.get_vertex_property_id(schema.vertex_labels[0], v_prop)
        # get vertex property type
        v_prop_type = graph_def_pb2.NULLVALUE
        v_props = schema.get_vertex_properties(schema.vertex_labels[0])
        for v_prop in v_props:
            if v_prop.id == v_prop_id:
                v_prop_type = v_prop.type
                break

    # check and get edge property
    e_prop = op.attr[types_pb2.E_PROP_KEY].s.decode("utf-8")
    if e_prop == "None":
        e_prop_id = -1
        e_prop_type = graph_def_pb2.NULLVALUE
        if not need_flatten_graph:
            # for projected graph
            # if there is only one property on the label, uses this property
            e_label = schema.edge_labels[0]
            if schema.edge_properties_num(e_label) == 1:
                e_prop = schema.get_edge_properties(e_label)[0]
                e_prop_id = e_prop.id
                e_prop_type = e_prop.type
    else:
        # e_prop should exists in all labels
        if not _check_e_prop_exists_in_all_e_labels(schema, e_prop):
            raise RuntimeError(
                "Property {0} doesn't exists in all edge labels".format(e_prop)
            )
        # get edge property id
        e_prop_id = schema.get_edge_property_id(schema.edge_labels[0], e_prop)
        # get edge property type
        e_props = schema.get_edge_properties(schema.edge_labels[0])
        e_prop_type = graph_def_pb2.NULLVALUE
        for e_prop in e_props:
            if e_prop.id == e_prop_id:
                e_prop_type = e_prop.type
                break

    op.attr[types_pb2.GRAPH_NAME].CopyFrom(
        attr_value_pb2.AttrValue(s=graph_name.encode("utf-8"))
    )
    op.attr[types_pb2.OID_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(schema.oid_type))
    )
    op.attr[types_pb2.VID_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(schema.vid_type))
    )
    op.attr[types_pb2.V_DATA_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(v_prop_type))
    )
    op.attr[types_pb2.E_DATA_TYPE].CopyFrom(
        utils.s_to_attr(utils.data_type_to_cpp(e_prop_type))
    )
    if need_flatten_graph:
        op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
            utils.graph_type_to_attr(graph_def_pb2.ARROW_FLATTENED)
        )
        op.attr[types_pb2.V_PROP_KEY].CopyFrom(utils.s_to_attr(str(v_prop_id)))
        op.attr[types_pb2.E_PROP_KEY].CopyFrom(utils.s_to_attr(str(e_prop_id)))
    else:
        v_label = schema.vertex_labels[0]
        e_label = schema.edge_labels[0]
        relation = (v_label, v_label)
        check_argument(
            relation in schema.get_relationships(e_label),
            f"Cannot project to simple, Graph doesn't contain such relationship: {v_label} -> {e_label} <- {v_label}.",
        )
        v_label_id = schema.get_vertex_label_id(v_label)
        e_label_id = schema.get_edge_label_id(e_label)
        op.attr[types_pb2.GRAPH_TYPE].CopyFrom(
            utils.graph_type_to_attr(graph_def_pb2.ARROW_PROJECTED)
        )
        op.attr[types_pb2.V_LABEL_ID].CopyFrom(utils.i_to_attr(v_label_id))
        op.attr[types_pb2.V_PROP_ID].CopyFrom(utils.i_to_attr(v_prop_id))
        op.attr[types_pb2.E_LABEL_ID].CopyFrom(utils.i_to_attr(e_label_id))
        op.attr[types_pb2.E_PROP_ID].CopyFrom(utils.i_to_attr(e_prop_id))


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


# Below are selector transformation part, which will transform label / property
# names to corresponding id.


def _transform_vertex_data_v(selector):
    if selector not in ("v.id", "v.data", "v.label_id"):
        raise SyntaxError("selector of v must be 'id', 'data' or 'label_id'")
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
    prop_id = schema.get_vertex_property_id(label, prop)
    return f"label{label_id}.property{prop_id}"


def _transform_labeled_vertex_data_e(schema, label, prop):
    label_id = schema.get_edge_label_id(label)
    if prop in ("src", "dst"):
        return f"label{label_id}.{prop}"
    prop_id = schema.get_vertex_property_id(label, prop)
    return f"label{label_id}.property{prop_id}"


def _transform_labeled_vertex_data_r(schema, label):
    label_id = schema.get_vertex_label_id(label)
    return f"label{label_id}"


def _transform_labeled_vertex_property_data_r(schema, label, prop):
    label_id = schema.get_vertex_label_id(label)
    return f"label{label_id}.{prop}"


def transform_vertex_data_selector(schema, selector):
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


def transform_vertex_property_data_selector(schema, selector):
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


_transform_selector_func_map = {
    "tensor": lambda _, _2: None,
    "vertex_data": transform_vertex_data_selector,
    "labeled_vertex_data": transform_labeled_vertex_data_selector,
    "vertex_property": transform_vertex_property_data_selector,
    "labeled_vertex_property": transform_labeled_vertex_property_data_selector,
}


def _transform_numpy_selector(context_type, schema, selector):
    return _transform_selector_func_map[context_type](schema, selector)


def _transform_dataframe_selector(context_type, schema, selector):
    selector = json.loads(selector)
    transform_func = _transform_selector_func_map[context_type]
    selector = {key: transform_func(schema, value) for key, value in selector.items()}
    return json.dumps(selector)


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


def _parse_java_app_type(java_class_path, real_algo):
    _java_app_type = ""
    _frag_param_str = ""
    _java_inner_context_type = ""
    _java_executable = find_java()
    parse_user_app_cmd = [
        _java_executable,
        "-cp",
        "{}".format(java_class_path),
        "com.alibaba.graphscope.utils.AppBaseParser",
        real_algo,
    ]
    logger.info(" ".join(parse_user_app_cmd))
    parse_user_app_process = subprocess.Popen(
        parse_user_app_cmd,
        env=os.environ.copy(),
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
    )
    out, err = parse_user_app_process.communicate()
    lines = out.split("\n") + err.split("\n")
    for line in lines:
        logger.info(line)
        if len(line) == 0:
            continue
        elif line.find("Giraph") != -1:
            _java_app_type = "giraph"
        elif line.find("DefaultPropertyApp") != -1:
            _java_app_type = "default_property"
        elif line.find("ParallelPropertyApp") != -1:
            _java_app_type = "parallel_property"
        elif line.find("DefaultAppBase") != -1:
            _java_app_type = "default_simple"
        elif line.find("ParallelAppBase") != -1:
            _java_app_type = "parallel_simple"
        elif line.find("Error") != -1:
            raise Exception("Error occured in verifying user app")
        elif line.find("TypeParams") != -1:
            _frag_param_str = line.split(":")[-1].strip()
        elif line.find("ContextType") != -1:
            _java_inner_context_type = line.split(":")[-1].strip()
        elif line.find("VertexData") != -1:
            _vd_type = line.split(":")[-1].strip()
    # for giraph app, we manually set java inner ctx type
    logger.info(
        "Java app type: {}, frag type str: {}, ctx type: {}, vd type {}".format(
            _java_app_type, _frag_param_str, _java_inner_context_type, _vd_type
        )
    )
    if (
        not _java_app_type
        or not _frag_param_str
        or not _java_inner_context_type
        or not _vd_type
    ):
        raise RuntimeError("Parsed java app error")

    parse_user_app_process.wait()
    return _java_app_type, _frag_param_str, _java_inner_context_type, _vd_type


def _probe_for_java_app(attr, java_class_path, real_algo):
    (
        _java_app_type,
        _frag_param_str,
        _java_inner_context_type,
        _vd_type,
    ) = _parse_java_app_type(java_class_path, real_algo)
    if _java_app_type == "giraph":
        driver_header = "apps/java_pie/java_pie_projected_default_app.h"
        class_name = "gs::JavaPIEProjectedDefaultApp"
    elif _java_app_type == "default_property":
        driver_header = "apps/java_pie/java_pie_property_default_app.h"
        class_name = "gs::JavaPIEPropertyDefaultApp"
    elif _java_app_type == "parallel_property":
        driver_header = "apps/java_pie/java_pie_property_parallel_app.h"
        class_name = "gs::JavaPIEPropertyParallelApp"
    elif _java_app_type == "default_simple":
        driver_header = "apps/java_pie/java_pie_projected_default_app.h"
        class_name = "gs::JavaPIEProjectedDefaultApp"
    elif _java_app_type == "parallel_simple":
        driver_header = "apps/java_pie/java_pie_projected_parallel_app.h"
        class_name = "gs::JavaPIEProjectedParallelApp"
    else:
        raise RuntimeError(f"Not a supported java_app_type: {_java_app_type}")
    return driver_header, class_name, _vd_type, _frag_param_str


def _codegen_app_info(attr, meta_file: str, java_class_path: str):
    """Codegen application by instantiate the template specialization.

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
    # for algo start with giraph:, we don't find info in meta
    if algo.startswith("giraph:") or algo.startswith("java_pie:"):
        real_algo = algo.split(":")[1]
        logger.info("codegen app info for java app : {}".format(real_algo))
        src_header, app_class, vd_type, java_app_template_str = _probe_for_java_app(
            attr, java_class_path, real_algo
        )
        return (
            "java_pie",
            src_header,
            "{}<_GRAPH_TYPE>".format(app_class),
            vd_type,
            None,
            None,
            java_class_path,
            "{}<{}>".format(real_algo, java_app_template_str),
        )

    for app in config_yaml["app"]:
        if app["algo"] == algo:
            app_type = app["type"]  # cpp_pie or cython_pregel or cython_pie, java_pie
            if app_type in ("cpp_pie", "cpp_pregel"):
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

    raise KeyError("Algorithm does not exist in the gar resource.")


# a mapping for class name to header file.
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

VERETX_MAP_CLASS_MAP = {
    graph_def_pb2.GLOBAL_VERTEX_MAP: "vineyard::ArrowVertexMap<{},{}>",
    graph_def_pb2.LOCAL_VERTEX_MAP: "vineyard::ArrowLocalVertexMap<{},{}>",
}


def _codegen_graph_info(attr):
    # These getter functions are intended for lazy evaluation,
    # cause they are not always avaiable in all types of graphs
    def oid_type():
        if types_pb2.OID_TYPE in attr:
            return attr[types_pb2.OID_TYPE].s.decode("utf-8")
        else:  # DynamicProjectedFragment doesn't have oid
            return None

    def vid_type():
        return attr[types_pb2.VID_TYPE].s.decode("utf-8")

    def vdata_type():
        return attr[types_pb2.V_DATA_TYPE].s.decode("utf-8")

    def edata_type():
        return attr[types_pb2.E_DATA_TYPE].s.decode("utf-8")

    def vertex_map_type():
        if types_pb2.VERTEX_MAP_TYPE not in attr:
            vm_type_enum = graph_def_pb2.GLOBAL_VERTEX_MAP
        else:
            vm_type_enum = attr[types_pb2.VERTEX_MAP_TYPE].i

        def internal_type(t):  # The template of vertex map needs special care.
            if t == "std::string":
                return "vineyard::arrow_string_view"
            return t

        return VERETX_MAP_CLASS_MAP[vm_type_enum].format(
            internal_type(oid_type()), vid_type()
        )

    graph_type = attr[types_pb2.GRAPH_TYPE].i
    graph_class, graph_header = GRAPH_HEADER_MAP[graph_type]

    # graph_type is a literal of graph template in c++ side
    if graph_type == graph_def_pb2.ARROW_PROPERTY:
        # in a format of full qualified name, e.g.
        # vineyard::ArrowFragment<int64_t, uin64_t, vineyard::ArrowLocalVertexMap<int64_t, uint64_t>>
        graph_fqn = f"{graph_class}<{oid_type()},{vid_type()},{vertex_map_type()}>"
    elif graph_type == graph_def_pb2.ARROW_PROJECTED:
        # gs::ArrowProjectedFragment<int64_t, uint64_t, double, double,vineyard::ArrowLocalVertexMap<int64_t, uint64_t>>
        graph_fqn = f"{graph_class}<{oid_type()},{vid_type()},{vdata_type()},{edata_type()},{vertex_map_type()}>"
    elif graph_type == graph_def_pb2.IMMUTABLE_EDGECUT:
        # grape::ImmutableEdgecutFragment<int64_t, uint32_t, double, double>
        graph_fqn = (
            f"{graph_class}<{oid_type()},{vid_type()},{vdata_type()},{edata_type()}>"
        )
    elif graph_type == graph_def_pb2.ARROW_FLATTENED:
        # grape::ArrowFlattenFragment<int64_t, uint32_t, double, double>
        graph_fqn = (
            f"{graph_class}<{oid_type()},{vid_type()},{vdata_type()},{edata_type()}>"
        )
    elif graph_type == graph_def_pb2.DYNAMIC_PROJECTED:
        # gs::DynamicProjectedFragment<double, double>
        graph_fqn = f"{graph_class}<{vdata_type()},{edata_type()}>"
    else:
        raise ValueError(
            f"Unknown graph type: {graph_def_pb2.GraphTypePb.Name(graph_type)}"
        )
    logger.info("Codegened graph type: %s, Graph header: %s", graph_fqn, graph_header)
    return graph_header, graph_fqn, oid_type()


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
    for i, vertex_label in enumerate(schema.vertex_labels):
        vertex = {
            "id": idx,
            "label": vertex_label,
            "type": "VERTEX",
            "propertyDefList": [],
        }
        for j, value in enumerate(schema.vertex_property_names[i].s):
            names = schema.vertex_property_names[i]
            types = schema.vertex_property_types[i]
            vertex["propertyDefList"].append(
                {"id": j, "name": names.s[j], "data_type": types.s[j].upper()}
            )
        vertex["indexes"] = []
        vertex["indexes"].append({"propertyNames": [names.s[0]]})
        items.append(vertex)
        idx += 1

    for i, edge_label in enumerate(schema.edge_labels):
        edge = {"id": idx, "label": edge_label, "type": "EDGE", "propertyDefList": []}
        for j, value in enumerate(schema.edge_property_names[i].s):
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
    python_to_glog = {0: 100, 10: 10, 20: 1}
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
        >>> (cmd, env) = rmcp.resolve(4, 'h1,h2,h3')
        >>> cmd
        ['mpirun', '--allow-run-as-root',
         '-n', '4', '-host', 'h1:2,h2:1,h3:1']

        >>> env
        {'e': '/usr/local/bin/kube_ssh', # if kube_ssh in $PATH
         'OMPI_MCA_btl_vader_single_copy_mechanism': 'none',
         'OMPI_MCA_orte_allowed_exit_without_sync': '1'}

        >>> # if openmpi not found, change to mpich
        >>> rmcp = ResolveMPICmdPrefix()
        >>> (cmd, env) = rmcp.resolve(4, 'h1,h2,h3')
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
        mpi = None
        if ResolveMPICmdPrefix.openmpi():
            if "OPAL_PREFIX" in os.environ:
                mpi = os.path.expandvars("$OPAL_PREFIX/bin/mpirun")
            if mpi is None:
                if "OPAL_BINDIR" in os.environ:
                    mpi = os.path.expandvars("$OPAL_BINDIR/mpirun")
        if mpi is None:
            mpi = shutil.which("mpirun")
        if mpi is None:
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
            # set the following MCA parameter to zero will eliminate the chances
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


def check_gremlin_server_ready(endpoint):
    def _check_task(endpoint):
        from gremlin_python.driver.client import Client

        if "MY_POD_NAME" in os.environ:
            # inner kubernetes env
            if endpoint == "localhost" or endpoint == "127.0.0.1":
                # now, used in macOS with docker-desktop kubernetes cluster,
                # which external ip is 'localhost' when service type is 'LoadBalancer'
                return True

        try:
            client = Client(f"ws://{endpoint}/gremlin", "g")
            # May throw
            client.submit("g.V().limit(1)").all().result()
        finally:
            try:
                client.close()
            except:  # noqa: E722
                pass
        return True

    executor = ThreadPoolExecutor(max_workers=20)

    begin_time = time.time()
    while True:
        t = executor.submit(_check_task, endpoint)
        try:
            _ = t.result(timeout=30)
        except Exception as e:
            t.cancel()
            error_message = str(e)
        else:
            executor.shutdown(wait=False)
            return True
        time.sleep(3)
        if time.time() - begin_time > INTERACTIVE_INSTANCE_TIMEOUT_SECONDS:
            executor.shutdown(wait=False)
            raise TimeoutError(f"Gremlin check query failed: {error_message}")
