# Copyright 2020 Alibaba Group Holding Limited.
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

"""User can use this file to run self-defined java app locally."""
import argparse
import logging
import os
import shutil
import subprocess
import sys

import graphscope
from graphscope import JavaApp
from graphscope.dataset import load_p2p_network

graphscope.set_option(show_log=True)

POSSIBLE_APP_TYPES = [
    "default_property",
    "parallel_property",
    "default_simple",
    "parallel_simple",
]
JAVA_LONG = "java.lang.Long"
JAVA_INT = "java.lang.Integer"
JAVA_DOUBLE = "java.lang.Double"
JAVA_FLOAT = "java.lang.Float"

LOG_FORMAT = "[%(asctime)s]-[%(levelname)s]: %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("java-app-runner")


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--app",
        type=str,
        required=True,
        default="{}",
        help="The fully-specified name of your java app",
    )
    parser.add_argument(
        "--jar_path",
        type=str,
        required=True,
        default="{}",
        help="The path where your packed jar resides.",
    )
    parser.add_argument(
        "--arguments",
        type=str,
        default="{}",
        help="The params you want to pass to this app's context, format them like 'src=4,threadNum=1'",
    )
    parser.add_argument(
        "--directed", type=bool, default=False, help="Run on directed graph or not"
    )
    return parser.parse_args()


def parse_java_app(java_app_class: str, java_jar_full_path: str):
    _java_app_type = ""
    _frag_param_str = ""
    _java_inner_context_type = ""
    _java_executable = "java"
    if shutil.which("java") is None:
        if os.environ.get("JAVA_HOME", None) is not None:
            _java_executable = os.path.join(os.environ.get("JAVA_HOME"), "bin", "java")
        if not os.path.isfile(_java_executable) or not os.access(
            _java_executable, os.X_OK
        ):
            raise RuntimeError(
                "Java executable not found, you shall install a java runtime."
            )
    parse_user_app_cmd = [
        _java_executable,
        "-cp",
        "{}".format(java_jar_full_path),
        "com.alibaba.graphscope.utils.AppBaseParser",
        java_app_class,
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
    logger.info(err)
    for line in out.split("\n"):
        logger.info(line)
        if len(line) == 0:
            continue
        if line.find("DefaultPropertyApp") != -1:
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
    logger.info(
        "Java app type: {}, frag type str: {}, ctx type: {}".format(
            _java_app_type, _frag_param_str, _java_inner_context_type
        )
    )

    parse_user_app_process.wait()
    return _java_app_type, _frag_param_str, _java_inner_context_type


def java_type_to_gs_type(java_type: str):
    if java_type == JAVA_LONG:
        dataType = "int64"
    elif java_type == JAVA_INT:
        dataType = "int"
    elif java_type == JAVA_DOUBLE:
        dataType = "double"
    elif java_type == JAVA_FLOAT:
        dataType = "float"
    else:
        logger.error("Unrecognized type: {}".format(java_type))
    return dataType


def parse_and_check_type_params(type_params: str):
    type_params = type_params.strip()
    types = type_params.split(",")
    if len(types) != 4:
        raise Exception("Expected 4 type params in your app.")
    if types[0] != JAVA_LONG:
        logger.error("Currently we only accept int64 as oid")
        sys.exit(1)
    if types[1] != JAVA_LONG:
        logger.error("Currently we only accept int64_t as vid")
        sys.exit(1)
    vdataType = java_type_to_gs_type(types[2])
    edataType = java_type_to_gs_type(types[3])
    return vdataType, edataType


def run_app(
    vdataType: str,
    edataType: str,
    app_type: str,
    directed: bool,
    jar_path: str,
    java_app_class: str,
    param_str,
):
    sess = graphscope.session(cluster_type="hosts", num_workers=1)
    graph = sess.g(directed=directed)
    graph = load_p2p_network(sess)
    
    if "simple" in app_type:
        graph = graph.project(vertices={"host": ['id']}, edges={"connect": ["dist"]})
    app = JavaApp(full_jar_path=jar_path, java_app_class=java_app_class)
    exec("ctx=app(graph, {})".format(param_str))
    logger.info("Successfully verify app: {}".format(java_app_class))


if __name__ == "__main__":
    args = parse_args()

    logger.info("Running app\t\t\t\t={}".format(args.app))
    logger.info("Jar apth\t\t\t\t={}".format(args.jar_path))
    logger.info("Test data dir\t\t\t\t={}".format(args.test_dir))
    logger.info("Arguments to java context\t\t={}".format(args.arguments))
    logger.info("Directed: \t\t\t\t={}".format(args.directed))

    app_type, type_params, _ = parse_java_app(args.app, args.jar_path)
    if app_type not in POSSIBLE_APP_TYPES:
        logger.error("Unsupported app type:{}".format(app_type))

    vdataType, edataType = parse_and_check_type_params(type_params)
    logger.info("vdataType: [{}], edataType: [{}]".format(vdataType, edataType))

    run_app(
        vdataType,
        edataType,
        app_type,
        args.directed,
        args.jar_path,
        args.app,
        args.arguments,
    )


import graphscope 
from graphscope.dataset import load_p2p_network
from graphscope import sssp

graphscope.set_option(show_log=True) 
graph = load_p2p_network(directed=False)
print(graph.schema)
simple_graph = graph.project(vertices={"host": []}, edges={"connect": ["dist"]})

sssp_context = sssp(simple_graph, src=6)
