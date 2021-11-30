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

# from coordinator.gscoordinator.io_utils import PipeWatcher
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import yaml

from graphscope.analytical.udf.utils import InMemoryZip
from graphscope.framework.app import AppAssets
from graphscope.framework.app import AppDAGNode
from graphscope.framework.app import check_argument
from graphscope.framework.context import create_context_node
from graphscope.framework.dag import DAGNode
from graphscope.framework.dag_utils import bind_app
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.graph import Graph
from graphscope.proto import graph_def_pb2

__all__ = ["JavaApp"]

logger = logging.getLogger("graphscope")

# runtime workspace
try:
    WORKSPACE = os.environ["GRAPHSCOPE_RUNTIME"]
except KeyError:
    WORKSPACE = os.path.join("/", tempfile.gettempprefix(), "gs")
DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"

POSSIBLE_APP_TYPES = [
    "default_property",
    "parallel_property",
    "default_simple",
    "parallel_simple",
]


def _parse_user_app(java_app_class: str, java_jar_full_path: str):
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
            continue
        if line.find("ParallelPropertyApp") != -1:
            _java_app_type = "parallel_property"
            continue
        if line.find("DefaultAppBase") != -1:
            _java_app_type = "default_simple"
        if line.find("ParallelAppBase") != -1:
            _java_app_type = "parallel_simple"
            continue
        if line.find("Error") != -1:
            raise Exception("Error occured in verifying user app")
        if line.find("TypeParams") != -1:
            _frag_param_str = line.split(":")[1].strip()
            continue
        if line.find("ContextType") != -1:
            _java_inner_context_type = line.split(":")[1].strip()
            continue
    logger.info(
        "Java app type: {}, frag type str: {}, ctx type: {}".format(
            _java_app_type, _frag_param_str, _java_inner_context_type
        )
    )

    parse_user_app_process.wait()
    return _java_app_type, _frag_param_str, _java_inner_context_type


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


class JavaApp(AppAssets):
    """A class represents a java app assert node in a DAG that holds the jar file.

    It holds neccessary resouces to run a java app, including java class path, the gar
    file which consists jar and configuration yaml, and the specified java class.
    On creating a JavaApp, graphscope will try to load the specified java class, and parse
    the Base class for your app, and the base class for your Context Class. This operation
    requires a java runtime environment installed in your client machine where your graphscope
    session is created.

    To run your app, provide `JavaApp` with a property or projected graph and your querying args.

    """

    def __init__(self, full_jar_path: str, java_app_class: str):
        """Init JavaApp with the full path of your `jar` file and the fully-qualified name of your
        app class.

        Args:
            full_jar_path (str): The path where the jar file exists.
            java_app_class (str): the fully-qualified name of your app class.
        """
        self._java_app_class = java_app_class
        self._full_jar_path = full_jar_path
        self._jar_name = Path(self._full_jar_path).name
        gar = self._pack_jar(self._full_jar_path)
        gs_config = {
            "app": [
                {
                    "algo": "java_app",
                    "type": "java_pie",
                    "java_jar_path": self._full_jar_path,
                    "java_app_class": self.java_app_class,
                }
            ]
        }
        # extract java app type with help of java class.
        self._java_app_type, self._frag_param_str, _java_ctx_type = _parse_user_app(
            java_app_class, full_jar_path
        )
        # For four different java type, we use two different driver class
        if self._java_app_type not in POSSIBLE_APP_TYPES:
            raise RuntimeError("Unexpected app type: {}".format(self._java_app_type))
        if self._java_app_type.find("property") != -1:
            gs_config["app"][0]["compatible_graph"] = ["vineyard::ArrowFragment"]
        else:
            gs_config["app"][0]["compatible_graph"] = ["gs::ArrowProjectedFragment"]

        gs_config["app"][0]["context_type"] = _java_ctx_type
        if self._java_app_type == "default_property":
            gs_config["app"][0][
                "driver_header"
            ] = "apps/java_pie/java_pie_property_default_app.h"
            gs_config["app"][0]["class_name"] = "gs::JavaPIEPropertyDefaultApp"
        elif self._java_app_type == "parallel_property":
            gs_config["app"][0][
                "driver_header"
            ] = "apps/java_pie/java_pie_property_parallel_app.h"
            gs_config["app"][0]["class_name"] = "gs::JavaPIEPropertyParallelApp"
        elif self._java_app_type == "default_simple":
            gs_config["app"][0][
                "driver_header"
            ] = "apps/java_pie/java_pie_projected_default_app.h"
            gs_config["app"][0]["class_name"] = "gs::JavaPIEProjectedDefaultApp"
        else:
            gs_config["app"][0][
                "driver_header"
            ] = "apps/java_pie/java_pie_projected_parallel_app.h"
            gs_config["app"][0]["class_name"] = "gs::JavaPIEProjectedParallelApp"

        gar.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
        super().__init__("java_app", _java_ctx_type, gar.read_bytes())

    # Override is_compatible to make sure type params of graph consists with java app.
    def is_compatible(self, graph):
        splited = graph.template_str.split("<")
        java_app_type_params = self.frag_param_str.split(",")
        num_type_params = 0
        if len(splited) != 2:
            raise Exception(
                "Unrecoginizable graph template str: {}".format(graph.template_str)
            )
        if splited[0] == "vineyard::ArrowFragment":
            if self.java_app_type != "property":
                logger.error("Expected property app")
                return False
            if len(java_app_type_params) != 1:
                logger.error("Expected one type params.")
                return False
            num_type_params = 1
        if splited[1] == "gs::ArrowProjectedFragment":
            if self.java_app_type != "projected":
                logger.error("Expected projected app")
                return False
            if len(java_app_type_params) != 4:
                logger.error("Expected 4 type params")
                return False
            num_type_params = 4
        graph_actual_type_params = splited[1][:-1].split(",")
        for i in range(0, num_type_params):
            graph_actual_type_param = graph_actual_type_params[i]
            java_app_type_param = java_app_type_params[i]
            if not _type_param_consistent(graph_actual_type_param, java_app_type_param):
                return False
        return True

    def _pack_jar(self, full_jar_path: str):
        garfile = InMemoryZip()
        if not os.path.exists(full_jar_path):
            raise FileNotFoundError("Jar file not found in {}.".format(full_jar_path))
        if not full_jar_path.endswith(".jar") or not zipfile.is_zipfile(full_jar_path):
            raise KeyError(
                "{} is not a jar file, please feed your packed jar file to JavaApp.".format(
                    full_jar_path
                )
            )
        tmp_jar_file = open(full_jar_path, "rb")
        jar_bytes = tmp_jar_file.read()
        if len(jar_bytes) <= 0:
            raise KeyError("Expect a non-empty Jar.")
        garfile.append("{}".format(full_jar_path.split("/")[-1]), jar_bytes)
        return garfile

    def signature(self):
        s = hashlib.sha256()
        s.update(
            f"{self.type}.{self._full_jar_path}.{self.java_app_class}".encode("utf-8")
        )
        s.update(self.gar)
        return s.hexdigest()

    @property
    def java_app_class(self):
        return self._java_app_class

    @property
    def jar_name(self):
        return self._jar_name

    @property
    def java_app_type(self):
        return self._java_app_type

    @property
    def frag_param_str(self):
        return self._frag_param_str

    def __call__(self, graph: Graph, *args, **kwargs):
        kwargs_extend = dict(app_class=self.java_app_class, **kwargs)
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")

        if (
            self.java_app_type == "projected"
            and graph.graph_type == graph_def_pb2.ARROW_PROPERTY
        ):
            graph = graph._project_to_simple()
        app_ = graph.session._wrapper(JavaAppDagNode(graph, self))
        return app_(*args, **kwargs_extend)


class JavaAppDagNode(AppDAGNode):
    """retrict app assets to javaAppAssets"""

    def __init__(self, graph: Graph, app_assets: JavaApp):
        self._graph = graph
        self._app_assets = app_assets
        self._session = graph.session
        self._app_assets.is_compatible(self._graph)

        self._op = bind_app(graph, self._app_assets)
        # add op to dag
        self._session.dag.add_op(self._app_assets.op)
        self._session.dag.add_op(self._op)

    def _convert_arrow_frag_for_java(self, cpp_frag_str: str):
        """Convert vineyard::ArrowFragment<OID,VID> to gs::ArrowFragmentDefault<OID>"""
        res = cpp_frag_str.split(",")[0] + ">"
        return res.replace("<", "Default<", 1).replace("vineyard", "gs")

    def __call__(self, *args, **kwargs):
        """When called, check arguments based on app type, Then do build and query.

        Raises:
            InvalidArgumentError: If app_type is None,
                or positional argument found when app_type not `cpp_pie`.

        Returns:
            :class:`Context`: Query context, include running results of the app.
        """
        check_argument(self._app_assets.type == "java_pie", "expect java_pie app")

        if not isinstance(self._graph, DAGNode) and not self._graph.loaded():
            raise RuntimeError("The graph is not loaded")
        check_argument(not args, "Only support using keyword arguments in cython app.")

        if self._app_assets.java_app_type.find("property") != -1:
            frag_name_for_java = self._convert_arrow_frag_for_java(
                self._graph.template_str
            )
            logger.info(
                "Set frag name to {}, {}".format(
                    self._graph.template_str, frag_name_for_java
                )
            )
        else:
            frag_name_for_java = self._graph.template_str
        # get number of worker on each host, so we can determine the java memory settings.
        kwargs_extend = dict(
            frag_name=frag_name_for_java,
            jar_name=self._app_assets.jar_name,
            **kwargs,
        )

        logger.info("dumping to json {}".format(json.dumps(kwargs_extend)))
        return create_context_node(
            self._app_assets.context_type, self, self._graph, json.dumps(kwargs_extend)
        )
