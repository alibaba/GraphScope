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

import functools
import hashlib
import json
import logging
import os
import zipfile
from copy import deepcopy
from io import BytesIO

import yaml

from graphscope.framework.context import create_context_node
from graphscope.framework.dag import DAGNode
from graphscope.framework.dag_utils import bind_app
from graphscope.framework.dag_utils import create_app
from graphscope.framework.dag_utils import unload_app
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.utils import graph_type_to_cpp_class
from graphscope.proto import graph_def_pb2

logger = logging.getLogger("graphscope")

DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"


def project_to_simple(func):
    """Decorator to project a property graph to the simple graph.

    Default to uses `weight` as edge data key to correspond to the edge weight,
    and uses `attribute` as node data key to correspond to the node attribute.

    Examples:
        >>> @project_to_simple
        >>> def sssp(G, src, weight="dist")
        >>>     pass
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        graph = args[0]
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")
        if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
            if "weight" in kwargs:
                # func has 'weight' argument
                weight = kwargs.get("weight", None)
                projected = graph._project_to_simple(e_prop=weight)
            elif "attribute" in kwargs:
                # func has 'attribute' argument
                attribute = kwargs.get("attribute", None)
                projected = graph._project_to_simple(v_prop=attribute)
            else:
                projected = graph._project_to_simple()
            projected._base_graph = graph
        else:
            projected = graph
        return func(projected, *args[1:], **kwargs)

    return wrapper


def not_compatible_for(*graph_types):
    """Decorator to mark builtin algorithms as not compatible with graph.

    Args:
        graph_types: list of string
            Entries must be one of 'arrow_property', 'dynamic_property',
                'arrow_projected', 'dynamic_projected', 'directed', 'undirected'

    Returns:
        The decorated function.

    Raises:
        RuntimeError: If graph is not compatible.
        KeyError: If parameter is not correctly.

    Notes:
        Multiple types or use multiple @not_compatible_for() lines
        are joined logically with "or".

    Examples:
        >>> @not_compatible_for('arrow_property', 'dynamic_property')
        >>> def sssp(G, src, weight="dist"):
        >>>     pass
    """

    def _not_compatible_for(not_compatible_for_func, *args, **kwargs):
        @functools.wraps(not_compatible_for_func)
        def wrapper(*args, **kwargs):
            graph = args[0]
            if not hasattr(graph, "graph_type"):
                raise InvalidArgumentError(
                    "Missing graph_type attribute in graph object."
                )

            terms = {
                "arrow_property": graph.graph_type == graph_def_pb2.ARROW_PROPERTY,
                "dynamic_property": graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY,
                "arrow_projected": graph.graph_type == graph_def_pb2.ARROW_PROJECTED,
                "dynamic_projected": graph.graph_type
                == graph_def_pb2.DYNAMIC_PROJECTED,
                "arrow_flattened": graph.graph_type == graph_def_pb2.ARROW_FLATTENED,
                "directed": graph.is_directed(),
                "undirected": graph.is_directed() is False,
            }
            match = False
            try:
                for t in graph_types:
                    match = match or terms[t]
            except KeyError:
                raise InvalidArgumentError(
                    "Use one or more of arrow_property,dynamic_property,"
                    "arrow_projected,dynamic_projected,arrow_flattened,directed,undirected",
                )
            if match:
                raise InvalidArgumentError(
                    "Not compatible for %s type" % " ".join(graph_types)
                )
            else:
                return not_compatible_for_func(*args, **kwargs)

        return wrapper

    return _not_compatible_for


class AppAssets(DAGNode):
    """A class represents an app asset node in a DAG that holds the bytes of the gar resource.

    Assets includes an algorithm name, and gar (for user defined algorithm),
    a context type (one of 'tensor', 'vertex_data', 'vertex_property',
    'labeled_vertex_data', 'dynamic_vertex_data', 'labeled_vertex_property'),
    and its type (one of `cpp_pie`, `cpp_pregel`, `cython_pie`, `cython_pregel`),

    The instance of this class can be passed to init :class:`graphscope.framework.app.AppDAGNode`
    """

    _support_context_type = [
        "tensor",
        "vertex_data",
        "vertex_property",
        "labeled_vertex_data",
        "dynamic_vertex_data",
        "labeled_vertex_property",
    ]

    def __init__(self, algo=None, context=None, gar=None, cmake_extra_options=None):
        """Init assets of the algorithm.

        Args:
            algo (str): Represent specific algo inside resource.
            context (str): Type of context that hold the calculation results.
            It will get from gar if param is None. Defaults to None.
            gar (bytes or BytesIO, optional): The bytes that encodes the application's source code.
                Defaults to None.
        """
        self._algo = algo
        self._context_type = context
        if isinstance(self._algo, str) and (
            "giraph:" in self._algo or "java_pie:" in self._algo
        ):
            self._type = "java_pie"
        else:
            self._type = "cpp_pie"  # default is builtin app with `built_in` type
        self._meta = {}

        # used for gar resource
        if gar is not None and isinstance(gar, (BytesIO, bytes)):
            self._gar = gar if isinstance(gar, bytes) else gar.getvalue()
            self._extract_meta_info()
        else:
            # built_in apps has no gar resource.
            self._gar = None

        self._cmake_extra_options = cmake_extra_options

        if self._context_type not in self._support_context_type:
            raise InvalidArgumentError(
                "Unsupport context type: {0}".format(self._context_type)
            )

        self._op = create_app(self)

    def __repr__(self) -> str:
        return f"graphscope.framework.app.AppAssets <type: {self._type}, algo: {self._algo}, context: {self._context_type}>"

    def _extract_meta_info(self):
        """Extract app meta info from gar resource.
        Raises:
            InvalidArgumentError:
                - :code:`gs_conf.yaml` not exist in gar resource.
                - App not found in gar resource.
        """
        fp = BytesIO(self._gar)
        archive = zipfile.ZipFile(fp, "r")
        config = yaml.safe_load(archive.read(DEFAULT_GS_CONFIG_FILE))
        # default app will used if there is only one app in it
        if self._algo is None and len(config["app"]) == 1:
            self._algo = config["app"][0]["algo"]
            logger.info("Default app %s will be used.", self._algo)
        for meta in config["app"]:
            if self._algo == meta["algo"]:
                if "context_type" in meta:
                    self._context_type = meta["context_type"]
                self._type = meta["type"]
                self._meta = meta
                return
        raise InvalidArgumentError("App not found in gar: {}".format(self._algo))

    @property
    def algo(self):
        """Algorithm name, e.g. sssp, pagerank.

        Returns:
            str: Algorithm name of this asset.
        """
        return self._algo

    @property
    def context_type(self):
        """Context type, e.g. vertex_property, labeled_vertex_data.

        Returns:
            str: Type of the app context.
        """
        return self._context_type

    @property
    def type(self):
        """Algorithm type, one of `cpp_pie`, `cpp_pregel`, `cython_pie`, `java_pie` or `cython_pregel`.

        Returns:
            str: Algorithm type of this asset.
        """
        return self._type

    @property
    def gar(self):
        """Gar resource.

        Returns:
            bytes: gar resource of this asset.
        """
        return self._gar

    @classmethod
    def to_gar(cls, path):
        if os.path.exists(path):
            raise RuntimeError("Path exist: {}.".format(path))
        with open(path, "wb") as f:
            f.write(cls._gar)

    @classmethod
    def bytes(cls):
        return cls._gar

    @property
    def cmake_extra_options(self):
        return self._cmake_extra_options

    @property
    def signature(self):
        """Generate a signature of the app assets by its algo name (and gar resources).

        Used to uniquely identify a app assets.

        Returns:
            str: signature of this assets
        """
        s = hashlib.sha256()
        s.update(self._algo.encode("utf-8", errors="ignore"))
        if self._gar:
            s.update(self._gar)
        return s.hexdigest()

    def is_compatible(self, graph):
        """Determine if this algorithm can run on this type of graph.

        Args:
            graph (:class:`GraphDAGNode`): A graph instance.

        Raises:
            InvalidArgumentError:
                - App is not compatible with graph

            ScannerError:
                - Yaml file format is incorrect.
        """
        # builtin app
        if self._gar is None:
            return
        # check yaml file
        graph_type = graph_type_to_cpp_class(graph.graph_type)
        if graph_type not in self._meta["compatible_graph"]:
            raise InvalidArgumentError(
                "App is uncompatible with graph {}".format(graph_type)
            )
        return True

    def __call__(self, graph, *args, **kwargs):
        """Instantiate an App and do queries over it."""
        app_ = graph.session._wrapper(AppDAGNode(graph, self))
        return app_(*args, **kwargs)


class AppDAGNode(DAGNode):
    """A class represents a app node in a DAG.

    In GraphScope, an app node binding a concrete graph node that query executed on.
    """

    def __init__(self, graph, app_assets: AppAssets):
        """Create an application using given :code:`gar` file, or given application
            class name.

        Args:
            graph (:class:`GraphDAGNode`): A :class:`GraphDAGNode` instance.
            app_assets: A :class:`AppAssets` instance.
        """
        self._graph = graph

        self._app_assets = app_assets
        self._session = graph.session
        self._app_assets.is_compatible(self._graph)

        self._op = bind_app(graph, self._app_assets)
        # add app_assets op to dag is not exist
        if not self._session.dag.exists(self._app_assets.op):
            self._session.dag.add_op(self._app_assets.op)
        # add op to dag
        self._session.dag.add_op(self._op)

    def __repr__(self):
        s = f"graphscope.App <type: {self._app_assets.type}, algorithm: {self._app_assets.algo} "
        s += f"bounded_graph: {str(self._graph)}>"
        return s

    @property
    def algo(self):
        """Algorithm name, e.g. sssp, pagerank.

        Returns:
            str: Algorithm name of this asset.
        """
        return self._app_assets.algo

    @property
    def gar(self):
        """Gar resource.

        Returns:
            bytes: gar resource of this asset.
        """
        return self._app_assets.gar

    def __call__(self, *args, **kwargs):
        """When called, check arguments based on app type, Then do build and query.

        Raises:
            InvalidArgumentError: If app_type is None,
                or positional argument found when app_type not `cpp_pie`.

        Returns:
            :class:`Context`: Query context, include running results of the app.
        """
        app_type = self._app_assets.type
        check_argument(app_type is not None)
        context_type = self._app_assets.context_type

        if not isinstance(self._graph, DAGNode) and not self._graph.loaded():
            raise RuntimeError("The graph is not loaded")

        if self._app_assets.type in [
            "cpp_pregel",
            "cython_pie",
            "cython_pregel",
            "java_pie",
        ]:
            # cython app support kwargs only
            check_argument(
                not args, "Only support using keyword arguments in cython app."
            )
            return create_context_node(
                context_type, self, self._graph, json.dumps(kwargs)
            )

        return create_context_node(context_type, self, self._graph, *args, **kwargs)

    def __del__(self):
        try:
            self.session.run(self._unload())
        except Exception:  # pylint: disable=broad-except
            pass

    def _unload(self):
        """Unload this app from graphscope engine.

        Returns:
            :class:`graphscope.framework.app.UnloadedApp`: Evaluated in eager mode.
        """
        op = unload_app(self)
        return UnloadedApp(self._session, op)


class App(object):
    """An application that can run on graphs and produce results.

    Analytical engine will build the app dynamic library when instantiate a app instance.
    And the dynamic library will be reused if subsequent app's signature matches one of
    previous ones.
    """

    def __init__(self, app_node, key):
        self._app_node = app_node
        self._session = self._app_node.session
        self._key = key
        # copy and set op evaluated
        self._app_node.op = deepcopy(self._app_node.op)
        self._app_node.evaluated = True
        self._session.dag.add_op(self._app_node.op)
        self._saved_signature = self.signature

    def __getattr__(self, name):
        if hasattr(self._app_node, name):
            return getattr(self._app_node, name)
        raise AttributeError("{0} not found.".format(name))

    @property
    def key(self):
        """A unique identifier of App."""
        return self._key

    @property
    def signature(self):
        """Signature is computed by all critical components of the App."""
        return hashlib.sha256(
            "{}.{}".format(self._app_assets.signature, self._graph.template_str).encode(
                "utf-8", errors="ignore"
            )
        ).hexdigest()

    def _unload(self):
        return self._session._wrapper(self._app_node._unload())

    def __del__(self):
        """Unload app. Both on engine side and python side. Set the key to None."""
        try:
            self.session.run(self._unload())
        except Exception:  # pylint: disable=broad-except
            pass

    def __call__(self, *args, **kwargs):
        return self._session._wrapper(self._app_node(*args, **kwargs))


class UnloadedApp(DAGNode):
    """Unloaded app node in a DAG."""

    def __init__(self, session, op):
        self._session = session
        self._op = op
        # add op to dag
        self._session.dag.add_op(self._op)


def load_app(gar=None, algo=None, context=None, **kwargs):
    """Load an app from gar.

    Args:
        algo: str
          Algo name inside resource. None will extract name from gar resource
          if there is only one app in it.
        gar: bytes or BytesIO or str
          str represent the path of resource, bytes or the resource of the
          specified path or bytes.

          For java apps, gar can be none to indicate we should find the app in
          previouse added libs.

    Returns:
        Instance of <graphscope.framework.app.AppAssets>

    Raises:
        FileNotFoundError: File not exist.
        PermissionError: Permission denied of path.
        TypeError: File is not a zip file.

    Examples:
        >>> sssp = load_app(gar='./resource.gar', algo='sssp')
        >>> sssp(src=4)

        which will have following `.gs_conf.yaml` in resource.gar:
          app:
            - algo: sssp
              type: cpp_pie
              class_name: grape:SSSP
              context_type: vertex_data
              src: sssp/sssp.h
              compatible_graph:
                - gs::ArrowProjectedFragment
    """
    if isinstance(gar, (BytesIO, bytes)):
        return AppAssets(algo, context, gar, **kwargs)
    elif isinstance(gar, str):
        with open(gar, "rb") as f:
            content = f.read()
        if not zipfile.is_zipfile(gar):
            raise InvalidArgumentError("{} is not a zip file.".format(gar))
        return AppAssets(algo, context, content, **kwargs)
    elif isinstance(algo, str) and (
        algo.startswith("giraph:") or algo.startswith("java_pie:")
    ):
        if gar is not None:
            raise InvalidArgumentError("Running java app expect no gar resource")
        return AppAssets(algo, "vertex_data", None, **kwargs)
    else:
        raise InvalidArgumentError("Wrong type with {}".format(gar))
