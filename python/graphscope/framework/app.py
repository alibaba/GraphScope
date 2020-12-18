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

import hashlib
import json
import os
import zipfile
from io import BytesIO

import yaml
from decorator import decorator

import graphscope
from graphscope.framework.context import create_context
from graphscope.framework.dag_utils import create_app
from graphscope.framework.dag_utils import run_app
from graphscope.framework.dag_utils import unload_app
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.utils import graph_type_to_cpp_class
from graphscope.proto import types_pb2

DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"


def not_compatible_for(*graph_types):
    """Decorator to mark builtin algorithms as not compatible with graph.

    Args:
        graph_types: list of string
            Entries must be one of 'arrow_property', 'dynamic_property', 'arrow_projected', 'dynamic_projected'

    Returns:
        The decorated function.

    Raises:
        RuntimeError: If graph is not compatible.
        KeyError: If parameter is not correctly.

    Notes:
        Multiple types or use multile @not_compatible_for() lines
        are joined logically with "or".

    Examples:
        >>> @not_compatible_for('arrow_property', 'dynamic_property')
        >>> def sssp(G, src):
        >>>     pass
    """

    @decorator
    def _not_compatible_for(not_compatible_for_func, *args, **kwargs):
        graph = args[0]
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")

        terms = {
            "arrow_property": graph.graph_type == types_pb2.ARROW_PROPERTY,
            "dynamic_property": graph.graph_type == types_pb2.DYNAMIC_PROPERTY,
            "arrow_projected": graph.graph_type == types_pb2.ARROW_PROJECTED,
            "dynamic_projected": graph.graph_type == types_pb2.DYNAMIC_PROJECTED,
        }
        match = False
        try:
            for t in graph_types:
                match = match or terms[t]
        except KeyError:
            raise InvalidArgumentError(
                "Use one or more of arrow_property,dynamic_property,arrow_projected,dynamic_projected",
            )
        if match:
            raise InvalidArgumentError(
                "Not compatible for %s type" % " ".join(graph_types)
            )
        else:
            return not_compatible_for_func(*args, **kwargs)

    return _not_compatible_for


class AppAssets(object):
    """A class holds the bytes of the gar resource.

    Assets includes name (for builtin algorithm), and gar (for user defined algorithm),
    and its type (one of `cpp_pie`, `cython_pie`, `cython_pregel`.

    The instance of this class can be passed to init :class:`graphscope.App`.

    Attributes:
        algo (str): Name of the algorithm
        type (str): Type of the algorithm
        gar (bytes): Byte content of user defined algorithm
        signature (str): Unique identifier of this assets.
    """

    def __init__(self, algo, gar=None, **kwargs):
        """Init assets of the algorithm.

        Args:
            algo (str): Represent specific algo inside resource.
            gar (bytes or BytesIO, optional): The bytes that encodes the application's source code.
                Default to None
            kwargs: Other params, e.g. vd_type and md_type in cython app.
        """
        self._algo = algo
        self._type = "cpp_pie"  # default is built_in app with `built_in` type

        # used for gar resource
        if gar and isinstance(gar, (BytesIO, bytes)):
            self._gar = gar if isinstance(gar, bytes) else gar.getvalue()
        else:
            # built_in apps has no gar resource.
            self._gar = None

        self._saved_signature = self.signature

    @property
    def algo(self):
        """Algorithm name, e.g. sssp, pagerank.

        Returns:
            str: Algorithm name of this asset.
        """
        return self._algo

    @property
    def type(self):
        """Algorithm type, one of `cpp_pie`, `cython_pie` or `cython_pregel`.

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
    def signature(self):
        """Generate a signature of the app assets by its algo name (and gar resources).

        Used to uniquely identify a app assets.
        Returns:
            str: signature of this assets
        """
        s = hashlib.sha256()
        s.update(self._algo.encode("utf-8"))
        if self._gar:
            s.update(self._gar)
        return s.hexdigest()

    def is_compatible(self, graph):
        """Determine if this algorithm can run on this type of graph.

        Args:
            graph (:class:`Graph`): A graph instance.

        Raises:
            InvalidArgumentError:
                - :code:`gs_conf.yaml` not exist in gar resource.
                - App is not compatible with graph or
                - Algo not found in gar resource.

            ScannerError:
                - Yaml file format is incorrect.
        """
        if not isinstance(
            graph,
            (
                graphscope.framework.graph.Graph,
                graphscope.experimental.nx.classes.graph.Graph,
                graphscope.experimental.nx.classes.digraph.DiGraph,
            ),
        ):
            raise InvalidArgumentError("Wrong type of graph.")
        # builtin app
        if self._gar is None:
            self._type = "cpp_pie"
            return
        # check yaml file
        fp = BytesIO(self._gar)
        archive = zipfile.ZipFile(fp, "r")
        config = yaml.safe_load(archive.read(DEFAULT_GS_CONFIG_FILE))

        # check the compatibility with graph
        for application in config["app"]:
            if self._algo == application["algo"]:
                self._type = application["type"]
                graph_type = graph_type_to_cpp_class(graph.graph_type)
                if graph_type not in application["compatible_graph"]:
                    raise InvalidArgumentError(
                        "App is uncompatible with graph {}".format(graph_type)
                    )
                return True
        raise InvalidArgumentError("App not found in gar: {}".format(self._algo))

    def __call__(self, graph, *args, **kwargs):
        """Instantiate an App and do queries over it."""
        app_ = App(graph, self)
        return app_(*args, **kwargs)


class App(object):
    """An application that can run on graphs and produce results.

    Analytical engine will build the app dynamic library when instantiate a app instance.
    The dynamic library will be reused if subsequent app's signature matches one of previous ones.

    Attributes:
        key (str): Identifier of the app, associated with the dynamic library path.
            set by analytical engine after library is built.
        signature (str): Combination of app_assets's and graph's signature.
        session_id (str): Session id of the session that associated with the app.
        algo (str): Algorithm name of app_assets.
        gar (str): Gar content of app_assets.

    """

    def __init__(self, graph, app_assets: AppAssets):
        """Create an application using given :code:`gar` file, or given application
            class name.

        Args:
            graph (:class:`Graph`): A :class:`Graph` instance.
            app_assets: A :class:`AppAssets` instance.

        Raise:
            TypeError: The type of app_assets incorrect.
        """
        app_assets.is_compatible(graph)

        self._key = None
        self._graph = graph
        self._app_assets = app_assets
        self._session_id = graph.session_id

        opr = create_app(graph, self)
        self._key = opr.eval()
        self._saved_signature = self.signature

    def __repr__(self):
        return "<graphscope.App '%s'>" % self.key

    @property
    def key(self):
        """A unique identifier of App."""
        return self._key

    @property
    def signature(self):
        """Signature is computed by all critical components of the App."""
        return hashlib.sha256(
            "{}.{}".format(self._app_assets.signature, self._graph.signature).encode(
                "utf-8"
            )
        ).hexdigest()

    @property
    def session_id(self):
        """Return the session_id, which is copied from the graph.

        Returns:
            str: Id of the session which loaded the app.
        """
        return self._session_id

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

        if self._app_assets.type in ["cython_pie", "cython_pregel"]:
            # cython app support kwargs only
            check_argument(
                not args, "Only support using keyword arguments in cython app."
            )
            return self._query(json.dumps(kwargs))

        return self._query(*args, **kwargs)

    def _query(self, *args, **kwargs):
        """Create a `RUN_APP` Operation, and send it to analytical engine to do actual query.
        Then retrieve query contexts and return.
        """
        self._check_unmodified()
        op = run_app(self._graph, self, *args, **kwargs)
        ret = op.eval()
        ret = json.loads(ret)
        context_key, context_type = ret["context_key"], ret["context_type"]
        results = create_context(
            context_type, self._session_id, context_key, self._graph
        )
        return results

    def _check_unmodified(self):
        """Ensure app is not modified, cause it may need to recompile the dynamic library."""
        check_argument(self.signature == self._saved_signature)

    def loaded(self):
        """Since key is only set by engine after it load the app, and unset to None when unload,
           we can use the key to detect whether the app is loaded.

        Returns:
            bool: The app is loaded or not.
        """
        return self._key is not None

    def unload(self):
        """Unload app. Both on engine side and python side. Set the key to None."""
        if self._key:
            op = unload_app(self)
            op.eval()
            self._key = None
            self._graph = None
            self._app_assets = None
            self._session_id = None


def load_app(algo, gar=None, **kwargs):
    """Load an app from gar.
    bytes orthe resource of the specified path or bytes.

    Args:
        algo: str
          Algo name inside resource.
        gar: bytes or BytesIO or str
          str represent the path of resource.

    Returns:
        Instance of <graphscope.AppAssets>

    Raises:
        FileNotFoundError: File not exist.
        PermissionError: Permission denied of path.
        TypeError: File is not a zip file.

    Examples:
        >>> sssp = load_app('sssp', gar='./resource.gar')
        >>> sssp(src=4)

        which will have following `.gs_conf.yaml` in resource.gar:
          app:
            - algo: sssp
              type: cpp_pie
              class_name: grape:SSSP
              src: sssp/sssp.h
              compatible_graph:
                - gs::ArrowProjectedFragment
    """
    if isinstance(gar, (BytesIO, bytes)):
        return AppAssets(str(algo), gar, **kwargs)
    elif isinstance(gar, str):
        with open(gar, "rb") as f:
            content = f.read()

        if not zipfile.is_zipfile(gar):
            raise InvalidArgumentError("{} is not a zip file.".format(gar))

        return AppAssets(str(algo), content, **kwargs)
    else:
        raise InvalidArgumentError("Wrong type with {}".format(gar))
