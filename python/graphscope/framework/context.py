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

import collections
import hashlib
import itertools
import json
from copy import deepcopy
from typing import Mapping

import pandas as pd

from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.dag import DAGNode
from graphscope.framework.dag_utils import run_app
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument


class ResultDAGNode(DAGNode):
    """A class represents a result node in a DAG.

    In GraphScope, result node is always a leaf node in a DAG.
    """

    def __init__(self, dag_node, op):
        self._base_dag_node = dag_node
        self._session = self._base_dag_node.session
        self._op = op
        # add op to dag
        self._session.dag.add_op(self._op)


class UnloadedContext(DAGNode):
    """Unloaded context node in a DAG."""

    def __init__(self, session, op):
        self._session = session
        self._op = op
        # add op to dag
        self._session.dag.add_op(self._op)


class BaseContextDAGNode(DAGNode):
    """Base class of concrete context DAG node.

    In GraphScope, it will return a instance of concrete class `ContextDAGNode`
    after evaluating an app, that will be automatically executed by :meth:`Session.run`
    in eager mode and return a instance of :class:`graphscope.framework.context.Context`

    We can further use the handle to retrieve data:
        - as a numpy.ndarray( `to_numpy()` ),
        - as a pandas.DataFrame( `to_dataframe()` ),
        - as a vineyard tensor ( `to_vineyard_tensor()` ),
        - or as a vineyard dataframe ( `to_vineyard_dataframe()` ).

    The following example demonstrates its usage:

    .. code:: python

        >>> # lazy mode
        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="lazy")
        >>> g = load_p2p_network(sess)
        >>> sg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.sssp(sg, 20)
        >>> print(c) # <graphscope.framework.context.VertexDataContextDAGNode>
        >>> r1 = c.to_numpy("r")
        >>> print(r1) # <graphscope.ramework.context.ResultDAGNode>
        >>> r2 = c.to_dataframe({"id": "v.id", "result": "r"})
        >>> r3 = c.to_vineyard_tensor("r")
        >>> r4 = c.to_vineyard_dataframe({"id": "v.id", "result": "r"})
        >>> r = sess.run([r1, r2, r3, r4])
        >>> r[0].shape
        (62586,)
        >>> r[1].shape
        (62586, 2)
        >>> r[2] # return an object id
        >>> r[3] # return an object id

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> sg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = sssp(sg, 20)
        >>> print(c) # <graphscope.framework.context.Context>
        >>> r1 = c.to_numpy("r")
        >>> r1.shape
        (62586,)
        >>> r2 = c.to_dataframe({"id": "v.id", "result": "r"})
        >>> r2.shape
        (62586, 2)
        >>> r3 = c.to_vineyard_tensor() # return an object id
        >>> r4 = c.to_vineyard_dataframe() # return an object id
    """

    def __init__(self, bound_app, graph, *args, **kwargs):
        self._bound_app = bound_app
        self._graph = graph
        self._session = self._bound_app.session
        # add op to dag
        self._op = run_app(self._bound_app, *args, **kwargs)
        self._session.dag.add_op(self._op)

        # statically create the unload op
        self._unload_op = dag_utils.unload_context(self)

    def _check_selector(self, selector):
        raise NotImplementedError()

    @property
    def context_type(self):
        raise NotImplementedError()

    def _build_schema(self, result_properties):
        raise NotImplementedError()

    def to_numpy(self, selector, vertex_range=None, axis=0):
        """Get the context data as a numpy array.

        Args:
            selector (str): Describes how to select values of context.
                See more details in derived context DAG node class.
            vertex_range (dict): optional, default to None.
                Works as slicing. The expression {'begin': m, 'end': n} select a portion
                of vertices from `m` to, but not including `n`. Type of `m`, `n` must be
                identical with vertices' oid type.
                Omitting the first index starts the slice at the beginning of the vertices,
                and omitting the second index extends the slice to the end of the vertices.
                Note the comparision is not based on numeric order, but on alphabetic order.
            axis (int): optional, default to 0.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result holds the `numpy.ndarray`, evaluated in eager mode.
        """
        self._check_selector(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.context_to_numpy(self, selector, vertex_range, axis)
        return ResultDAGNode(self, op)

    def to_dataframe(self, selector, vertex_range=None):
        """Get the context data as a pandas DataFrame.

        Args:
            selector: dict
                The key is column name in dataframe, and the value describes how to select
                values of context. See more details in derived context DAG node class.
            vertex_range: dict, optional, default to None.
                Works as slicing. The expression {'begin': m, 'end': n} select a portion
                of vertices from `m` to, but not including `n`. Type of `m`, `n` must be
                identical with vertices' oid type.
                Only the sub-ranges of vertices data will be retrieved.
                Note the comparision is not based on numeric order, but on alphabetic order.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result holds the `pandas.DataFrame`, evaluated in eager mode.
        """
        check_argument(
            isinstance(selector, Mapping), "selector of to_dataframe must be a dict"
        )
        for _, value in selector.items():
            self._check_selector(value)
        _ensure_consistent_label(self.context_type, selector)
        selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.context_to_dataframe(self, selector, vertex_range)
        return ResultDAGNode(self, op)

    def to_vineyard_tensor(self, selector=None, vertex_range=None, axis=0):
        """Get the context data as a vineyard tensor and return the vineyard object id.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result hold the object id of vineyard tensor, evaluated in eager mode.
        """
        self._check_selector(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.to_vineyard_tensor(self, selector, vertex_range, axis)
        return ResultDAGNode(self, op)

    def to_vineyard_dataframe(self, selector=None, vertex_range=None):
        """Get the context data as a vineyard dataframe and return the vineyard object id.

        Args:
            selector:  dict
                Key is used as column name of the dataframe, and the value describes how to
                select values of context. See more details in derived context DAG node class.
            vertex_range: dict, optional, default to None
                Works as slicing. The expression {'begin': m, 'end': n} select a portion
                of vertices from `m` to, but not including `n`. Type of `m`, `n` must be
                identical with vertices' oid type.
                Only the sub-ranges of vertices data will be retrieved.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result hold the object id of vineyard dataframe, evaluated in eager mode.
        """
        if selector is not None:
            check_argument(
                isinstance(selector, Mapping),
                "selector of to_vineyard_dataframe must be a dict",
            )
            for _, value in selector.items():
                self._check_selector(value)
            _ensure_consistent_label(self.context_type, selector)
            selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.to_vineyard_dataframe(self, selector, vertex_range)
        return ResultDAGNode(self, op)

    def output(self, fd, selector, vertex_range=None, **kwargs):
        """Dump results to `fd`.
        Support dumps data to local (respect to pod) files, hdfs or oss.
        It first write results to a vineyard dataframe, and let vineyard
        do the data dumping job.
        `fd` must meet specific formats, with auth information if needed. As follows:

            - local
                `file:///tmp/result_path`
            - oss
                `oss:///bucket/object`
            - hdfs
                `hdfs:///tmp/result_path`

        Args:
            fd (str): Output location.
            selector (dict): Similar to `to_dataframe`.
            vertex_range (dict, optional): Similar to `to_dataframe`. Defaults to None.
            kwargs (dict, optional): Storage options with respect to output storage type.
                    for example:
                    key, secret, endpoint for oss,
                    key, secret, client_kwargs for s3,
                    host, port for hdfs,
                    None for local.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`, evaluated in eager mode.
        """
        protocol = fd.split("://")[0]
        # Still use the stream to write to file,
        # as the C++ adaptor in Vineyard requires arrow >= 4.0.0
        if protocol in ("file", "hdfs", "hive", "oss", "s3"):
            df = self.to_vineyard_dataframe(selector, vertex_range)
            op = dag_utils.to_data_sink(df, fd, **kwargs)
        else:
            check_argument(
                isinstance(selector, Mapping), "selector of to_dataframe must be a dict"
            )
            for _, value in selector.items():
                self._check_selector(value)
            _ensure_consistent_label(self.context_type, selector)
            selector = json.dumps(selector)
            vertex_range = utils.transform_vertex_range(vertex_range)
            op = dag_utils.output(self, fd, selector, vertex_range, **kwargs)
        return ResultDAGNode(self, op)

    def __del__(self):
        try:
            self.session.run(self._unload())
        except Exception:  # pylint: disable=broad-except
            pass

    def _unload(self):
        return UnloadedContext(self._session, self._unload_op)


class TensorContextDAGNode(BaseContextDAGNode):
    """Tensor context DAG node holds a tensor.
    Only axis is meaningful when considering a TensorContext.
    """

    @property
    def context_type(self):
        return "tensor"

    def _build_schema(self, result_properties):
        return "axis"

    def _check_selector(self, selector):
        return True


class VertexDataContextDAGNode(BaseContextDAGNode):
    """The most simple kind of context.
    A vertex has a single value as results.

    - The syntax of selector on vertex is:
        - `v.id`: Get the Id of vertices
        - `v.data`: Get the data of vertices
                If there is any, means origin data on the graph, not results.

    - The syntax of selector of edge is (not supported yet):
        - `e.src`: Get the source Id of edges
        - `e.dst`: Get the destination Id of edges
        - `e.data`: Get the edge data on the edges
                If there is any, means origin data on the graph

    - The syntax of selector of results is:
        - `r`: Get quering results of algorithms. e.g. Rankings of vertices after doing PageRank.
    """

    @property
    def context_type(self):
        return "vertex_data"

    def _build_schema(self, result_properties):
        v_items = [["v", "id"], ["v", "data"]]
        r_items = [["r", ""]]
        index = pd.MultiIndex.from_tuples(
            itertools.chain(v_items, r_items), names=["type", "property"]
        )
        v_values = [f"{t}.{p}" for t, p in v_items]
        r_values = [f"{t}" for t, _ in r_items]
        return pd.Series(v_values + r_values, index=index, name="Context schema")

    def _check_selector(self, selector):
        """
        Raises:
            InvalidArgumentError:
                - Selector in vertex data context is None
            SyntaxError:
                - The syntax of selector is incorrect
            NotImplementedError:
                - Selector of e not supported
        """
        if selector is None:
            raise InvalidArgumentError("Selector in vertex data context cannot be None")
        segments = selector.split(".")
        err_msg = f"Invalid selector: `{selector}`. "
        err_msg += (
            "Please inspect the result with `ret.schema` and choose a valid selector."
        )
        if segments[0] == "v":
            if selector not in ("v.id", "v.data"):
                raise SyntaxError(err_msg)
        elif segments[0] == "e":
            raise NotImplementedError("Selector of e is not supported yet")
            if selector not in ("e.src", "e.dst", "e.data"):
                raise SyntaxError(err_msg)
        elif segments[0] == "r":
            if selector != "r":
                raise SyntaxError(err_msg)
        else:
            raise SyntaxError(err_msg)
        return True


class LabeledVertexDataContextDAGNode(BaseContextDAGNode):
    """The labeled kind of context.
    This context has several vertex labels and edge labels, and each label has several properties.
    Selection are performed on labels first, then on properties.

    We use `:` to filter labels, and `.` to select properties. And the results has no property,
    only have labels.

    - The syntax of selector of vertex is:
        - `v:label_name.id`: Get Id that belongs to a specific vertex label.
        - `v:label_name.property_name`: Get data that on a specific property of a specific vertex label.

    - The syntax of selector of edge is (not supported yet):
        - `e:label_name.src`: Get source Id of a specific edge label.
        - `e:label_name.dst`: Get destination Id of a specific edge label.
        - `e:label_name.property_name`: Get data on a specific property of a specific edge label.

    - The syntax of selector of results is:
        - `r:label_name`: Get results data of a vertex label.
    """

    @property
    def context_type(self):
        return "labeled_vertex_data"

    def _build_schema(self, result_properties):
        schema = self._graph.schema
        v_items = [["v"] + item for item in _get_property_v_context_schema(schema)]
        r_items = [["r"] + [label, ""] for label in schema.vertex_labels]
        index = pd.MultiIndex.from_tuples(
            itertools.chain(v_items, r_items), names=["type", "label", "property"]
        )
        v_values = [f"{t}:{l}.{p}" for t, l, p in v_items]
        r_values = [f"{t}:{l}" for t, l, _ in r_items]
        return pd.Series(v_values + r_values, index=index, name="Context schema")

    def _check_selector(self, selector):
        """
        Raises:
            InvalidArgumentError:
                - Selector in labeled vertex data context is None
            SyntaxError:
                - The syntax of selector is incorrect
            NotImplementedError:
                - Selector of e not supported
        """
        if selector is None:
            raise InvalidArgumentError(
                "Selector in labeled vertex data context cannot be None"
            )
        segments = selector.split(":")
        err_msg = f"Invalid selector: `{selector}`. "
        err_msg += (
            "Please inspect the result with `ret.schema` and choose a valid selector."
        )
        if len(segments) != 2:
            raise SyntaxError(err_msg)
        stype, segments = segments[0], segments[1]
        segments = segments.split(".")
        if stype == "v":
            if len(segments) != 2:
                raise SyntaxError(err_msg)
        elif stype == "e":
            raise NotImplementedError("Selector of e not supported yet")
        elif stype == "r":
            if len(segments) != 1:
                raise SyntaxError(err_msg)
        else:
            raise SyntaxError(err_msg)
        return True


class VertexPropertyContextDAGNode(BaseContextDAGNode):
    """The simple kind of context with property.
    A vertex can have multiple values (a.k.a. properties) as results.

    - The syntax of selector on vertex is:
        - `v.id`: Get the Id of vertices
        - `v.data`: Get the data of vertices
            If there is any, means origin data on the graph, not results
        - `v.label_id`: Get the label ID of each vertex.

    - The syntax of selector of edge is (not supported yet):
        - `e.src`: Get the source Id of edges
        - `e.dst`: Get the destination Id of edges
        - `e.data`: Get the edge data on the edges
            If there is any, means origin data on the graph

    - The syntax of selector of results is:
        - `r.column_name`: Get the property named `column_name` in results.
            e.g. `r.hub` in :func:`graphscope.hits`.
    """

    @property
    def context_type(self):
        return "vertex_property"

    def _build_schema(self, result_properties):
        v_items = [["v", "id"], ["v", "data"]]
        r_items = [["r", prop] for prop in result_properties.split(",") if prop]
        index = pd.MultiIndex.from_tuples(
            itertools.chain(v_items, r_items), names=["type", "property"]
        )
        v_values = [f"{t}.{p}" for t, p in v_items]
        r_values = [f"{t}.{p}" for t, p in r_items]
        return pd.Series(v_values + r_values, index=index, name="Context schema")

    def _check_selector(self, selector):
        """
        Raises:
            InvalidArgumentError:
                - Selector in labeled vertex data context is None
            SyntaxError:
                - The syntax of selector is incorrect
            NotImplementedError:
                - Selector of e not supported
        """
        if selector is None:
            raise InvalidArgumentError(
                "Selector in vertex property context cannot be None"
            )
        segments = selector.split(".")
        err_msg = f"Invalid selector: `{selector}`. "
        err_msg += (
            "Please inspect the result with `ret.schema` and choose a valid selector."
        )
        if len(segments) != 2:
            raise SyntaxError(err_msg)
        if segments[0] == "v":
            if selector not in ("v.id", "v.data", "v.label_id"):
                raise SyntaxError(err_msg)
        elif segments[0] == "e":
            raise NotImplementedError("Selector of e not supported yet")
        elif segments[0] == "r":
            # The second part of selector or r is user defined name.
            # So we will allow any str
            pass
        else:
            raise SyntaxError(err_msg)
        return True


class LabeledVertexPropertyContextDAGNode(BaseContextDAGNode):
    """The labeld kind of context with properties.
    This context has several vertex labels and edge labels, And each label has several properties.
    Selection are performed on labels first, then on properties.

    We use `:` to filter labels, and `.` to select properties.
    And the results can have several properties.

    - The syntax of selector of vertex is:
        - `v:label_name.id`: Get Id that belongs to a specific vertex label.
        - `v:label_name.property_name`: Get data that on a specific property of a specific vertex label.

    - The syntax of selector of edge is (not supported yet):
        - `e:label_name.src`: Get source Id of a specific edge label.
        - `e:label_name.dst`: Get destination Id of a specific edge label.
        - `e:label_name.property_name`: Get data on a specific property of a specific edge label.

    - The syntax of selector of results is:
        - `r:label_name.column_name`: Get the property named `column_name` of `label_name`.

    """

    @property
    def context_type(self):
        return "labeled_vertex_property"

    def _build_schema(self, result_properties):
        """Build context schema.

        Args:
            result_properties (str): Returned by c++,
            example_format:
                0:a,b,c,
                1:e,f,g,

        Returns:
            str: return schema as human readable string
        """
        schema = self._graph.schema
        v_items = [["v"] + item for item in _get_property_v_context_schema(schema)]
        r_items = []
        result_properties = [i for i in result_properties.split("\n") if i]
        label_property_dict = {}
        for r_props in result_properties:
            label_id, props = r_props.split(":")
            label_property_dict[label_id] = [i for i in props.split(",") if i]

        for label in schema.vertex_labels:
            label_id = schema.get_vertex_label_id(label)
            props = label_property_dict.get(str(label_id), [])
            r_items.extend([["r", label, prop] for prop in props])

        index = pd.MultiIndex.from_tuples(
            itertools.chain(v_items, r_items), names=["type", "label", "property"]
        )
        v_values = [f"{t}:{l}.{p}" for t, l, p in v_items]
        r_values = [f"{t}:{l}.{p}" for t, l, p in r_items]
        return pd.Series(v_values + r_values, index=index, name="Context schema")

    def _check_selector(self, selector):
        if selector is None:
            raise InvalidArgumentError(
                "Selector in labeled vertex property context cannot be None"
            )
        segments = selector.split(":")
        err_msg = f"Invalid selector: `{selector}`. "
        err_msg += (
            "Please inspect the result with `ret.schema` and choose a valid selector."
        )
        if len(segments) != 2:
            raise SyntaxError(err_msg)
        stype, segments = segments[0], segments[1]
        segments = segments.split(".")
        if stype == "v":
            if len(segments) != 2:
                raise SyntaxError(err_msg)
        elif stype == "e":
            raise NotImplementedError("Selector of e not supported yet")
        elif stype == "r":
            if len(segments) != 2:
                raise SyntaxError(err_msg)
        else:
            raise SyntaxError(err_msg)
        return True


class Context(object):
    """Hold a handle of app querying context.

    After evaluating an app, the context (vertex data, partial results, etc.) are preserved,
    and can be referenced through a handle.
    """

    def __init__(self, context_node, key, result_schema):
        self._context_node = context_node
        self._session = context_node.session
        self._graph = self._context_node._graph
        self._key = key
        self._result_schema = result_schema
        # copy and set op evaluated
        self._context_node.op = deepcopy(self._context_node.op)
        self._context_node.evaluated = True
        self._context_node._unload_op = dag_utils.unload_context(self._context_node)
        self._saved_signature = self.signature

    @property
    def op(self):
        return self._context_node.op

    @property
    def key(self):
        """Unique identifier of a context."""
        return self._key

    @property
    def context_type(self):
        return self._context_node.context_type

    @property
    def schema(self):
        return self._context_node._build_schema(self._result_schema)

    @property
    def signature(self):
        """Compute digest by key and graph signatures.
        Used to ensure the critical information of context is untouched.
        """
        check_argument(
            self._key is not None,
            "Context key error, maybe it is not connected to engine.",
        )
        s = hashlib.sha256()
        s.update(self._key.encode("utf-8", errors="ignore"))
        if not isinstance(self._graph, DAGNode):
            s.update(self._graph.signature.encode("utf-8", errors="ignore"))
        return s.hexdigest()

    def __repr__(self):
        return f"graphscope.framework.context.{self.__class__.__name__} from graph {str(self._graph)}"

    def _check_unmodified(self):
        check_argument(self._saved_signature == self.signature)

    def _check_selector(self, selector):
        return self._context_node._check_selector(selector)

    def to_numpy(self, selector, vertex_range=None, axis=0):
        self._check_unmodified()
        return self._session._wrapper(
            self._context_node.to_numpy(selector, vertex_range, axis)
        )

    def to_dataframe(self, selector, vertex_range=None):
        self._check_unmodified()
        return self._session._wrapper(
            self._context_node.to_dataframe(selector, vertex_range)
        )

    def to_vineyard_tensor(self, selector=None, vertex_range=None, axis=0):
        self._check_unmodified()
        return self._session._wrapper(
            self._context_node.to_vineyard_tensor(selector, vertex_range, axis)
        )

    def to_vineyard_dataframe(self, selector=None, vertex_range=None):
        self._check_unmodified()
        return self._session._wrapper(
            self._context_node.to_vineyard_dataframe(selector, vertex_range)
        )

    def output(self, fd, selector, vertex_range=None, **kwargs):
        """
        Examples:
            context.output('s3://test-bucket/res.csv', selector={'id': 'v.id', 'rank': 'r'},
                           key='access-key', secret='access-secret', client_kwargs={})
            context.output('hdfs:///output/res.csv', selector={'id': 'v.id', 'rank': 'r'},
                           host='localhost', port=9000)
        """
        self._check_unmodified()
        return self._session._wrapper(
            self._context_node.output(fd, selector, vertex_range, **kwargs)
        )

    def output_to_client(self, fd, selector, vertex_range=None):
        """Fetch result to client side"""
        df = self.to_dataframe(selector, vertex_range)
        df.to_csv(fd, header=True, index=False)

    def __del__(self):
        # cleanly ignore all exceptions, cause session may already closed / destroyed.
        try:
            self._session.run(self._unload())
        except Exception:  # pylint: disable=broad-except
            pass

    def _unload(self):
        return self._session._wrapper(self._context_node._unload())


class DynamicVertexDataContext(collections.abc.Mapping):
    """Vertex data context for complicated result store.
    A vertex has a single value as results.
    """

    def __init__(self, context_node, key):
        self._key = key
        self._graph = context_node._graph
        self._session_id = context_node.session_id
        self._saved_signature = self.signature

    @property
    def session_id(self):
        return self._session_id

    @property
    def key(self):
        return self._key

    @property
    def signature(self):
        check_argument(
            self._key is not None,
            "Context key error, maybe it is not connected to engine.",
        )
        return hashlib.sha256(
            "{}.{}".format(self._key, self._graph.signature).encode(
                "utf-8", errors="ignore"
            )
        )

    def __repr__(self):
        return f"graphscope.framework.context.{self.__class__.__name__} from graph {str(self._graph)}"

    def __len__(self):
        return self._graph._graph.number_of_nodes()

    def __getitem__(self, key):
        if key not in self._graph._graph:
            raise KeyError(key)
        op = dag_utils.get_context_data(self, json.dumps(key))
        return dict(json.loads(op.eval()))

    def __iter__(self):
        return iter(self._graph._graph)


def create_context_node(context_type, bound_app, graph, *args, **kwargs):
    """A context DAG node factory, create concrete context class by context type."""
    if context_type == "tensor":
        return TensorContextDAGNode(bound_app, graph, *args, **kwargs)
    if context_type == "vertex_data":
        return VertexDataContextDAGNode(bound_app, graph, *args, **kwargs)
    elif context_type == "labeled_vertex_data":
        return LabeledVertexDataContextDAGNode(bound_app, graph, *args, **kwargs)
    elif context_type == "vertex_property":
        return VertexPropertyContextDAGNode(bound_app, graph, *args, **kwargs)
    elif context_type == "labeled_vertex_property":
        return LabeledVertexPropertyContextDAGNode(bound_app, graph, *args, **kwargs)
    else:
        # dynamic_vertex_data for networkx
        return BaseContextDAGNode(bound_app, graph, *args, **kwargs)


def _get_property_v_context_schema(schema):
    ret = []
    for label in schema.vertex_labels:
        ret.append([label, "id"])
        for prop in schema.get_vertex_properties(label):
            if prop.name != "id":  # avoid property name duplicate
                ret.append([label, prop.name])
    return ret


def _get_property_e_context_schema(schema):
    ret = []
    for label in schema.edge_labels:
        ret.append([label, "src"])
        ret.append([label, "dst"])
        for prop in schema.get_edge_properties(label):
            if prop.name not in ("src", "dst"):
                ret.append([label, prop.name])
    return ret


def _ensure_consistent_label(context_type, selector):
    """Ensure the labels in all selectors are same label.
    Note this method assumes that the selector is valid.
    """
    if context_type in ("vertex_data", "vertex_property"):
        return True
    if context_type in ("labeled_vertex_data", "labeled_vertex_property"):
        label_set = set()
        for _, value in selector.items():
            # The format is x:y or x:y.z
            label = value.split(":")[1].split(".")[0]
            if label_set and label not in label_set:
                raise SyntaxError(
                    f"Found different labels: {label_set.pop()} and {label}."
                )
            else:
                label_set.add(label)
    return True
