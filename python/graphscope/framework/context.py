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
import json
from copy import deepcopy
from typing import Mapping

from graphscope.client.session import get_session_by_id
from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.dag import DAGNode
from graphscope.framework.dag_utils import run_app
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument


class ResultDAGNode(DAGNode):
    def __init__(self, context, op):
        self._context = context
        self._session = self._context.session
        self._op = op
        # add op to dag
        self._session.dag.add_op(self._op)


class ContextDAGNode(DAGNode):
    """Base class of concrete contexts.
    Hold a handle of app querying context.

    After evaluating an app, the context (vertex data, partial results, etc.) are preserved,
    and can be referenced through a handle.

    We can further use the handle to retrieve data:

    - as a numpy.ndarray( `to_numpy()` ),
    - as a pandas.DataFrame( `to_dataframe()` ),
    - as a vineyard tensor ( `to_vineyard_tensor()` ),
    - or as a vineyard dataframe ( `to_vineyard_dataframe()` ).

    Examples:

    .. code:: python

        >>> g = get_test_property_graph()
        >>> sg = g.project(vertices={'person': ['id']}, edges={'knows': ['weight']})
        >>> ret = sssp(sg, 20)
        >>> out = ret.to_numpy('r')
        >>> out.shape
        (20345,)
        >>> out = ret.to_dataframe({'id': 'v.id', 'result': 'r'})
        >>> out.shape
        (20345, 2)
        >>> out = ret.to_vineyard_tensor()  # return an object id
        >>> out = ret.to_vineyard_dataframe()  # return an object id
    """

    def __init__(self, bound_app, graph, *args, **kwargs):
        self._bound_app = bound_app
        self._graph = graph
        self._session = self._bound_app.session
        # add op to dag
        self._op = run_app(self._bound_app, *args, **kwargs)
        self._session.dag.add_op(self._op)

    def __repr__(self):
        return f"graphscope.{self.__class__.__name__} from graph {str(self._graph)}"

    def _transform_selector(self, selector):
        raise NotImplementedError()

    def to_numpy(self, selector, vertex_range=None, axis=0):
        """Return context data as numpy array

        Args:
            selector (str): Describes how to select values of context.
                See more details in derived context class.
            vertex_range (dict): optional, default to None.
                Works as slicing. The expression {'begin': m, 'end': n} select a portion
                of vertices from `m` to, but not including `n`. Type of `m`, `n` must be identical with vertices'
                oid type.
                Omitting the first index starts the slice at the beginning of the vertices,
                and omitting the second index extends the slice to the end of the vertices.
                Note the comparision is not based on numeric order, but on alphabetic order.
            axis (int): optional, default to 0.

        Returns:
            numpy.ndarray.
        """
        if selector is None:
            raise RuntimeError("selector cannot be None")
        vertex_range = utils.transform_vertex_range(vertex_range)

        op = dag_utils.context_to_numpy(self, selector, vertex_range, axis)
        return ResultDAGNode(self, op)

    def to_dataframe(self, selector, vertex_range=None):
        """Return results as a pandas DataFrame

        Args:
            selector: dict
                The key is column name in dataframe, and the value describes how to select values of context.
                See more details in derived context class.
            vertex_range: dict, optional, default to None.
                Works as slicing. The expression {'begin': m, 'end': n} select a portion
                of vertices from `m` to, but not including `n`. Type of `m`, `n` must be identical with vertices'
                oid type.
                Only the sub-ranges of vertices data will be retrieved.
                Note the comparision is not based on numeric order, but on alphabetic order.

        Returns:
            pandas.DataFrame
        """
        check_argument(
            isinstance(selector, Mapping), "selector of to_dataframe must be a dict"
        )
        selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.context_to_dataframe(self, selector, vertex_range)
        return ResultDAGNode(self, op)

    def to_vineyard_tensor(self, selector=None, vertex_range=None, axis=0):
        """Return results as a vineyard tensor.
        Only object id is returned.

        Returns:
            str: object id of vineyard tensor
        """
        vertex_range = utils.transform_vertex_range(vertex_range)

        op = dag_utils.to_vineyard_tensor(self, selector, vertex_range, axis)
        return ResultDAGNode(self, op)

    def to_vineyard_dataframe(self, selector=None, vertex_range=None):
        """Return results as a vineyard dataframe.
        Only object id is returned.

        Args:
            selector:  dict
                Key is used as column name of the dataframe,
                and the value describes how to select values of context.
                See more details in derived context class.
            vertex_range: dict, optional, default to None
                Works as slicing. The expression {'begin': m, 'end': n} select a portion
                of vertices from `m` to, but not including `n`. Type of `m`, `n` must be identical with vertices'
                oid type.
                Only the sub-ranges of vertices data will be retrieved.

        Returns:
            str: object id of vineyard tensor
        """
        if selector is not None:
            selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.to_vineyard_dataframe(self, selector, vertex_range)
        return ResultDAGNode(self, op)


class Context(collections.abc.Mapping):
    def __init__(self, context_node, key, type):
        self._context_node = context_node
        self._session = context_node.session
        self._session_id = self._session.session_id
        self._graph = self._context_node._graph
        self._key = key
        self._type = type
        # copy and set op evaluated
        self._context_node.op = deepcopy(self._context_node.op)
        self._context_node.evaluated = True
        self._saved_signature = self.signature

    @property
    def session_id(self):
        return self._session_id

    @property
    def op(self):
        return self._context_node.op

    @property
    def key(self):
        """Unique identifier of a context."""
        return self._key

    @property
    def signature(self):
        """Compute digest by key and graph signatures.
        Used to ensure the critical information of context is untouched.
        """
        check_argument(
            self._key is not None,
            "Context key error, maybe it is not connected to engine.",
        )
        return hashlib.sha256(
            "{}.{}".format(self._key, self._graph.signature).encode("utf-8")
        ).hexdigest()

    def __repr__(self):
        return f"graphscope.{self.__class__.__name__} from graph {str(self._graph)}"

    def __len__(self):
        return self._graph._graph.number_of_nodes()

    def __getitem__(self, key):
        if key not in self._graph._graph:
            raise KeyError(key)
        op = dag_utils.get_context_data(self, json.dumps([key]))
        return dict(json.loads(op.eval()))

    def __iter__(self):
        return iter(self._graph._graph)

    def _check_unmodified(self):
        check_argument(self._saved_signature == self.signature)

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
        """
        import vineyard
        import vineyard.io

        df = self.to_vineyard_dataframe(selector, vertex_range)
        sess = get_session_by_id(self.session_id)
        deployment = "kubernetes" if sess.info["type"] == "k8s" else "ssh"
        conf = sess.info["engine_config"]
        vineyard_endpoint = conf["vineyard_rpc_endpoint"]
        vineyard_ipc_socket = conf["vineyard_socket"]
        if sess.info["type"] == "k8s":
            hosts = [
                "{}:{}".format(sess.info["namespace"], s)
                for s in sess.info["engine_hosts"].split(",")
            ]
        else:  # type == "hosts"
            hosts = sess.info["engine_hosts"].split(",")
        # Write vineyard dataframe as a readable stream
        dfstream = vineyard.io.open(
            "vineyard://" + str(df),
            mode="r",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            deployment=deployment,
            hosts=hosts,
        )
        vineyard.io.open(
            fd,
            dfstream,
            mode="w",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            storage_options=kwargs,
            deployment=deployment,
            hosts=hosts,
        )

    def output_to_client(self, fd, selector, vertex_range=None):
        """Fetch result to client side"""
        df = self.to_dataframe(selector, vertex_range)
        df.to_csv(fd, header=True, index=False)


class TensorContext(ContextDAGNode):
    """Tensor context holds a tensor.
    Only axis is meaningful when considering a TensorContext.
    """

    def _transform_selector(self, selector):
        return None


class VertexDataContext(ContextDAGNode):
    """The most simple kind of context.
    A vertex has a single value as results.

    - The syntax of selector on vertex is:
        - `v.id`: Get the Id of vertices
        - `v.data`: Get the data of vertices (If there is any, means origin data on the graph, not results)

    - The syntax of selector of edge is:
        - `e.src`: Get the source Id of edges
        - `e.dst`: Get the destination Id of edges
        - `e.data`: Get the edge data on the edges (If there is any, means origin data on the graph)

    - The syntax of selector of results is:
        - `r`: Get quering results of algorithms. e.g. Rankings of vertices after doing PageRank.
    """

    def _transform_selector(self, selector):
        return utils.transform_vertex_data_selector(selector)


class DynamicVertexDataContext(collections.abc.Mapping):
    """Vertex data context for complicated result store.
    A vertex has a single value as results.
    """

    def __init__(self, session_id, context_key, graph):
        self._key = context_key
        self._graph = graph
        self._session_id = session_id
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
            "{}.{}".format(self._key, self._graph.signature).encode("utf-8")
        )

    def __repr__(self):
        return f"graphscope.{self.__class__.__name__} from graph {str(self._graph)}"

    def __len__(self):
        return self._graph._graph.number_of_nodes()

    def __getitem__(self, key):
        if key not in self._graph._graph:
            raise KeyError(key)
        op = dag_utils.get_context_data(self, json.dumps([key]))
        return dict(json.loads(op.eval()))

    def __iter__(self):
        return iter(self._graph._graph)


class LabeledVertexDataContext(ContextDAGNode):
    """The labeld kind of context.
    This context has several vertex labels and edge labels,
    and each label has several properties.
    Selection are performed on labels first, then on properties.

    We use `:` to filter labels, and `.` to select properties.
    And the results has no property, only have labels.

    - The syntax of selector of vertex is:
        - `v:label_name.id`: Get Id that belongs to a specific vertex label.
        - `v:label_name.property_name`: Get data that on a specific property of a specific vertex label.

    - The syntax of selector of edge is:
        - `e:label_name.src`: Get source Id of a specific edge label.
        - `e:label_name.dst`: Get destination Id of a specific edge label.
        - `e:label_name.property_name`: Get data on a specific property of a specific edge label.

    - The syntax of selector of results is:
        - `r:label_name`: Get results data of a vertex label.
    """

    def _transform_selector(self, selector):
        return utils.transform_labeled_vertex_data_selector(
            self._graph.schema, selector
        )


class VertexPropertyContext(ContextDAGNode):
    """The simple kind of context with property.
    A vertex can have multiple values (a.k.a. properties) as results.

    - The syntax of selector on vertex is:
        - `v.id`: Get the Id of vertices
        - `v.data`: Get the data of vertices (If there is any, means origin data on the graph, not results)

    - The syntax of selector of edge is:
        - `e.src`: Get the source Id of edges
        - `e.dst`: Get the destination Id of edges
        - `e.data`: Get the edge data on the edges (If there is any, means origin data on the graph)

    - The syntax of selector of results is:
        - `r.column_name`: Get the property named `column_name` in results. e.g. `r.hub` in :func:`graphscope.hits`.
    """

    def _transform_selector(self, selector):
        return utils.transform_vertex_property_data_selector(selector)


class LabeledVertexPropertyContext(ContextDAGNode):
    """The labeld kind of context with properties.
    This context has several vertex labels and edge labels,
    And each label has several properties.
    Selection are performed on labels first, then on properties.

    We use `:` to filter labels, and `.` to select properties.
    And the results can have several properties.
    - The syntax of selector of vertex is:
        - `v:label_name.id`: Get Id that belongs to a specific vertex label.
        - `v:label_name.property_name`: Get data that on a specific property of a specific vertex label.

    - The syntax of selector of edge is:
        - `e:label_name.src`: Get source Id of a specific edge label.
        - `e:label_name.dst`: Get destination Id of a specific edge label.
        - `e:label_name.property_name`: Get data on a specific property of a specific edge label.

    - The syntax of selector of results is:
        - `r:label_name.column_name`: Get the property named `column_name` of `label_name`.

    """

    def _transform_selector(self, selector):
        return utils.transform_labeled_vertex_property_data_selector(
            self._graph.schema, selector
        )


def create_context(context_type, session_id, context_key, graph):
    """A context factory, create concrete context class by context_type."""
    if context_type == "tensor":
        return TensorContext(session_id, context_key, graph)
    if context_type == "vertex_data":
        return VertexDataContext(session_id, context_key, graph)
    elif context_type == "labeled_vertex_data":
        return LabeledVertexDataContext(session_id, context_key, graph)
    elif context_type == "dynamic_vertex_data":
        return DynamicVertexDataContext(session_id, context_key, graph)
    elif context_type == "vertex_property":
        return VertexPropertyContext(session_id, context_key, graph)
    elif context_type == "labeled_vertex_property":
        return LabeledVertexPropertyContext(session_id, context_key, graph)
    else:
        raise InvalidArgumentError("Not supported context type: " + context_type)
