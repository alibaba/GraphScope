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
from typing import Mapping

from graphscope.client.session import get_session_by_id
from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.utils import decode_dataframe
from graphscope.framework.utils import decode_numpy


class BaseContext(object):
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
        >>> sg = g.project_to_simple('person', 'knows', 'id', 'weight')
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

    def __init__(self, session_id, context_key, graph):
        self._key = context_key
        self._graph = graph
        self._session_id = session_id
        self._saved_signature = self.signature

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

    def _check_unmodified(self):
        check_argument(self._saved_signature == self.signature)

    @property
    def session_id(self):
        """Return the session id associated with the context."""
        return self._session_id

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
        self._check_unmodified()
        selector = self._transform_selector(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)

        op = dag_utils.context_to_numpy(self, selector, vertex_range, axis)
        raw_values = op.eval()
        return decode_numpy(raw_values)

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
        self._check_unmodified()

        check_argument(
            isinstance(selector, Mapping), "selector of to_dataframe must be a dict"
        )
        selector = {
            key: self._transform_selector(value) for key, value in selector.items()
        }
        selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.context_to_dataframe(self, selector, vertex_range)
        raw_values = op.eval()
        return decode_dataframe(raw_values)

    def to_vineyard_tensor(self, selector=None, vertex_range=None, axis=0):
        """Return results as a vineyard tensor.
        Only object id is returned.

        Returns:
            str: object id of vineyard tensor
        """
        self._check_unmodified()
        selector = self._transform_selector(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)

        op = dag_utils.to_vineyard_tensor(self, selector, vertex_range, axis)
        ret = op.eval()
        object_id = json.loads(ret)["object_id"]
        return object_id

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
        self._check_unmodified()
        if selector is not None:
            check_argument(
                isinstance(selector, Mapping),
                "selector of to_vineyard_dataframe must be a dict",
            )
            selector = {
                key: self._transform_selector(value) for key, value in selector.items()
            }
            selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.to_vineyard_dataframe(self, selector, vertex_range)
        ret = op.eval()
        object_id = json.loads(ret)["object_id"]
        return object_id

    def output(self, fd, selector, vertex_range=None):
        """Dump results to `fd`.
        Support dumps data to local (respect to pod) files, hdfs or oss.
        It first write results to a vineyard dataframe, and let vineyard
        do the data dumping job.
        `fd` must meet specific formats, with auth information if needed. As follows:
            - local
                `file:///tmp/result_path`
            - oss
                `oss://id:key@endpoint/bucket/object`
            - hdfs
                `hdfs://endpoint/result_path`

        Args:
            fd (str): Output location.
            selector (dict): Similar to `to_dataframe`.
            vertex_range (dict, optional): Similar to `to_dataframe`. Defaults to None.
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
            deployment=deployment,
            hosts=hosts,
        )

    def output_to_client(self, fd, selector, vertex_range=None):
        """Fetch result to client side"""
        df = self.to_dataframe(selector, vertex_range)
        df.to_csv(fd, header=True, index=False)


class TensorContext(BaseContext):
    """Tensor context holds a tensor.
    Only axis is meaningful when considering a TensorContext.
    """

    def _transform_selector(self, selector):
        return None


class VertexDataContext(BaseContext):
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


class LabeledVertexDataContext(BaseContext):
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
        return utils.transform_labeled_vertex_data_selector(self._graph, selector)


class VertexPropertyContext(BaseContext):
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


class LabelVertexPropertyContext(BaseContext):
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
            self._graph, selector
        )


def create_context(context_type, session_id, context_key, graph):
    """A context factory, create concrete context class by context_type."""
    if context_type == "tensor":
        return TensorContext(session_id, context_key, graph)
    if context_type == "vertex_data":
        return VertexDataContext(session_id, context_key, graph)
    elif context_type == "labeled_vertex_data":
        return LabeledVertexDataContext(session_id, context_key, graph)
    elif context_type == "vertex_property":
        return VertexPropertyContext(session_id, context_key, graph)
    elif context_type == "labeled_vertex_property":
        return LabelVertexPropertyContext(session_id, context_key, graph)
    else:
        raise InvalidArgumentError("Not supported context type: " + context_type)
