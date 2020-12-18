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
import logging
from typing import Mapping

from graphscope.framework.dag_utils import add_column
from graphscope.framework.dag_utils import copy_graph
from graphscope.framework.dag_utils import create_graph
from graphscope.framework.dag_utils import dynamic_to_arrow
from graphscope.framework.dag_utils import graph_to_dataframe
from graphscope.framework.dag_utils import graph_to_numpy
from graphscope.framework.dag_utils import project_arrow_property_graph
from graphscope.framework.dag_utils import unload_graph
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.utils import b_to_attr
from graphscope.framework.utils import decode_dataframe
from graphscope.framework.utils import decode_numpy
from graphscope.framework.utils import i_to_attr
from graphscope.framework.utils import s_to_attr
from graphscope.framework.utils import transform_labeled_vertex_property_data_selector
from graphscope.framework.utils import transform_vertex_range
from graphscope.framework.vineyard_object import VineyardObject
from graphscope.proto import types_pb2
from graphscope.proto.graph_def_pb2 import GraphDef

logger = logging.getLogger("graphscope")


class Graph(object):
    """A class for representing metadata of a graph in the GraphScope.

    A :class:`Graph` object holds the metadata of a graph, such as key, schema, and the graph is directed or not.

    It is worth noting that the graph is stored by the backend such as Analytical Engine, Vineyard.
    In other words, the graph object holds nothing but metadata.

    The graph object should not be created directly from :class:`Graph`.
    Instead, the graph should be created by `Session.load_from`

    The following example demonstrates its usage:

    .. code:: python

        >>> import graphscope as gs
        >>> from graphscope.framework.loader import Loader
        >>> sess = gs.session()
        >>> g = sess.load_from(
        ...     edges={
        ...         "knows": (
        ...             Loader("{}/p2p-31_property_e_0".format(property_dir), header_row=True),
        ...             ["src_label_id", "dst_label_id", "dist"],
        ...             ("src_id", "person"),
        ...             ("dst_id", "person"),
        ...         ),
        ...     },
        ...     vertices={
        ...         "person": Loader(
        ...             "{}/p2p-31_property_v_0".format(property_dir), header_row=True
        ...         ),
        ...     }
        ... )
    """

    def __init__(self, session_id, incoming_data=None):
        """Construct a :class:`Graph` object.

        Args:
            session_id (str): Session id of the session the graph is created in.
            incoming_data: Graph can be initialized through various type of sources,
                which can be one of:
                    - :class:`GraphDef`
                    - :class:`nx.Graph`
                    - :class:`Graph`
                    - :class:`VineyardObject`
        """

        # Don't import the :code:`NXGraph` in top-level statments to improve the
        # performance of :code:`import graphscope`.
        from graphscope.experimental.nx.classes.graph import Graph as NXGraph

        self._key = None
        self._op = None
        self._graph_type = None
        self.directed = False
        self._vineyard_id = 0
        self._schema = GraphSchema()

        self._session_id = session_id
        self._detached = False

        self._interactive_instance_list = []
        self._learning_instance_list = []

        if isinstance(incoming_data, GraphDef):
            graph_def = incoming_data
        elif isinstance(incoming_data, NXGraph):
            graph_def = self._from_nx_graph(incoming_data)
        elif isinstance(incoming_data, Graph):
            graph_def = self._copy_from(incoming_data)
        elif isinstance(incoming_data, VineyardObject):
            graph_def = self._from_vineyard(incoming_data)
        else:
            raise ValueError(
                "Failed to create a graph on graphscope engine: %s", incoming_data
            )

        if graph_def:
            self._key = graph_def.key
            self._vineyard_id = graph_def.vineyard_id
            self._graph_type = graph_def.graph_type
            self._directed = graph_def.directed
            self._schema.get_schema_from_def(graph_def.schema_def)
            self._schema_path = graph_def.schema_path
            # init saved_signature (must be after init schema)
            self._saved_signature = self.signature

    def __del__(self):
        # cleanly ignore all exceptions, cause session may already closed / destroyed.
        try:
            self.unload()
        except Exception:  # pylint: disable=broad-except
            pass

    def _close_interactive_instances(self):
        # Close related interactive instances when graph unloaded.
        # Since the graph is gone, quering via interactive client is meaningless.
        for instance in self._interactive_instance_list:
            instance.close()
        self._interactive_instance_list.clear()

    def _close_learning_instances(self):
        for instance in self._learning_instance_list:
            instance.close()
        self._learning_instance_list.clear()

    @property
    def op(self):
        """The DAG op of this graph."""
        return self._op

    @property
    def key(self):
        """The key of the corresponding graph in engine."""
        return self._key

    @property
    def graph_type(self):
        """The type of the graph object.

        Returns:
            type (`types_pb2.GraphType`): the type of the graph.
        """
        return self._graph_type

    @property
    def schema(self):
        """Schema of the graph.

        Returns:
            :class:`GraphSchema`: the schema of the graph
        """
        return self._schema

    @property
    def schema_path(self):
        """Path that Coordinator will write interactive schema path to.

        Returns:
            str: The path contains the schema. for interactive engine.
        """
        return self._schema_path

    @property
    def signature(self):
        if self._key is None:
            raise RuntimeError("graph should be registered in remote.")
        return hashlib.sha256(
            "{}.{}".format(self._schema.signature(), self._key).encode("utf-8")
        ).hexdigest()

    @property
    def template_sigature(self):
        if self._key is None:
            raise RuntimeError("graph should be registered in remote.")
        return hashlib.sha256(
            "{}.{}.{}.{}.{}".format(
                self._graph_type,
                self._schema.oid_type,
                self._schema.vid_type,
                self._schema.vdata_type,
                self._schema.edata_type,
            ).encode("utf-8")
        ).hexdigest()

    @property
    def vineyard_id(self):
        """Get the vineyard object_id of this graph.

        Returns:
            str: return vineyard id of this graph
        """
        return self._vineyard_id

    @property
    def session_id(self):
        """Get the currrent session_id.

        Returns:
            str: Return session id that the graph belongs to.
        """
        return self._session_id

    def detach(self):
        """Detaching a graph makes it being left in vineyard even when the varaible for
        this :class:`Graph` object leaves the lexical scope.

        The graph can be accessed using the graph's :code:`ObjectID` or its name later.
        """
        self._detached = True

    def loaded(self):
        return self._key is not None

    def __repr__(self):
        return "<grape.Graph '%s'>" % self._key

    def unload(self):
        """Unload this graph from graphscope engine."""
        if not self.loaded():
            raise RuntimeError("The graph is not registered in remote.")
        # close interactive instances first
        try:
            self._close_interactive_instances()
        except Exception as e:
            logger.error("Failed to close interactive instances: %s" % e)
        try:
            self._close_learning_instances()
        except Exception as e:
            logger.error("Failed to close learning instances: %s" % e)
        if not self._detached:
            op = unload_graph(self)
            op.eval()
        self._key = None

    def project_to_simple(self, v_label="_", e_label="_", v_prop=None, e_prop=None):
        """Project a property graph to a simple graph, useful for analytical engine.
        Will translate name represented label or property to index, which is broadedly used
        in internal engine.

        Args:
            v_label (str, optional): vertex label to project. Defaults to "_".
            e_label (str, optional): edge label to project. Defaults to "_".
            v_prop (str, optional): vertex property of the v_label. Defaults to None.
            e_prop (str, optional): edge property of the e_label. Defaults to None.

        Returns:
            :class:`Graph`: A `Graph` instance, which graph_type is `ARROW_PROJECTED`
        """
        if not self.loaded():
            raise RuntimeError(
                "The graph is not registered in remote, and can't project to simple"
            )
        self.check_unmodified()
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        check_argument(isinstance(v_label, (int, str)))
        check_argument(isinstance(e_label, (int, str)))

        def check_out_of_range(id, length):
            if id < length and id > -1:
                return id
            else:
                raise KeyError("id {} is out of range.".format(id))

        try:
            v_label_id = (
                check_out_of_range(v_label, self._schema.vertex_label_num)
                if isinstance(v_label, int)
                else self._schema.vertex_label_index(v_label)
            )
        except ValueError as e:
            raise ValueError(
                "graph not contains the vertex label {}.".format(v_label)
            ) from e

        try:
            e_label_id = (
                check_out_of_range(e_label, self._schema.edge_label_num)
                if isinstance(e_label, int)
                else self._schema.edge_label_index(e_label)
            )
        except ValueError as e:
            raise InvalidArgumentError(
                "graph not contains the edge label {}.".format(e_label)
            ) from e

        if v_prop is None:
            # NB: -1 means vertex property is None
            v_prop_id = -1
            v_properties = None
        else:
            check_argument(isinstance(v_prop, (int, str)))
            v_properties = self._schema.vertex_properties[v_label_id]
            try:
                v_prop_id = (
                    check_out_of_range(v_prop, len(v_properties))
                    if isinstance(v_prop, int)
                    else self._schema.vertex_property_index(v_label_id, v_prop)
                )
            except ValueError as e:
                raise ValueError(
                    "vertex label {} not contains the property {}".format(
                        v_label, v_prop
                    )
                ) from e

        if e_prop is None:
            # NB: -1 means edge property is None
            e_prop_id = -1
            e_properties = None
        else:
            check_argument(isinstance(e_prop, (int, str)))
            e_properties = self._schema.edge_properties[e_label_id]
            try:
                e_prop_id = (
                    check_out_of_range(e_prop, len(e_properties))
                    if isinstance(e_prop, int)
                    else self._schema.edge_property_index(e_label_id, e_prop)
                )
            except ValueError as e:
                raise ValueError(
                    "edge label {} not contains the property {}".format(e_label, e_prop)
                ) from e

        oid_type = self._schema.oid_type
        vid_type = self._schema.vid_type
        vdata_type = None
        if v_properties:
            vdata_type = list(v_properties.values())[v_prop_id]
        edata_type = None
        if e_properties:
            edata_type = list(e_properties.values())[e_prop_id]

        op = project_arrow_property_graph(
            self,
            v_label_id,
            v_prop_id,
            e_label_id,
            e_prop_id,
            vdata_type,
            edata_type,
            oid_type,
            vid_type,
        )
        graph_def = op.eval()
        return Graph(self.session_id, graph_def)

    def add_column(self, results, selector):
        """Add the results as a column to the graph. Modification rules are given by the selector.

        Args:
            results (:class:`Context`): A `Context` that created by doing a query.
            selector (dict): Select results to add as column. Format is similar to selectors in `Context`

        Returns:
            :class:`Graph`: A new `Graph` with new columns.
        """
        check_argument(
            isinstance(selector, Mapping), "selector of add column must be a dict"
        )
        self.check_unmodified()
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        selector = {
            key: results._transform_selector(value) for key, value in selector.items()
        }
        selector = json.dumps(selector)
        op = add_column(self, results, selector)
        graph_def = op.eval()
        return Graph(self.session_id, graph_def)

    def to_numpy(self, selector, vertex_range=None):
        """Select some elements of the graph and output to numpy.

        Args:
            selector (str): Select a portion of graph as a numpy.ndarray.
            vertex_range(dict, optional): Slice vertices. Defaults to None.
        Returns:
            `numpy.ndarray`
        """
        self.check_unmodified()
        selector = transform_labeled_vertex_property_data_selector(self, selector)
        vertex_range = transform_vertex_range(vertex_range)
        op = graph_to_numpy(self, selector, vertex_range)
        ret = op.eval()
        return decode_numpy(ret)

    def to_dataframe(self, selector, vertex_range=None):
        """Select some elements of the graph and output as a pandas.DataFrame

        Args:
            selector (dict): Select some portions of graph.
            vertex_range (dict, optional): Slice vertices. Defaults to None.

        Returns:
            `pandas.DataFrame`
        """
        self.check_unmodified()
        check_argument(
            isinstance(selector, Mapping),
            "selector of to_vineyard_dataframe must be a dict",
        )
        selector = {
            key: transform_labeled_vertex_property_data_selector(self, value)
            for key, value in selector.items()
        }
        selector = json.dumps(selector)
        vertex_range = transform_vertex_range(vertex_range)

        op = graph_to_dataframe(self, selector, vertex_range)
        ret = op.eval()
        return decode_dataframe(ret)

    def is_directed(self):
        return self._directed

    def check_unmodified(self):
        check_argument(
            self.signature == self._saved_signature, "Graph has been modified!"
        )

    def _from_nx_graph(self, incoming_graph):
        """Create a gs graph from a nx graph.
        Args:
            incoming_graph (:class:`nx.graph`): A nx graph that contains graph data.

        Returns:
            that will be used to construct a gs.Graph

        Raises:
            TypeError: Raise Error if graph type not match.

        Examples:
            >>> nx_g = nx.path_graph(10)
            >>> gs_g = gs.Graph(nx_g)
        """
        if hasattr(incoming_graph, "_graph"):
            msg = "graph view can not convert to gs graph"
            raise TypeError(msg)
        op = dynamic_to_arrow(incoming_graph)
        graph_def = op.eval()
        return graph_def

    def _copy_from(self, incoming_graph):
        """Copy a graph.

        Args:
            incoming_graph (:class:`Graph`): Source graph to be copied from

        Returns:
            :class:`Graph`: An identical graph, but with a new vineyard id.
        """
        check_argument(incoming_graph.graph_type == types_pb2.ARROW_PROPERTY)
        check_argument(incoming_graph.loaded())
        op = copy_graph(incoming_graph)
        graph_def = op.eval()
        return graph_def

    def _from_vineyard(self, vineyard_object):
        """Load a graph from a already existed vineyard graph.

        Args:
            vineyard_object (:class:`VineyardObject`): vineyard object, which contains a graph.

        Returns:
            A graph_def.
        """
        if vineyard_object.object_id is not None:
            return self._from_vineyard_id(vineyard_object.object_id)
        elif vineyard_object.object_name is not None:
            return self._from_vineyard_name(vineyard_object.object_name)

    def _from_vineyard_id(self, vineyard_id):
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = b_to_attr(True)
        config[types_pb2.VINEYARD_ID] = i_to_attr(vineyard_id)
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrived from vineyard
        config[types_pb2.OID_TYPE] = s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = s_to_attr("uint64_t")
        op = create_graph(self._session_id, types_pb2.ARROW_PROPERTY, attrs=config)
        graph_def = op.eval()
        return graph_def

    def _from_vineyard_name(self, vineyard_name):
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = b_to_attr(True)
        config[types_pb2.VINEYARD_NAME] = s_to_attr(vineyard_name)
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrived from vineyard
        config[types_pb2.OID_TYPE] = s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = s_to_attr("uint64_t")
        op = create_graph(self._session_id, types_pb2.ARROW_PROPERTY, attrs=config)
        graph_def = op.eval()
        return graph_def

    def attach_interactive_instance(self, instance):
        """Store the instance when a new interactive instance is started.

        Args:
            instance: interactive instance
        """
        self._interactive_instance_list.append(instance)

    def attach_learning_instance(self, instance):
        """Store the instance when a new learning instance is created.

        Args:
            instance: learning instance
        """
        self._learning_instance_list.append(instance)
