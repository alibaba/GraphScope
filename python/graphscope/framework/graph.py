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
import threading
from typing import Mapping

import vineyard

from graphscope.client.session import get_session_by_id
from graphscope.config import GSConfig as gs_config
from graphscope.framework import dag_utils
from graphscope.framework import graph_utils
from graphscope.framework import utils
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.graph_schema import GraphSchema
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
                    - :class:`vineyard.Object`, :class:`vineyard.ObjectId` or :class:`vineyard.ObjectName`
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

        self._interactive_instance_launching_thread = None
        self._interactive_instance_list = []
        self._learning_instance_list = []

        if isinstance(incoming_data, GraphDef):
            graph_def = incoming_data
        elif isinstance(incoming_data, NXGraph):
            graph_def = self._from_nx_graph(incoming_data)
        elif isinstance(incoming_data, Graph):
            graph_def = self._copy_from(incoming_data)
        elif isinstance(
            incoming_data, (vineyard.Object, vineyard.ObjectID, vineyard.ObjectName)
        ):
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
            self._generate_eid = graph_def.generate_eid
            self._schema.get_schema_from_def(graph_def.schema_def)
            self._schema_path = graph_def.schema_path
            # init saved_signature (must be after init schema)
            self._saved_signature = self.signature

            # create gremlin server pod asynchronously
            if gs_config.initializing_interactive_engine:
                self._interactive_instance_launching_thread = threading.Thread(
                    target=self._launch_interactive_instance_impl, args=()
                )
                self._interactive_instance_launching_thread.start()

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

    def _launch_interactive_instance_impl(self):
        try:
            sess = get_session_by_id(self.session_id)
            sess.gremlin(self)
        except:  # noqa: E722
            # Record error msg in `InteractiveQuery` when launching failed.
            # Unexpect and suppress all exceptions here.
            pass

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
    def template_str(self):
        if self._key is None:
            raise RuntimeError("graph should be registered in remote.")
        graph_type = self._graph_type
        # transform str/string to std::string
        oid_type = utils.normalize_data_type_str(self._schema.oid_type)
        vid_type = self._schema.vid_type
        vdata_type = utils.data_type_to_cpp(self._schema.vdata_type)
        edata_type = utils.data_type_to_cpp(self._schema.edata_type)
        if graph_type == types_pb2.ARROW_PROPERTY:
            template = f"vineyard::ArrowFragment<{oid_type},{vid_type}>"
        elif graph_type == types_pb2.ARROW_PROJECTED:
            template = f"gs::ArrowProjectedFragment<{oid_type},{vid_type},{vdata_type},{edata_type}>"
        elif graph_type == types_pb2.DYNAMIC_PROJECTED:
            template = f"gs::DynamicProjectedFragment<{vdata_type},{edata_type}>"
        else:
            raise ValueError(f"Unsupported graph type: {graph_type}")
        return template

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

    def __str__(self):
        return f"graphscope.Graph <{self.template_str}> {self._vineyard_id}"

    def __repr__(self):
        return (
            "graphscope.Graph\n"
            f"type: {self.template_str.split('<')[0]}\n"
            f"vineyard_id: {self._vineyard_id}\n\n"
            f"{str(self._schema)}"
        )

    def unload(self):
        """Unload this graph from graphscope engine."""
        if not self.loaded():
            raise RuntimeError("The graph is not registered in remote.")
        # close interactive instances first
        try:
            if (
                self._interactive_instance_launching_thread is not None
                and self._interactive_instance_launching_thread.is_alive()
            ):
                # join raises a RuntimeError if an attempt is made to join the current thread.
                # this exception occurs when a object collected by gc mechanism contains a running thread.
                if (
                    threading.current_thread()
                    != self._interactive_instance_launching_thread
                ):
                    self._interactive_instance_launching_thread.join()
            self._close_interactive_instances()
        except Exception as e:
            logger.error("Failed to close interactive instances: %s" % e)
        try:
            self._close_learning_instances()
        except Exception as e:
            logger.error("Failed to close learning instances: %s" % e)
        if not self._detached:
            op = dag_utils.unload_graph(self)
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
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        self.check_unmodified()

        def check_out_of_range(id, length):
            if id >= length or id < 0:
                raise IndexError("id {} is out of range.".format(id))

        try:
            if isinstance(v_label, str):
                v_label_id = self._schema.vertex_label_index(v_label)
            else:
                v_label_id = v_label
                check_out_of_range(v_label_id, self._schema.vertex_label_num)
                v_label = self._schema.vertex_labels[v_label_id]
            if isinstance(e_label, str):
                e_label_id = self._schema.edge_label_index(e_label)
            else:
                e_label_id = e_label
                check_out_of_range(e_label_id, self._schema.edge_label_num)
                e_label = self._schema.edge_labels[e_label]
        except ValueError as e:
            raise ValueError("Label does not exists.") from e

        # Check relation v_label -> e_label <- v_label exists.
        relation = (v_label, v_label)
        if relation not in self._schema.edge_relationships[e_label_id]:
            raise ValueError(
                f"Graph doesn't contain such relationship: {v_label} -> {e_label} <- {v_label}."
            )

        try:
            if v_prop is None:
                v_prop_id = -1
                vdata_type = None
            else:
                if isinstance(v_prop, str):
                    v_prop_id = self._schema.vertex_property_index(v_label_id, v_prop)
                else:
                    v_prop_id = v_prop
                properties = self._schema.vertex_properties[v_label_id]
                check_out_of_range(v_prop_id, len(properties))
                vdata_type = list(properties.values())[v_prop_id]
            if e_prop is None:
                e_prop_id = -1
                edata_type = None
            else:
                if isinstance(e_prop, str):
                    e_prop_id = self._schema.edge_property_index(e_label_id, e_prop)
                else:
                    e_prop_id = e_prop
                properties = self._schema.edge_properties[e_label_id]
                check_out_of_range(e_prop_id, len(properties))
                edata_type = list(properties.values())[e_prop_id]
        except ValueError as e:
            raise ValueError("Property does not exists.") from e

        oid_type = self._schema.oid_type
        vid_type = self._schema.vid_type

        op = dag_utils.project_arrow_property_graph(
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
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        self.check_unmodified()
        selector = {
            key: results._transform_selector(value) for key, value in selector.items()
        }
        selector = json.dumps(selector)
        op = dag_utils.add_column(self, results, selector)
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
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        self.check_unmodified()
        selector = utils.transform_labeled_vertex_property_data_selector(self, selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.graph_to_numpy(self, selector, vertex_range)
        ret = op.eval()
        return utils.decode_numpy(ret)

    def to_dataframe(self, selector, vertex_range=None):
        """Select some elements of the graph and output as a pandas.DataFrame

        Args:
            selector (dict): Select some portions of graph.
            vertex_range (dict, optional): Slice vertices. Defaults to None.

        Returns:
            `pandas.DataFrame`
        """
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        self.check_unmodified()
        check_argument(
            isinstance(selector, Mapping),
            "selector of to_vineyard_dataframe must be a dict",
        )
        selector = {
            key: utils.transform_labeled_vertex_property_data_selector(self, value)
            for key, value in selector.items()
        }
        selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)

        op = dag_utils.graph_to_dataframe(self, selector, vertex_range)
        ret = op.eval()
        return utils.decode_dataframe(ret)

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
        op = dag_utils.dynamic_to_arrow(incoming_graph)
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
        op = dag_utils.copy_graph(incoming_graph)
        graph_def = op.eval()
        return graph_def

    def _from_vineyard(self, vineyard_object):
        """Load a graph from a already existed vineyard graph.

        Args:
            vineyard_object (:class:`vineyard.Object`, :class:`vineyard.ObjectID`
                            or :class:`vineyard.ObjectName`): vineyard object,
                            which represents a graph.

        Returns:
            A graph_def.
        """
        if isinstance(vineyard_object, vineyard.Object):
            return self._from_vineyard_id(vineyard_object.id)
        if isinstance(vineyard_object, vineyard.ObjectID):
            return self._from_vineyard_id(vineyard_object)
        if isinstance(vineyard_object, vineyard.ObjectName):
            return self._from_vineyard_name(vineyard_object)

    def _from_vineyard_id(self, vineyard_id):
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(True)
        config[types_pb2.VINEYARD_ID] = utils.i_to_attr(int(vineyard_id))
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrived from vineyard
        config[types_pb2.OID_TYPE] = utils.s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
        op = dag_utils.create_graph(
            self._session_id, types_pb2.ARROW_PROPERTY, attrs=config
        )
        graph_def = op.eval()
        return graph_def

    def _from_vineyard_name(self, vineyard_name):
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(True)
        config[types_pb2.VINEYARD_NAME] = utils.s_to_attr(str(vineyard_name))
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrived from vineyard
        config[types_pb2.OID_TYPE] = utils.s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
        op = dag_utils.create_graph(
            self._session_id, types_pb2.ARROW_PROPERTY, attrs=config
        )
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

    def serialize(self, path, **kwargs):
        """Serialize graph to a location.
        The meta and data of graph is dumped to specified location,
        and can be restored by `Graph.deserialize` in other sessions.

        Each worker will write a `path_{worker_id}.meta` file and
        a `path_{worker_id}` file to storage.
        Args:
            path (str): supported storages are local, hdfs, oss, s3
        """
        import vineyard
        import vineyard.io

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
        vineyard.io.serialize(
            path,
            vineyard.ObjectID(self._vineyard_id),
            type="global",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            storage_options=kwargs,
            deployment=deployment,
            hosts=hosts,
        )

    @classmethod
    def deserialize(cls, path, sess, **kwargs):
        """Construct a `Graph` by deserialize from `path`.
        It will read all serialization files, which is dumped by
        `Graph.serialize`.
        If any serialize file doesn't exists or broken, will error out.

        Args:
            path (str): Path contains the serialization files.
            sess (`graphscope.Session`): The target session
                that the graph will be construct in

        Returns:
            `Graph`: A new graph object. Schema and data is supposed to be
                identical with the one that called serialized method.
        """
        import vineyard
        import vineyard.io

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
        graph_id = vineyard.io.deserialize(
            path,
            type="global",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            storage_options=kwargs,
            deployment=deployment,
            hosts=hosts,
        )
        return cls(sess.session_id, vineyard.ObjectID(graph_id))

    def draw(self, vertices, hop=1):
        """Visualize the graph data in the result cell when the draw functions are invoked

        Args:
            vertices (list): selected vertices.
            hop (int): draw induced subgraph with hop extension. Defaults to 1.

        Returns:
            A GraphModel.
        """
        from ipygraphin import GraphModel

        sess = get_session_by_id(self.session_id)
        interactive_query = sess.gremlin(self)

        graph = GraphModel()
        graph.queryGraphData(vertices, hop, interactive_query)

        # listen on the 1~2 hops operation of node
        graph.on_msg(graph.queryNeighbor)
        return graph

    def add_vertices(self, vertices):
        vertices = graph_utils.normalize_parameter_vertices(vertices)
        # Configurations inherited from input graph
        # oid_type
        # CHECK label name not in existed edge labels
        vertex_labels = self._schema.vertex_labels
        for vertex in vertices:
            check_argument(
                vertex.label not in vertex_labels,
                f"Duplicate label name with existing vertex labels: {vertex.label}",
            )

        config = graph_utils.assemble_op_config(
            [], vertices, self._directed, self._schema.oid_type, self._generate_eid
        )
        op = dag_utils.add_vertices(self, attrs=config)
        graph_def = op.eval()
        return Graph(self.session_id, graph_def)

    def add_edges(self, edges):
        edges = graph_utils.normalize_parameter_edges(edges)
        # directed, oid_type, generate_eid
        # CHECK:
        # 1. edge's src/dst labels must existed in vertex_labels
        # 2. label name not in existed edge labels
        vertex_labels = self._schema.vertex_labels
        edge_labels = self.schema.edge_labels
        graph_utils.check_edge_validity(edges, vertex_labels)
        for edge in edges:
            check_argument(
                edge.label not in edge_labels,
                f"Duplicate label name with existing edge labels: {edge.label}",
            )

        config = graph_utils.assemble_op_config(
            edges, [], self._directed, self._schema.oid_type, self._generate_eid
        )
        op = dag_utils.add_edges(self, attrs=config)
        graph_def = op.eval()
        return Graph(self.session_id, graph_def)
