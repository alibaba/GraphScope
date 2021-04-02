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
from copy import deepcopy
from typing import List
from typing import Mapping
from typing import Union

import vineyard

from graphscope.config import GSConfig as gs_config
from graphscope.framework import dag_utils
from graphscope.framework import graph_utils
from graphscope.framework import utils
from graphscope.framework.errors import check_argument
from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.graph_utils import EdgeLabel
from graphscope.framework.graph_utils import EdgeSubLabel
from graphscope.framework.graph_utils import VertexLabel
from graphscope.framework.operation import Operation
from graphscope.proto import attr_value_pb2
from graphscope.proto import types_pb2

logger = logging.getLogger("graphscope")


class Graph(object):
    """A class for representing metadata of a graph in the GraphScope.

    A :class:`Graph` object holds the metadata of a graph, such as key, schema, and the graph is directed or not.

    It is worth noting that the graph is stored by the backend such as Analytical Engine, Vineyard.
    In other words, the graph object holds nothing but metadata.

    The following example demonstrates its usage:

    .. code:: python

        >>> import graphscope as gs
        >>> from graphscope.framework.loader import Loader
        >>> sess = gs.session()
        >>> graph = sess.g()
        >>> graph = graph.add_vertices("person.csv","person")
        >>> graph = graph.add_vertices("software.csv", "software")
        >>> graph = graph.add_edges("knows.csv", "knows", src_label="person", dst_label="person")
        >>> graph = graph.add_edges("created.csv", "created", src_label="person", dst_label="software")
        >>> print(graph)
        >>> print(graph.schema)
    """

    def __init__(
        self,
        session,
        incoming_data=None,
        oid_type="int64",
        directed=True,
        generate_eid=True,
    ):
        """Construct a :class:`Graph` object.

        Args:
            session_id (str): Session id of the session the graph is created in.
            incoming_data: Graph can be initialized through various type of sources,
                which can be one of:

                    - :class:`Operation`
                    - :class:`nx.Graph`
                    - :class:`Graph`
                    - :class:`vineyard.Object`, :class:`vineyard.ObjectId` or :class:`vineyard.ObjectName`
        """

        self._key = None
        self._graph_type = types_pb2.ARROW_PROPERTY
        self._vineyard_id = 0
        self._schema = GraphSchema()
        self._session = session
        self._detached = False

        self._interactive_instance_launching_thread = None
        self._interactive_instance_list = []
        self._learning_instance_list = []

        # Hold uncompleted operation for lazy evaluation
        self._pending_op = None
        # Hold a reference to base graph of modify operation,
        # to avoid being garbage collected
        self._base_graph = None

        oid_type = utils.normalize_data_type_str(oid_type)
        if oid_type not in ("int64_t", "std::string"):
            raise ValueError("oid_type can only be int64_t or string.")
        self._oid_type = oid_type
        self._directed = directed
        self._generate_eid = generate_eid

        self._unsealed_vertices = {}
        self._unsealed_edges = {}
        # Used to isplay schema without load into vineyard,
        # and do sanity checking for newly added vertices and edges.
        self._v_labels = []
        self._e_labels = []
        self._e_relationships = []

        if incoming_data is not None:
            # Don't import the :code:`NXGraph` in top-level statements to improve the
            # performance of :code:`import graphscope`.
            from graphscope.experimental import nx

            if isinstance(incoming_data, Operation):
                self._pending_op = incoming_data
                if self._pending_op.type == types_pb2.PROJECT_TO_SIMPLE:
                    self._graph_type = types_pb2.ARROW_PROJECTED
            elif isinstance(incoming_data, nx.Graph):
                self._pending_op = self._from_nx_graph(incoming_data)
            elif isinstance(incoming_data, Graph):
                self._pending_op = self._copy_from(incoming_data)
            elif isinstance(
                incoming_data, (vineyard.Object, vineyard.ObjectID, vineyard.ObjectName)
            ):
                self._pending_op = self._from_vineyard(incoming_data)
            else:
                raise RuntimeError("Not supported incoming data.")

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
            self._session.gremlin(self)
        except:  # noqa: E722
            # Record error msg in `InteractiveQuery` when launching failed.
            # Unexpect and suppress all exceptions here.
            pass

    def _from_graph_def(self, graph_def):
        check_argument(
            self._graph_type == graph_def.graph_type, "Graph type doesn't match."
        )

        self._key = graph_def.key
        self._vineyard_id = graph_def.vineyard_id
        self._oid_type = graph_def.schema_def.oid_type
        self._directed = graph_def.directed
        self._generate_eid = graph_def.generate_eid

        self._schema_path = graph_def.schema_path
        self._schema.get_schema_from_def(graph_def.schema_def)
        self._v_labels = self._schema.vertex_labels
        self._e_labels = self._schema.edge_labels
        self._e_relationships = self._schema.edge_relationships

    def _ensure_loaded(self):
        if self._key is not None and self._pending_op is None:
            return
        # Unloaded
        if self._session is None:
            raise RuntimeError("The graph is not loaded")
        # Empty graph
        if self._key is None and self._pending_op is None:
            raise RuntimeError("Empty graph.")
        # Try to load
        if self._pending_op is not None:
            # Create a graph from scratch.
            graph_def = self._pending_op.eval()
            self._from_graph_def(graph_def)
            self._pending_op = None
            self._base_graph = None
            self._unsealed_vertices.clear()
            self._unsealed_edges.clear()
            # init saved_signature (must be after init schema)
            self._saved_signature = self.signature
            # create gremlin server pod asynchronously
            if gs_config.initializing_interactive_engine:
                self._interactive_instance_launching_thread = threading.Thread(
                    target=self._launch_interactive_instance_impl, args=()
                )
                self._interactive_instance_launching_thread.start()

    @property
    def key(self):
        """The key of the corresponding graph in engine."""
        self._ensure_loaded()
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
        self._ensure_loaded()
        return self._schema

    @property
    def schema_path(self):
        """Path that Coordinator will write interactive schema path to.

        Returns:
            str: The path contains the schema. for interactive engine.
        """
        self._ensure_loaded()
        return self._schema_path

    @property
    def signature(self):
        self._ensure_loaded()
        return hashlib.sha256(
            "{}.{}".format(self._schema.signature(), self._key).encode("utf-8")
        ).hexdigest()

    @property
    def template_str(self):
        self._ensure_loaded()

        # transform str/string to std::string
        oid_type = utils.normalize_data_type_str(self._oid_type)
        vid_type = self._schema.vid_type
        vdata_type = utils.data_type_to_cpp(self._schema.vdata_type)
        edata_type = utils.data_type_to_cpp(self._schema.edata_type)
        if self._graph_type == types_pb2.ARROW_PROPERTY:
            template = f"vineyard::ArrowFragment<{oid_type},{vid_type}>"
        elif self._graph_type == types_pb2.ARROW_PROJECTED:
            template = f"gs::ArrowProjectedFragment<{oid_type},{vid_type},{vdata_type},{edata_type}>"
        elif self._graph_type == types_pb2.DYNAMIC_PROJECTED:
            template = f"gs::DynamicProjectedFragment<{vdata_type},{edata_type}>"
        else:
            raise ValueError(f"Unsupported graph type: {self._graph_type}")
        return template

    @property
    def vineyard_id(self):
        """Get the vineyard object_id of this graph.

        Returns:
            str: return vineyard id of this graph
        """
        self._ensure_loaded()
        return self._vineyard_id

    @property
    def session_id(self):
        """Get the currrent session_id.

        Returns:
            str: Return session id that the graph belongs to.
        """
        return self._session.session_id

    def detach(self):
        """Detaching a graph makes it being left in vineyard even when the varaible for
        this :class:`Graph` object leaves the lexical scope.

        The graph can be accessed using the graph's :code:`ObjectID` or its name later.
        """
        self._detached = True

    def loaded(self):
        try:
            self._ensure_loaded()
        except RuntimeError:
            return False
        return self._key is not None

    def __str__(self):
        v_str = "\n".join([f"VERTEX: {label}" for label in self._v_labels])
        relations = []
        for i in range(len(self._e_labels)):
            relations.extend(
                [(self._e_labels[i], src, dst) for src, dst in self._e_relationships[i]]
            )
        e_str = "\n".join(
            [f"EDGE: {label}\tsrc: {src}\tdst: {dst}" for label, src, dst in relations]
        )

        return f"graphscope.Graph\n{types_pb2.GraphType.Name(self._graph_type)}\n{v_str}\n{e_str}"

    def __repr__(self):
        return self.__str__()

    def unload(self):
        """Unload this graph from graphscope engine."""
        if self._session is None:
            raise RuntimeError("The graph is not loaded")

        if self._key is None:
            self._session = None
            self._pending_op = None
            return

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
        self._session = None
        self._pending_op = None

    def _project_to_simple(self):
        self._ensure_loaded()
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        check_argument(
            self.schema.vertex_label_num == 1,
            "Cannot project to simple, vertex label number is more than 1.",
        )
        check_argument(
            self.schema.edge_label_num == 1,
            "Cannot project to simple, edge label number is more than 1.",
        )
        # Check relation v_label -> e_label <- v_label exists.
        v_label = self.schema.vertex_labels[0]
        e_label = self.schema.edge_labels[0]
        relation = (v_label, v_label)
        check_argument(
            relation in self._schema.get_relationships(e_label),
            f"Cannot project to simple, Graph doesn't contain such relationship: {v_label} -> {e_label} <- {v_label}.",
        )
        v_props = self.schema.get_vertex_properties(v_label)
        e_props = self.schema.get_edge_properties(e_label)
        check_argument(len(v_props) <= 1)
        check_argument(len(e_props) <= 1)

        v_label_id = self.schema.get_vertex_label_id(v_label)
        e_label_id = self.schema.get_edge_label_id(e_label)
        v_prop_id, vdata_type = (
            (v_props[0].id, v_props[0].type) if v_props else (-1, None)
        )
        e_prop_id, edata_type = (
            (e_props[0].id, e_props[0].type) if e_props else (-1, None)
        )
        oid_type = self._schema.oid_type
        vid_type = self._schema.vid_type

        op = dag_utils.project_arrow_property_graph_to_simple(
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
        graph = Graph(self._session, op)
        graph._base_graph = self
        return graph

    def add_column(self, results, selector):
        """Add the results as a column to the graph. Modification rules are given by the selector.

        Args:
            results (:class:`Context`): A `Context` that created by doing a query.
            selector (dict): Select results to add as column. Format is similar to selectors in `Context`

        Returns:
            :class:`Graph`: A new `Graph` with new columns.
        """
        self._ensure_loaded()
        check_argument(
            isinstance(selector, Mapping), "selector of add column must be a dict"
        )
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        self._check_unmodified()
        selector = {
            key: results._transform_selector(value) for key, value in selector.items()
        }
        selector = json.dumps(selector)
        op = dag_utils.add_column(self, results, selector)
        graph = Graph(self._session, op)
        graph._base_graph = self
        return graph

    def to_numpy(self, selector, vertex_range=None):
        """Select some elements of the graph and output to numpy.

        Args:
            selector (str): Select a portion of graph as a numpy.ndarray.
            vertex_range(dict, optional): Slice vertices. Defaults to None.
        Returns:
            `numpy.ndarray`
        """
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)
        self._ensure_loaded()
        self._check_unmodified()
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
        self._ensure_loaded()
        self._check_unmodified()
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
        self._ensure_loaded()
        return self._directed

    def _check_unmodified(self):
        self._ensure_loaded()
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
        return dag_utils.dynamic_to_arrow(incoming_graph)

    def _copy_from(self, incoming_graph):
        """Copy a graph.

        Args:
            incoming_graph (:class:`Graph`): Source graph to be copied from

        Returns:
            :class:`Graph`: An identical graph, but with a new vineyard id.
        """
        check_argument(incoming_graph.graph_type == types_pb2.ARROW_PROPERTY)
        check_argument(incoming_graph.loaded())
        return dag_utils.copy_graph(incoming_graph)

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
        return dag_utils.create_graph(
            self.session_id, types_pb2.ARROW_PROPERTY, attrs=config
        )

    def _from_vineyard_name(self, vineyard_name):
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(True)
        config[types_pb2.VINEYARD_NAME] = utils.s_to_attr(str(vineyard_name))
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrived from vineyard
        config[types_pb2.OID_TYPE] = utils.s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
        return dag_utils.create_graph(
            self.session_id, types_pb2.ARROW_PROPERTY, attrs=config
        )

    def _attach_interactive_instance(self, instance):
        """Store the instance when a new interactive instance is started.

        Args:
            instance: interactive instance
        """
        self._interactive_instance_list.append(instance)

    def _attach_learning_instance(self, instance):
        """Store the instance when a new learning instance is created.

        Args:
            instance: learning instance
        """
        self._learning_instance_list.append(instance)

    def save_to(self, path, **kwargs):
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

        self._ensure_loaded()
        sess = self._session
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
    def load_from(cls, path, sess, **kwargs):
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
        return cls(sess, vineyard.ObjectID(graph_id))

    def _construct_graph(
        self, vertices, edges, v_labels, e_labels, e_relations, mutation_func=None
    ):
        """Construct graph.
           1. Construct a graph from scratch.
              If the vertices and edges is empty, return a empty graph.
           2. Construct a graph from existed builded graph.
              If the vertices and edges is empty, return a copied graph.

        Args:
            vertices ([type]): [description]
            edges ([type]): [description]
            v_labels ([type]): [description]
            e_labels ([type]): [description]
            e_relations ([type]): [description]
            mutation_func ([type], optional): [description]. Defaults to None.

        Returns:
            [type]: [description]
        """
        config = graph_utils.assemble_op_config(
            vertices.values(),
            edges.values(),
            self._oid_type,
            self._directed,
            self._generate_eid,
        )

        # edge case.
        if not vertices and not edges:
            if mutation_func:
                # Rely on `self._key`
                return Graph(self._session, self)
            else:
                return Graph(
                    self._session,
                    None,
                    self._oid_type,
                    self._directed,
                    self._generate_eid,
                )
        if mutation_func:
            op = mutation_func(self, attrs=config)
        else:
            op = dag_utils.create_graph(
                self.session_id, types_pb2.ARROW_PROPERTY, attrs=config
            )

        graph = Graph(
            self._session, op, self._oid_type, self._directed, self._generate_eid
        )
        graph._unsealed_vertices = vertices
        graph._unsealed_edges = edges
        graph._v_labels = v_labels
        graph._e_labels = e_labels
        graph._e_relationships = e_relations
        # propage info about whether is a loaded graph.
        # graph._key = self._key
        if mutation_func:
            graph._base_graph = self._base_graph or self
        return graph

    def add_vertices(self, vertices, label="_", properties=[], vid_field=0):
        is_from_existed_graph = len(self._unsealed_vertices) != len(
            self._v_labels
        ) or len(self._unsealed_edges) != len(self._e_labels)

        if label in self._v_labels:
            raise ValueError(f"Label {label} already existed in graph.")
        if not self._v_labels and self._e_labels:
            raise ValueError("Cannot manually add vertices after inferred vertices.")
        unsealed_vertices = deepcopy(self._unsealed_vertices)
        unsealed_vertices[label] = VertexLabel(
            label=label, loader=vertices, properties=properties, vid_field=vid_field
        )
        v_labels = deepcopy(self._v_labels)
        v_labels.append(label)

        # Load after validity check and before create add_vertices op.
        # TODO(zsy): Add ability to add vertices and edges to existed graph simultaneously.
        if is_from_existed_graph and self._unsealed_edges:
            self._ensure_loaded()

        func = dag_utils.add_vertices if is_from_existed_graph else None
        return self._construct_graph(
            unsealed_vertices,
            self._unsealed_edges,
            v_labels,
            self._e_labels,
            self._e_relationships,
            func,
        )

    def add_edges(
        self,
        edges,
        label="_",
        properties=[],
        src_label=None,
        dst_label=None,
        src_field=0,
        dst_field=1,
    ):
        """Add edges to graph.
        1. Add edges to a uninitialized graph.

            i.   src_label and dst_label both unspecified. In this case, current graph must
                 has 0 (we deduce vertex label from edge table, and set vertex label name to '_'),
                 or 1 vertex label (we set src_label and dst label to this).
            ii.  src_label and dst_label both specified and existed in current graph's vertex labels.
            iii. src_label and dst_label both specified and there is no vertex labels in current graph.
                 we deduce all vertex labels from edge tables.
                 Note that you either provide all vertex labels, or let graphscope deduce all vertex labels.
                 We don't support mixed style.

        2. Add edges to a existed graph.
            Must add a new kind of edge label, not a new relation to builded graph.
            But you can add a new relation to uninitialized part of the graph.
            src_label and dst_label must be specified and existed in current graph.

        Args:
            edges ([type]): [description]
            label (str, optional): [description]. Defaults to "_".
            properties ([type], optional): [description]. Defaults to None.
            src_label ([type], optional): [description]. Defaults to None.
            dst_label ([type], optional): [description]. Defaults to None.
            src_field (int, optional): [description]. Defaults to 0.
            dst_field (int, optional): [description]. Defaults to 1.

        Raises:
            RuntimeError: [description]

        Returns:
            Graph: [description]
        """
        is_from_existed_graph = len(self._unsealed_vertices) != len(
            self._v_labels
        ) or len(self._unsealed_edges) != len(self._e_labels)

        if is_from_existed_graph:
            if label in self._e_labels and label not in self._unsealed_edges:
                raise ValueError("Cannot add new relation to existed graph.")
            if src_label is None or dst_label is None:
                raise ValueError("src label and dst label cannot be None.")
            if src_label not in self._v_labels or dst_label not in self._v_labels:
                raise ValueError("src label or dst_label not existed in graph.")
        else:
            if src_label is None and dst_label is None:
                check_argument(len(self._v_labels) <= 1, "ambiguous vertex label")
                if len(self._v_labels) == 1:
                    src_label = dst_label = self._v_labels[0]
                else:
                    src_label = dst_label = "_"
            elif src_label is not None and dst_label is not None:
                if self._v_labels:
                    if (
                        src_label not in self._v_labels
                        or dst_label not in self._v_labels
                    ):
                        raise ValueError("src label or dst_label not existed in graph.")
                else:
                    # Infer all v_labels from edge tables.
                    pass
            else:
                raise ValueError(
                    "src and dst label must be both specified or either unspecified."
                )

        check_argument(
            src_field != dst_field, "src and dst field cannot refer to the same field"
        )

        unsealed_edges = deepcopy(self._unsealed_edges)
        e_labels = deepcopy(self._e_labels)
        relations = deepcopy(self._e_relationships)
        if label in unsealed_edges:
            assert label in self._e_labels
            label_idx = self._e_labels.index(label)
            # Will check conflict in `add_sub_label`
            relations[label_idx].append((src_label, dst_label))
            cur_label = unsealed_edges[label]
        else:
            e_labels.append(label)
            relations.append([(src_label, dst_label)])
            cur_label = EdgeLabel(label)
        cur_label.add_sub_label(
            EdgeSubLabel(edges, properties, src_label, dst_label, src_field, dst_field)
        )
        unsealed_edges[label] = cur_label

        # Load after validity check and before create add_vertices op.
        # TODO(zsy): Add ability to add vertices and edges to existed graph simultaneously.
        if is_from_existed_graph and self._unsealed_vertices:
            self._ensure_loaded()

        func = dag_utils.add_edges if is_from_existed_graph else None
        return self._construct_graph(
            self._unsealed_vertices,
            unsealed_edges,
            self._v_labels,
            e_labels,
            relations,
            func,
        )

    def project(
        self,
        vertices: Mapping[str, Union[List[str], None]],
        edges: Mapping[str, Union[List[str], None]],
    ):
        check_argument(self.graph_type == types_pb2.ARROW_PROPERTY)

        def get_all_v_props_id(label) -> List[int]:
            props = self.schema.get_vertex_properties(label)
            return [
                self.schema.get_vertex_property_id(label, prop.name) for prop in props
            ]

        def get_all_e_props_id(label) -> List[int]:
            props = self.schema.get_edge_properties(label)
            return [
                self.schema.get_edge_property_id(label, prop.name) for prop in props
            ]

        vertex_collections = {}
        edge_collections = {}

        for label, props in vertices.items():
            label_id = self.schema.get_vertex_label_id(label)
            if props is None:
                vertex_collections[label_id] = get_all_v_props_id(label)
            else:
                vertex_collections[label_id] = sorted(
                    [self.schema.get_vertex_property_id(label, prop) for prop in props]
                )
        for label, props in edges.items():
            # find whether exist a valid relation
            relations = self.schema.get_relationships(label)
            valid = False
            for src, dst in relations:
                if src in vertices and dst in vertices:
                    valid = True
                    break
            if not valid:
                raise ValueError(
                    "Cannot find a valid relation in given vertices and edges"
                )

            label_id = self.schema.get_edge_label_id(label)
            if props is None:
                edge_collections[label_id] = get_all_e_props_id(label)
            else:
                edge_collections[label_id] = sorted(
                    [self.schema.get_edge_property_id(label, prop) for prop in props]
                )

        vertex_collections = dict(sorted(vertex_collections.items()))
        edge_collections = dict(sorted(edge_collections.items()))

        op = dag_utils.project_arrow_property_graph(
            self, vertex_collections, edge_collections
        )
        graph = Graph(self._session, op)
        graph._base_graph = self
        return graph
