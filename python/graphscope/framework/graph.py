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
import warnings
from abc import ABCMeta
from abc import abstractmethod
from copy import deepcopy
from typing import Dict
from typing import List
from typing import Mapping
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

try:
    import vineyard
except (ImportError, TypeError):
    vineyard = None

from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.dag import DAGNode
from graphscope.framework.errors import check_argument
from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.graph_utils import EdgeLabel
from graphscope.framework.graph_utils import EdgeSubLabel
from graphscope.framework.graph_utils import VertexLabel
from graphscope.framework.operation import Operation
from graphscope.framework.utils import apply_docstring
from graphscope.framework.utils import data_type_to_cpp
from graphscope.proto import attr_value_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2

logger = logging.getLogger("graphscope")


class GraphInterface(metaclass=ABCMeta):
    """Base Class to derive GraphDAGNode and Graph"""

    def __init__(self):
        self._session = None
        self._directed = True
        self._generate_eid = True
        self._retain_oid = True
        self._oid_type = "int64"
        self._vid_type = "uint64"
        self._vertex_map = graph_def_pb2.GLOBAL_VERTEX_MAP
        self._compact_edges = False
        self._use_perfect_hash = False
        self._extend_label_data = 0

    @property
    def session_id(self):
        raise NotImplementedError

    @abstractmethod
    def add_column(self, results, selector):
        raise NotImplementedError

    @abstractmethod
    def add_vertices(self, vertices, label="_", properties=None, vid_field=0):
        raise NotImplementedError

    @abstractmethod
    def add_edges(
        self,
        edges,
        label="_",
        properties=None,
        src_label=None,
        dst_label=None,
        src_field=0,
        dst_field=1,
    ):
        raise NotImplementedError

    @abstractmethod
    def consolidate_columns(
        self,
        label: str,
        columns: Union[List[str], Tuple[str]],
        result_column: str,
    ):
        raise NotImplementedError

    def is_directed(self):
        return self._directed

    def to_numpy(self, selector, vertex_range=None):
        raise NotImplementedError

    def to_dataframe(self, selector, vertex_range=None):
        raise NotImplementedError

    def save_to(self, path, **kwargs):
        raise NotImplementedError

    @classmethod
    def load_from(cls, path, sess, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def project(self, vertices, edges):
        raise NotImplementedError

    def unload(self):
        with warnings.catch_warnings():
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(
                "The Graph.unload() method has been deprecated, please using the `del` operator instead, i.e., `del graph`",
                DeprecationWarning,
            )

    def _from_nx_graph(self, g):
        """Create a gs graph from a nx graph.
        Args:
            g (:class:`graphscope.nx.graph`): A nx graph that contains graph data.

        Raises:
            RuntimeError: NX graph and gs graph not in the same session.
            TypeError: Convert a graph view of nx graph to gs graph.

        Returns: :class:`graphscope.framework.operation.Operation`
            that will be used to construct a :class:`graphscope.Graph`

        Examples:
        .. code:: python

            >>> import graphscope as gs
            >>> nx_g = gs.nx.path_graph(10)
            >>> gs_g = gs.Graph(nx_g)
        """
        if self.session_id != g.session_id:
            raise RuntimeError(
                "networkx graph and graphscope graph not in the same session."
            )
        if hasattr(g, "_graph"):
            raise TypeError("graph view can not convert to gs graph")
        return dag_utils.dynamic_to_arrow(g)

    def _from_vineyard(self, vineyard_object):
        """Load a graph from a already existed vineyard graph.

        Args:
            vineyard_object (:class:`vineyard.Object`, :class:`vineyard.ObjectID`
            or :class:`vineyard.ObjectName`): vineyard object,
            which represents a graph.

        Returns:
            :class:`graphscope.framework.operation.Operation`
        """
        if isinstance(vineyard_object, vineyard.Object):
            return self._construct_op_from_vineyard_id(vineyard_object.id)
        if isinstance(vineyard_object, vineyard.ObjectID):
            return self._construct_op_from_vineyard_id(vineyard_object)
        if isinstance(vineyard_object, vineyard.ObjectName):
            return self._construct_op_from_vineyard_name(vineyard_object)

    def _construct_op_from_vineyard_id(self, vineyard_id):
        assert self._session is not None
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(True)
        config[types_pb2.VINEYARD_ID] = utils.i_to_attr(int(vineyard_id))
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrieved from vineyard
        config[types_pb2.OID_TYPE] = utils.s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
        return dag_utils.create_graph(
            self.session_id, graph_def_pb2.ARROW_PROPERTY, attrs=config
        )

    def _construct_op_from_vineyard_name(self, vineyard_name):
        assert self._session is not None
        config = {}
        config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(True)
        config[types_pb2.VINEYARD_NAME] = utils.s_to_attr(str(vineyard_name))
        # FIXME(hetao) hardcode oid/vid type for codegen, when loading from vineyard
        #
        # the metadata should be retrieved from vineyard
        config[types_pb2.OID_TYPE] = utils.s_to_attr("int64_t")
        config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
        return dag_utils.create_graph(
            self.session_id, graph_def_pb2.ARROW_PROPERTY, attrs=config
        )

    def _construct_op_of_empty_graph(self):
        config = {}
        config[types_pb2.ARROW_PROPERTY_DEFINITION] = attr_value_pb2.AttrValue()
        config[types_pb2.DIRECTED] = utils.b_to_attr(self._directed)
        config[types_pb2.GENERATE_EID] = utils.b_to_attr(self._generate_eid)
        config[types_pb2.RETAIN_OID] = utils.b_to_attr(self._retain_oid)
        config[types_pb2.OID_TYPE] = utils.s_to_attr(self._oid_type)
        config[types_pb2.VID_TYPE] = utils.s_to_attr(self._vid_type)
        config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(False)
        config[types_pb2.IS_FROM_GAR] = utils.b_to_attr(False)
        config[types_pb2.VERTEX_MAP_TYPE] = utils.i_to_attr(self._vertex_map)
        config[types_pb2.COMPACT_EDGES] = utils.b_to_attr(self._compact_edges)
        config[types_pb2.USE_PERFECT_HASH] = utils.b_to_attr(self._use_perfect_hash)
        config[types_pb2.EXTEND_LABEL_DATA] = utils.i_to_attr(self._extend_label_data)
        return dag_utils.create_graph(
            self.session_id, graph_def_pb2.ARROW_PROPERTY, inputs=None, attrs=config
        )


class GraphDAGNode(DAGNode, GraphInterface):
    """A class represents a graph node in a DAG.

    In GraphScope, all operations that generate a new graph will return
    a instance of :class:`GraphDAGNode`, which will be automatically
    executed by :meth:`Session.run` in `eager` mode.

    The following example demonstrates its usage:

    .. code:: python

        >>> # lazy mode
        >>> import graphscope as gs
        >>> sess = gs.session(mode="lazy")
        >>> g = sess.g()
        >>> g1 = g.add_vertices("person.csv","person")
        >>> print(g1) # <graphscope.framework.graph.GraphDAGNode object>
        >>> g2 = sess.run(g1)
        >>> print(g2) # <graphscope.framework.graph.Graph object>

        >>> # eager mode
        >>> import graphscope as gs
        >>> sess = gs.session(mode="eager")
        >>> g = sess.g()
        >>> g1 = g.add_vertices("person.csv","person")
        >>> print(g1) # <graphscope.framework.graph.Graph object>
        >>> del g1
    """

    def __init__(
        self,
        session,
        incoming_data=None,
        oid_type="int64",
        vid_type="uint64",
        directed=True,
        generate_eid=True,
        retain_oid=True,
        vertex_map: Union[str, int] = "global",
        compact_edges=False,
        use_perfect_hash=False,
    ):
        """Construct a :class:`GraphDAGNode` object.

        Args:
            session (:class:`Session`): A graphscope session instance.
            incoming_data: Graph can be initialized through various type of sources,
                which can be one of:

                - :class:`graphscope.framework.operation.Operation`
                - :class:`graphscope.nx.Graph`
                - :class:`graphscope.Graph`
                - :class:`vineyard.Object`, :class:`vineyard.ObjectId` or :class:`vineyard.ObjectName`

            oid_type: (str, optional): Type of vertex original id. Defaults to "int64".
            vid_type: (str, optional): Type of vertex internal id. Defaults to "uint64".
            directed: (bool, optional): Directed graph or not. Defaults to True.
            generate_eid: (bool, optional): Generate id for each edge when set True. Defaults to True.
            retain_oid: (bool, optional): Keep original ID in vertex table when set True. Defaults to True.
            vertex_map (str, optional): Indicate use global vertex map or local vertex map. Can be "global" or "local".
                Defaults to global.
            compact_edges (bool, optional): Compact edges (CSR) using varint and delta encoding. Defaults to False.
                Note that compact edges helps to half the memory usage of edges in graph data structure, but may cause
                at most 10%~20% performance degeneration in some algorithms. Defaults to False.
            use_perfect_hash (bool, optional): Use perfect hash in vertex map to optimize the memory usage. Defaults to False.
        """

        super().__init__()
        self._session = session
        oid_type = utils.normalize_data_type_str(oid_type)
        if oid_type not in ("int32_t", "int64_t", "std::string"):
            raise ValueError("oid_type can only be int32_t, int64_t or string.")
        vid_type = utils.normalize_data_type_str(vid_type)
        if vid_type not in ("uint32_t", "uint64_t"):
            raise ValueError("vid_type can only be uint32_t or uint64_t.")
        self._oid_type = oid_type
        self._vid_type = vid_type
        self._directed = directed
        self._generate_eid = generate_eid
        self._retain_oid = retain_oid
        self._graph_type = graph_def_pb2.ARROW_PROPERTY
        self._vertex_map = utils.vertex_map_type_to_enum(vertex_map)
        self._compact_edges = compact_edges
        self._use_perfect_hash = use_perfect_hash
        # for need to extend label in 'eager mode' when add_vertices and add_edges
        # 0 - not extending label
        # 1 - extend vertex label
        # 2 - extend edge label
        self._extend_label_data = 0

        # list of pair <parent_op_key, VertexLabel/EdgeLabel>
        self._unsealed_vertices_and_edges = list()
        # check for newly added vertices and edges.
        self._v_labels = list()
        self._e_labels = list()
        self._e_relationships = list()
        self._base_graph = None
        # add op to dag
        self._resolve_op(incoming_data)
        self._session.dag.add_op(self._op)

        # statically create the unload op, as the op may change, the
        # unload op should be refreshed as well.
        if self._op is None:
            self._unload_op = None
        else:
            self._unload_op = dag_utils.unload_graph(self)

    @property
    def v_labels(self):
        return self._v_labels

    @v_labels.setter
    def v_labels(self, value):
        self._v_labels = value

    @property
    def e_labels(self):
        return self._e_labels

    @e_labels.setter
    def e_labels(self, value):
        self._e_labels = value

    @property
    def e_relationships(self):
        return self._e_relationships

    @e_relationships.setter
    def e_relationships(self, value):
        self._e_relationships = value

    @property
    def graph_type(self):
        """The type of the graph object.

        Returns:
            type (`types_pb2.GraphType`): the type of the graph.
        """
        return self._graph_type

    @property
    def oid_type(self):
        return utils.normalize_data_type_str(self._oid_type)

    @property
    def vid_type(self):
        return utils.normalize_data_type_str(self._vid_type)

    def _project_to_simple(self, v_prop=None, e_prop=None):
        check_argument(self.graph_type == graph_def_pb2.ARROW_PROPERTY)
        op = dag_utils.project_to_simple(self, str(v_prop), str(e_prop))
        # construct dag node
        graph_dag_node = GraphDAGNode(
            self._session,
            op,
            self._oid_type,
            self._vid_type,
            self._directed,
            self._generate_eid,
            self._retain_oid,
            self._vertex_map,
            self._compact_edges,
            self._use_perfect_hash,
        )
        graph_dag_node._base_graph = self
        return graph_dag_node

    def _resolve_op(self, incoming_data):
        if incoming_data is None:
            # create dag node of empty graph
            self._op = self._construct_op_of_empty_graph()
        elif isinstance(incoming_data, Operation):
            self._op = incoming_data
            if self._op.type == types_pb2.PROJECT_TO_SIMPLE:
                self._graph_type = graph_def_pb2.ARROW_PROJECTED
        elif isinstance(incoming_data, Graph):
            self._op = dag_utils.copy_graph(incoming_data)
            self._graph_type = incoming_data.graph_type
        elif isinstance(incoming_data, GraphDAGNode):
            if incoming_data.session_id != self.session_id:
                raise RuntimeError(f"{incoming_data} not in the same session.")
            raise NotImplementedError
        elif vineyard is not None and isinstance(
            incoming_data, (vineyard.Object, vineyard.ObjectID, vineyard.ObjectName)
        ):
            self._op = self._from_vineyard(incoming_data)
        else:
            # Don't import the :code:`NXGraph` in top-level statements to improve the
            # performance of :code:`import graphscope`.
            from graphscope import nx

            if isinstance(incoming_data, nx.classes.graph._GraphBase):
                self._op = self._from_nx_graph(incoming_data)
            else:
                raise RuntimeError("Not supported incoming data.")
        # update the unload op
        self._unload_op = dag_utils.unload_graph(self)

    def to_numpy(self, selector, vertex_range=None):
        """Select some elements of the graph and output to numpy.

        Args:
            selector (str): Select a portion of graph as a numpy.ndarray.
            vertex_range(dict, optional): Slice vertices. Defaults to None.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result holds the `numpy.ndarray`, evaluated in eager mode.
        """
        # avoid circular import
        from graphscope.framework.context import ResultDAGNode

        check_argument(self.graph_type == graph_def_pb2.ARROW_PROPERTY)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.graph_to_numpy(self, selector, vertex_range)
        return ResultDAGNode(self, op)

    def to_dataframe(self, selector, vertex_range=None):
        """Select some elements of the graph and output as a pandas.DataFrame

        Args:
            selector (dict): Select some portions of graph.
            vertex_range (dict, optional): Slice vertices. Defaults to None.

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result holds the `pandas.DataFrame`, evaluated in eager mode.
        """
        # avoid circular import
        from graphscope.framework.context import ResultDAGNode

        check_argument(self.graph_type == graph_def_pb2.ARROW_PROPERTY)
        check_argument(
            isinstance(selector, Mapping),
            "selector of to dataframe must be a dict",
        )
        selector = json.dumps(selector)
        vertex_range = utils.transform_vertex_range(vertex_range)
        op = dag_utils.graph_to_dataframe(self, selector, vertex_range)
        return ResultDAGNode(self, op)

    def to_directed(self):
        op = dag_utils.to_directed(self)
        graph_dag_node = GraphDAGNode(self._session, op)
        return graph_dag_node

    def to_undirected(self):
        op = dag_utils.to_undirected(self)
        graph_dag_node = GraphDAGNode(self._session, op)
        return graph_dag_node

    def add_vertices(
        self, vertices, label="_", properties=None, vid_field: Union[int, str] = 0
    ):
        """Add vertices to the graph, and return a new graph.

        Args:
            vertices (Union[str, Loader]): Vertex data source.
            label (str, optional): Vertex label name. Defaults to "_".
            properties (list[str], optional): List of column names loaded as properties. Defaults to None.
            vid_field (int or str, optional): Column index or property name used as id field. Defaults to 0.

        Raises:
            ValueError: If the given value is invalid or conflict with current graph.

        Returns:
            :class:`graphscope.framework.graph.GraphDAGNode`:
                A new graph with vertex added, evaluated in eager mode.
        """
        if self._vertex_map == graph_def_pb2.LOCAL_VERTEX_MAP:
            raise ValueError(
                "Cannot incrementally add vertices to graphs with local vertex map, "
                "please use `graphscope.load_from()` instead."
            )
        if self._compact_edges:
            raise ValueError(
                "Cannot incrementally add vertices to graphs with compacted edges, "
                "please use `graphscope.load_from()` instead."
            )
        if not self._v_labels and self._e_labels:
            raise ValueError("Cannot manually add vertices after inferred vertices.")
        # currently not support local_vertex_map
        if label in self._v_labels:
            self._extend_label_data = 1
            warnings.warn(
                f"Label {label} already existed in graph"
                ", origin label data will be extend."
            )
        unsealed_vertices_and_edges = deepcopy(self._unsealed_vertices_and_edges)
        vertex_label = VertexLabel(
            label=label,
            loader=vertices,
            properties=properties,
            vid_field=vid_field,
            id_type=self._oid_type,
            session_id=self._session.session_id,
        )
        unsealed_vertices_and_edges.append((self.op.key, vertex_label))
        v_labels = deepcopy(self._v_labels)
        if self._extend_label_data == 0:
            v_labels.append(label)
        # generate and add a loader op to dag
        loader_op = dag_utils.create_loader(vertex_label)
        self._session.dag.add_op(loader_op)
        # construct add label op
        op = dag_utils.add_labels_to_graph(self, loader_op)
        # construct dag node
        graph_dag_node = GraphDAGNode(
            self._session,
            op,
            self._oid_type,
            self._vid_type,
            self._directed,
            self._generate_eid,
            self._retain_oid,
            self._vertex_map,
            self._compact_edges,
            self._use_perfect_hash,
        )
        graph_dag_node._v_labels = v_labels
        graph_dag_node._e_labels = self._e_labels
        graph_dag_node._e_relationships = self._e_relationships
        graph_dag_node._unsealed_vertices_and_edges = unsealed_vertices_and_edges
        graph_dag_node._base_graph = self
        return graph_dag_node

    def add_edges(
        self,
        edges,
        label="_e",
        properties=None,
        src_label=None,
        dst_label=None,
        src_field: Union[int, str] = 0,
        dst_field: Union[int, str] = 1,
    ):
        """Add edges to the graph, and return a new graph.
        Here the src_label and dst_label must be both specified or both unspecified,

        i. src_label and dst_label both unspecified and current graph has no vertex label.

            We deduce vertex label from edge table, and set vertex label name to '_'.

        ii. src_label and dst_label both unspecified and current graph has one vertex label.

            We set src_label and dst label to this single vertex label.

        ii. src_label and dst_label both specified and existed in current graph's vertex labels.

        iii. src_label and dst_label both specified and some are not existed in current graph's vertex labels.

            We deduce missing vertex labels from edge tables.


        Args:
            edges (Union[str, Loader]): Edge data source.
            label (str, optional): Edge label name. Defaults to "_e".
            properties (list[str], optional): List of column names loaded as properties. Defaults to None.
            src_label (str, optional): Source vertex label. Defaults to None.
            dst_label (str, optional): Destination vertex label. Defaults to None.
            src_field (int, optional): Column index or name used as src field. Defaults to 0.
            dst_field (int, optional): Column index or name used as dst field. Defaults to 1.

        Raises:
            ValueError: If the given value is invalid or conflict with current graph.

        Returns:
            :class:`graphscope.framework.graph.GraphDAGNode`:
                A new graph with edge added, evaluated in eager mode.
        """
        if self._compact_edges:
            raise ValueError(
                "Cannot incrementally add edges to graphs with compacted edges, "
                "please use `graphscope.load_from()` instead."
            )
        if src_label is None and dst_label is None:
            check_argument(
                len(self._v_labels) <= 1,
                "Ambiguous vertex label, please specify the src_label and dst_label.",
            )
            if len(self._v_labels) == 1:
                src_label = dst_label = self._v_labels[0]
            else:
                src_label = dst_label = "_"

        if src_label is None or dst_label is None:
            raise ValueError(
                "src and dst label must be both specified or either unspecified."
            )

        check_argument(
            src_field != dst_field, "src and dst field cannot refer to the same field"
        )

        if self.evaluated:
            if label in self._e_labels:
                self._extend_label_data = 2

        unsealed_vertices = list()
        unsealed_edges = list()

        v_labels = deepcopy(self._v_labels)
        e_labels = deepcopy(self._e_labels)
        relations = deepcopy(self._e_relationships)

        if src_label not in self._v_labels:
            logger.warning("Deducing vertex labels %s", src_label)
            v_labels.append(src_label)

        if src_label != dst_label and dst_label not in self._v_labels:
            logger.warning("Deducing vertex labels %s", dst_label)
            v_labels.append(dst_label)

        parent = self
        if not self.evaluated and label in self.e_labels:
            # aggregate op with the same edge label
            fork = False
            unsealed_vertices_and_edges = list()
            for parent_op_key, unsealed_v_or_e in self._unsealed_vertices_and_edges:
                if (
                    isinstance(unsealed_v_or_e, EdgeLabel)
                    and unsealed_v_or_e.label == label
                ):
                    parent = self._backtrack_graph_dag_node_by_op_key(parent_op_key)
                    cur_label = unsealed_v_or_e
                    cur_label.add_sub_label(
                        EdgeSubLabel(
                            edges,
                            properties,
                            src_label,
                            dst_label,
                            src_field,
                            dst_field,
                            id_type=self._oid_type,
                        )
                    )
                    fork = True
                else:
                    unsealed_vertices_and_edges.append((parent_op_key, unsealed_v_or_e))
                    if fork:
                        if isinstance(unsealed_v_or_e, VertexLabel):
                            unsealed_vertices.append(unsealed_v_or_e)
                        else:
                            unsealed_edges.append(unsealed_v_or_e)
            unsealed_edges.append(cur_label)
            unsealed_vertices_and_edges.append((parent.op.key, cur_label))
        else:
            unsealed_vertices_and_edges = deepcopy(self._unsealed_vertices_and_edges)
            e_labels.append(label)
            relations.append([(src_label, dst_label)])
            cur_label = EdgeLabel(label, self._oid_type, self._session.session_id)
            cur_label.add_sub_label(
                EdgeSubLabel(
                    edges,
                    properties,
                    src_label,
                    dst_label,
                    src_field,
                    dst_field,
                    id_type=self._oid_type,
                )
            )
            unsealed_edges.append(cur_label)
            unsealed_vertices_and_edges.append((parent.op.key, cur_label))
        # generate and add a loader op to dag
        loader_op = dag_utils.create_loader(unsealed_vertices + unsealed_edges)
        self._session.dag.add_op(loader_op)
        # construct add label op
        op = dag_utils.add_labels_to_graph(parent, loader_op)
        # construct dag node
        graph_dag_node = GraphDAGNode(
            self._session,
            op,
            self._oid_type,
            self._vid_type,
            self._directed,
            self._generate_eid,
            self._retain_oid,
            self._vertex_map,
            self._compact_edges,
            self._use_perfect_hash,
        )
        graph_dag_node._v_labels = v_labels
        graph_dag_node._e_labels = e_labels
        graph_dag_node._e_relationships = relations
        graph_dag_node._unsealed_vertices_and_edges = unsealed_vertices_and_edges
        graph_dag_node._base_graph = parent
        return graph_dag_node

    def consolidate_columns(
        self,
        label: str,
        columns: Union[List[str], Tuple[str]],
        result_column: str,
    ):
        """Consolidate columns of given vertex / edge properties (of same type) into one column.

        For example, if we have a graph with vertex label "person", and edge labels "knows"
        and "follows", and we want to consolidate the "weight0", "weight1" properties of the
        vertex and both edges into a new column "weight", we can do:

        .. code:: python

            >>> g = ...
            >>> g = g.consolidate_columns("person", ["weight0", "weight1"], "weight")
            >>> g = g.consolidate_columns("knows", ["weight0", "weight1"], "weight")
            >>> g = g.consolidate_columns("follows", ["weight0", "weight1"], "weight")

        Args:
            label: the label of the vertex or edge.
            columns (dict): the properties of given vertex or edge to be consolidated.
            result_column: the name of the new column.

        Returns:
            :class:`graphscope.framework.graph.GraphDAGNode`:
                A new graph with column consolidated, evaluated in eager mode.
        """
        check_argument(
            isinstance(columns, (list, tuple)),
            "columns must be a list or tuple of strings",
        )
        op = dag_utils.consolidate_columns(self, label, columns, result_column)
        graph_dag_node = GraphDAGNode(
            self._session,
            op,
            self._oid_type,
            self._vid_type,
            self._directed,
            self._generate_eid,
            self._retain_oid,
            self._vertex_map,
            self._compact_edges,
            self._use_perfect_hash,
        )
        graph_dag_node._base_graph = self
        return graph_dag_node

    def _backtrack_graph_dag_node_by_op_key(self, key):
        if self.op.key == key:
            return self
        graph_dag_node = self._base_graph
        while graph_dag_node is not None:
            if graph_dag_node.op.key == key:
                return graph_dag_node
            graph_dag_node = graph_dag_node._base_graph

    def add_column(self, results, selector):
        """Add the results as a column to the graph. Modification rules are given by the selector.

        Args:
            results: A instance of concrete class derive from (:class:`graphscope.framework.context.BaseContextDAGNode`):
                A context that created by doing an app query on a graph, and holds the corresponding results.
            selector (dict): Select results to add as column.
                Format is similar to selectors in :class:`graphscope.framework.context.Context`

        Returns:
            :class:`graphscope.framework.graph.GraphDAGNode`:
                A new graph with new columns, evaluated in eager mode.
        """
        check_argument(
            isinstance(selector, Mapping), "selector of add column must be a dict"
        )
        for key, value in selector.items():
            results._check_selector(value)
        selector = json.dumps(selector)
        op = dag_utils.add_column(self, results, selector)
        graph_dag_node = GraphDAGNode(
            self._session,
            op,
            vertex_map=self._vertex_map,
            compact_edges=self._compact_edges,
            use_perfect_hash=self._use_perfect_hash,
        )
        graph_dag_node._base_graph = self
        return graph_dag_node

    def __del__(self):
        try:
            self.session.run(self._unload())
        except Exception:  # pylint: disable=broad-except
            pass

    def _unload(self):
        """Unload this graph from graphscope engine.

        Returns:
            :class:`graphscope.framework.graph.UnloadedGraph`: Evaluated in eager mode.
        """
        return UnloadedGraph(self._session, self._unload_op)

    def project(
        self,
        vertices: Mapping[str, Union[List[str], None]],
        edges: Mapping[str, Union[List[str], None]],
    ):
        """Project a subgraph from the property graph, and return a new graph.
        A graph produced by project just like a normal property graph, and can be projected further.

        Args:
            vertices (dict):
                key is the vertex label name, the value is a list of str, which represents the
                name of properties. Specifically, it will select all properties if value is None.
                Note that, the label of the vertex in all edges you want to project should be included.
            edges (dict):
                key is the edge label name, the value is a list of str, which represents the
                name of properties. Specifically, it will select all properties if value is None.

        Returns:
            :class:`graphscope.framework.graph.GraphDAGNode`:
                A new graph projected from the property graph, evaluated in eager mode.
        """
        check_argument(self.graph_type == graph_def_pb2.ARROW_PROPERTY)
        if isinstance(vertices, (list, set)) or isinstance(edges, (list, set)):
            raise ValueError(
                "\nThe project vertices or edges cannot be a set or a list, rather, a dict is expected, \n"
                "where the key is the label name and the value is a list of property name. E.g.,\n"
                "\n"
                "    g.project(vertices={'person': ['name', 'age']},\n"
                "              edges={'knows': ['weight']})\n"
                "\n"
                "The property list for vertices and edges can be empty if not needed, e.g.,\n"
                "\n"
                "    g.project(vertices={'person': []}, edges={'knows': []})\n"
            )

        op = dag_utils.project_arrow_property_graph(
            self, json.dumps(vertices), json.dumps(edges)
        )
        # construct dag node
        graph_dag_node = GraphDAGNode(
            self._session,
            op,
            self._oid_type,
            self._vid_type,
            self._directed,
            self._generate_eid,
            self._retain_oid,
            self._vertex_map,
            self._compact_edges,
            self._use_perfect_hash,
        )
        graph_dag_node._base_graph = self
        return graph_dag_node


class Graph(GraphInterface):
    """A class for representing metadata of a graph in the GraphScope.

    A :class:`Graph` object holds the metadata of a graph, such as key, schema, and the graph is directed or not.

    It is worth noticing that the graph is stored by the backend such as Analytical Engine, Vineyard.
    In other words, the graph object holds nothing but metadata.

    The following example demonstrates its usage:

    .. code:: python

        >>> import graphscope as gs
        >>> sess = gs.session()
        >>> graph = sess.g()
        >>> graph = graph.add_vertices("person.csv", "person")
        >>> graph = graph.add_vertices("software.csv", "software")
        >>> graph = graph.add_edges("knows.csv", "knows", src_label="person", dst_label="person")
        >>> graph = graph.add_edges("created.csv", "created", src_label="person", dst_label="software")
        >>> print(graph)
        >>> print(graph.schema)
    """

    def __init__(
        self,
        graph_node,
    ):
        """Construct a :class:`Graph` object."""

        self._graph_node = graph_node
        self._session = self._graph_node.session
        # copy and set op evaluated
        self._graph_node.op = deepcopy(self._graph_node.op)
        self._graph_node.evaluated = True
        self._graph_node._unload_op = dag_utils.unload_graph(self._graph_node)
        self._session.dag.add_op(self._graph_node.op)

        self._key = None
        self._vineyard_id = 0
        self._fragments = None
        self._schema = GraphSchema()
        self._detached = False

        self._vertex_map = graph_node._vertex_map
        self._compact_edges = graph_node._compact_edges
        self._use_perfect_hash = graph_node._use_perfect_hash

        self._interactive_instance_list = []
        self._learning_instance_list = []

    def update_from_graph_def(self, graph_def):
        if graph_def.graph_type == graph_def_pb2.ARROW_FLATTENED:
            self._graph_node._graph_type = graph_def_pb2.ARROW_FLATTENED
        check_argument(
            self._graph_node.graph_type == graph_def.graph_type,
            "Graph type doesn't match {} versus {}".format(
                self._graph_node.graph_type, graph_def.graph_type
            ),
        )
        self._key = graph_def.key
        self._directed = graph_def.directed
        self._is_multigraph = graph_def.is_multigraph
        self._compact_edges = graph_def.compact_edges
        self._use_perfect_hash = graph_def.use_perfect_hash
        vy_info = graph_def_pb2.VineyardInfoPb()
        graph_def.extension.Unpack(vy_info)
        self._vineyard_id = vy_info.vineyard_id
        self._fragments = list(vy_info.fragments)
        self._oid_type = data_type_to_cpp(vy_info.oid_type)
        self._vid_type = data_type_to_cpp(vy_info.vid_type)
        self._generate_eid = vy_info.generate_eid
        self._retain_oid = vy_info.retain_oid

        self._schema_path = vy_info.schema_path
        self._schema.from_graph_def(graph_def)
        self._v_labels = self._schema.vertex_labels
        self._e_labels = self._schema.edge_labels
        self._e_relationships = self._schema.edge_relationships
        # init saved_signature (must be after init schema)
        self._saved_signature = self.signature

    def __getattr__(self, name):
        if hasattr(self._graph_node, name):
            return getattr(self._graph_node, name)
        raise AttributeError("{0} not found.".format(name))

    @property
    def key(self):
        """The key of the corresponding graph in engine."""
        return self._key

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
        return hashlib.sha256(
            "{}.{}".format(self._schema.signature(), self._key).encode(
                "utf-8", errors="ignore"
            )
        ).hexdigest()

    @property
    def op(self):
        return self._graph_node.op

    @property
    def oid_type(self):
        return self._graph_node.oid_type

    @property
    def vid_type(self):
        return self._graph_node.vid_type

    @property
    def template_str(self):
        # transform str/string to std::string
        oid_type = utils.normalize_data_type_str(self._oid_type)
        vid_type = utils.normalize_data_type_str(self._vid_type)
        vdata_type = utils.data_type_to_cpp(self._schema.vdata_type)
        edata_type = utils.data_type_to_cpp(self._schema.edata_type)
        vertex_map_type = utils.vertex_map_type_to_cpp(self._vertex_map)
        vertex_map_type = f"{vertex_map_type}<{oid_type},{vid_type}>"
        compact_type = "true" if self._compact_edges else "false"
        if self._graph_type == graph_def_pb2.ARROW_PROPERTY:
            template = f"vineyard::ArrowFragment<{oid_type},{vid_type},{vertex_map_type},{compact_type}>"
        elif self._graph_type == graph_def_pb2.ARROW_PROJECTED:
            template = f"gs::ArrowProjectedFragment<{oid_type},{vid_type},{vdata_type},{edata_type},{vertex_map_type},{compact_type}>"  # noqa: E501
        elif self._graph_type == graph_def_pb2.ARROW_FLATTENED:
            template = f"ArrowFlattenedFragment<{oid_type},{vid_type},{vdata_type},{edata_type},{compact_type}>"
        elif self._graph_type == graph_def_pb2.DYNAMIC_PROJECTED:
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
        return self._vineyard_id

    @property
    def fragments(self):
        return self._fragments

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
        """True if current graph has been loaded in the session."""
        return self._session.info["status"] == "active" and self._key is not None

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

        return f"graphscope.Graph\n{graph_def_pb2.GraphTypePb.Name(self._graph_type)}\n{v_str}\n{e_str}"

    def __repr__(self):
        return self.__str__()

    def _unload(self):
        """Unload this graph from graphscope engine."""
        rlt = None
        if self._session.info["status"] != "active" or self._key is None:
            return
        if self._detached:
            return

        # close the associated interactive and learning instances
        self._close_interactive_instances()
        self._close_learning_instances()

        # unload the graph
        rlt = self._session._wrapper(self._graph_node._unload())
        self._key = None
        return rlt

    def __del__(self):
        # cleanly ignore all exceptions, cause session may already closed / destroyed.
        try:
            self._session.run(self._unload())
        except Exception:  # pylint: disable=broad-except
            pass

    @apply_docstring(GraphDAGNode._project_to_simple)
    def _project_to_simple(self, v_prop=None, e_prop=None):
        return self._session._wrapper(
            self._graph_node._project_to_simple(v_prop, e_prop)
        )

    @apply_docstring(GraphDAGNode.add_column)
    def add_column(self, results, selector):
        return self._session._wrapper(self._graph_node.add_column(results, selector))

    def to_numpy(self, selector, vertex_range=None):
        """Select some elements of the graph and output to numpy.

        Args:
            selector (str): Select a portion of graph as a numpy.ndarray.
            vertex_range(dict, optional): Slice vertices. Defaults to None.

        Returns:
            `numpy.ndarray`
        """
        self._check_unmodified()
        return self._session._wrapper(self._graph_node.to_numpy(selector, vertex_range))

    def to_dataframe(self, selector, vertex_range=None):
        """Select some elements of the graph and output as a pandas.DataFrame

        Args:
            selector (dict): Select some portions of graph.
            vertex_range (dict, optional): Slice vertices. Defaults to None.

        Returns:
            `pandas.DataFrame`
        """
        self._check_unmodified()
        return self._session._wrapper(
            self._graph_node.to_dataframe(selector, vertex_range)
        )

    def to_directed(self):
        """Returns a directed representation of the graph.

        Returns:
            :class:`Graph`: A directed graph with the same name, same nodes, and
                with each edge (u, v, data) replaced by two directed edges (u, v, data) and (v, u, data).

        """
        if self._directed:
            return self
        return self._session._wrapper(self._graph_node.to_directed())

    def to_undirected(self):
        """Returns an undirected representation of the digraph.

        Returns:
            :class:`Graph`: An undirected graph with the same name and nodes and
                with edge (u, v, data) if either (u, v, data) or (v, u, data) is in the digraph.
                If both edges exist in digraph, they will both be preserved.
                You must check and correct for this manually if desired.
        """
        if not self._directed:
            return self
        return self._session._wrapper(self._graph_node.to_undirected())

    def is_directed(self):
        return self._directed

    def is_multigraph(self):
        return self._is_multigraph

    def _check_unmodified(self):
        check_argument(
            self.signature == self._saved_signature, "Graph has been modified!"
        )

    @staticmethod
    def _load_from_graphar(path, sess, **kwargs):
        # graphar now only support global vertex map.
        vertex_map = utils.vertex_map_type_to_enum("global")
        # oid_type = utils.get_oid_type_from_graph_info(path)
        config = {
            types_pb2.OID_TYPE: utils.s_to_attr(
                "int64_t"
            ),  # graphar use vertex index as oid, so it always be int64_t
            types_pb2.VID_TYPE: utils.s_to_attr("uint64_t"),
            types_pb2.IS_FROM_VINEYARD_ID: utils.b_to_attr(False),
            types_pb2.IS_FROM_GAR: utils.b_to_attr(True),
            types_pb2.VERTEX_MAP_TYPE: utils.i_to_attr(vertex_map),
            types_pb2.COMPACT_EDGES: utils.b_to_attr(False),
            types_pb2.GRAPH_INFO_PATH: utils.s_to_attr(path),
            types_pb2.STORAGE_OPTIONS: utils.s_to_attr(json.dumps(kwargs)),
        }
        op = dag_utils.create_graph(
            sess.session_id, graph_def_pb2.ARROW_PROPERTY, inputs=[], attrs=config
        )
        return sess._wrapper(GraphDAGNode(sess, op))

    @classmethod
    def load_from(cls, uri, sess=None, **kwargs):
        """Load a ArrowProperty graph from a certain data source. The data source
        can be vineyard serialized files, graphar serialized files, or other data
        sources supported by graphscope.

        Args:
            uri (str): URI contains the description of the data source or
                path contains the serialization files,
                example: "graphar+file:///tmp/graphar/xxx"
            sess (`graphscope.Session`): The target session that the graph
                will be construct, if None, use the default session.
            selector (dict, optional): the selector to select the data to read.
            graphar_store_in_local (bool, optional): whether store graphar format in local, default is False.
        Returns:
            `Graph`: A new graph object.
        """
        from graphscope.client.session import get_default_session

        def _check_load_options(load_options):
            for k, v in load_options.items():
                if k == "selector":
                    if not isinstance(v, dict):
                        raise ValueError(
                            "selector should be a dict, but got {}".format(type(v))
                        )
                elif k == "graphar_store_in_local":
                    if not isinstance(v, bool):
                        raise ValueError(
                            "graphar_store_in_local should be a bool, but got {}".format(
                                v
                            )
                        )

        if sess is None:
            sess = get_default_session()
        uri_str = uri
        uri = urlparse(uri)
        if uri.scheme and "+" in uri.scheme:
            source = uri.scheme.split("+")[0]
            if uri.scheme.split("+")[-1] not in ["file", "s3", "oss", "hdfs"]:
                raise ValueError(
                    "Unknown file system %s, currently only support file, s3, oss and hdfs"
                    % uri.scheme.split("+")[-1]
                )
            path = uri.scheme.split("+")[-1] + "://" + uri.netloc + uri.path
            if source == "graphar":
                _check_load_options(kwargs)
                return cls._load_from_graphar(path, sess, **kwargs)
            else:
                raise ValueError("Unknown source %s with uri $s:" % source, uri_str)
        else:
            # not a uri string, assume it is a path for deserialization
            op = dag_utils.deserialize_graph(uri_str, sess, **kwargs)
            return sess._wrapper(GraphDAGNode(sess, op))

    def save_to(
        self,
        path,
        format="serialization",
        **kwargs,
    ):
        """Save graph to specified location with specified format.

        Args:
            path (str): the directory path to write graph.
            format (str): the format to write graph, default is "serialization".
            selector (dict, optional): the selector to select the data to write.
            graphar_graph_name (str, optional): the name of graph in graphar format.
            graphar_file_type (str, optional): the file type of graphar format,
                support "parquet", "orc", "csv", default is "parquet".
            graphar_vertex_chunk_size (int, optional): the chunk size of vertex in graphar format, default is 2^18.
            graphar_edge_chunk_size (int, optional): the chunk size of edge in graphar format, default is 2^22.
            graphar_store_in_local (bool, optional): whether store graphar format in local, default is False.

        Return (dict): A dict contains the type and uri string of output data.
        """

        def _check_write_options(write_options):
            for k, v in write_options.items():
                if k == "graphar_graph_name" and not isinstance(v, str):
                    raise ValueError(
                        "graphar_graph_name should be a string, but got {}".format(
                            type(v)
                        )
                    )
                elif k == "graphar_file_type" and v not in ["parquet", "orc", "csv"]:
                    raise ValueError(
                        "graphar_file_type should be one of ['parquet', 'orc', 'csv'], but got {}".format(
                            v
                        )
                    )
                elif k == "graphar_vertex_chunk_size":
                    if not isinstance(v, int) or v <= 0:
                        raise ValueError(
                            "graphar_vertex_chunk_size should be a positive integer, but got {}".format(
                                v
                            )
                        )
                elif k == "graphar_edge_chunk_size":
                    if not isinstance(v, int) or v <= 0:
                        raise ValueError(
                            "graphar_edge_chunk_size should be a positive integer, but got {}".format(
                                v
                            )
                        )
                elif k == "graphar_store_in_local":
                    if not isinstance(v, bool):
                        raise ValueError(
                            "graphar_store_in_local should be a bool, but got {}".format(
                                v
                            )
                        )
                elif k == "selector":
                    if not isinstance(v, dict):
                        raise ValueError(
                            "selector should be a dict, but got {}".format(type(v))
                        )

        if format == "graphar":
            if "graphar_graph_name" not in kwargs:
                kwargs["graphar_graph_name"] = "graph"  # default graph name
            _check_write_options(kwargs)
            graph_name = kwargs["graphar_graph_name"]

            maybe_uri = urlparse(path)
            if maybe_uri.scheme and maybe_uri.scheme not in [
                "file",
                "s3",
                "oss",
                "hdfs",
            ]:
                raise ValueError(
                    "Unknown file system %s, currently only support file, s3, oss and hdfs"
                    % maybe_uri.scheme
                )
            if not maybe_uri.scheme:
                maybe_uri = maybe_uri._replace(scheme="file")

            op = dag_utils.save_to_graphar(self, path, **kwargs)
            self._session.dag.add_op(op)
            self._session._wrapper(op)
            return {
                "type": format,
                "URI": "graphar+" + maybe_uri.geturl() + graph_name + ".graph.yaml",
            }
        elif format == "serialization":
            # serialize graph
            op = dag_utils.serialize_graph(self, path, **kwargs)
            self._session.dag.add_op(op)
            self._session._wrapper(op)
            return {"type": format, "URI": path}
        else:
            raise ValueError("Unknown format: %s" % format)

    @apply_docstring(GraphDAGNode.add_vertices)
    def add_vertices(
        self, vertices, label="_", properties=None, vid_field: Union[int, str] = 0
    ) -> Union["Graph", GraphDAGNode]:
        if not self.loaded():
            raise RuntimeError("The graph is not loaded")
        return self._session._wrapper(
            self._graph_node.add_vertices(vertices, label, properties, vid_field)
        )

    @apply_docstring(GraphDAGNode.add_edges)
    def add_edges(
        self,
        edges,
        label="_",
        properties=None,
        src_label=None,
        dst_label=None,
        src_field: Union[int, str] = 0,
        dst_field: Union[int, str] = 1,
    ) -> Union["Graph", GraphDAGNode]:
        if not self.loaded():
            raise RuntimeError("The graph is not loaded")
        return self._session._wrapper(
            self._graph_node.add_edges(
                edges, label, properties, src_label, dst_label, src_field, dst_field
            )
        )

    @apply_docstring(GraphDAGNode.consolidate_columns)
    def consolidate_columns(
        self,
        label: str,
        columns: Union[List[str], Tuple[str]],
        result_column: str,
    ) -> Union["Graph", GraphDAGNode]:
        if not self.loaded():
            raise RuntimeError("The graph is not loaded")
        return self._session._wrapper(
            self._graph_node.consolidate_columns(label, columns, result_column)
        )

    @apply_docstring(GraphDAGNode.project)
    def project(
        self,
        vertices: Mapping[str, Union[List[str], None]],
        edges: Mapping[str, Union[List[str], None]],
    ) -> Union["Graph", GraphDAGNode]:
        if not self.loaded():
            raise RuntimeError("The graph is not loaded")
        return self._session._wrapper(self._graph_node.project(vertices, edges))

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

    def _close_interactive_instances(self):
        for instance in self._interactive_instance_list:
            try:
                instance.close()
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to close interactive instances")
        self._interactive_instance_list.clear()

    def _close_learning_instances(self):
        for instance in self._learning_instance_list:
            try:
                instance.close()
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to close interactive instances")
        self._learning_instance_list.clear()


class UnloadedGraph(DAGNode):
    """Unloaded graph node in a DAG."""

    def __init__(self, session, op):
        self._session = session
        self._op = op
        # add op to dag
        self._session.dag.add_op(self._op)
