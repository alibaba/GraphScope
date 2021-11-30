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

import json
import pickle

from graphscope.framework import utils
from graphscope.framework.errors import check_argument
from graphscope.framework.operation import Operation
from graphscope.proto import attr_value_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import query_args_pb2
from graphscope.proto import types_pb2


def create_app(app_assets):
    """Wrapper for create an `CREATE_APP` Operation with configuration.

    This op will do nothing but provide required information for `BOUND_APP`
    """
    config = {types_pb2.APP_ALGO: utils.s_to_attr(app_assets.algo)}
    if app_assets.gar is not None:
        config[types_pb2.GAR] = utils.bytes_to_attr(app_assets.gar)
    op = Operation(
        None, types_pb2.CREATE_APP, config=config, output_types=types_pb2.APP
    )
    return op


def bind_app(graph, app_assets):
    """Wrapper for create an `BIND_APP` Operation with configuration.
    Compile and load an application after evaluated.

    Args:
        graph (:class:`GraphDAGNode`): A :class:`GraphDAGNode` instance
        app (:class:`AppAssets`): A :class:`AppAssets` instance.

    Returns:
        An :class:`Operation` with configuration that instruct
        analytical engine how to build the app.
    """
    inputs = [graph.op, app_assets.op]
    op = Operation(
        graph.session_id,
        types_pb2.BIND_APP,
        inputs=inputs,
        config={types_pb2.APP_ALGO: utils.s_to_attr(app_assets.algo)},
        output_types=types_pb2.BOUND_APP,
    )
    return op


def run_app(app, *args, **kwargs):
    """Run `bound app` on the `graph`.

    Args:
        app (:class:`AppDAGNode`): A :class:`AppDAGNode` instance which represent a bound app.
        key (str): Key of query results, can be used to retrieve results.
        *args: Additional query params that will be used in evaluation.
        **kwargs: Key-value formated query params that mostly used in Cython apps.

    Returns:
        An op to run app on the specified graph, with optional query parameters.
    """
    inputs = [app.op]
    config = {}
    output_prefix = kwargs.pop("output_prefix", ".")
    config[types_pb2.OUTPUT_PREFIX] = utils.s_to_attr(output_prefix)
    # optional query arguments.
    params = utils.pack_query_params(*args, **kwargs)
    query_args = query_args_pb2.QueryArgs()
    query_args.args.extend(params)
    op = Operation(
        app.session_id,
        types_pb2.RUN_APP,
        inputs=inputs,
        config=config,
        output_types=types_pb2.RESULTS,
        query_args=query_args,
    )
    return op


def create_graph(session_id, graph_type, inputs=None, **kwargs):
    """Create an `CREATE_GRAPH` op, add op to default dag.

    Args:
        session_id (str): Refer to session that the graph will be create on.
        graph_type (:enum:`GraphType`): GraphType defined in proto.types.proto.
        **kwargs: additional properties respect to different `graph_type`.

    Returns:
        An op to create a graph in c++ side with necessary configurations.
    """
    config = {
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph_type),
    }

    if graph_type == graph_def_pb2.ARROW_PROPERTY:
        attrs = kwargs.pop("attrs", None)
        if attrs:
            for k, v in attrs.items():
                if isinstance(v, attr_value_pb2.AttrValue):
                    config[k] = v
    elif graph_type == graph_def_pb2.DYNAMIC_PROPERTY:
        config[types_pb2.E_FILE] = utils.s_to_attr(kwargs["efile"])
        config[types_pb2.V_FILE] = utils.s_to_attr(kwargs["vfile"])
        config[types_pb2.DIRECTED] = utils.b_to_attr(kwargs["directed"])
        config[types_pb2.DISTRIBUTED] = utils.b_to_attr(kwargs["distributed"])
    else:
        raise RuntimeError("Not supported graph type {}".format(graph_type))

    op = Operation(
        session_id,
        types_pb2.CREATE_GRAPH,
        inputs=inputs,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def create_loader(vertex_or_edge_label_list):
    """Create a loader operation.
    Args:
        vertex_or_edge_label_list: List of
            (:class:`graphscope.framework.graph_utils.VertexLabel`) or
            (:class:`graphscope.framework.graph_utils.EdgeLabel`)
    Returns:
        An op to take various data sources as a loader.
    """
    if not isinstance(vertex_or_edge_label_list, list):
        vertex_or_edge_label_list = [vertex_or_edge_label_list]
    attr = attr_value_pb2.AttrValue()
    attr.list.func.extend([label.attr() for label in vertex_or_edge_label_list])
    config = {}
    config[types_pb2.ARROW_PROPERTY_DEFINITION] = attr
    op = Operation(
        vertex_or_edge_label_list[0]._session_id,
        types_pb2.DATA_SOURCE,
        config=config,
        output_types=types_pb2.NULL_OUTPUT,
    )
    return op


def add_labels_to_graph(graph, loader_op):
    """Add new labels to existed graph.

    Args:
        graph (:class:`Graph`): A graph instance.
            May not be fully loaded. i.e. it's in a building
            procedure.
        loader_op (:class:`graphscope.framework.operation.Operation`):
            Operation of loader.

    Raises:
        NotImplementedError: When encountered not supported graph type.

    Returns:
        The operation.

    Notes:
        Since we don't want to trigger the loading, we must not use
        any api that can trigger the loading process implicitly.
    """
    from graphscope.framework.graph import GraphDAGNode

    assert isinstance(graph, GraphDAGNode)
    inputs = [graph.op, loader_op]
    # vid_type is fixed
    config = {
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph._graph_type),
        types_pb2.DIRECTED: utils.b_to_attr(graph._directed),
        types_pb2.OID_TYPE: utils.s_to_attr(graph._oid_type),
        types_pb2.GENERATE_EID: utils.b_to_attr(graph._generate_eid),
        types_pb2.VID_TYPE: utils.s_to_attr("uint64_t"),
        types_pb2.IS_FROM_VINEYARD_ID: utils.b_to_attr(False),
    }
    # inferred from the context of the dag.
    config.update({types_pb2.GRAPH_NAME: utils.place_holder_to_attr()})
    if graph._graph_type != graph_def_pb2.ARROW_PROPERTY:
        raise NotImplementedError(
            f"Add vertices or edges is not supported yet on graph type {graph._graph_type}"
        )
    op = Operation(
        graph._session.session_id,
        types_pb2.ADD_LABELS,
        inputs=inputs,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def dynamic_to_arrow(graph):
    """Create an op to transform a :class:`nx.Graph` object to :class:`Graph`.

    Args:
        graph (:class:`Graph`): Source graph, which type should be DYNAMIC_PROPERTY

    Returns: An op of transform dynamic graph to arrow graph with necessary configurations.
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    oid_type = None
    for node in graph:
        if oid_type is None:
            oid_type = type(node)
        elif oid_type != type(node):
            raise RuntimeError(
                "The vertex type is not consistent {} vs {}, can not convert it to arrow graph".format(
                    str(oid_type), str(type(node))
                )
            )
    if oid_type == int or oid_type is None:
        oid_type = utils.data_type_to_cpp(graph_def_pb2.LONG)
    elif oid_type == str:
        oid_type = utils.data_type_to_cpp(graph_def_pb2.STRING)
    else:
        raise RuntimeError("Unsupported oid type: " + str(oid_type))
    vid_type = utils.data_type_to_cpp(graph_def_pb2.ULONG)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph_def_pb2.ARROW_PROPERTY),
        types_pb2.DST_GRAPH_TYPE: utils.graph_type_to_attr(
            graph_def_pb2.ARROW_PROPERTY
        ),
        types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
        types_pb2.VID_TYPE: utils.s_to_attr(vid_type),
    }

    op = Operation(
        graph.session_id,
        types_pb2.TRANSFORM_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def arrow_to_dynamic(graph):
    """Transform a :class:`Graph` object to :class:`nx.Graph`.

    Args:
        graph (:class:`Graph`): Source graph, which type should be ARROW_PROPERTY.

    Returns:
        An op of transform arrow graph to dynamic graph with necessary configurations.
    """
    check_argument(graph.graph_type == graph_def_pb2.ARROW_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph_def_pb2.ARROW_PROPERTY),
        types_pb2.DST_GRAPH_TYPE: utils.graph_type_to_attr(
            graph_def_pb2.DYNAMIC_PROPERTY
        ),
        types_pb2.OID_TYPE: utils.s_to_attr(
            utils.data_type_to_cpp(graph.schema.oid_type)
        ),
        types_pb2.VID_TYPE: utils.s_to_attr(
            utils.data_type_to_cpp(graph.schema.vid_type)
        ),
        types_pb2.DEFAULT_LABEL_ID: utils.i_to_attr(graph._default_label_id),
    }
    op = Operation(
        graph.session_id,
        types_pb2.TRANSFORM_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def modify_edges(graph, modify_type, edges):
    """Create modify edges operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        modify_type (`type_pb2.(NX_ADD_EDGES | NX_DEL_EDGES | NX_UPDATE_EDGES)`): The modify type
        edges (list): List of edges to be inserted into or delete from graph based on `modify_type`

    Returns:
        An op to modify edges on the graph.
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {}
    config[types_pb2.GRAPH_NAME] = utils.s_to_attr(graph.key)
    config[types_pb2.MODIFY_TYPE] = utils.modify_type_to_attr(modify_type)
    config[types_pb2.EDGES] = utils.list_str_to_attr(edges)
    op = Operation(
        graph.session_id,
        types_pb2.MODIFY_EDGES,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def modify_vertices(graph, modify_type, vertices):
    """Create modify vertices operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        modify_type (`type_pb2.(NX_ADD_NODES | NX_DEL_NODES | NX_UPDATE_NODES)`): The modify type
        vertices (list): node list.

    Returns:
        An op to modify vertices on the graph.
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {}
    config[types_pb2.GRAPH_NAME] = utils.s_to_attr(graph.key)
    config[types_pb2.MODIFY_TYPE] = utils.modify_type_to_attr(modify_type)
    config[types_pb2.NODES] = utils.list_str_to_attr(vertices)
    op = Operation(
        graph.session_id,
        types_pb2.MODIFY_VERTICES,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def report_graph(
    graph,
    report_type,
    node=None,
    edge=None,
    fid=None,
    lid=None,
    key=None,
    label_id=None,
):
    """Create report operation for nx graph.

    This operation is used to simulate networkx graph reporting methods with variaty
    report type and corresponding config parameters.

    Args:
        graph (`nx.Graph`): A nx graph.
        report_type: report type, can be
            type_pb2.(NODE_NUM,
                      EDGE_NUM,
                      HAS_NODE,
                      HAS_EDGE,
                      NODE_DATA,
                      EDGE_DATA,
                      NEIGHBORS_BY_NODE,
                      SUCCS_BY_NODE,
                      PREDS_BY_NODE,
                      NEIGHBORS_BY_LOC,
                      SUCCS_BY_LOC,
                      PREDS_BY_LOC,
                      DEG_BY_NODE,
                      IN_DEG_BY_NODE,
                      OUT_DEG_BY_NODE,
                      DEG_BY_LOC,
                      IN_DEG_BY_LOC,
                      OUT_DEG_BY_LOC,
                      NODES_BY_LOC)
        node (str): node id, used as node id with 'NODE' report types. (optional)
        edge (str): an edge with 'EDGE' report types. (optional)
        fid (int): fragment id, with 'LOC' report types. (optional)
        lid (int): local id of node in grape_engine, with 'LOC; report types. (optional)
        key (str): edge key for MultiGraph or MultiDiGraph, with 'EDGE' report types. (optional)

    Returns:
        An op to do reporting job.
    """
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.REPORT_TYPE: utils.report_type_to_attr(report_type),
    }
    if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
        config[types_pb2.DEFAULT_LABEL_ID] = utils.i_to_attr(graph._default_label_id)

    if node is not None:
        config[types_pb2.NODE] = utils.s_to_attr(node)
    if edge is not None:
        config[types_pb2.EDGE] = utils.s_to_attr(edge)
    if fid is not None:
        config[types_pb2.FID] = utils.i_to_attr(fid)
    if lid is not None:
        config[types_pb2.LID] = utils.i_to_attr(lid)
    if label_id is not None:
        config[types_pb2.V_LABEL_ID] = utils.i_to_attr(label_id)

    config[types_pb2.EDGE_KEY] = utils.s_to_attr(str(key) if key is not None else "")
    op = Operation(
        graph.session_id,
        types_pb2.REPORT_GRAPH,
        config=config,
        output_types=types_pb2.RESULTS,
    )
    return op


def project_arrow_property_graph(graph, vertex_collections, edge_collections):
    check_argument(graph.graph_type == graph_def_pb2.ARROW_PROPERTY)
    config = {
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph.graph_type),
    }
    config.update(
        {
            types_pb2.VERTEX_COLLECTIONS: utils.s_to_attr(vertex_collections),
            types_pb2.EDGE_COLLECTIONS: utils.s_to_attr(edge_collections),
        }
    )
    op = Operation(
        graph.session_id,
        types_pb2.PROJECT_GRAPH,
        config=config,
        inputs=[graph.op],
        output_types=types_pb2.GRAPH,
    )
    return op


def project_arrow_property_graph_to_simple(
    graph,
    v_label_id=None,
    v_prop_id=None,
    e_label_id=None,
    e_prop_id=None,
    v_data_type=None,
    e_data_type=None,
    oid_type=None,
    vid_type=None,
):
    """Project arrow property graph to a simple graph.

    Args:
        graph (:class:`Graph`): Source graph, which type should be ARROW_PROPERTY
        dst_graph_key (str): The key of projected graph.
        v_label_id (int): Label id of vertex used to project.
        v_prop_id (int): Property id of vertex used to project.
        e_label_id (int): Label id of edge used to project.
        e_prop_id (int): Property id of edge used to project.

    Returns:
        An op to project `graph`, results in a simple ARROW_PROJECTED graph.
    """
    check_argument(graph.graph_type == graph_def_pb2.ARROW_PROPERTY)
    config = {}
    op = Operation(
        graph.session_id,
        types_pb2.PROJECT_TO_SIMPLE,
        config=config,
        inputs=[graph.op],
        output_types=types_pb2.GRAPH,
    )
    return op


def project_dynamic_property_graph(graph, v_prop, e_prop, v_prop_type, e_prop_type):
    """Create project graph operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        v_prop (str): The node attribute key to project.
        e_prop (str): The edge attribute key to project.
        v_prop_type (str): Type of the node attribute.
        e_prop_type (str): Type of the edge attribute.

    Returns:
        Operation to project a dynamic property graph. Results in a simple graph.
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph_def_pb2.DYNAMIC_PROJECTED),
        types_pb2.V_PROP_KEY: utils.s_to_attr(v_prop),
        types_pb2.E_PROP_KEY: utils.s_to_attr(e_prop),
        types_pb2.V_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(v_prop_type)),
        types_pb2.E_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(e_prop_type)),
    }

    op = Operation(
        graph.session_id,
        types_pb2.PROJECT_TO_SIMPLE,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def flatten_arrow_property_graph(
    graph, v_prop, e_prop, v_prop_type, e_prop_type, oid_type=None, vid_type=None
):
    """Flatten arrow property graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph hosts an arrow property graph.
        v_prop (str): The vertex property id.
        e_prop (str): The edge property id.
        v_prop_type (str): Type of the node attribute.
        e_prop_type (str): Type of the edge attribute.
        oid_type (str): Type of oid.
        vid_type (str): Type of vid.

    Returns:
        Operation to flatten an arrow property graph. Results in a arrow flattened graph.
    """
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph_def_pb2.ARROW_FLATTENED),
        types_pb2.DST_GRAPH_TYPE: utils.graph_type_to_attr(graph.graph_type),
        types_pb2.V_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(v_prop_type)),
        types_pb2.E_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(e_prop_type)),
    }
    if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
        config[types_pb2.V_PROP_KEY] = utils.s_to_attr(str(v_prop))
        config[types_pb2.E_PROP_KEY] = utils.s_to_attr(str(e_prop))
        config[types_pb2.OID_TYPE] = utils.s_to_attr(utils.data_type_to_cpp(oid_type))
        config[types_pb2.VID_TYPE] = utils.s_to_attr(utils.data_type_to_cpp(vid_type))
    else:
        config[types_pb2.V_PROP_KEY] = utils.s_to_attr(v_prop)
        config[types_pb2.E_PROP_KEY] = utils.s_to_attr(e_prop)

    op = Operation(
        graph.session_id,
        types_pb2.PROJECT_TO_SIMPLE,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def copy_graph(graph, copy_type="identical"):
    """Create copy operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        copy_type (str): 'identical': copy graph to destination graph without any change.
                         'reverse': copy graph to destination graph with reversing the graph edges

    Returns:
        Operation
    """
    check_argument(
        graph.graph_type
        in (graph_def_pb2.ARROW_PROPERTY, graph_def_pb2.DYNAMIC_PROPERTY)
    )
    check_argument(copy_type in ("identical", "reverse"))
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.COPY_TYPE: utils.s_to_attr(copy_type),
    }

    op = Operation(
        graph.session_id,
        types_pb2.COPY_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def to_directed(graph):
    """Create to_directed operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.

    Returns:
        Operation
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }

    op = Operation(
        graph.session_id,
        types_pb2.TO_DIRECTED,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def to_undirected(graph):
    """Create to_undirected operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.

    Returns:
        Operation
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }

    op = Operation(
        graph.session_id,
        types_pb2.TO_UNDIRECTED,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def create_graph_view(graph, view_type):
    """Create view of nx graph.
    Args:
        graph (:class:`nx.Graph`): A nx graph.
        view_type (str): 'reversed': get a reverse view of graph.
                         'directed': get a directed view of graph
                         'undirected': get a undirected view of graph
    Returns:
        Operation
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    check_argument(view_type in ("reversed", "directed", "undirected"))
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.VIEW_TYPE: utils.s_to_attr(view_type),
    }

    op = Operation(
        graph.session_id,
        types_pb2.VIEW_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def clear_graph(graph):
    """Create clear graph operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.

    Returns:
        An op to modify edges on the graph.
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }
    op = Operation(
        graph.session_id,
        types_pb2.CLEAR_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def clear_edges(graph):
    """Create clear edges operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.

    Returns:
        An op to modify edges on the graph.
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }
    op = Operation(
        graph.session_id,
        types_pb2.CLEAR_EDGES,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def create_subgraph(graph, nodes=None, edges=None):
    """Create subgraph operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        nodes (list): the nodes to induce a subgraph.
        edges (list): the edges to induce a edge-induced subgraph.

    Returns:
        Operation
    """
    check_argument(graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }
    if nodes is not None:
        config[types_pb2.NODES] = utils.list_str_to_attr(nodes)
    if edges is not None:
        config[types_pb2.EDGES] = utils.list_str_to_attr(edges)

    op = Operation(
        graph.session_id,
        types_pb2.INDUCE_SUBGRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def create_unload_op(session_id, op_type, inputs):
    """Uility method to create a unload `Operation` based on op type and op."""
    op = Operation(
        session_id,
        op_type,
        inputs=inputs,
        output_types=types_pb2.NULL_OUTPUT,
    )
    return op


def unload_app(app):
    """Unload a loaded app.

    Args:
        app (:class:`AppDAGNode`): The app to unload.

    Returns:
        An op to unload the `app`.
    """
    return create_unload_op(app.session_id, types_pb2.UNLOAD_APP, [app.op])


def unload_graph(graph):
    """Unload a graph.

    Args:
        graph (:class:`GraphDAGNode`): The graph to unload.

    Returns:
        An op to unload the `graph`.
    """
    return create_unload_op(graph.session_id, types_pb2.UNLOAD_GRAPH, [graph.op])


def unload_context(context):
    return create_unload_op(context.session_id, types_pb2.UNLOAD_CONTEXT, [context.op])


def context_to_numpy(context, selector=None, vertex_range=None, axis=0):
    """Retrieve results as a numpy ndarray.

    Args:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to retrieve query results and convert to numpy ndarray.
    """
    config = {}
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    if axis is not None:
        config[types_pb2.AXIS] = utils.i_to_attr(axis)
    op = Operation(
        context.session_id,
        types_pb2.CONTEXT_TO_NUMPY,
        config=config,
        inputs=[context.op],
        output_types=types_pb2.TENSOR,
    )
    return op


def context_to_dataframe(context, selector=None, vertex_range=None):
    """Retrieve results as a pandas DataFrame.

    Args:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to retrieve query results and convert to pandas DataFrame.
    """
    config = {}
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        context.session_id,
        types_pb2.CONTEXT_TO_DATAFRAME,
        config=config,
        inputs=[context.op],
        output_types=types_pb2.DATAFRAME,
    )
    return op


def to_vineyard_tensor(context, selector=None, vertex_range=None, axis=None):
    """Retrieve results as vineyard tensor.

    Parameters:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.
    Returns:
        An op to convert query results into a vineyard tensor.
    """
    config = {}
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    if axis is not None:
        config[types_pb2.AXIS] = utils.i_to_attr(axis)
    op = Operation(
        context.session_id,
        types_pb2.TO_VINEYARD_TENSOR,
        config=config,
        inputs=[context.op],
        output_types=types_pb2.VINEYARD_TENSOR,
    )
    return op


def to_vineyard_dataframe(context, selector=None, vertex_range=None):
    """Retrieve results as vineyard dataframe.

    Parameters:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.
    Returns:
        An op to convert query results into a vineyard dataframe.
    """
    config = {}
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        context.session_id,
        types_pb2.TO_VINEYARD_DATAFRAME,
        config=config,
        inputs=[context.op],
        output_types=types_pb2.VINEYARD_DATAFRAME,
    )
    return op


def output(result, fd, **kwargs):
    """Dump result to `fd`

    Parameters:
        result (:class:`graphscope.framework.context.ResultDAGNode`):
            Dataframe or numpy or result hold the object id of vineyard dataframe.
        fd (str): Such as `file:///tmp/result_path`
        kwargs (dict, optional): Storage options with respect to output storage type

    Returns:
        An op to dump result to `fd`.
    """
    config = {
        types_pb2.STORAGE_OPTIONS: utils.s_to_attr(json.dumps(kwargs)),
        types_pb2.FD: utils.s_to_attr(str(fd)),
    }
    op = Operation(
        result.session_id,
        types_pb2.OUTPUT,
        config=config,
        inputs=[result.op],
        output_types=types_pb2.NULL_OUTPUT,
    )
    return op


def get_context_data(results, node):
    config = {
        types_pb2.CONTEXT_KEY: utils.s_to_attr(results.key),
        types_pb2.NODE: utils.s_to_attr(node),
    }
    op = Operation(
        results._session_id,
        types_pb2.GET_CONTEXT_DATA,
        config=config,
        output_types=types_pb2.RESULTS,
    )
    return op


def add_column(graph, results, selector):
    """Add a column to `graph`, produce a new graph.

    Args:
        graph (:class:`Graph`): Source ArrowProperty graph.
        results (:class:`Context`): Results that generated by previous app querying.
        selector (str): Used to select a subrange of data of results, add them
            as one column of graph.

    Returns:
        A new graph with new columns added.
    """
    config = {types_pb2.SELECTOR: utils.s_to_attr(selector)}
    op = Operation(
        graph.session_id,
        types_pb2.ADD_COLUMN,
        config=config,
        inputs=[graph.op, results.op],
        output_types=types_pb2.GRAPH,
    )
    return op


def graph_to_numpy(graph, selector=None, vertex_range=None):
    """Retrieve graph raw data as a numpy ndarray.

    Args:
        graph (:class:`graphscope.framework.graph.GraphDAGNode`): Source graph.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to convert a graph's data to numpy ndarray.
    """
    config = {}
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        graph.session_id,
        types_pb2.GRAPH_TO_NUMPY,
        config=config,
        inputs=[graph.op],
        output_types=types_pb2.TENSOR,
    )
    return op


def graph_to_dataframe(graph, selector=None, vertex_range=None):
    """Retrieve graph raw data as a pandas DataFrame.

    Args:
        graph (:class:`graphscope.framework.graph.GraphDAGNode`): Source graph.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to convert a graph's data to pandas DataFrame.
    """
    config = {}
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        graph.session_id,
        types_pb2.GRAPH_TO_DATAFRAME,
        config=config,
        inputs=[graph.op],
        output_types=types_pb2.DATAFRAME,
    )
    return op


def create_interactive_query(graph, engine_params):
    """Create a interactive engine that query on the :code:`graph`

    Args:
        graph (:class:`graphscope.framework.graph.GraphDAGNode`):
            Source property graph.
        engine_params (dict, optional):
            Configuration to startup the interactive engine. See detail in:
            `interactive_engine/deploy/docker/dockerfile/executor.vineyard.properties`

    Returns:
        An op to create a interactive engine based on a graph.
    """
    config = {}
    if engine_params is not None:
        config[types_pb2.GIE_GREMLIN_ENGINE_PARAMS] = utils.s_to_attr(
            json.dumps(engine_params)
        )
    op = Operation(
        graph.session_id,
        types_pb2.CREATE_INTERACTIVE_QUERY,
        config=config,
        inputs=[graph.op],
        output_types=types_pb2.INTERACTIVE_QUERY,
    )
    return op


def create_learning_instance(graph, nodes=None, edges=None, gen_labels=None):
    """Create an engine for graph learning.

    Args:
        graph (:class:`graphscope.framework.graph.GraphDAGNode`):
            Source property graph.
        nodes (list): The node types that will be used for gnn training.
        edges (list): The edge types that will be used for gnn training.
        gen_labels (list): Extra node and edge labels on original graph for gnn training.

    Returns:
        An op to create a learning engine based on a graph.
    """
    config = {}
    # pickle None is expected
    config[types_pb2.NODES] = utils.bytes_to_attr(pickle.dumps(nodes))
    config[types_pb2.EDGES] = utils.bytes_to_attr(pickle.dumps(edges))
    config[types_pb2.GLE_GEN_LABELS] = utils.bytes_to_attr(pickle.dumps(gen_labels))
    op = Operation(
        graph.session_id,
        types_pb2.CREATE_LEARNING_INSTANCE,
        config=config,
        inputs=[graph.op],
        output_types=types_pb2.LEARNING_GRAPH,
    )
    return op


def close_interactive_query(interactive_query):
    """Close the interactive instance.

    Args:
        interactive_query (:class:`graphscope.interactive.query.InteractiveQueryDAGNode`):
            The GIE instance holds the graph that gremlin query on.
    Returns:
        An op to close the instance.
    """
    config = {}
    op = Operation(
        interactive_query.session_id,
        types_pb2.CLOSE_INTERACTIVE_QUERY,
        config=config,
        inputs=[interactive_query.op],
        output_types=types_pb2.NULL_OUTPUT,
    )
    return op


def close_learning_instance(learning_instance):
    """Close the learning instance.

    Args:
        learning_instance (:class:`graphscope.learning.graph.GraphDAGNode`):
            The learning instance.
    Returns:
        An op to close the instance.
    """
    config = {}
    op = Operation(
        learning_instance.session_id,
        types_pb2.CLOSE_LEARNING_INSTANCE,
        config=config,
        inputs=[learning_instance.op],
        output_types=types_pb2.NULL_OUTPUT,
    )
    return op


def gremlin_query(interactive_query, query, request_options=None):
    """Execute a gremlin query.

    Args:
        interactive_query (:class:`graphscope.interactive.query.InteractiveQueryDAGNode`):
            The GIE instance holds the graph that gremlin query on.
        query (str):
            Scripts that written in gremlin quering language.
        request_options (dict, optional): gremlin request options. format:
            {
                "engine": "gae"
            }

    Returns:
        An op to execute a gremlin query on the GIE instance.
    """
    config = {}
    config[types_pb2.GIE_GREMLIN_QUERY_MESSAGE] = utils.s_to_attr(query)
    if request_options:
        config[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS] = utils.s_to_attr(
            json.dumps(request_options)
        )
    op = Operation(
        interactive_query.session_id,
        types_pb2.GREMLIN_QUERY,
        config=config,
        inputs=[interactive_query.op],
        output_types=types_pb2.GREMLIN_RESULTS,
    )
    return op


def gremlin_to_subgraph(
    interactive_query, gremlin_script, request_options=None, oid_type="int64"
):
    """Create a subgraph from gremlin output.

    Args:
        interactive_query (:class:`graphscope.interactive.query.InteractiveQueryDAGNode`):
            The GIE instance holds the graph that gremlin query on.
        gremlin_script (str):
            gremlin script to be executed.
        request_options (dict, optional): gremlin request options. format:
            {
                "engine": "gae"
            }
        oid_type (str, optional):
            Type of vertex original id. Defaults to "int64".

    Returns:
        An op to create the subgraph from gremlin script
    """
    config = {}
    config[types_pb2.GIE_GREMLIN_QUERY_MESSAGE] = utils.s_to_attr(gremlin_script)
    config[types_pb2.OID_TYPE] = utils.s_to_attr(oid_type)
    if request_options:
        config[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS] = utils.s_to_attr(
            json.dumps(request_options)
        )
    op = Operation(
        interactive_query.session_id,
        types_pb2.SUBGRAPH,
        config=config,
        inputs=[interactive_query.op],
        output_types=types_pb2.GRAPH,
    )
    return op


def fetch_gremlin_result(result_set, fetch_type="one"):
    """Fetch the gremlin query result.

    Args:
        result_set (:class:`raphscope.interactive.query.ResultSetDAGNode`):
            The instance holds the resultSet in coordinator that can fetch the gremlin result from.
        fetch_type (str): "one" or "all". Defaults to "one".

    Returns:
        An op to fetch the gremlin result.
    """
    config = {}
    config[types_pb2.GIE_GREMLIN_FETCH_RESULT_TYPE] = utils.s_to_attr(fetch_type)
    op = Operation(
        result_set.session_id,
        types_pb2.FETCH_GREMLIN_RESULT,
        config=config,
        inputs=[result_set.op],
        output_types=types_pb2.RESULTS,
    )
    return op
