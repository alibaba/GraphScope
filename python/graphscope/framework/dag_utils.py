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

import numpy as np

from graphscope.framework import utils
from graphscope.framework.errors import check_argument
from graphscope.framework.operation import Operation
from graphscope.proto import attr_value_pb2
from graphscope.proto import query_args_pb2
from graphscope.proto import types_pb2


def create_app(graph, app):
    """Wrapper for create an `CREATE_APP` Operation with configuration.
    Compile and load an application after evaluated.

    Args:
        graph (:class:`Graph`): A :class:`Graph` instance
        app (:class:`App`): A :class:`App` instance.

    Returns:
        An :class:`Operation` with configuration that instruct
        analytical engine how to build the app.
    """
    config = {
        types_pb2.APP_ALGO: utils.s_to_attr(app.algo),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph.graph_type),
        types_pb2.OID_TYPE: utils.s_to_attr(graph.schema.oid_type),
        types_pb2.VID_TYPE: utils.s_to_attr(graph.schema.vid_type),
        types_pb2.V_DATA_TYPE: utils.s_to_attr(
            utils.data_type_to_cpp(graph.schema.vdata_type)
        ),
        types_pb2.E_DATA_TYPE: utils.s_to_attr(
            utils.data_type_to_cpp(graph.schema.edata_type)
        ),
        types_pb2.APP_SIGNATURE: utils.s_to_attr(app.signature),
        types_pb2.GRAPH_SIGNATURE: utils.s_to_attr(graph.template_sigature),
    }
    if app.gar is not None:
        config[types_pb2.GAR] = utils.bytes_to_attr(app.gar)

    opr = Operation(
        graph.session_id,
        types_pb2.CREATE_APP,
        config=config,
        output_types=types_pb2.APP,
    )
    return opr


def create_graph(session_id, graph_type, **kwargs):
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

    if graph_type == types_pb2.ARROW_PROPERTY:
        attrs = kwargs.pop("attrs", None)
        if attrs:
            for k, v in attrs.items():
                if isinstance(v, attr_value_pb2.AttrValue):
                    config[k] = v
    elif graph_type == types_pb2.DYNAMIC_PROPERTY:
        config[types_pb2.E_FILE] = utils.s_to_attr(kwargs["efile"])
        config[types_pb2.V_FILE] = utils.s_to_attr(kwargs["vfile"])
        config[types_pb2.DIRECTED] = utils.b_to_attr(kwargs["directed"])
    else:
        raise RuntimeError("Not supported graph type {}".format(graph_type))

    op = Operation(
        session_id, types_pb2.CREATE_GRAPH, config=config, output_types=types_pb2.GRAPH
    )
    return op


def dynamic_to_arrow(graph):
    """Create an op to transform a :class:`nx.Graph` object to :class:`Graph`.

    Args:
        graph (:class:`Graph`): Source graph, which type should be DYNAMIC_PROPERTY

    Returns: An op of transform dynamic graph to arrow graph with necessary configurations.
    """
    check_argument(graph.graph_type == types_pb2.DYNAMIC_PROPERTY)
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
        oid_type = utils.data_type_to_cpp(types_pb2.INT64)
    elif oid_type == str:
        oid_type = utils.data_type_to_cpp(types_pb2.STRING)
    else:
        raise RuntimeError("Unsupported oid type: " + str(oid_type))
    vid_type = utils.data_type_to_cpp(types_pb2.UINT64)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(types_pb2.ARROW_PROPERTY),
        types_pb2.DST_GRAPH_TYPE: utils.graph_type_to_attr(types_pb2.ARROW_PROPERTY),
        types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
        types_pb2.VID_TYPE: utils.s_to_attr(vid_type),
    }

    op = Operation(
        graph._session_id,
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
    check_argument(graph.graph_type == types_pb2.ARROW_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(types_pb2.ARROW_PROPERTY),
        types_pb2.DST_GRAPH_TYPE: utils.graph_type_to_attr(types_pb2.DYNAMIC_PROPERTY),
        types_pb2.OID_TYPE: utils.s_to_attr(graph.schema.oid_type),
        types_pb2.VID_TYPE: utils.s_to_attr(graph.schema.vid_type),
    }
    op = Operation(
        graph._session_id,
        types_pb2.TRANSFORM_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def modify_edges(graph, modify_type, edges):
    """Create modify edges operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        modify_type (`type_pb2.(ADD_EDGES | DEL_EDGES | UPDATE_EDGES)`): The modify type
        edges (list): List of edges to be inserted into or delete from graph based on `modify_type`

    Returns:
        An op to modify edges on the graph.
    """
    check_argument(graph.graph_type == types_pb2.DYNAMIC_PROPERTY)
    config = {}
    config[types_pb2.GRAPH_NAME] = utils.s_to_attr(graph.key)
    config[types_pb2.MODIFY_TYPE] = utils.modify_type_to_attr(modify_type)
    config[types_pb2.EDGES] = utils.list_str_to_attr(edges)
    op = Operation(
        graph._session_id,
        types_pb2.MODIFY_EDGES,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def modify_vertices(graph, modify_type, vertices):
    """Create modify vertices operation for nx graph.

    Args:
        graph (:class:`nx.Graph`): A nx graph.
        modify_type (`type_pb2.(ADD_NODES | DEL_NODES | UPDATE_NODES)`): The modify type
        vertices (list): node list.

    Returns:
        An op to modify vertices on the graph.
    """
    check_argument(graph.graph_type == types_pb2.DYNAMIC_PROPERTY)
    config = {}
    config[types_pb2.GRAPH_NAME] = utils.s_to_attr(graph.key)
    config[types_pb2.MODIFY_TYPE] = utils.modify_type_to_attr(modify_type)
    config[types_pb2.NODES] = utils.list_str_to_attr(vertices)
    op = Operation(
        graph._session_id,
        types_pb2.MODIFY_VERTICES,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def run_app(graph, app, *args, **kwargs):
    """Run `app` on the `graph`.

    Args:
        graph (:class:`Graph`): A loaded graph.
        app (:class:`App`): A loaded app that will be queried.
        key (str): Key of query results, can be used to retrieve results.
        *args: Additional query params that will be used in evaluation.
        **kwargs: Key-value formated query params that mostly used in Cython apps.

    Returns:
        An op to run app on the specified graph, with optional query parameters.
    """
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.APP_NAME: utils.s_to_attr(app.key),
    }
    output_prefix = kwargs.pop("output_prefix", ".")
    config[types_pb2.OUTPUT_PREFIX] = utils.s_to_attr(output_prefix)
    # optional query arguments.
    params = utils.pack_query_params(*args, **kwargs)
    query_args = query_args_pb2.QueryArgs()
    query_args.args.extend(params)

    op = Operation(
        graph._session_id,
        types_pb2.RUN_APP,
        config=config,
        output_types=types_pb2.RESULTS,
        query_args=query_args,
    )
    return op


def report_graph(
    graph, report_type, node=None, edge=None, fid=None, lid=None, key=None
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
    if node is not None:
        config[types_pb2.NODE] = utils.s_to_attr(node)
    if edge is not None:
        config[types_pb2.EDGE] = utils.s_to_attr(edge)
    if fid is not None:
        config[types_pb2.FID] = utils.i_to_attr(fid)
    if lid is not None:
        config[types_pb2.LID] = utils.i_to_attr(lid)

    config[types_pb2.EDGE_KEY] = utils.s_to_attr(str(key) if key is not None else "")
    op = Operation(
        graph._session_id,
        types_pb2.REPORT_GRAPH,
        config=config,
        output_types=types_pb2.RESULTS,
    )
    return op


def project_arrow_property_graph(
    graph,
    v_label_id,
    v_prop_id,
    e_label_id,
    e_prop_id,
    v_data_type,
    e_data_type,
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
    check_argument(graph.graph_type == types_pb2.ARROW_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(types_pb2.ARROW_PROJECTED),
        types_pb2.V_LABEL_ID: utils.i_to_attr(v_label_id),
        types_pb2.V_PROP_ID: utils.i_to_attr(v_prop_id),
        types_pb2.E_LABEL_ID: utils.i_to_attr(e_label_id),
        types_pb2.E_PROP_ID: utils.i_to_attr(e_prop_id),
        types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
        types_pb2.VID_TYPE: utils.s_to_attr(vid_type),
        types_pb2.V_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(v_data_type)),
        types_pb2.E_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(e_data_type)),
    }
    op = Operation(
        graph._session_id,
        types_pb2.PROJECT_GRAPH,
        config=config,
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
    check_argument(graph.graph_type == types_pb2.DYNAMIC_PROPERTY)
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(types_pb2.DYNAMIC_PROJECTED),
        types_pb2.V_PROP_KEY: utils.s_to_attr(v_prop),
        types_pb2.E_PROP_KEY: utils.s_to_attr(e_prop),
        types_pb2.V_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(v_prop_type)),
        types_pb2.E_DATA_TYPE: utils.s_to_attr(utils.data_type_to_cpp(e_prop_type)),
    }

    op = Operation(
        graph._session_id,
        types_pb2.PROJECT_GRAPH,
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
        graph.graph_type in (types_pb2.ARROW_PROPERTY, types_pb2.DYNAMIC_PROPERTY)
    )
    check_argument(copy_type in ("identical", "reverse"))
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.COPY_TYPE: utils.s_to_attr(copy_type),
    }

    op = Operation(
        graph._session_id,
        types_pb2.COPY_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def unload_app(app):
    """Unload a loaded app.

    Args:
        app (:class:`App`): The app to unload.

    Returns:
        An op to unload the `app`.
    """
    config = {types_pb2.APP_NAME: utils.s_to_attr(app.key)}
    op = Operation(
        app._session_id, types_pb2.UNLOAD_APP, config=config, output_types=types_pb2.APP
    )
    return op


def unload_graph(graph):
    """Unload a graph.

    Args:
        graph (:class:`Graph`): The graph to unload.

    Returns:
        An op to unload the `graph`.
    """
    config = {types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key)}
    # Dynamic graph doesn't have a vineyard id
    if hasattr(graph, "vineyard_id"):
        config[types_pb2.VINEYARD_ID] = utils.i_to_attr(graph.vineyard_id)
    op = Operation(
        graph._session_id,
        types_pb2.UNLOAD_GRAPH,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def context_to_numpy(results, selector=None, vertex_range=None, axis=0):
    """Retrieve results as a numpy ndarray.

    Args:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to retrieve query results and convert to numpy ndarray.
    """
    config = {
        types_pb2.CTX_NAME: utils.s_to_attr(results.key),
    }
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    if axis is not None:
        config[types_pb2.AXIS] = utils.i_to_attr(axis)
    op = Operation(
        results._session_id,
        types_pb2.CONTEXT_TO_NUMPY,
        config=config,
        output_types=types_pb2.TENSOR,
    )
    return op


def context_to_dataframe(results, selector=None, vertex_range=None):
    """Retrieve results as a pandas DataFrame.

    Args:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to retrieve query results and convert to pandas DataFrame.
    """
    config = {
        types_pb2.CTX_NAME: utils.s_to_attr(results.key),
    }
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        results._session_id,
        types_pb2.CONTEXT_TO_DATAFRAME,
        config=config,
        output_types=types_pb2.DATAFRAME,
    )
    return op


def to_vineyard_tensor(results, selector=None, vertex_range=None, axis=None):
    """Retrieve results as vineyard tensor.

    Parameters:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.
    Returns:
        An op to convert query results into a vineyard tensor.
    """
    config = {
        types_pb2.CTX_NAME: utils.s_to_attr(results.key),
    }
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    if axis is not None:
        config[types_pb2.AXIS] = utils.i_to_attr(axis)
    op = Operation(
        results._session_id,
        types_pb2.TO_VINEYARD_TENSOR,
        config=config,
        output_types=types_pb2.VINEYARD_TENSOR,
    )
    return op


def to_vineyard_dataframe(results, selector=None, vertex_range=None):
    """Retrieve results as vineyard dataframe.

    Parameters:
        results (:class:`Context`): Results return by `run_app` operation, store the query results.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.
    Returns:
        An op to convert query results into a vineyard dataframe.
    """
    config = {
        types_pb2.CTX_NAME: utils.s_to_attr(results.key),
    }
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        results._session_id,
        types_pb2.TO_VINEYARD_DATAFRAME,
        config=config,
        output_types=types_pb2.VINEYARD_DATAFRAME,
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
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
        types_pb2.GRAPH_TYPE: utils.graph_type_to_attr(graph.graph_type),
        types_pb2.CTX_NAME: utils.s_to_attr(results.key),
        types_pb2.SELECTOR: utils.s_to_attr(selector),
    }
    op = Operation(
        graph._session_id,
        types_pb2.ADD_COLUMN,
        config=config,
        output_types=types_pb2.GRAPH,
    )
    return op


def graph_to_numpy(graph, selector=None, vertex_range=None):
    """Retrieve graph raw data as a numpy ndarray.

    Args:
        graph (:class:`Graph`): Source graph.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to convert a graph's data to numpy ndarray.
    """
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        graph._session_id,
        types_pb2.GRAPH_TO_NUMPY,
        config=config,
        output_types=types_pb2.TENSOR,
    )
    return op


def graph_to_dataframe(graph, selector=None, vertex_range=None):
    """Retrieve graph raw data as a pandas DataFrame.

    Args:
        graph (:class:`Graph`): Source graph.
        selector (str): Select the type of data to retrieve.
        vertex_range (str): Specify a range to retrieve.

    Returns:
        An op to convert a graph's data to pandas DataFrame.
    """
    config = {
        types_pb2.GRAPH_NAME: utils.s_to_attr(graph.key),
    }
    if selector is not None:
        config[types_pb2.SELECTOR] = utils.s_to_attr(selector)
    if vertex_range is not None:
        config[types_pb2.VERTEX_RANGE] = utils.s_to_attr(vertex_range)
    op = Operation(
        graph._session_id,
        types_pb2.GRAPH_TO_DATAFRAME,
        config=config,
        output_types=types_pb2.DATAFRAME,
    )
    return op
