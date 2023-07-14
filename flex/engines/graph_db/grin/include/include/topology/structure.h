/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_TOPOLOGY_STRUCTURE_H_
#define GRIN_INCLUDE_TOPOLOGY_STRUCTURE_H_

/**
 * @brief Get a (non-partitioned) graph from storage
 * @param uri The URI of the graph.
 * Current URI for supported storage includes:
 * 1. gart://{etcd_endpoint}?prefix={etcd_prefix}&version={version}
 * 2. graphar://{yaml_path}?partition_num={partition_num}&strategy={strategy}
 * 3. v6d://{object_id}?ipc_socket={ipc_socket} where ipc_socket is optional.
 * @return A graph handle.
*/
GRIN_GRAPH grin_get_graph_from_storage(const char*);

void grin_destroy_graph(GRIN_GRAPH);

// Graph
#if defined(GRIN_ASSUME_HAS_DIRECTED_GRAPH) && defined(GRIN_ASSUME_HAS_UNDIRECTED_GRAPH)
/**
 * @brief Check if the graph is directed.
 * This API is only available when the storage supports both directed and
 * undirected graph. Otherwise, check which of ``GRIN_ASSUME_HAS_DIRECTED_GRAPH``
 * and ``GRIN_ASSUME_HAS_UNDIRECTED_GRAPH`` is defined.
 * @param GRIN_GRAPH The graph.
 * @return True if the graph is directed, otherwise false.
*/
bool grin_is_directed(GRIN_GRAPH);
#endif

#ifdef GRIN_ASSUME_HAS_MULTI_EDGE_GRAPH
/**
 * @brief Check if the graph is a multigraph.
 * This API is only available when the storage supports multigraph.
 * @param GRIN_GRAPH The graph.
 * @return True if the graph is a multigraph, otherwise false.
*/
bool grin_is_multigraph(GRIN_GRAPH);
#endif

#ifndef GRIN_WITH_VERTEX_PROPERTY
/**
 * @brief Get the number of vertices in the graph.
 * This API is only available for simple graph.
 * @param GRIN_GRAPH The graph.
 * @return The number of vertices in the graph.
*/
size_t grin_get_vertex_num(GRIN_GRAPH);
#endif

#ifndef GRIN_WITH_EDGE_PROPERTY
/**
 * @brief Get the number of edges in the graph.
 * This API is only available for simple graph.
 * @param GRIN_GRAPH The graph.
 * @return The number of edges in the graph.
*/
size_t grin_get_edge_num(GRIN_GRAPH);
#endif


// Vertex
void grin_destroy_vertex(GRIN_GRAPH, GRIN_VERTEX);

bool grin_equal_vertex(GRIN_GRAPH, GRIN_VERTEX, GRIN_VERTEX);

// Data
#ifdef GRIN_WITH_VERTEX_DATA
GRIN_DATATYPE grin_get_vertex_data_datatype(GRIN_GRAPH, GRIN_VERTEX);

const void* grin_get_vertex_data_value(GRIN_GRAPH, GRIN_VERTEX);
#endif

// Edge
void grin_destroy_edge(GRIN_GRAPH, GRIN_EDGE);

/**
 * @brief Get the source vertex of an edge.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_EDGE The edge.
 * @return The source vertex of the edge.
*/
GRIN_VERTEX grin_get_src_vertex_from_edge(GRIN_GRAPH, GRIN_EDGE);

/**
 * @brief Get the destination vertex of an edge.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_EDGE The edge.
 * @return The destination vertex of the edge.
*/
GRIN_VERTEX grin_get_dst_vertex_from_edge(GRIN_GRAPH, GRIN_EDGE);

#ifdef GRIN_WITH_EDGE_DATA
GRIN_DATATYPE grin_get_edge_data_datatype(GRIN_GRAPH, GRIN_EDGE);

const void* grin_get_edge_data_value(GRIN_GRAPH, GRIN_EDGE);
#endif

#endif  // GRIN_INCLUDE_TOPOLOGY_STRUCTURE_H_

#ifdef __cplusplus
}
#endif