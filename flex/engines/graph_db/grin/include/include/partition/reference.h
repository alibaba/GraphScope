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

/**
 @file reference.h
 @brief Define the reference related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PARTITION_REFERENCE_H_
#define GRIN_INCLUDE_PARTITION_REFERENCE_H_

#ifdef GRIN_ENABLE_VERTEX_REF
/**
 * @brief Get the vertex ref of a vertex.
 * A vertex ref is a reference for a "local" vertex, and the reference can
 * be recognized by other partitions.
 * To transfer the vertex ref handle between partitions, users should
 * first call serialization methods to serialize the vertex ref handle
 * into string or int64 based on the storage's features; 
 * then send the messages to remote partitions and deserialize the string or 
 * int64 remotely to get the vertex ref handle on the remote partition; 
 * finally use ``grin_get_vertex_by_vertex_ref`` to get the vertex handle
 * on the remote partition.
 * These two vertices should represent the same vertex in the partitioned graph.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
 * @return The vertex ref
*/
GRIN_VERTEX_REF grin_get_vertex_ref_by_vertex(GRIN_GRAPH, GRIN_VERTEX);

void grin_destroy_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

/**
 * @brief get the local vertex handle from the vertex ref handle
 * if the vertex ref handle is not recognized, a null vertex is returned
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_REF The vertex ref
 * @return The vertex handle
 */
GRIN_VERTEX grin_get_vertex_from_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

/**
 * @brief get the master partition of a vertex ref.
 * Some storage can still provide the master partition of the vertex ref,
 * even if the vertex ref can NOT be recognized locally.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_REF The vertex ref
 */
GRIN_PARTITION grin_get_master_partition_from_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

/**
 * @brief serialize the vertex ref handle to string
 * The returned string should be freed by ``grin_destroy_serialized_vertex_ref``
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_REF The vertex ref
*/
const char* grin_serialize_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

void grin_destroy_serialized_vertex_ref(GRIN_GRAPH, const char*);

/**
 * @brief deserialize the string to vertex ref handle
 * If the string is invalid, a null vertex ref is returned
 * @param GRIN_GRAPH The graph
 * @param msg The string message to be deserialized
*/
GRIN_VERTEX_REF grin_deserialize_to_vertex_ref(GRIN_GRAPH, const char* msg);

/**
 * @brief check if the vertex is a master vertex
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
*/
bool grin_is_master_vertex(GRIN_GRAPH, GRIN_VERTEX);

/**
 * @brief check if the vertex is a mirror vertex
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
*/
bool grin_is_mirror_vertex(GRIN_GRAPH, GRIN_VERTEX);
#endif

#ifdef GRIN_TRAIT_FAST_VERTEX_REF
/**
 * @brief serialize the vertex ref handle to int64
 * This API is enabled by ``GRIN_TRAIT_FAST_VERTEX_REF``, meaning the vertex ref
 * can be serialized into int64 instead of string.
 * Obviously transferring and serializing int64 is faster than string.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_REF The vertex ref
*/
long long int grin_serialize_vertex_ref_as_int64(GRIN_GRAPH, GRIN_VERTEX_REF);

/**
 * @brief deserialize the int64 to vertex ref handle
 * @param GRIN_GRAPH The graph
 * @param msg The int64 message to be deserialized
*/
GRIN_VERTEX_REF grin_deserialize_int64_to_vertex_ref(GRIN_GRAPH, long long int msg);
#endif

#ifdef GRIN_TRAIT_MASTER_VERTEX_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_master_vertex_mirror_partition_list(GRIN_GRAPH, GRIN_VERTEX);
#endif

#ifdef GRIN_TRAIT_MIRROR_VERTEX_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_mirror_vertex_mirror_partition_list(GRIN_GRAPH, GRIN_VERTEX);
#endif

#ifdef GRIN_ENABLE_EDGE_REF
GRIN_EDGE_REF grin_get_edge_ref_by_edge(GRIN_GRAPH, GRIN_EDGE);

void grin_destroy_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

GRIN_EDGE grin_get_edge_from_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

GRIN_PARTITION grin_get_master_partition_from_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

const char* grin_serialize_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

void grin_destroy_serialized_edge_ref(GRIN_GRAPH, const char*);

GRIN_EDGE_REF grin_deserialize_to_edge_ref(GRIN_GRAPH, const char*);

bool grin_is_master_edge(GRIN_GRAPH, GRIN_EDGE);

bool grin_is_mirror_edge(GRIN_GRAPH, GRIN_EDGE);
#endif

#ifdef GRIN_TRAIT_MASTER_EDGE_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_master_edge_mirror_partition_list(GRIN_GRAPH, GRIN_EDGE);
#endif

#ifdef GRIN_TRAIT_MIRROR_EDGE_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_mirror_edge_mirror_partition_list(GRIN_GRAPH, GRIN_EDGE);
#endif

#endif  // GRIN_INCLUDE_PARTITION_REFERENCE_H_

#ifdef __cplusplus
}
#endif
