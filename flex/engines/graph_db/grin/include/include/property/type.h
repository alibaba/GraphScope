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
 @file type.h
 @brief Define the vertex/edge type related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PROPERTY_TYPE_H_
#define GRIN_INCLUDE_PROPERTY_TYPE_H_


#ifdef GRIN_WITH_VERTEX_PROPERTY
bool grin_equal_vertex_type(GRIN_GRAPH, GRIN_VERTEX_TYPE, GRIN_VERTEX_TYPE);

GRIN_VERTEX_TYPE grin_get_vertex_type(GRIN_GRAPH, GRIN_VERTEX);

void grin_destroy_vertex_type(GRIN_GRAPH, GRIN_VERTEX_TYPE);

/**
 * @brief Get the vertex type list of the graph
 * This API is only available for property graph.
 * It lists all the vertex types in the graph.
 * @param GRIN_GRAPH The graph.
 * @return The vertex type list.
*/
GRIN_VERTEX_TYPE_LIST grin_get_vertex_type_list(GRIN_GRAPH);

void grin_destroy_vertex_type_list(GRIN_GRAPH, GRIN_VERTEX_TYPE_LIST);

GRIN_VERTEX_TYPE_LIST grin_create_vertex_type_list(GRIN_GRAPH);

bool grin_insert_vertex_type_to_list(GRIN_GRAPH, GRIN_VERTEX_TYPE_LIST, GRIN_VERTEX_TYPE);

size_t grin_get_vertex_type_list_size(GRIN_GRAPH, GRIN_VERTEX_TYPE_LIST);

GRIN_VERTEX_TYPE grin_get_vertex_type_from_list(GRIN_GRAPH, GRIN_VERTEX_TYPE_LIST, size_t);
#endif

#ifdef GRIN_WITH_VERTEX_TYPE_NAME
/**
 * @brief Get the vertex type name.
 * This API is enabled by ``GRIN_WITH_VERTEX_TYPE_NAME``,
 * meaning that the graph has a unique name for each vertex type.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @return The vertex type name of string.
*/
const char* grin_get_vertex_type_name(GRIN_GRAPH, GRIN_VERTEX_TYPE);

/**
 * @brief Get the vertex type by name.
 * This API is enabled by ``GRIN_WITH_VERTEX_TYPE_NAME``,
 * meaning that the graph has a unique name for each vertex type.
 * @param GRIN_GRAPH The graph.
 * @param name The vertex type name.
 * @return The vertex type.
*/
GRIN_VERTEX_TYPE grin_get_vertex_type_by_name(GRIN_GRAPH, const char* name);
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE
/**
 * @brief Get the vertex type id.
 * This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE``,
 * meaning that the graph has naturally increasing ids for vertex types.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @return The vertex type id.
*/
GRIN_VERTEX_TYPE_ID grin_get_vertex_type_id(GRIN_GRAPH, GRIN_VERTEX_TYPE);

/**
 * @brief Get the vertex type by id.
 * This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE``,
 * meaning that the graph has naturally increasing ids for vertex types.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE_ID The vertex type id.
 * @return The vertex type.
*/
GRIN_VERTEX_TYPE grin_get_vertex_type_by_id(GRIN_GRAPH, GRIN_VERTEX_TYPE_ID);
#endif


#ifdef GRIN_WITH_EDGE_PROPERTY
bool grin_equal_edge_type(GRIN_GRAPH, GRIN_EDGE_TYPE, GRIN_EDGE_TYPE);

GRIN_EDGE_TYPE grin_get_edge_type(GRIN_GRAPH, GRIN_EDGE);

void grin_destroy_edge_type(GRIN_GRAPH, GRIN_EDGE_TYPE);

GRIN_EDGE_TYPE_LIST grin_get_edge_type_list(GRIN_GRAPH);

void grin_destroy_edge_type_list(GRIN_GRAPH, GRIN_EDGE_TYPE_LIST);

GRIN_EDGE_TYPE_LIST grin_create_edge_type_list(GRIN_GRAPH);

bool grin_insert_edge_type_to_list(GRIN_GRAPH, GRIN_EDGE_TYPE_LIST, GRIN_EDGE_TYPE);

size_t grin_get_edge_type_list_size(GRIN_GRAPH, GRIN_EDGE_TYPE_LIST);

GRIN_EDGE_TYPE grin_get_edge_type_from_list(GRIN_GRAPH, GRIN_EDGE_TYPE_LIST, size_t);
#endif

#ifdef GRIN_WITH_EDGE_TYPE_NAME
const char* grin_get_edge_type_name(GRIN_GRAPH, GRIN_EDGE_TYPE);

GRIN_EDGE_TYPE grin_get_edge_type_by_name(GRIN_GRAPH, const char*);
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_TYPE
GRIN_EDGE_TYPE_ID grin_get_edge_type_id(GRIN_GRAPH, GRIN_EDGE_TYPE);

GRIN_EDGE_TYPE grin_get_edge_type_by_id(GRIN_GRAPH, GRIN_EDGE_TYPE_ID);
#endif


#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_WITH_EDGE_PROPERTY)
/** 
 * @brief Get source vertex types related to an edge type.
 * GRIN assumes the relation between edge type and pairs of vertex types is 
 * many-to-many. 
 * To return the related pairs of vertex types, GRIN provides two APIs to get
 * the src and dst vertex types respectively.
 * The returned vertex type lists are of the same size, 
 * and the src/dst vertex types are aligned with their positions in the lists.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_EDGE_TYPE The edge type.
 * @return The vertex type list of source.
 */
GRIN_VERTEX_TYPE_LIST grin_get_src_types_by_edge_type(GRIN_GRAPH, GRIN_EDGE_TYPE);

/**
 * @brief Get destination vertex types related to an edge type.
 * GRIN assumes the relation between edge type and pairs of vertex types is
 * many-to-many.
 * To return the related pairs of vertex types, GRIN provides two APIs to get
 * the src and dst vertex types respectively.
 * The returned vertex type lists are of the same size,
 * and the src/dst vertex types are aligned with their positions in the lists.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_EDGE_TYPE The edge type.
 * @return The vertex type list of destination.
*/
GRIN_VERTEX_TYPE_LIST grin_get_dst_types_by_edge_type(GRIN_GRAPH, GRIN_EDGE_TYPE);

/**
 * @brief Get edge types related to a pair of vertex types.
 * GRIN assumes the relation between edge type and pairs of vertex types is
 * many-to-many.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The source vertex type.
 * @param GRIN_VERTEX_TYPE The destination vertex type.
 * @return The related edge type list.
*/
GRIN_EDGE_TYPE_LIST grin_get_edge_types_by_vertex_type_pair(GRIN_GRAPH, GRIN_VERTEX_TYPE, GRIN_VERTEX_TYPE);
#endif

#endif  // GRIN_INCLUDE_PROPERTY_TYPE_H_

#ifdef __cplusplus
}
#endif