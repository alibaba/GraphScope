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
 @file internal_id.h
 @brief Define the internal ID related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_INDEX_INTERNAL_ID_H_
#define GRIN_INCLUDE_INDEX_INTERNAL_ID_H_


#if defined(GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX) && !defined(GRIN_WITH_VERTEX_PROPERTY)
/**
 * @brief Get the int64 internal id of a vertex
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
 * @return The int64 internal id of the vertex
*/
long long int grin_get_vertex_internal_id(GRIN_GRAPH, GRIN_VERTEX);

/**
 * @brief Get the vertex by internal id. 
 * Different from pk_of_int64, the internal id is unique over all vertex types.
 * @param GRIN_GRAPH The graph
 * @param id The internal id of the vertex
 * @return The vertex
*/
GRIN_VERTEX grin_get_vertex_by_internal_id(GRIN_GRAPH, long long int id);

/**
 * @brief Get the upper bound of internal id.
 * @param GRIN_GRAPH The graph
 * @return The upper bound
*/
long long int grin_get_vertex_internal_id_upper_bound(GRIN_GRAPH);

/**
 * @brief Get the lower bound of internal id.
 * @param GRIN_GRAPH The graph
 * @return The lower bound
*/
long long int grin_get_vertex_internal_id_lower_bound(GRIN_GRAPH);
#endif

#if defined(GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX) && defined(GRIN_WITH_VERTEX_PROPERTY)
/**
 * @brief Get the int64 internal id of a vertex
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
 * @return The int64 internal id of the vertex
*/
long long int grin_get_vertex_internal_id_by_type(GRIN_GRAPH, GRIN_VERTEX_TYPE, GRIN_VERTEX);

/**
 * @brief Get the vertex by internal id under type
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_TYPE The vertex type
 * @param id The internal id of the vertex under type
 * @return The vertex
*/
GRIN_VERTEX grin_get_vertex_by_internal_id_by_type(GRIN_GRAPH, GRIN_VERTEX_TYPE, long long int id);

/**
 * @brief Get the upper bound of internal id under type.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_TYPE The vertex type
 * @return The upper bound of internal id under type
*/
long long int grin_get_vertex_internal_id_upper_bound_by_type(GRIN_GRAPH, GRIN_VERTEX_TYPE);

/**
 * @brief Get the lower bound internal id under type.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_TYPE The vertex type
 * @return The lower bound internal id under type
*/
long long int grin_get_vertex_internal_id_lower_bound_by_type(GRIN_GRAPH, GRIN_VERTEX_TYPE);
#endif

#endif // GRIN_INCLUDE_INDEX_INTERNAL_ID_H_

#ifdef __cplusplus
}
#endif