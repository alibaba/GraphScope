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
 @file primarykey.h
 @brief Define the primary key related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PROPERTY_PRIMARY_KEY_H_
#define GRIN_INCLUDE_PROPERTY_PRIMARY_KEY_H_


#ifdef GRIN_ENABLE_VERTEX_PRIMARY_KEYS
/**
 * @brief Get the vertex types that have primary keys
 * In some graph, not every vertex type has primary keys.
 * @param GRIN_GRAPH The graph
 * @return The vertex type list of types that have primary keys
*/
GRIN_VERTEX_TYPE_LIST grin_get_vertex_types_with_primary_keys(GRIN_GRAPH);

/**
 * @brief Get the primary keys properties of a vertex type
 * The primary keys properties are the properties that can be used to identify a vertex.
 * They are a subset of the properties of a vertex type.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_TYPE The vertex type
 * @return The primary keys properties list
*/
GRIN_VERTEX_PROPERTY_LIST grin_get_primary_keys_by_vertex_type(GRIN_GRAPH, GRIN_VERTEX_TYPE);

/**
 * @brief Get the primary keys values row of a vertex
 * The values in the row are in the same order as the primary keys properties.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
 * @return The primary keys values row
*/
GRIN_ROW grin_get_vertex_primary_keys_row(GRIN_GRAPH, GRIN_VERTEX);
#endif

#ifdef GRIN_ENABLE_EDGE_PRIMARY_KEYS
GRIN_EDGE_TYPE_LIST grin_get_edge_types_with_primary_keys(GRIN_GRAPH);

GRIN_EDGE_PROPERTY_LIST grin_get_primary_keys_by_edge_type(GRIN_GRAPH, GRIN_EDGE_TYPE);

GRIN_ROW grin_get_edge_primary_keys_row(GRIN_GRAPH, GRIN_EDGE);
#endif

#endif  // GRIN_INCLUDE_PROPERTY_PRIMARY_KEY_H_

#ifdef __cplusplus
}
#endif