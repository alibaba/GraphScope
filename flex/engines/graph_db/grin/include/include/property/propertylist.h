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
 @file propertylist.h
 @brief Define the property list related and graph projection APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PROPERTY_PROPERTY_LIST_H_
#define GRIN_INCLUDE_PROPERTY_PROPERTY_LIST_H_


#ifdef GRIN_WITH_VERTEX_PROPERTY
/**
 * @brief Get the vertex property list of the graph.
 * This API is only available for property graph.
 * @param GRIN_GRAPH The graph.
 * @return The vertex property list.
*/
GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_property_list_by_type(GRIN_GRAPH, GRIN_VERTEX_TYPE);

size_t grin_get_vertex_property_list_size(GRIN_GRAPH, GRIN_VERTEX_PROPERTY_LIST);

GRIN_VERTEX_PROPERTY grin_get_vertex_property_from_list(GRIN_GRAPH, GRIN_VERTEX_PROPERTY_LIST, size_t);

GRIN_VERTEX_PROPERTY_LIST grin_create_vertex_property_list(GRIN_GRAPH);

void grin_destroy_vertex_property_list(GRIN_GRAPH, GRIN_VERTEX_PROPERTY_LIST);

bool grin_insert_vertex_property_to_list(GRIN_GRAPH, GRIN_VERTEX_PROPERTY_LIST, GRIN_VERTEX_PROPERTY);
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
/**
 * @brief Get the vertex property handle by id.
 * This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY``,
 * meaning that the storage has naturally increasing ids for vertex properties
 * under a certain vertex type.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @param GRIN_VERTEX_PROPERTY_ID The vertex property id.
 * @return The vertex property handle.
*/
GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_id(GRIN_GRAPH, GRIN_VERTEX_TYPE, GRIN_VERTEX_PROPERTY_ID);

/**
 * @brief Get the vertex property's natural id.
 * This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY``,
 * meaning that the storage has naturally increasing ids for vertex properties
 * under a certain vertex type.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @param GRIN_VERTEX_PROPERTY The vertex property handle.
 * @return The vertex property id.
*/
GRIN_VERTEX_PROPERTY_ID grin_get_vertex_property_id(GRIN_GRAPH, GRIN_VERTEX_TYPE, GRIN_VERTEX_PROPERTY);
#endif


#ifdef GRIN_WITH_EDGE_PROPERTY
GRIN_EDGE_PROPERTY_LIST grin_get_edge_property_list_by_type(GRIN_GRAPH, GRIN_EDGE_TYPE);

size_t grin_get_edge_property_list_size(GRIN_GRAPH, GRIN_EDGE_PROPERTY_LIST);

GRIN_EDGE_PROPERTY grin_get_edge_property_from_list(GRIN_GRAPH, GRIN_EDGE_PROPERTY_LIST, size_t);

GRIN_EDGE_PROPERTY_LIST grin_create_edge_property_list(GRIN_GRAPH);

void grin_destroy_edge_property_list(GRIN_GRAPH, GRIN_EDGE_PROPERTY_LIST);

bool grin_insert_edge_property_to_list(GRIN_GRAPH, GRIN_EDGE_PROPERTY_LIST, GRIN_EDGE_PROPERTY);
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
GRIN_EDGE_PROPERTY grin_get_edge_property_by_id(GRIN_GRAPH, GRIN_EDGE_TYPE, GRIN_EDGE_PROPERTY_ID);

/// We must specify the edge type here, because the edge property id is unique only under a specific edge type
GRIN_EDGE_PROPERTY_ID grin_get_edge_property_id(GRIN_GRAPH, GRIN_EDGE_TYPE, GRIN_EDGE_PROPERTY);
#endif

#endif  // GRIN_INCLUDE_PROPERTY_PROPERTY_LIST_H_

#ifdef __cplusplus
}
#endif