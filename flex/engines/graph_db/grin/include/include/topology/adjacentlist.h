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

#ifndef GRIN_INCLUDE_TOPOLOGY_ADJACENTLIST_H_
#define GRIN_INCLUDE_TOPOLOGY_ADJACENTLIST_H_


#if defined(GRIN_ENABLE_ADJACENT_LIST) && !defined(GRIN_WITH_EDGE_PROPERTY)
/**
 * @brief Get the adjacent list of a vertex.
 * This API is only available when the graph has no edge property.
 * Otherwise, use ``grin_get_adjacent_list_by_edge_type`` instead.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_DIRECTION The direction of the adjacent list.
 * @param GRIN_VERTEX The vertex.
 * @return The adjacent list.
*/
GRIN_ADJACENT_LIST grin_get_adjacent_list(GRIN_GRAPH, GRIN_DIRECTION, GRIN_VERTEX);
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST
void grin_destroy_adjacent_list(GRIN_GRAPH, GRIN_ADJACENT_LIST);
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ARRAY
size_t grin_get_adjacent_list_size(GRIN_GRAPH, GRIN_ADJACENT_LIST);

/**
 * @brief Get the neighbor vertex from the adjacent list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST The adjacent list.
 * @param index The index of the edge to/from the neighbor in the adjacent list.
 * @return The neighbor vertex.
*/
GRIN_VERTEX grin_get_neighbor_from_adjacent_list(GRIN_GRAPH, GRIN_ADJACENT_LIST, size_t index);

/**
 * @brief Get the edge from the adjacent list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST The adjacent list.
 * @param index The index of the edge in the adjacent list.
 * @return The edge. Note that when the direction is OUT, the destination vertex
 * of the edge is the neighbor vertex. While the direction is IN, the source 
 * vertex of the edge is the neighbor vertex.
*/
GRIN_EDGE grin_get_edge_from_adjacent_list(GRIN_GRAPH, GRIN_ADJACENT_LIST, size_t);
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
/**
 * @brief Get the begin iterator of the adjacent list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST The adjacent list.
 * @return The begin iterator of the adjacent list.
*/
GRIN_ADJACENT_LIST_ITERATOR grin_get_adjacent_list_begin(GRIN_GRAPH, GRIN_ADJACENT_LIST);

void grin_destroy_adjacent_list_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_ITERATOR);

/**
 * @brief Update the iterator to the next of the adjacent list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST_ITERATOR The adjacent list iterator to be updated.
*/
void grin_get_next_adjacent_list_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_ITERATOR);

/**
 * @brief Check if the adjacent list iterator is at the end.
 * Note that we may get an end iterator when calling ``grin_get_adjacent_list_begin``
 * if the adjacent list is empty.
 * Users should check if the iterator is at the end before using it.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST_ITERATOR The adjacent list iterator.
 * @return True if the iterator is at the end, otherwise false.
*/
bool grin_is_adjacent_list_end(GRIN_GRAPH, GRIN_ADJACENT_LIST_ITERATOR);

/**
 * @brief Get the neighbor vertex from the adjacent list iterator.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST_ITERATOR The adjacent list iterator.
 * @return The neighbor vertex.
*/
GRIN_VERTEX grin_get_neighbor_from_adjacent_list_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_ITERATOR);

/**
 * @brief Get the edge from the adjacent list iterator.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_ADJACENT_LIST_ITERATOR The adjacent list iterator.
 * @return The edge. Note that when the direction is OUT, the destination vertex
 * of the edge is the neighbor vertex. While the direction is IN, the source 
 * vertex of the edge is the neighbor vertex.
*/
GRIN_EDGE grin_get_edge_from_adjacent_list_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_ITERATOR);
#endif

#endif  // GRIN_INCLUDE_TOPOLOGY_ADJACENTLIST_H_

#ifdef __cplusplus
}
#endif
