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

#ifndef GRIN_INCLUDE_TOPOLOGY_VERTEXLIST_H_
#define GRIN_INCLUDE_TOPOLOGY_VERTEXLIST_H_


#if defined(GRIN_ENABLE_VERTEX_LIST) && !defined(GRIN_WITH_VERTEX_PROPERTY)
/**
 * @brief Get the vertex list of the graph
 * This API is only available for simple graph.
 * In property graph, use ``grin_get_vertex_list_by_type`` instead.
 * @param GRIN_GRAPH The graph.
 * @return The vertex list. 
*/
GRIN_VERTEX_LIST grin_get_vertex_list(GRIN_GRAPH);
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST
void grin_destroy_vertex_list(GRIN_GRAPH, GRIN_VERTEX_LIST);
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
size_t grin_get_vertex_list_size(GRIN_GRAPH, GRIN_VERTEX_LIST);

/**
 * @brief Get the vertex from the vertex list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_LIST The vertex list.
 * @param index The index of the vertex in the vertex list.
 * @return The vertex.
*/
GRIN_VERTEX grin_get_vertex_from_list(GRIN_GRAPH, GRIN_VERTEX_LIST, size_t index);
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
/**
 * @brief Get the begin iterator of the vertex list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_LIST The vertex list.
 * @return The begin iterator.
*/
GRIN_VERTEX_LIST_ITERATOR grin_get_vertex_list_begin(GRIN_GRAPH, GRIN_VERTEX_LIST);

void grin_destroy_vertex_list_iter(GRIN_GRAPH, GRIN_VERTEX_LIST_ITERATOR);

/**
 * @brief Update the iterator to the next of the vertex list.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_LIST_ITERATOR The iterator to be updated.
*/
void grin_get_next_vertex_list_iter(GRIN_GRAPH, GRIN_VERTEX_LIST_ITERATOR);

/**
 * @brief Check whether the iterator reaches the end of the vertex list.
 * Note that we may get an end iterator when calling ``grin_get_vertex_list_begin``
 * if the vertex list is empty.
 * Users should check if the iterator is at the end before using it.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_LIST_ITERATOR The iterator.
 * @return True if the iterator reaches the end of the vertex list.
*/
bool grin_is_vertex_list_end(GRIN_GRAPH, GRIN_VERTEX_LIST_ITERATOR);

/**
 * @brief Get the vertex from the iterator.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_LIST_ITERATOR The iterator.
 * @return The vertex.
*/
GRIN_VERTEX grin_get_vertex_from_iter(GRIN_GRAPH, GRIN_VERTEX_LIST_ITERATOR);
#endif

#endif  // GRIN_INCLUDE_TOPOLOGY_VERTEXLIST_H_

#ifdef __cplusplus
}
#endif
