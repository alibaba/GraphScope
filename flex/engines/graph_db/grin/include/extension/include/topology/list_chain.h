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

#ifndef GRIN_EXTENSION_INCLUDE_TOPOLOGY_LIST_CHAIN_H_
#define GRIN_EXTENSION_INCLUDE_TOPOLOGY_LIST_CHAIN_H_

#if defined(GRIN_ENABLE_VERTEX_LIST) && defined(GRIN_WITH_VERTEX_PROPERTY)
GRIN_VERTEX_LIST_CHAIN grin_get_vertex_list_chain_of_all_types(GRIN_GRAPH);

void grin_destroy_vertex_list_chain(GRIN_GRAPH, GRIN_VERTEX_LIST_CHAIN);

GRIN_VERTEX_LIST_CHAIN_ITERATOR grin_get_vertex_list_chain_begin(GRIN_GRAPH, GRIN_VERTEX_LIST_CHAIN);

void grin_destroy_vertex_list_chain_iter(GRIN_GRAPH, GRIN_VERTEX_LIST_CHAIN_ITERATOR);

void grin_get_next_vertex_list_chain_iter(GRIN_GRAPH, GRIN_VERTEX_LIST_CHAIN_ITERATOR);

bool grin_is_vertex_list_chain_end(GRIN_GRAPH, GRIN_VERTEX_LIST_CHAIN_ITERATOR);

GRIN_VERTEX grin_get_vertex_from_vertex_list_chain_iter(GRIN_GRAPH, GRIN_VERTEX_LIST_CHAIN_ITERATOR);
#endif

#if defined(GRIN_ENABLE_VERTEX_LIST) && defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_TRAIT_SELECT_MASTER_FOR_VERTEX_LIST)
GRIN_VERTEX_LIST_CHAIN grin_get_vertex_list_chain_of_all_types_select_master(GRIN_GRAPH);

GRIN_VERTEX_LIST_CHAIN grin_get_vertex_list_chain_of_all_types_select_mirror(GRIN_GRAPH);
#endif

#if defined(GRIN_ENABLE_EDGE_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_EDGE_LIST_CHAIN grin_get_edge_list_chain_of_all_types(GRIN_GRAPH);

void grin_destroy_edge_list_chain(GRIN_GRAPH, GRIN_EDGE_LIST_CHAIN);

GRIN_EDGE_LIST_CHAIN_ITERATOR grin_get_edge_list_chain_begin(GRIN_GRAPH, GRIN_EDGE_LIST_CHAIN);

void grin_destroy_edge_list_chain_iter(GRIN_GRAPH, GRIN_EDGE_LIST_CHAIN_ITERATOR);

void grin_get_next_edge_list_chain_iter(GRIN_GRAPH, GRIN_EDGE_LIST_CHAIN_ITERATOR);

bool grin_is_edge_list_chain_end(GRIN_GRAPH, GRIN_EDGE_LIST_CHAIN_ITERATOR);

GRIN_EDGE grin_get_edge_from_edge_list_chain_iter(GRIN_GRAPH, GRIN_EDGE_LIST_CHAIN_ITERATOR);
#endif

#if defined(GRIN_ENABLE_EDGE_LIST) && defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_TRAIT_SELECT_MASTER_FOR_EDGE_LIST)
GRIN_EDGE_LIST_CHAIN grin_get_edge_list_chain_of_all_types_select_master(GRIN_GRAPH);

GRIN_EDGE_LIST_CHAIN grin_get_edge_list_chain_of_all_types_select_mirror(GRIN_GRAPH);
#endif

#if defined(GRIN_ENABLE_ADJACENT_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_ADJACENT_LIST_CHAIN grin_get_adjacent_list_chain_of_all_edge_types(GRIN_GRAPH, GRIN_DIRECTION, GRIN_VERTEX);

void grin_destroy_adjacent_list_chain(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN);

GRIN_ADJACENT_LIST_CHAIN_ITERATOR grin_get_adjacent_list_chain_begin(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN);

void grin_destroy_adjacent_list_chain_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN_ITERATOR);

void grin_get_next_adjacent_list_chain_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN_ITERATOR);

bool grin_is_adjacent_list_chain_end(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN_ITERATOR);

GRIN_EDGE grin_get_edge_from_adjacent_list_chain_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN_ITERATOR);

GRIN_VERTEX grin_get_neighbor_from_adjacent_list_chain_iter(GRIN_GRAPH, GRIN_ADJACENT_LIST_CHAIN_ITERATOR);
#endif

#endif // GRIN_EXTENSION_INCLUDE_TOPOLOGY_VERTEX_LIST_CHAIN_H_

#ifdef __cplusplus
}
#endif