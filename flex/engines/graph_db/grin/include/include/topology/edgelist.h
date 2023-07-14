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

#ifndef GRIN_INCLUDE_TOPOLOGY_EDGELIST_H_
#define GRIN_INCLUDE_TOPOLOGY_EDGELIST_H_


#if defined(GRIN_ENABLE_EDGE_LIST) && !defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_EDGE_LIST grin_get_edge_list(GRIN_GRAPH);
#endif

#ifdef GRIN_ENABLE_EDGE_LIST
void grin_destroy_edge_list(GRIN_GRAPH, GRIN_EDGE_LIST);
#endif

#ifdef GRIN_ENABLE_EDGE_LIST_ARRAY
size_t grin_get_edge_list_size(GRIN_GRAPH, GRIN_EDGE_LIST);

GRIN_EDGE grin_get_edge_from_list(GRIN_GRAPH, GRIN_EDGE_LIST, size_t);
#endif

#ifdef GRIN_ENABLE_EDGE_LIST_ITERATOR
GRIN_EDGE_LIST_ITERATOR grin_get_edge_list_begin(GRIN_GRAPH, GRIN_EDGE_LIST);

void grin_destroy_edge_list_iter(GRIN_GRAPH, GRIN_EDGE_LIST_ITERATOR);

void grin_get_next_edge_list_iter(GRIN_GRAPH, GRIN_EDGE_LIST_ITERATOR);

bool grin_is_edge_list_end(GRIN_GRAPH, GRIN_EDGE_LIST_ITERATOR);

GRIN_EDGE grin_get_edge_from_iter(GRIN_GRAPH, GRIN_EDGE_LIST_ITERATOR);
#endif

#endif  // GRIN_INCLUDE_TOPOLOGY_EDGELIST_H_

#ifdef __cplusplus
}
#endif