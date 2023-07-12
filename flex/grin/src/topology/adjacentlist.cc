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

#include "grin/src/predefine.h"

#include "grin/include/include/topology/adjacentlist.h"

#if defined(GRIN_ENABLE_ADJACENT_LIST) && !defined(GRIN_ENABLE_EDGE_PROPERTY)
GRIN_ADJACENT_LIST grin_get_adjacent_list(GRIN_GRAPH g, GRIN_DIRECTION dir,
                                          GRIN_VERTEX v){
    auto _g = static_cast<GRIN_GRAPH_T*>(g);
    auto& schema = _g->schema();
    GRIN_ADJACENT_LIST_T* alt = new GRIN_ADJACENT_LIST_T();
    alt->dir = dir;
    alt->v = v;
    if(dir == GRIN_DIRECTION::OUT){
      std::string src_label = schema.get_vertex_label_name(v.label);
      for(size_t edge_label_i = 0; edge_label_i < _g->edge_label_num_; ++edge_label_i){
        std::string edge_label = schema.get_edge_label_name(edge_label_i);
        for(size_t dst_label_i = 0; dst_label_i < _g->vertex_label_num_; ++dst_label_i){
          std::string dst_label = schema.get_vertex_label_name(dst_label_i);
          if(schema.exist(src_label,dst_label,edge_label)){
            alt->edges_label.emplace_back((edge_label_i << 16) + dst_label_i);
          }
        }
      }
    }else{
      std::string dst_label = schema.get_vertex_label_name(v.label);
      for(size_t edge_label_i = 0; edge_label_i < _g->edge_label_num_; ++edge_label_i){
        std::string edge_label = schema.get_edge_label_name(edge_label_i);
        for(size_t src_label_i = 0; src_label_i < _g->vertex_label_num_; ++src_label_i){
          std::string src_label = schema.get_vertex_label_name(src_label_i);
          if(schema.exist(src_label,dst_label,edge_label)){
            alt->edges_label.emplace_back((edge_label_i << 16) + src_label_i);
          }
        }
      }
    }
    return alt;
}
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST
void grin_destroy_adjacent_list(GRIN_GRAPH g, GRIN_ADJACENT_LIST adj_list) {
  auto adj = static_cast<GRIN_ADJACENT_LIST_T*>(adj_list);
  delete adj;
}
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
GRIN_ADJACENT_LIST_ITERATOR grin_get_adjacent_list_begin(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST adj_list) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _adj_list = static_cast<GRIN_ADJACENT_LIST_T*>(adj_list);
  auto iter = new GRIN_ADJACENT_LIST_ITERATOR_T(_adj_list);
  iter->get_cur_edge_iter(_g);
  return iter;
}

void grin_destroy_adjacent_list_iter(GRIN_GRAPH g,
                                     GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  delete _iter;
}

void grin_get_next_adjacent_list_iter(GRIN_GRAPH g,
                                      GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  _iter->next(_g);
}

bool grin_is_adjacent_list_end(GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  return !_iter->is_valid();
}

GRIN_VERTEX grin_get_neighbor_from_adjacent_list_iter(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  return _iter->neighbor();
}

GRIN_EDGE grin_get_edge_from_adjacent_list_iter(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  GRIN_EDGE_T* edge = new GRIN_EDGE_T();
  if (_iter->adj_list.dir == GRIN_DIRECTION::IN) {
    edge->src = _iter->neighbor();
    edge->dst = _iter->adj_list.v;
  } else {
    edge->src = _iter->adj_list.v;
    edge->dst = _iter->neighbor();
  }
  edge->dir = _iter->adj_list.dir;
  edge->data = _iter->cur_edge_iter->get_data();
  edge->label = _iter->edge_type();
  return edge;
}
#endif