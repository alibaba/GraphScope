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
                                          GRIN_VERTEX v) {
  GRIN_ADJACENT_LIST_T* alt = new GRIN_ADJACENT_LIST_T();
  alt->dir = dir;
  alt->v = *static_cast<GRIN_VERTEX_T*>(v);
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
  auto iter = new GRIN_ADJACENT_LIST_ITERATOR_T();
  auto& v = _adj_list->v;
  iter->adj_list = _adj_list;
  auto label = _adj_list->edge_label;
  auto src_label = label >> 16;
  auto dst_label = (label >> 8) & 0xff;
  auto edge_label = label & 0xff;
  if (_adj_list->dir == GRIN_DIRECTION::OUT) {
    if (src_label == v.label) {
      iter->edge_iter =
          _g->get_outgoing_edges(src_label, v.vid, dst_label, edge_label);
    } else {
      iter->edge_iter = nullptr;
    }
  } else {
    if (dst_label == v.label) {
      iter->edge_iter =
          _g->get_incoming_edges(dst_label, v.vid, src_label, edge_label);
    } else {
      iter->edge_iter = nullptr;
    }
  }
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
  _iter->edge_iter->next();
}

bool grin_is_adjacent_list_end(GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  return _iter->edge_iter == nullptr || !_iter->edge_iter->is_valid();
}

GRIN_VERTEX grin_get_neighbor_from_adjacent_list_iter(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  auto v = new GRIN_VERTEX_T();
  v->vid = _iter->edge_iter->get_neighbor();
  auto label = _iter->adj_list->edge_label;
  if (_iter->adj_list->dir == GRIN_DIRECTION::OUT) {
    v->label = (label >> 8) & 0xff;
  } else {
    v->label = label >> 16;
  }
  return v;
}

GRIN_EDGE grin_get_edge_from_adjacent_list_iter(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(iter);
  GRIN_EDGE_T* edge = new GRIN_EDGE_T();
  auto nbr = *static_cast<GRIN_VERTEX_T*>(
      grin_get_neighbor_from_adjacent_list_iter(g, iter));
  if (_iter->adj_list->dir == GRIN_DIRECTION::IN) {
    edge->src = nbr;
    edge->dst = _iter->adj_list->v;
  } else {
    edge->src = _iter->adj_list->v;
    edge->dst = nbr;
  }
  edge->dir = _iter->adj_list->dir;
  edge->data = _iter->edge_iter->get_data();
  auto label = _iter->adj_list->edge_label;
  edge->label = label & 0xff;
  return edge;
}
#endif