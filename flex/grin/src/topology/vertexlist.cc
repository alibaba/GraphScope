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

#include "grin/include/include/topology/vertexlist.h"

#if defined(GRIN_ENABLE_VERTEX_LIST) && !defined(GRIN_WITH_VERTEX_PROPERTY)
GRIN_VERTEX_LIST grin_get_vertex_list(GRIN_GRAPH g) {}
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST
void grin_destroy_vertex_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {}
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
GRIN_VERTEX_LIST_ITERATOR grin_get_vertex_list_begin(GRIN_GRAPH g,
                                                     GRIN_VERTEX_LIST vl) {
  GRIN_VERTEX_LIST_ITERATOR_T* vlt = new GRIN_VERTEX_LIST_ITERATOR_T();
  vlt->cur_vid = 0;
  vlt->vertex_list = vl;
  return vlt;
}

void grin_destroy_vertex_list_iter(GRIN_GRAPH g,
                                   GRIN_VERTEX_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(iter);
  delete _iter;
}

void grin_get_next_vertex_list_iter(GRIN_GRAPH g,
                                    GRIN_VERTEX_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(iter);
  if (_iter->cur_vid < _iter->vertex_list.vertex_num)
    ++_iter->cur_vid;
}

bool grin_is_vertex_list_end(GRIN_GRAPH g, GRIN_VERTEX_LIST_ITERATOR iter) {
  auto _iter = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(iter);
  return _iter->cur_vid == _iter->vertex_list.vertex_num;
}

GRIN_VERTEX grin_get_vertex_from_iter(GRIN_GRAPH g,
                                      GRIN_VERTEX_LIST_ITERATOR iter) {
  auto v = new GRIN_VERTEX_T();
  auto _iter = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(iter);
  v->label = _iter->vertex_list.label;
  v->vid = _iter->cur_vid;
  return v;
}
#endif