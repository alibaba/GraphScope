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

#include "grin/include/property/topology.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
size_t grin_get_vertex_num_by_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->g.vertex_num(vt);
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
size_t grin_get_edge_num_by_type(GRIN_GRAPH g, GRIN_EDGE_TYPE et) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto edge_triplet_tuple = _g->g.schema().get_edge_triplet(et);
  auto src_label = std::get<0>(edge_triplet_tuple);
  auto dst_label = std::get<1>(edge_triplet_tuple);
  auto edge_label = std::get<2>(edge_triplet_tuple);
  auto oe = _g->g.get_oe_csr(src_label, dst_label, edge_label);
  auto vertex_num = _g->g.vertex_num(src_label);
  size_t edge_num = 0;
  for (size_t i = 0; i < vertex_num; ++i) {
    edge_num += oe->edge_iter(i)->size();
  }
  if (edge_num != 0) {
    return edge_num;
  }
  auto ie = _g->g.get_ie_csr(dst_label, src_label, edge_label);
  vertex_num = _g->g.vertex_num(dst_label);
  for (size_t i = 0; i < vertex_num; ++i) {
    edge_num += ie->edge_iter(i)->size();
  }
  return edge_num;
}
#endif

#if defined(GRIN_ENABLE_VERTEX_LIST) && defined(GRIN_WITH_VERTEX_PROPERTY)
GRIN_VERTEX_LIST grin_get_vertex_list_by_type(GRIN_GRAPH g,
                                              GRIN_VERTEX_TYPE vt) {
  GRIN_VERTEX_LIST vl;
  vl.label = vt;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  vl.vertex_num = _g->g.vertex_num(vt);
  return vl;
}
#endif

#if defined(GRIN_ENABLE_EDGE_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_EDGE_LIST grin_get_edge_list_by_type(GRIN_GRAPH, GRIN_EDGE_TYPE);
#endif

#if defined(GRIN_ENABLE_ADJACENT_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_ADJACENT_LIST grin_get_adjacent_list_by_edge_type(GRIN_GRAPH g,
                                                       GRIN_DIRECTION dir,
                                                       GRIN_VERTEX v,
                                                       GRIN_EDGE_TYPE et) {
  GRIN_ADJACENT_LIST adj_list;
  adj_list.v = v;
  adj_list.dir = dir;
  adj_list.edge_label = et;
  return adj_list;
}
#endif