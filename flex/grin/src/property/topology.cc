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

#include "grin/include/include/property/topology.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
size_t grin_get_vertex_num_by_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_num(vt);
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
size_t grin_get_edge_num_by_type(GRIN_GRAPH g, GRIN_EDGE_TYPE et){
  return 0;
}
#endif

#if defined(GRIN_ENABLE_VERTEX_LIST) && defined(GRIN_WITH_VERTEX_PROPERTY)
GRIN_VERTEX_LIST grin_get_vertex_list_by_type(GRIN_GRAPH g,
                                              GRIN_VERTEX_TYPE vt) {
  GRIN_VERTEX_LIST gvl;
  gvl.label = vt;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  gvl.vertex_num = _g->vertex_num(vt);
  return gvl;
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
  GRIN_ADJACENT_LIST_T* adj_list = new GRIN_ADJACENT_LIST_T();
  adj_list->v = v;
  adj_list->dir = dir;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  
  const auto &schema =  _g->schema();
  if(dir == GRIN_DIRECTION::OUT){
    std::string src_label =
        schema.get_vertex_label_name(v.label);
    std::string edge_label = schema.get_edge_label_name(et);
    for(size_t dst_label_i = 0; dst_label_i != schema.vertex_label_num(); ++dst_label_i){
        std::string dst_label = schema.get_vertex_label_name(static_cast<gs::label_t>(dst_label_i));
        if (!schema.exist(src_label, dst_label, edge_label)) {
            continue;
        }
        auto label = (static_cast<unsigned>(et) << 16) + dst_label_i; 
        adj_list->edges_label.emplace_back(label);
    }
  } else{
    std::string dst_label =
        schema.get_vertex_label_name(v.label);
    std::string edge_label = schema.get_edge_label_name(et);
    for(size_t src_label_i = 0; src_label_i != schema.vertex_label_num(); ++src_label_i){
        std::string src_label = schema.get_vertex_label_name(static_cast<gs::label_t>(src_label_i));
        if (!schema.exist(src_label, dst_label, edge_label)) {
            continue;
        }
        auto label = (static_cast<unsigned>(et) << 16) + src_label_i; 
        adj_list->edges_label.emplace_back(label);
    }
  }
 
  return adj_list;
}
#endif