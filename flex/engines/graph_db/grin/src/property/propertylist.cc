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

#include "grin/include/include/property/propertylist.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_property_list_by_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& table = _g->get_vertex_table(vt);

  auto vertex_prop_num = table.col_num();
  GRIN_VERTEX_PROPERTY_LIST_T* vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  const auto& prop_names = table.column_names();
  const auto& prop_types = table.column_types();
  for (size_t i = 0; i < vertex_prop_num; ++i) {
    GRIN_VERTEX_PROPERTY_T vpt;
    vpt.name = prop_names[i];
    vpt.label = vt;
    vpt.dt = _get_data_type(prop_types[i]);
    vpl->emplace_back(vpt);
  }
  return vpl;
}

size_t grin_get_vertex_property_list_size(GRIN_GRAPH g,
                                          GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  return _vpl->size();
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_from_list(
    GRIN_GRAPH g, GRIN_VERTEX_PROPERTY_LIST vpl, size_t idx) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  GRIN_VERTEX_PROPERTY_T* vp = new GRIN_VERTEX_PROPERTY_T();
  *vp = (*_vpl)[idx];
  return vp;
}

GRIN_VERTEX_PROPERTY_LIST grin_create_vertex_property_list(GRIN_GRAPH g) {
  return new GRIN_VERTEX_PROPERTY_LIST_T();
}

void grin_destroy_vertex_property_list(GRIN_GRAPH g,
                                       GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  delete _vpl;
}

bool grin_insert_vertex_property_to_list(GRIN_GRAPH g,
                                         GRIN_VERTEX_PROPERTY_LIST vpl,
                                         GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto p = *_vp;
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  _vpl->push_back(p);
  return true;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_id(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vt, GRIN_VERTEX_PROPERTY_ID pid) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);

  auto& table = _g->get_vertex_table(vt);
  auto vertex_prop_num = table.col_num();

  if (pid >= vertex_prop_num) {
    return GRIN_NULL_VERTEX_PROPERTY;
  }
  const auto& prop_names = table.column_names();
  const auto& prop_types = table.column_types();
  GRIN_VERTEX_PROPERTY_T* vpt = new GRIN_VERTEX_PROPERTY_T();
  vpt->name = prop_names[pid];
  vpt->label = vt;
  vpt->dt = _get_data_type(prop_types[pid]);
  return vpt;
}

GRIN_VERTEX_PROPERTY_ID grin_get_vertex_property_id(GRIN_GRAPH g,
                                                    GRIN_VERTEX_TYPE vt,
                                                    GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& table = _g->get_vertex_table(vt);
  const auto& prop_names = table.column_names();
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  for (size_t i = 0; i < prop_names.size(); ++i) {
    if (prop_names.at(i) == _vp->name) {
      return i;
    }
  }
  return GRIN_NULL_VERTEX_PROPERTY_ID;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
GRIN_EDGE_PROPERTY_LIST grin_get_edge_property_list_by_type(GRIN_GRAPH g,
                                                            GRIN_EDGE_TYPE et) {
  GRIN_EDGE_PROPERTY_LIST_T* p = new GRIN_EDGE_PROPERTY_LIST_T();

  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto src_label_i = et >> 16;
  auto src_label = _g->schema().get_vertex_label_name(src_label_i);
  auto dst_label_i = (et >> 8) & (0xff);
  auto dst_label = _g->schema().get_vertex_label_name(dst_label_i);
  auto edge_label_i = et & 0xff;
  auto edge_label = _g->schema().get_edge_label_name(edge_label_i);
  auto sz =
      _g->schema().get_edge_properties(src_label, dst_label, edge_label).size();
  for (auto i = 0; i < sz; ++i) {
    p->emplace_back(et + (i << 24));
  }
  return p;
}

size_t grin_get_edge_property_list_size(GRIN_GRAPH g,
                                        GRIN_EDGE_PROPERTY_LIST epl) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  return _epl->size();
}

GRIN_EDGE_PROPERTY grin_get_edge_property_from_list(GRIN_GRAPH g,
                                                    GRIN_EDGE_PROPERTY_LIST epl,
                                                    size_t idx) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  if (_epl->size() <= idx) {
    return GRIN_NULL_EDGE_PROPERTY;
  }
  return (*_epl)[idx];
}

GRIN_EDGE_PROPERTY_LIST grin_create_edge_property_list(GRIN_GRAPH g) {
  return new GRIN_EDGE_PROPERTY_LIST_T();
}

void grin_destroy_edge_property_list(GRIN_GRAPH g,
                                     GRIN_EDGE_PROPERTY_LIST epl) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  delete _epl;
}

bool grin_insert_edge_property_to_list(GRIN_GRAPH g,
                                       GRIN_EDGE_PROPERTY_LIST epl,
                                       GRIN_EDGE_PROPERTY ep) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  _epl->emplace_back(ep);
  return true;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
GRIN_EDGE_PROPERTY grin_get_edge_property_by_id(GRIN_GRAPH g, GRIN_EDGE_TYPE et,
                                                GRIN_EDGE_PROPERTY_ID pid) {
  return NULL;
}

GRIN_EDGE_PROPERTY_ID grin_get_edge_property_id(GRIN_GRAPH g, GRIN_EDGE_TYPE et,
                                                GRIN_EDGE_PROPERTY ep) {
  return 0;
}
#endif