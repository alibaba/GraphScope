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

#include "grin/include/common/error.h"
#include "grin/include/property/property.h"

void grin_destroy_string_value(GRIN_GRAPH g, const char* value) {
  delete value;
}

#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
const char* grin_get_vertex_property_name(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt,
                                          GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& table = _g->g.get_vertex_table(vt);

  const auto& name = table.column_name(vp & (0xff));
  auto len = name.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", name.c_str());
  return out;
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_name(GRIN_GRAPH g,
                                                      GRIN_VERTEX_TYPE vt,
                                                      const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& table = _g->g.get_vertex_table(vt);
  auto col = table.get_column(name);
  if (col == nullptr) {
    return GRIN_NULL_VERTEX_PROPERTY;
  }
  GRIN_VERTEX_PROPERTY vp;
  vp = table.get_column_id_by_name(name);
  vp += (vt * 1u) << 8;
  vp += (_get_data_type(col->type()) * 1u) << 16;
  return vp;
}

GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_properties_by_name(GRIN_GRAPH g,
                                                             const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  std::string prop_name(name);
  auto vps = new GRIN_VERTEX_PROPERTY_LIST_T();
  std::string _name = std::string(name);
  for (auto idx = 0; idx < _g->g.vertex_label_num_; idx++) {
    auto& table = _g->g.get_vertex_table(static_cast<GRIN_VERTEX_TYPE>(idx));

    auto col = table.get_column(name);

    if (col != nullptr) {
      GRIN_VERTEX_PROPERTY vp;
      vp = table.get_column_id_by_name(name);
      vp += (idx * 1u) << 8;
      vp += (_get_data_type(col->type()) * 1u) << 16;
      vps->emplace_back(vp);
    }
  }
  if (vps->size() == 0) {
    delete vps;
    return GRIN_NULL_VERTEX_PROPERTY_LIST;
  }
  return vps;
}
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
bool grin_equal_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp1,
                                GRIN_VERTEX_PROPERTY vp2) {
  return vp1 == vp2;
}

void grin_destroy_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp) {}

/**
 * @TODO add type for GRIN_VERTEX_PROPERTY_T
 */
GRIN_DATATYPE grin_get_vertex_property_datatype(GRIN_GRAPH g,
                                                GRIN_VERTEX_PROPERTY vp) {
  return (GRIN_DATATYPE) (vp >> 16);
}

int grin_get_vertex_property_value_of_int32(GRIN_GRAPH g, GRIN_VERTEX v,
                                            GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto label = v >> 32;
  auto vid = v & (0xffffffff);
  auto plabel = (vp >> 8) & (0xff);
  auto pdt = (vp >> 16);
  auto pid = vp & (0xff);
  if (label != plabel || pdt != GRIN_DATATYPE::Int32) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  if (label >= _g->g.vertex_label_num_ ||
      pid >= _g->vproperties[label].size()) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto pcol = _g->vproperties[label][pid];
  if (pcol == NULL) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto col = static_cast<const gs::IntColumn*>(pcol);
  return col->get_view(vid);
}

unsigned int grin_get_vertex_property_value_of_uint32(GRIN_GRAPH g,
                                                      GRIN_VERTEX v,
                                                      GRIN_VERTEX_PROPERTY vp) {
  grin_error_code = INVALID_VALUE;
  return 0;
}

long long int grin_get_vertex_property_value_of_int64(GRIN_GRAPH g,
                                                      GRIN_VERTEX v,
                                                      GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto label = v >> 32;
  auto vid = v & (0xffffffff);
  auto plabel = (vp >> 8) & (0xff);
  auto pdt = (vp >> 16);
  auto pid = vp & (0xff);

  if (label != plabel || pdt != GRIN_DATATYPE::Int64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }

  if (label >= _g->g.vertex_label_num_ ||
      pid >= _g->vproperties[label].size()) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto pcol = _g->vproperties[label][pid];
  if (pcol == NULL) {
    grin_error_code = INVALID_VALUE;
    return 0.0f;
  }
  auto col = static_cast<const gs::LongColumn*>(pcol);
  return col->get_view(vid);
}

unsigned long long int grin_get_vertex_property_value_of_uint64(
    GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  grin_error_code = INVALID_VALUE;
  return 0;
}

float grin_get_vertex_property_value_of_float(GRIN_GRAPH g, GRIN_VERTEX v,
                                              GRIN_VERTEX_PROPERTY vp) {
  grin_error_code = INVALID_VALUE;
  return 0.0f;
}

double grin_get_vertex_property_value_of_double(GRIN_GRAPH g, GRIN_VERTEX v,
                                                GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto label = v >> 32;
  auto vid = v & (0xffffffff);
  auto plabel = (vp >> 8) & (0xff);
  auto pdt = (vp >> 16);
  auto pid = vp & (0xff);

  if (label != plabel || pdt != GRIN_DATATYPE::Double) {
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }

  if (label >= _g->g.vertex_label_num_ ||
      pid >= _g->vproperties[label].size()) {
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }
  auto pcol = _g->vproperties[label][pid];
  if (pcol == NULL) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto col = static_cast<const gs::DoubleColumn*>(pcol);
  return col->get_view(vid);
}

const char* grin_get_vertex_property_value_of_string(GRIN_GRAPH g,
                                                     GRIN_VERTEX v,
                                                     GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto label = v >> 32;
  auto vid = v & (0xffffffff);
  auto plabel = (vp >> 8) & (0xff);
  auto pdt = (vp >> 16);
  auto pid = vp & (0xff);

  if (label != plabel || pdt != GRIN_DATATYPE::String) {
    grin_error_code = INVALID_VALUE;
    return NULL;
  }

  if (label >= _g->g.vertex_label_num_ ||
      pid >= _g->vproperties[label].size()) {
    grin_error_code = INVALID_VALUE;
    return "";
  }
  auto pcol = _g->vproperties[label][pid];
  if (pcol == NULL) {
    grin_error_code = INVALID_VALUE;
    return "";
  }
  auto col = static_cast<const gs::StringColumn*>(pcol);

  auto s = col->get_view(vid);
  auto len = s.size() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", s.data());
  return out;
}

int grin_get_vertex_property_value_of_date32(GRIN_GRAPH g, GRIN_VERTEX v,
                                             GRIN_VERTEX_PROPERTY vp) {
  grin_error_code = INVALID_VALUE;
  return 0;
}

int grin_get_vertex_property_value_of_time32(GRIN_GRAPH g, GRIN_VERTEX v,
                                             GRIN_VERTEX_PROPERTY vp) {
  grin_error_code = INVALID_VALUE;
  return 0;
}

long long int grin_get_vertex_property_value_of_timestamp64(
    GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto label = v >> 32;
  auto vid = v & (0xffffffff);
  auto plabel = (vp >> 8) & (0xff);
  auto pdt = (vp >> 16);
  auto pid = vp & (0xff);

  if (label != plabel || pdt != GRIN_DATATYPE::Timestamp64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }

  if (label >= _g->g.vertex_label_num_ ||
      pid >= _g->vproperties[label].size()) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto pcol = _g->vproperties[label][pid];
  if (pcol == NULL) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto col = static_cast<const gs::DateColumn*>(pcol);
  return col->get_view(vid).milli_second;
}

GRIN_VERTEX_TYPE grin_get_vertex_type_from_property(GRIN_GRAPH g,
                                                    GRIN_VERTEX_PROPERTY vp) {
  return (vp >> 8) & (0xff);
}
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_vertex_property_value(GRIN_GRAPH g, GRIN_VERTEX v,
                                           GRIN_VERTEX_PROPERTY vp) {
  auto plabel = (vp >> 8) & (0xff);
  auto type = (vp >> 16);
  auto pid = vp & (0xff);

  auto _g = static_cast<GRIN_GRAPH_T*>(g);

  if (plabel >= _g->g.vertex_label_num_ ||
      pid >= _g->vproperties[plabel].size()) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto col = _g->vproperties[plabel][pid];
  if (col == NULL) {
    grin_error_code = UNKNOWN_DATATYPE;
    return 0;
  }

  auto vid = v & (0xffffffff);

  switch (type) {
  case GRIN_DATATYPE::Bool: {
    auto _col = static_cast<const gs::BoolColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::Int32: {
    auto _col = static_cast<const gs::IntColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::UInt32: {
    auto _col = static_cast<const gs::UIntColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::Int64: {
    auto _col = static_cast<const gs::LongColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::UInt64: {
    auto _col = static_cast<const gs::ULongColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::String: {
    auto _col = static_cast<const gs::StringColumn*>(col);
    auto s = _col->get_view(vid);
    auto len = s.size() + 1;
    char* out = new char[len];
    snprintf(out, len, "%s", s.data());
    return out;
  }
  case GRIN_DATATYPE::Timestamp64: {
    auto _col = static_cast<const gs::DateColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::Double: {
    auto _col = static_cast<const gs::DoubleColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  case GRIN_DATATYPE::Float: {
    auto _col = static_cast<const gs::FloatColumn*>(col);
    auto basic_size = _col->basic_buffer_size();
    if (vid < basic_size) {
      return _col->basic_buffer().data() + vid;
    } else {
      return _col->extra_buffer().data() + vid - basic_size;
    }
  }
  default:
    grin_error_code = UNKNOWN_DATATYPE;
    return NULL;
  }
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
bool grin_equal_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep1,
                              GRIN_EDGE_PROPERTY ep2) {
  return ep1 == ep2;
}

void grin_destroy_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep) {}

GRIN_DATATYPE grin_get_edge_property_datatype(GRIN_GRAPH g,
                                              GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto src_label_i = (ep >> 16) & 0xff;
  const auto& src_label = _g->g.schema().get_vertex_label_name(src_label_i);
  auto dst_label_i = (ep >> 8) & 0xff;
  const auto& dst_label = _g->g.schema().get_vertex_label_name(dst_label_i);
  auto edge_label_i = ep & 0xff;
  const auto& edge_label = _g->g.schema().get_edge_label_name(edge_label_i);
  const auto& type =
      _g->g.schema().get_edge_properties(src_label, dst_label, edge_label);
  auto idx = ep >> 24;
  return _get_data_type(type[idx]);
}

int grin_get_edge_property_value_of_int32(GRIN_GRAPH g, GRIN_EDGE e,
                                          GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::Int32) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return _e->data.value.i;
}

unsigned int grin_get_edge_property_value_of_uint32(GRIN_GRAPH g, GRIN_EDGE e,
                                                    GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::UInt32) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return _e->data.value.ui;
}

long long int grin_get_edge_property_value_of_int64(GRIN_GRAPH g, GRIN_EDGE e,
                                                    GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::Int64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return _e->data.value.l;
}

unsigned long long int grin_get_edge_property_value_of_uint64(
    GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::UInt64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return _e->data.value.ul;
}

float grin_get_edge_property_value_of_float(GRIN_GRAPH g, GRIN_EDGE e,
                                            GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::Float) {
    grin_error_code = INVALID_VALUE;
    return 0.0f;
  }
  return _e->data.value.f;
}
double grin_get_edge_property_value_of_double(GRIN_GRAPH g, GRIN_EDGE e,
                                              GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::Double) {
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }
  return _e->data.value.db;
}

const char* grin_get_edge_property_value_of_string(GRIN_GRAPH g, GRIN_EDGE e,
                                                   GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::String) {
    grin_error_code = INVALID_VALUE;
    return NULL;
  }
  auto s = _e->data.value.s;
  auto len = s.size() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", s.data());
  return out;
}

int grin_get_edge_property_value_of_date32(GRIN_GRAPH g, GRIN_EDGE e,
                                           GRIN_EDGE_PROPERTY ep) {
  grin_error_code = INVALID_VALUE;
  return 0;
}
int grin_get_edge_property_value_of_time32(GRIN_GRAPH g, GRIN_EDGE e,
                                           GRIN_EDGE_PROPERTY ep) {
  grin_error_code = INVALID_VALUE;
  return 0;
}

long long int grin_get_edge_property_value_of_timestamp64(
    GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto idx = ep >> 24;
  if (idx > 0 || _get_data_type(_e->data.type) != GRIN_DATATYPE::Timestamp64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return _e->data.value.d.milli_second;
}

GRIN_EDGE_TYPE grin_get_edge_type_from_property(GRIN_GRAPH g,
                                                GRIN_EDGE_PROPERTY ep) {
  return ep & (~0xff000000);
}
#endif

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_edge_property_value(GRIN_GRAPH g, GRIN_EDGE e,
                                         GRIN_EDGE_PROPERTY ep) {
  return NULL;
}
#endif