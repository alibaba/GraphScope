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

#include "grin/include/include/common/error.h"
#include "grin/include/include/property/property.h"

void grin_destroy_string_value(GRIN_GRAPH g, const char* value) {
  delete value;
}

#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
const char* grin_get_vertex_property_name(GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype,
                                          GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  const auto& name = _vp->name;
  auto len = name.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", name.c_str());
  return out;
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_name(GRIN_GRAPH g,
                                                      GRIN_VERTEX_TYPE vt,
                                                      const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& table = _g->get_vertex_table(vt);
  auto col = table.get_column(name);
  if (col == nullptr) {
    return GRIN_NULL_VERTEX_PROPERTY;
  }
  auto vp = new GRIN_VERTEX_PROPERTY_T();
  vp->name = std::string(name);
  vp->label = vt;
  vp->dt = _get_data_type(col->type());
  return vp;
}

GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_properties_by_name(GRIN_GRAPH g,
                                                             const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  std::string prop_name(name);
  auto vps = new GRIN_VERTEX_PROPERTY_LIST_T();
  std::string _name = std::string(name);
  for (auto idx = 0; idx < _g->schema().vertex_label_num(); idx++) {
    auto& table = _g->get_vertex_table(static_cast<GRIN_VERTEX_TYPE>(idx));

    auto col = table.get_column(name);

    if (col != nullptr) {
      GRIN_VERTEX_PROPERTY_T vp;
      vp.name = _name;
      vp.label = idx;
      vp.dt = _get_data_type(col->type());
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

#ifdef GRIN_WITH_EDGE_PROPERTY_NAME
const char* grin_get_edge_property_name(GRIN_GRAPH g, GRIN_EDGE_TYPE etype,
                                        GRIN_EDGE_PROPERTY ep) {
  return static_cast<const char*>(NULL);
}

GRIN_EDGE_PROPERTY grin_get_edge_property_by_name(GRIN_GRAPH g,
                                                  GRIN_EDGE_TYPE et,
                                                  const char* name) {
  return NULL;
}

GRIN_EDGE_PROPERTY_LIST grin_get_edge_properties_by_name(GRIN_GRAPH g,
                                                         const char* name) {
  return NULL;
}
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
bool grin_equal_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp1,
                                GRIN_VERTEX_PROPERTY vp2) {
  auto _vp1 = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp1);
  auto _vp2 = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp2);
  return (_vp1->name == _vp2->name) && (_vp1->label == _vp2->label) &&
         (_vp1->dt == _vp2->dt);
}

void grin_destroy_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  delete _vp;
}

/**
 * @TODO add type for GRIN_VERTEX_PROPERTY_T
 */
GRIN_DATATYPE grin_get_vertex_property_datatype(GRIN_GRAPH g,
                                                GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  return _vp->dt;
}

int grin_get_vertex_property_value_of_int32(GRIN_GRAPH g, GRIN_VERTEX v,
                                            GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (_v->label != _vp->label || _vp->dt != GRIN_DATATYPE::Int32) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  auto col =
      std::dynamic_pointer_cast<gs::IntColumn>(table.get_column(_vp->name));
  if (col == nullptr) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return col->get_view(_v->vid);
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
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (_v->label != _vp->label || _vp->dt != GRIN_DATATYPE::Int64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  auto col =
      std::dynamic_pointer_cast<gs::LongColumn>(table.get_column(_vp->name));
  if (col == nullptr) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return col->get_view(_v->vid);
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
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (_v->label != _vp->label || _vp->dt != GRIN_DATATYPE::Double) {
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }
  auto& table = _g->get_vertex_table(_vp->label);

  auto col =
      std::dynamic_pointer_cast<gs::DoubleColumn>(table.get_column(_vp->name));
  if (col == nullptr) {
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }
  return col->get_view(_v->vid);
}

const char* grin_get_vertex_property_value_of_string(GRIN_GRAPH g,
                                                     GRIN_VERTEX v,
                                                     GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (_v->label != _vp->label || _vp->dt != GRIN_DATATYPE::String) {
    grin_error_code = INVALID_VALUE;
    return NULL;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  auto col =
      std::dynamic_pointer_cast<gs::StringColumn>(table.get_column(_vp->name));
  if (col == nullptr) {
    grin_error_code = INVALID_VALUE;
    return NULL;
  }
  auto s = col->get_view(_v->vid);
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
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (_v->label != _vp->label || _vp->dt != GRIN_DATATYPE::Timestamp64) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }

  auto& table = _g->get_vertex_table(_vp->label);
  auto col =
      std::dynamic_pointer_cast<gs::DateColumn>(table.get_column(_vp->name));
  if (col == nullptr) {
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return col->get_view(_v->vid).milli_second;
}

GRIN_VERTEX_TYPE grin_get_vertex_type_from_property(GRIN_GRAPH g,
                                                    GRIN_VERTEX_PROPERTY vp) {
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  return _vp->label;
}
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_vertex_property_value(GRIN_GRAPH g, GRIN_VERTEX v,
                                           GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto& table = _g->get_vertex_table(_vp->label);
  const auto& col = table.get_column(_vp->name);
  auto type = grin_get_vertex_property_datatype(g, vp);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  switch (type) {
  case GRIN_DATATYPE::Int32: {
    auto _col = std::dynamic_pointer_cast<gs::IntColumn>(col);
    return _col->buffer().data() + _v->vid;
  }
  case GRIN_DATATYPE::Int64: {
    auto _col = std::dynamic_pointer_cast<gs::LongColumn>(col);
    return _col->buffer().data() + _v->vid;
  }
  case GRIN_DATATYPE::String: {
    auto _col = std::dynamic_pointer_cast<gs::StringColumn>(col);
    return _col->buffer()[_v->vid].data();
  }
  case GRIN_DATATYPE::Timestamp64: {
    auto _col = std::dynamic_pointer_cast<gs::DateColumn>(col);
    return _col->buffer().data() + _v->vid;
  }
  case GRIN_DATATYPE::Double: {
    auto _col = std::dynamic_pointer_cast<gs::DoubleColumn>(col);
    return _col->buffer().data() + _v->vid;
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
  const auto& src_label = _g->schema().get_vertex_label_name(src_label_i);
  auto dst_label_i = (ep >> 8) & 0xff;
  const auto& dst_label = _g->schema().get_vertex_label_name(dst_label_i);
  auto edge_label_i = ep & 0xff;
  const auto& edge_label = _g->schema().get_edge_label_name(edge_label_i);
  const auto& type =
      _g->schema().get_edge_properties(src_label, dst_label, edge_label);
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
  grin_error_code = INVALID_VALUE;
  return 0;
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
  grin_error_code = INVALID_VALUE;
  return 0;
}

float grin_get_edge_property_value_of_float(GRIN_GRAPH g, GRIN_EDGE e,
                                            GRIN_EDGE_PROPERTY ep) {
  grin_error_code = INVALID_VALUE;
  return 0.0;
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