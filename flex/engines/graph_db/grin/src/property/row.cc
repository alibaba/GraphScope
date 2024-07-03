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

#include "grin/include/property/row.h"

#ifdef GRIN_ENABLE_ROW
void grin_destroy_row(GRIN_GRAPH g, GRIN_ROW r) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  delete _r;
}

int grin_get_int32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const int32_t*>((*_r)[idx]);
}

unsigned int grin_get_uint32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const uint32_t*>((*_r)[idx]);
}

long long int grin_get_int64_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const int64_t*>((*_r)[idx]);
}

unsigned long long int grin_get_uint64_from_row(GRIN_GRAPH g, GRIN_ROW r,
                                                size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const uint64_t*>((*_r)[idx]);
}

float grin_get_float_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const float*>((*_r)[idx]);
}

double grin_get_double_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const double*>((*_r)[idx]);
}

const char* grin_get_string_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  auto s = static_cast<const char*>((*_r)[idx]);
  return s;
}

int grin_get_date32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const int32_t*>((*_r)[idx]);
}

int grin_get_time32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return *static_cast<const int32_t*>((*_r)[idx]);
}

long long int grin_get_timestamp64_from_row(GRIN_GRAPH g, GRIN_ROW r,
                                            size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  return (*static_cast<const int64_t*>((*_r)[idx]));
}

GRIN_ROW grin_create_row(GRIN_GRAPH g) {
  auto r = new GRIN_ROW_T();
  return r;
}

bool grin_insert_int32_to_row(GRIN_GRAPH g, GRIN_ROW r, int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new int32_t(value));
  return true;
}

bool grin_insert_uint32_to_row(GRIN_GRAPH g, GRIN_ROW r, unsigned int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new uint32_t(value));
  return true;
}

bool grin_insert_int64_to_row(GRIN_GRAPH g, GRIN_ROW r, long long int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new int64_t(value));
  return true;
}

bool grin_insert_uint64_to_row(GRIN_GRAPH g, GRIN_ROW r,
                               unsigned long long int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new uint64_t(value));
  return true;
}

bool grin_insert_float_to_row(GRIN_GRAPH g, GRIN_ROW r, float value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new float(value));
  return true;
}

bool grin_insert_double_to_row(GRIN_GRAPH g, GRIN_ROW r, double value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new double(value));
  return true;
}

bool grin_insert_string_to_row(GRIN_GRAPH g, GRIN_ROW r, const char* value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_date32_to_row(GRIN_GRAPH g, GRIN_ROW r, int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new int32_t(value));
  return true;
}

bool grin_insert_time32_to_row(GRIN_GRAPH g, GRIN_ROW r, int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new int32_t(value));
  return true;
}

bool grin_insert_timestamp64_to_row(GRIN_GRAPH g, GRIN_ROW r,
                                    long long int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(new int64_t(value));
  return true;
}
#endif

#if defined(GRIN_ENABLE_ROW) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
/** @brief the value of a property from row by its position in row */
const void* grin_get_value_from_row(GRIN_GRAPH g, GRIN_ROW r, GRIN_DATATYPE dt,
                                    size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  switch (dt) {
  case GRIN_DATATYPE::Bool:
    return static_cast<const bool*>((*_r)[idx]);
  case GRIN_DATATYPE::Int32:
    return static_cast<const int32_t*>((*_r)[idx]);
  case GRIN_DATATYPE::UInt32:
    return static_cast<const uint32_t*>((*_r)[idx]);
  case GRIN_DATATYPE::Int64:
    return static_cast<const int64_t*>((*_r)[idx]);
  case GRIN_DATATYPE::UInt64:
    return static_cast<const uint64_t*>((*_r)[idx]);
  case GRIN_DATATYPE::Float:
    return static_cast<const float*>((*_r)[idx]);
  case GRIN_DATATYPE::Double:
    return static_cast<const double*>((*_r)[idx]);
  case GRIN_DATATYPE::StringView:
    return static_cast<const char*>((*_r)[idx]);
  case GRIN_DATATYPE::Date32:
    return static_cast<const int32_t*>((*_r)[idx]);
  case GRIN_DATATYPE::Time32:
    return static_cast<const int32_t*>((*_r)[idx]);
  case GRIN_DATATYPE::Timestamp64:
    return static_cast<const int64_t*>((*_r)[idx]);
  default:
    return NULL;
  }
  return NULL;
}
#endif
///@}

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_vertex_row(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vid = v & (0xffffffff);
  auto label = v >> 32;

  if (label >= _g->g.vertex_label_num_) {
    return NULL;
  }
  auto& table = _g->g.get_vertex_table(label);
  auto prop_size = table.col_num();
  const auto& types = table.column_types();
  auto r = new GRIN_ROW_T();
  for (size_t prop_id = 0; prop_id < prop_size; prop_id++) {
    auto col = _g->vproperties[label][prop_id];
    auto type = _get_data_type(types[prop_id]);
    switch (type) {
    case GRIN_DATATYPE::Bool: {
      auto _col = static_cast<const gs::BoolColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::Int32: {
      auto _col = static_cast<const gs::IntColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::Int64: {
      auto _col = static_cast<const gs::LongColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::UInt32: {
      auto _col = static_cast<const gs::UIntColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::UInt64: {
      auto _col = static_cast<const gs::ULongColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::StringView: {
      auto _col = static_cast<const gs::StringColumn*>(col);
      auto s = _col->get_view(vid);
      auto len = s.size() + 1;
      char* out = new char[len];
      snprintf(out, len, "%s", s.data());
      r->emplace_back(out);
      break;
    }
    case GRIN_DATATYPE::Timestamp64: {
      auto _col = static_cast<const gs::DateColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::Double: {
      auto _col = static_cast<const gs::DoubleColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    case GRIN_DATATYPE::Float: {
      auto _col = static_cast<const gs::FloatColumn*>(col);
      auto basic_size = _col->basic_buffer_size();
      if (vid < basic_size) {
        r->emplace_back(_col->basic_buffer().data() + vid);
      } else {
        r->emplace_back(_col->extra_buffer().data() + vid - basic_size);
      }
      break;
    }
    default:
      r->emplace_back(static_cast<const void*>(NULL));
    }
  }
  return r;
}
#endif

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_edge_row(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto type = _get_data_type(_e->data.type);
  GRIN_ROW_T* r = new GRIN_ROW_T();
  switch (type) {
  case GRIN_DATATYPE::Bool: {
    r->emplace_back(new bool(_e->data.value.b));
    break;
  }
  case GRIN_DATATYPE::Int32: {
    r->emplace_back(new int(_e->data.value.i));
    break;
  }
  case GRIN_DATATYPE::Int64: {
    r->emplace_back(new int64_t(_e->data.value.l));
    break;
  }
  case GRIN_DATATYPE::UInt32: {
    r->emplace_back(new uint32_t(_e->data.value.ui));
    break;
  }
  case GRIN_DATATYPE::UInt64: {
    r->emplace_back(new uint64_t(_e->data.value.ul));
    break;
  }
  case GRIN_DATATYPE::StringView: {
    auto s = _e->data.value.s;
    auto len = s.size() + 1;
    char* out = new char[len];
    snprintf(out, len, "%s", s.data());

    r->emplace_back(out);
    break;
  }
  case GRIN_DATATYPE::Timestamp64: {
    r->emplace_back(new int64_t(_e->data.value.d.milli_second));
    break;
  }
  case GRIN_DATATYPE::Double: {
    r->emplace_back(new double(_e->data.value.db));
    break;
  }
  case GRIN_DATATYPE::Float: {
    r->emplace_back(new float(_e->data.value.f));
    break;
  }
  default:
    r->emplace_back(static_cast<const void*>(NULL));
  }
  return r;
}
#endif