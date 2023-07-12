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


void grin_destroy_string_value(GRIN_GRAPH g, const char* value) {}

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
  if(table.get_column(name) == nullptr){
    return GRIN_NULL_VERTEX_PROPERTY;
  }
  auto gvp = new GRIN_VERTEX_PROPERTY_T();
  gvp->name = std::string(name);
  gvp->label = vt;
  return gvp;
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
      GRIN_VERTEX_PROPERTY_T gvp;
      gvp.name = _name;
      gvp.label = idx;
      vps->emplace_back(gvp);
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
  return (_vp1->name == _vp2->name) && (_vp1->label == _vp2->label);
}

void grin_destroy_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp) {
    auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
    delete _vp;
}

GRIN_DATATYPE grin_get_vertex_property_datatype(GRIN_GRAPH g,
                                                GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  auto& table = _g->get_vertex_table(_vp->label);
  const auto& property_names = table.column_names();
  const auto& property_types = table.column_types();
  for(size_t i = 0; i < property_names.size(); ++i){
    if(property_names.at(i) == _vp->name){
        return _get_data_type(property_types.at(i));
    } 
  }
  grin_error_code = UNKNOWN_DATATYPE;
  return GRIN_DATATYPE::Undefined; 
}

int grin_get_vertex_property_value_of_int32(GRIN_GRAPH g, GRIN_VERTEX v,
                                            GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  if(v.label != _vp->label){
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  auto col = std::dynamic_pointer_cast<gs::IntColumn>(table.get_column(_vp->name));
  if(col == nullptr){
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return col->get_view(v.vid);
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
  if(v.label != _vp->label){
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  auto col = std::dynamic_pointer_cast<gs::LongColumn>(table.get_column(_vp->name));
  if(col == nullptr){
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return col->get_view(v.vid);
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
  if(v.label != _vp->label){
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  
  auto col = std::dynamic_pointer_cast<gs::DoubleColumn>(table.get_column(_vp->name));
  if(col == nullptr){
    grin_error_code = INVALID_VALUE;
    return 0.0;
  }
  return col->get_view(v.vid);
}

const char* grin_get_vertex_property_value_of_string(GRIN_GRAPH g,
                                                     GRIN_VERTEX v,
                                                     GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  if(v.label != _vp->label){
    grin_error_code = INVALID_VALUE;
    return NULL;
  }
  auto& table = _g->get_vertex_table(_vp->label);
  auto col = std::dynamic_pointer_cast<gs::StringColumn>(table.get_column(_vp->name));
  if(col == nullptr){
    grin_error_code = INVALID_VALUE;
    return NULL;
  }
  return col->get_view(v.vid).data();

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
  if(v.label != _vp->label){
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  
  auto& table = _g->get_vertex_table(_vp->label);
  auto col = std::dynamic_pointer_cast<gs::DateColumn>(table.get_column(_vp->name));
  if(col == nullptr){
    grin_error_code = INVALID_VALUE;
    return 0;
  }
  return col->get_view(v.vid).milli_second;
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
  auto type = grin_get_vertex_property_datatype(g,vp);
  switch(type){
    case GRIN_DATATYPE::Int32:{
        auto _col = std::dynamic_pointer_cast<gs::IntColumn>(col);
        return _col->buffer().data() + v.vid;
    }
    case GRIN_DATATYPE::Int64:{
        auto _col = std::dynamic_pointer_cast<gs::LongColumn>(col);
        return _col->buffer().data() + v.vid;
    }
    case GRIN_DATATYPE::String:{
        auto _col = std::dynamic_pointer_cast<gs::StringColumn>(col);
        return _col->buffer()[v.vid].data();
    }
    case GRIN_DATATYPE::Timestamp64:{
       auto _col = std::dynamic_pointer_cast<gs::DateColumn>(col);
       return _col->buffer().data() + v.vid;
    }
    case GRIN_DATATYPE::Double:{
      auto _col = std::dynamic_pointer_cast<gs::DoubleColumn>(col);
      return _col->buffer().data() + v.vid;
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

void grin_destroy_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep){}

GRIN_DATATYPE grin_get_edge_property_datatype(GRIN_GRAPH g,
                                              GRIN_EDGE_PROPERTY ep){
    auto _g = static_cast<GRIN_GRAPH_T*>(g);
    const auto& type = _g->schema().get_edge_property(ep >> 16, (ep >> 8) & 0xff, ep & 0xff);
    return _get_data_type(type);
}

int grin_get_edge_property_value_of_int32(GRIN_GRAPH g, GRIN_EDGE e,
                                          GRIN_EDGE_PROPERTY ep){
    auto _e = static_cast<GRIN_EDGE_T*>(e);
    if(_get_data_type(_e->data.type) != GRIN_DATATYPE::Int32){
      grin_error_code = INVALID_VALUE;
      return 0;
    }
    return _e->data.value.i;
}

unsigned int grin_get_edge_property_value_of_uint32(GRIN_GRAPH g, GRIN_EDGE e,
                                                    GRIN_EDGE_PROPERTY ep){
    grin_error_code = INVALID_VALUE;
    return 0;
}

long long int grin_get_edge_property_value_of_int64(GRIN_GRAPH g, GRIN_EDGE e,
                                                    GRIN_EDGE_PROPERTY ep){
    auto _e = static_cast<GRIN_EDGE_T*>(e);
    if(_get_data_type(_e->data.type) != GRIN_DATATYPE::Int64){
      grin_error_code = INVALID_VALUE;
      return 0;
    }
    return _e->data.value.l;
}

unsigned long long int grin_get_edge_property_value_of_uint64(
    GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep){
    grin_error_code = INVALID_VALUE;
    return 0;
}


float grin_get_edge_property_value_of_float(GRIN_GRAPH g, GRIN_EDGE e,
                                            GRIN_EDGE_PROPERTY ep){
    grin_error_code = INVALID_VALUE;
    return 0.0;
}
double grin_get_edge_property_value_of_double(GRIN_GRAPH g, GRIN_EDGE e,
                                              GRIN_EDGE_PROPERTY ep){
    auto _e = static_cast<GRIN_EDGE_T*>(e);
    if(_get_data_type(_e->data.type) != GRIN_DATATYPE::Double){
      grin_error_code = INVALID_VALUE;
      return 0.0;
    }
    return _e->data.value.db;
}

const char* grin_get_edge_property_value_of_string(GRIN_GRAPH g, GRIN_EDGE e,
                                                   GRIN_EDGE_PROPERTY ep){
    auto _e = static_cast<GRIN_EDGE_T*>(e);
    if(_get_data_type(_e->data.type) != GRIN_DATATYPE::String){
      grin_error_code = INVALID_VALUE;
      return NULL;
    }
    //@TODO 怎么返回？
    return NULL;
}

int grin_get_edge_property_value_of_date32(GRIN_GRAPH g, GRIN_EDGE e,
                                           GRIN_EDGE_PROPERTY ep){
    grin_error_code = INVALID_VALUE;
    return 0;
}
int grin_get_edge_property_value_of_time32(GRIN_GRAPH g, GRIN_EDGE e,
                                           GRIN_EDGE_PROPERTY ep){
    grin_error_code = INVALID_VALUE;
    return 0;
}

long long int grin_get_edge_property_value_of_timestamp64(
    GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep){
      auto _e = static_cast<GRIN_EDGE_T*>(e);
      if(_get_data_type(_e->data.type) != GRIN_DATATYPE::Timestamp64){
        grin_error_code = INVALID_VALUE;
        return 0;
      }
      return _e->data.value.d.milli_second;
  }

GRIN_EDGE_TYPE grin_get_edge_type_from_property(GRIN_GRAPH g,
                                                GRIN_EDGE_PROPERTY ep){
    return ep;
}
#endif

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_edge_property_value(GRIN_GRAPH g, GRIN_EDGE e,
                                         GRIN_EDGE_PROPERTY ep){
    return NULL;
}
#endif