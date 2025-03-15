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

#include "grin/include/property/type.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
// Vertex type
bool grin_equal_vertex_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt1,
                            GRIN_VERTEX_TYPE vt2) {
  return (vt1 == vt2);
}

GRIN_VERTEX_TYPE grin_get_vertex_type(GRIN_GRAPH g, GRIN_VERTEX v) {
  return v >> 32;
}

void grin_destroy_vertex_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {}

// Vertex type list
GRIN_VERTEX_TYPE_LIST grin_get_vertex_type_list(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  auto vertex_label_num = _g->g.schema().vertex_label_num();
  for (size_t idx = 0; idx < vertex_label_num; ++idx) {
    vtl->push_back(idx);
  }
  return vtl;
}

void grin_destroy_vertex_type_list(GRIN_GRAPH g, GRIN_VERTEX_TYPE_LIST vtl) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  delete _vtl;
}

GRIN_VERTEX_TYPE_LIST grin_create_vertex_type_list(GRIN_GRAPH g) {
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  return vtl;
}

bool grin_insert_vertex_type_to_list(GRIN_GRAPH g, GRIN_VERTEX_TYPE_LIST vtl,
                                     GRIN_VERTEX_TYPE vt) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  _vtl->push_back(vt);
  return true;
}

size_t grin_get_vertex_type_list_size(GRIN_GRAPH g, GRIN_VERTEX_TYPE_LIST vtl) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  return _vtl->size();
}

GRIN_VERTEX_TYPE grin_get_vertex_type_from_list(GRIN_GRAPH g,
                                                GRIN_VERTEX_TYPE_LIST vtl,
                                                size_t idx) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  return (*_vtl)[idx];
}
#endif

#ifdef GRIN_WITH_VERTEX_TYPE_NAME
const char* grin_get_vertex_type_name(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  std::string type_name = _g->g.schema().get_vertex_label_name(vt);
  auto len = type_name.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", type_name.c_str());
  return out;
}

GRIN_VERTEX_TYPE grin_get_vertex_type_by_name(GRIN_GRAPH g, const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  std::string type_name(name);
  if ((!_g->g.schema().contains_vertex_label(type_name))) {
    return GRIN_NULL_VERTEX_TYPE;
  }
  auto type = _g->g.schema().get_vertex_label_id(type_name);
  return type;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE
GRIN_VERTEX_TYPE_ID grin_get_vertex_type_id(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  return vt;
}

GRIN_VERTEX_TYPE grin_get_vertex_type_by_id(GRIN_GRAPH g,
                                            GRIN_VERTEX_TYPE_ID tid) {
  return tid;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
// Edge type
bool grin_equal_edge_type(GRIN_GRAPH g, GRIN_EDGE_TYPE et1,
                          GRIN_EDGE_TYPE et2) {
  return (et1 == et2);
}

GRIN_EDGE_TYPE grin_get_edge_type(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto src_label = _e->src >> 32;
  auto dst_label = _e->dst >> 32;
  return _g->g.schema().get_edge_triplet_id(src_label, dst_label, _e->label);
}

void grin_destroy_edge_type(GRIN_GRAPH g, GRIN_EDGE_TYPE et) {
  // do nothing
}

// Edge type list
GRIN_EDGE_TYPE_LIST grin_get_edge_type_list(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto etl = new GRIN_EDGE_TYPE_LIST_T();
  auto edge_label_num = _g->g.edge_label_num_;
  auto vertex_label_num = _g->g.vertex_label_num_;
  for (size_t src_label_i = 0; src_label_i < vertex_label_num; ++src_label_i) {
    const auto& src_label = _g->g.schema().get_vertex_label_name(src_label_i);
    for (size_t dst_label_i = 0; dst_label_i < vertex_label_num;
         ++dst_label_i) {
      const auto& dst_label = _g->g.schema().get_vertex_label_name(dst_label_i);
      for (size_t edge_label_i = 0; edge_label_i < edge_label_num;
           ++edge_label_i) {
        const auto& edge_label =
            _g->g.schema().get_edge_label_name(edge_label_i);
        if (_g->g.schema().exist(src_label, dst_label, edge_label)) {
          auto label = _g->g.schema().get_edge_triplet_id(
              src_label_i, dst_label_i, edge_label_i);
          etl->push_back(label);
        }
      }
    }
  }
  return etl;
}

void grin_destroy_edge_type_list(GRIN_GRAPH g, GRIN_EDGE_TYPE_LIST etl) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  delete _etl;
}

GRIN_EDGE_TYPE_LIST grin_create_edge_type_list(GRIN_GRAPH g) {
  auto etl = new GRIN_EDGE_TYPE_LIST_T();
  return etl;
}

bool grin_insert_edge_type_to_list(GRIN_GRAPH g, GRIN_EDGE_TYPE_LIST etl,
                                   GRIN_EDGE_TYPE et) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  _etl->push_back(et);
  return true;
}

size_t grin_get_edge_type_list_size(GRIN_GRAPH g, GRIN_EDGE_TYPE_LIST etl) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  return _etl->size();
}

GRIN_EDGE_TYPE grin_get_edge_type_from_list(GRIN_GRAPH g,
                                            GRIN_EDGE_TYPE_LIST etl,
                                            size_t idx) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  return (*_etl)[idx];
}
#endif

#ifdef GRIN_WITH_EDGE_TYPE_NAME
const char* grin_get_edge_type_name(GRIN_GRAPH g, GRIN_EDGE_TYPE et) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  const auto& schema = _g->g.schema();
  auto edge_triplet_tuple = schema.get_edge_triplet(et);
  auto src_label_i = std::get<0>(edge_triplet_tuple);
  auto dst_label_i = std::get<1>(edge_triplet_tuple);
  auto edge_label_i = std::get<2>(edge_triplet_tuple);
  const auto& edge_label = schema.get_edge_label_name(edge_label_i);
  const auto& src_label = schema.get_vertex_label_name(src_label_i);
  const auto& dst_label = schema.get_vertex_label_name(dst_label_i);
  auto label = src_label + "#" + dst_label + "#" + edge_label;
  auto len = label.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", label.c_str());
  return out;
}

GRIN_EDGE_TYPE grin_get_edge_type_by_name(GRIN_GRAPH g, const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);

  const auto& schema = _g->g.schema();
  std::vector<std::string> vec;
  size_t len = strlen(name);
  std::string ss{};
  for (size_t i = 0; i <= len; ++i) {
    if (name[i] == '#' || name[i] == '\0') {
      vec.emplace_back(ss);
      ss = "";
    } else {
      ss += name[i];
    }
  }

  if (vec.size() != 3) {
    return GRIN_NULL_EDGE_TYPE;
  }
  if ((!schema.contains_vertex_label(vec[0])) ||
      (!schema.contains_vertex_label(vec[1])) ||
      (!schema.contains_edge_label(vec[2]))) {
    return GRIN_NULL_EDGE_TYPE;
  }
  auto src_label = schema.get_vertex_label_id(vec[0]);
  auto dst_label = schema.get_vertex_label_id(vec[1]);
  auto edge_label = schema.get_edge_label_id(vec[2]);
  return schema.get_edge_triplet_id(src_label, dst_label, edge_label);
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_TYPE
GRIN_EDGE_TYPE_ID grin_get_edge_type_id(GRIN_GRAPH g, GRIN_EDGE_TYPE et) {
  return et;
}

GRIN_EDGE_TYPE grin_get_edge_type_by_id(GRIN_GRAPH g, GRIN_EDGE_TYPE_ID etid) {
  return etid;
}
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_WITH_EDGE_PROPERTY)
/** @brief  the src vertex type list */
GRIN_VERTEX_TYPE_LIST grin_get_src_types_by_edge_type(GRIN_GRAPH g,
                                                      GRIN_EDGE_TYPE et) {
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  const auto& schema = _g->g.schema();
  auto triplet = schema.get_edge_triplet(et);
  vtl->emplace_back(std::get<0>(triplet));
  return vtl;
}

/** @brief get the dst vertex type list */
GRIN_VERTEX_TYPE_LIST grin_get_dst_types_by_edge_type(GRIN_GRAPH g,
                                                      GRIN_EDGE_TYPE et) {
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  const auto& schema = _g->g.schema();
  auto triplet = schema.get_edge_triplet(et);
  vtl->emplace_back(std::get<1>(triplet));
  return vtl;
}

/** @brief get the edge type list related to a given pair of vertex types */
GRIN_EDGE_TYPE_LIST grin_get_edge_types_by_vertex_type_pair(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vt1, GRIN_VERTEX_TYPE vt2) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);

  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  const auto& schema = _g->g.schema();
  auto edge_label_num = _g->g.edge_label_num_;
  std::string src_label =
      schema.get_vertex_label_name(static_cast<gs::label_t>(vt1));
  std::string dst_label =
      schema.get_vertex_label_name(static_cast<gs::label_t>(vt2));

  for (size_t edge_label_i = 0; edge_label_i != edge_label_num;
       ++edge_label_i) {
    std::string edge_label =
        schema.get_vertex_label_name(static_cast<gs::label_t>(edge_label_i));
    if (schema.exist(src_label, dst_label, edge_label)) {
      vtl->push_back(schema.get_edge_triplet_id(
          static_cast<gs::label_t>(vt1), static_cast<gs::label_t>(vt2),
          static_cast<gs::label_t>(edge_label_i)));
    }
  }
  return vtl;
}
#endif
///@}
