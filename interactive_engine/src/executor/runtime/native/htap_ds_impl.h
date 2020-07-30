/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ANALYTICAL_ENGINE_HTAP_HTAP_DS_IMPL_H_
#define ANALYTICAL_ENGINE_HTAP_HTAP_DS_IMPL_H_

#include <utility>

#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "global_store_ffi.h"
#include "graph_schema.h"

namespace htap_impl {

using FRAGMENT_TYPE = ::vineyard::ArrowFragment<int64_t, uint64_t>;
using VERTEX_MAP_TYPE = ::vineyard::ArrowVertexMap<int64_t, uint64_t>;

using OID_TYPE = typename FRAGMENT_TYPE::oid_t;
using VID_TYPE = typename FRAGMENT_TYPE::vid_t;
using FRAG_ID_TYPE = ::vineyard::fid_t;
using EID_TYPE = typename FRAGMENT_TYPE::eid_t;
using VERTEX_RANGE_TYPE = std::pair<VID_TYPE, VID_TYPE>;
using VERTEX_TYPE = typename FRAGMENT_TYPE::vertex_t;

struct GraphHandleImpl {
  vineyard::Client* client;
  FRAGMENT_TYPE* fragments;
  VERTEX_MAP_TYPE* vertex_map;
  FRAG_ID_TYPE fnum;
  vineyard::IdParser<VID_TYPE> vid_parser;
  vineyard::IdParser<EID_TYPE> eid_parser;
  vineyard::MGPropertyGraphSchema* schema;
  FRAG_ID_TYPE local_fnum;
  FRAG_ID_TYPE* local_fragments;
  int vertex_label_num;
  int edge_label_num;

  PartitionId channel_num;
  VID_TYPE** vertex_chunk_sizes;
};

inline int get_edge_partition_id(EID_TYPE id, GraphHandleImpl* handle) {
  return handle->eid_parser.GetFid(id);
}

void get_graph_handle(ObjectId id, PartitionId channel_num,
                      GraphHandleImpl* handle);

void free_graph_handle(GraphHandleImpl* handle);

struct GetVertexIteratorImpl {
  VID_TYPE* ids;
  int count;
  int index;
};

void get_vertices(FRAGMENT_TYPE* frag, LabelId* label, VertexId* ids, int count,
                  GetVertexIteratorImpl* out);

void free_get_vertex_iterator(GetVertexIteratorImpl* iter);

int get_vertices_next(GetVertexIteratorImpl* iter, Vertex* v_out);

struct GetAllVerticesIteratorImpl {
  VERTEX_RANGE_TYPE* ranges;
  int range_num;
  int range_id;

  VID_TYPE cur_vertex_id;
};

void get_all_vertices(FRAGMENT_TYPE* frag, PartitionId channel_id,
                      const VID_TYPE* chunk_sizes, LabelId* labels,
                      int labels_count, int64_t limit,
                      GetAllVerticesIteratorImpl* out);

void free_get_all_vertices_iterator(GetAllVerticesIteratorImpl* iter);

int get_all_vertices_next(GetAllVerticesIteratorImpl* iter, Vertex* v_out);

struct PropertiesIteratorImpl {
  GraphHandleImpl* handle;
  arrow::Table* table;
  bool vertex_or_edge;  // true: vertex, false: edge
  LabelId label_id;
  int64_t row_id;
  PropertyId col_num;
  PropertyId col_id;
};

OuterId get_outer_id(FRAGMENT_TYPE* frag, Vertex v);

int get_vertex_property(FRAGMENT_TYPE* frag, Vertex v, PropertyId id,
                        Property* p_out);

void get_vertex_properties(FRAGMENT_TYPE* frag, Vertex v,
                           PropertiesIteratorImpl* iter);

using NBR_TYPE = typename FRAGMENT_TYPE::nbr_unit_t;
// using ADJ_LIST_TYPE = std::pair<const NBR_TYPE*, const NBR_TYPE*>;
struct AdjListUnit {
  const NBR_TYPE* begin;
  const NBR_TYPE* end;
  LabelId label;
} __attribute__((packed));

struct EdgeIteratorImpl {
  // FRAG_ID_TYPE fid;
  FRAGMENT_TYPE* fragment;
  vineyard::IdParser<EID_TYPE>* eid_parser;

  int64_t src;
  AdjListUnit* lists;
  int list_num;

  int list_id;
  const NBR_TYPE* cur_edge;
};

void empty_edge_iterator(EdgeIteratorImpl* iter);

void get_out_edges(FRAGMENT_TYPE* frag,
                   vineyard::IdParser<EID_TYPE>* eid_parser, VertexId src_id,
                   LabelId* labels, int labels_count, int64_t limit,
                   EdgeIteratorImpl* iter);

int out_edge_next(EdgeIteratorImpl* iter, Edge* e_out);

void get_in_edges(FRAGMENT_TYPE* frag, vineyard::IdParser<EID_TYPE>* eid_parser,
                  VertexId dst_id, LabelId* labels, int labels_count,
                  int64_t limit, EdgeIteratorImpl* iter);

int in_edge_next(EdgeIteratorImpl* iter, Edge* e_out);

struct GetAllEdgesIteratorImpl {
  FRAGMENT_TYPE* fragment;
  LabelId* e_labels;
  vineyard::IdParser<EID_TYPE>* eid_parser;
  int e_labels_count;

  int cur_v_label;
  VERTEX_RANGE_TYPE cur_range;

  EdgeIteratorImpl ei;

  const VID_TYPE* chunk_sizes;
  PartitionId channel_id;

  int64_t index;
  int64_t limit;
};

void get_all_edges(FRAGMENT_TYPE* frag, PartitionId channel_id,
                   const VID_TYPE* chunk_sizes,
                   vineyard::IdParser<EID_TYPE>* eid_parser, LabelId* labels,
                   int labels_count, int64_t limit,
                   GetAllEdgesIteratorImpl* iter);

int get_all_edges_next(GetAllEdgesIteratorImpl* iter, Edge* e_out);

void free_edge_iterator(EdgeIteratorImpl* iter);

void free_get_all_edges_iterator(GetAllEdgesIteratorImpl* iter);

EdgeId get_edge_id(FRAGMENT_TYPE* frag, LabelId label, int64_t offset);

int get_edge_property(FRAGMENT_TYPE* frag, LabelId label, int64_t offset,
                      PropertyId id, Property* p_out);

void get_edge_properties(FRAGMENT_TYPE* frag, LabelId label, int64_t offset,
                         PropertiesIteratorImpl* iter);

int properties_next(PropertiesIteratorImpl* iter, Property* p_out);

void free_properties_iterator(PropertiesIteratorImpl* iter);

union PodProperties {
  bool bool_value;
  char char_value;
  int16_t int16_value;
  int int_value;
  int64_t long_value;
  float float_value;
  double double_value;
};

int get_property_as_bool(Property* property, bool* out);
int get_property_as_char(Property* property, char* out);
int get_property_as_short(Property* property, int16_t* out);
int get_property_as_int(Property* property, int* out);
int get_property_as_long(Property* property, int64_t* out);
int get_property_as_float(Property* property, float* out);
int get_property_as_double(Property* property, double* out);
int get_property_as_string(Property* property, const char** out, int* out_len);
int get_property_as_bytes(Property* property, const char** out, int* out_len);
int get_property_as_int_list(Property* property, const int** out, int* out_len);
int get_property_as_long_list(Property* property, const int64_t** out,
                              int* out_len);
int get_property_as_float_list(Property* property, const float** out,
                               int* out_len);
int get_property_as_double_list(Property* property, const double** out,
                                int* out_len);
int get_property_as_string_list(Property* property, const char*** out,
                                const int** out_len, int* out_num);

void free_property(Property* property);

}  // namespace htap_impl

#endif  // ANALYTICAL_ENGINE_HTAP_HTAP_DS_IMPL_H_
