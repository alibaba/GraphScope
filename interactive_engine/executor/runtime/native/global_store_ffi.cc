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
#include "global_store_ffi.h"
#include "htap_ds_impl.h"

#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif

GraphHandle get_graph_handle(ObjectId object_id, PartitionId channel_num) {
  // FIXME: handle exception here
  GraphHandle handle = malloc(sizeof(htap_impl::GraphHandleImpl));
  if (handle) {
    std::memset(handle, 0, sizeof(htap_impl::GraphHandleImpl));
    htap_impl::get_graph_handle(object_id, channel_num, (htap_impl::GraphHandleImpl*)handle);
  }
  return handle;
}

void free_graph_handle(GraphHandle handle) {
  htap_impl::free_graph_handle((htap_impl::GraphHandleImpl*)handle);
  free(handle);
}

GetVertexIterator get_vertices(GraphHandle graph, PartitionId partition_id,
                               LabelId* labels, VertexId* ids, int count) {
  GetVertexIterator ret = malloc(sizeof(htap_impl::GetVertexIteratorImpl));
  htap_impl::GraphHandleImpl* casted_graph =
      static_cast<htap_impl::GraphHandleImpl*>(graph);

  htap_impl::get_vertices(
      &(casted_graph->fragments[partition_id / casted_graph->channel_num]),
      labels, ids, count, (htap_impl::GetVertexIteratorImpl*)ret);
  return ret;
}

void free_get_vertex_iterator(GetVertexIterator iter) {
  htap_impl::free_get_vertex_iterator((htap_impl::GetVertexIteratorImpl*)iter);
  free(iter);
}

int get_vertices_next(GetVertexIterator iter, Vertex* v_out) {
  return htap_impl::get_vertices_next((htap_impl::GetVertexIteratorImpl*)iter,
                                      v_out);
}

GetAllVerticesIterator get_all_vertices(GraphHandle graph,
                                        PartitionId partition_id,
                                        LabelId* labels, int labels_count,
                                        int64_t limit) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__ << ": partition_id = " << partition_id
            << ", labels_count = " << labels_count;
  for (int i = 0; i < labels_count; ++i) {
    LOG(INFO) << "label[" << i << "] = " << labels[i];
  }
#endif
  GetAllVerticesIterator ret =
      malloc(sizeof(htap_impl::GetAllVerticesIteratorImpl));
  htap_impl::GraphHandleImpl* casted_graph =
      static_cast<htap_impl::GraphHandleImpl*>(graph);

  PartitionId fid = partition_id / casted_graph->channel_num;

  htap_impl::get_all_vertices(
      &(casted_graph->fragments[fid]), partition_id % casted_graph->channel_num,
      casted_graph->vertex_chunk_sizes[fid], labels, labels_count, limit,
      (htap_impl::GetAllVerticesIteratorImpl*)ret);
  return ret;
}

void free_get_all_vertices_iterator(GetAllVerticesIterator iter) {
  htap_impl::free_get_all_vertices_iterator(
      (htap_impl::GetAllVerticesIteratorImpl*)iter);
  free(iter);
}

int get_all_vertices_next(GetAllVerticesIterator iter, Vertex* v_out) {
  return htap_impl::get_all_vertices_next(
      (htap_impl::GetAllVerticesIteratorImpl*)iter, v_out);
}

VertexId get_vertex_id(GraphHandle graph, Vertex v) { return (VertexId)v; }

OuterId get_outer_id(GraphHandle graph, Vertex v) {
  OuterId ret;
  if (((htap_impl::GraphHandleImpl*)graph)
          ->vertex_map->GetOid((htap_impl::VID_TYPE)v, ret)) {
    return ret;
  }
  return OuterId();
}

int get_vertex_by_outer_id(GraphHandle graph, LabelId label_id,
                           OuterId outer_id, Vertex* v) {
  if (label_id < 0 || v == nullptr) {
    return -1;
  }
  auto casted_graph = static_cast<htap_impl::GraphHandleImpl*>(graph);
  htap_impl::VID_TYPE gid;
  if (casted_graph->vertex_map->GetGid(label_id, outer_id, gid)) {
    *v = gid;
    return 0;
  }
  return -1;
}

OuterId get_outer_id_by_vertex_id(GraphHandle graph, VertexId v) {
  return get_outer_id(graph, (Vertex)v);
}

LabelId get_vertex_label(GraphHandle graph, Vertex v) {
  return ((htap_impl::GraphHandleImpl*)graph)->vid_parser.GetLabelId(v);
}

int get_vertex_property(GraphHandle graph, Vertex v, PropertyId id,
                        Property* p_out) {
  htap_impl::GraphHandleImpl* handle =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  int partition_id = handle->vid_parser.GetFid((htap_impl::VID_TYPE)v);
  LabelId label_id = handle->vid_parser.GetLabelId((htap_impl::VID_TYPE)v);
  PropertyId transformed_id =
      handle->schema->VertexEntries()[label_id].reverse_mapping[id];
  if (transformed_id == -1) {
    return -1;
  }
  int r = htap_impl::get_vertex_property(
      &(static_cast<htap_impl::GraphHandleImpl*>(graph)
            ->fragments[partition_id]),
      v, transformed_id, p_out);
  if (r == 0) {
    p_out->id = id;
  }
  return r;
}

PropertiesIterator get_vertex_properties(GraphHandle graph, Vertex v) {
  PropertiesIterator ret = malloc(sizeof(htap_impl::PropertiesIteratorImpl));
  htap_impl::GraphHandleImpl* handle =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  ((htap_impl::PropertiesIteratorImpl*)ret)->handle = handle;
  int partition_id = handle->vid_parser.GetFid((htap_impl::VID_TYPE)v);
  htap_impl::get_vertex_properties(&(handle->fragments[partition_id]), v,
                                   (htap_impl::PropertiesIteratorImpl*)ret);
  return ret;
}

OutEdgeIterator get_out_edges(GraphHandle graph, PartitionId partition_id,
                              VertexId src_id, LabelId* labels,
                              int labels_count, int64_t limit) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
  LOG(INFO) << "label count " << labels_count;
  for (int i = 0; i < labels_count; i++) {
    LOG(INFO) << "label index " << i << " label value " << labels[i];
  }
#endif

  htap_impl::GraphHandleImpl* casted_graph =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  OutEdgeIterator ret = malloc(sizeof(htap_impl::EdgeIteratorImpl));
  std::vector<LabelId> transformed_labels(labels_count);
  for (int i = 0; i < labels_count; ++i) {
    transformed_labels[i] = labels[i] - casted_graph->vertex_label_num;
  }
  htap_impl::get_out_edges(
      &(casted_graph->fragments[partition_id / casted_graph->channel_num]),
      &(casted_graph->eid_parser), src_id, transformed_labels.data(),
      labels_count, limit, (htap_impl::EdgeIteratorImpl*)ret);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  return ret;
}

void free_out_edge_iterator(OutEdgeIterator iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::free_edge_iterator((htap_impl::EdgeIteratorImpl*)iter);
  free(iter);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

int out_edge_next(OutEdgeIterator iter, struct Edge* e_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  return htap_impl::out_edge_next((htap_impl::EdgeIteratorImpl*)iter, e_out);
}

InEdgeIterator get_in_edges(GraphHandle graph, PartitionId partition_id,
                            VertexId dst_id, LabelId* labels, int labels_count,
                            int64_t limit) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::GraphHandleImpl* casted_graph =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  InEdgeIterator ret = malloc(sizeof(htap_impl::EdgeIteratorImpl));
  PartitionId dst_partition_id = get_partition_id(graph, dst_id);
  if (dst_partition_id != partition_id) {
    htap_impl::empty_edge_iterator((htap_impl::EdgeIteratorImpl*)ret);
  } else {
    std::vector<LabelId> transformed_labels(labels_count);
    for (int i = 0; i < labels_count; ++i) {
      transformed_labels[i] = labels[i] - casted_graph->vertex_label_num;
    }
    htap_impl::get_in_edges(
        &(casted_graph->fragments[partition_id / casted_graph->channel_num]),
        &(casted_graph->eid_parser), dst_id, transformed_labels.data(),
        labels_count, limit, (htap_impl::EdgeIteratorImpl*)ret);
  }
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  return ret;
}

void free_in_edge_iterator(InEdgeIterator iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::free_edge_iterator((htap_impl::EdgeIteratorImpl*)iter);
  free(iter);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

int in_edge_next(InEdgeIterator iter, struct Edge* e_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  return htap_impl::in_edge_next((htap_impl::EdgeIteratorImpl*)iter, e_out);
}

GetAllEdgesIterator get_all_edges(GraphHandle graph, PartitionId partition_id,
                                  LabelId* labels, int labels_count,
                                  int64_t limit) {
#ifndef NDEBUG
  LOG(INFO) << "enter get all edges";
#endif
  htap_impl::GraphHandleImpl* casted_graph =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  GetAllEdgesIterator ret = malloc(sizeof(htap_impl::GetAllEdgesIteratorImpl));
  std::vector<LabelId> transformed_labels(labels_count);
  for (int i = 0; i < labels_count; ++i) {
    transformed_labels[i] = labels[i] - casted_graph->vertex_label_num;
  }
  PartitionId fid = partition_id / casted_graph->channel_num;
  htap_impl::get_all_edges(
      &(casted_graph->fragments[fid]), partition_id % casted_graph->channel_num,
      casted_graph->vertex_chunk_sizes[fid], &(casted_graph->eid_parser),
      transformed_labels.data(), labels_count, limit,
      (htap_impl::GetAllEdgesIteratorImpl*)ret);
#ifndef NDEBUG
  LOG(INFO) << "finish get all edges";
#endif
  return ret;
}

void free_get_all_edges_iterator(GetAllEdgesIterator iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::free_get_all_edges_iterator(
      (htap_impl::GetAllEdgesIteratorImpl*)iter);
  free(iter);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

int get_all_edges_next(GetAllEdgesIterator iter, struct Edge* e_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  return htap_impl::get_all_edges_next(
      (htap_impl::GetAllEdgesIteratorImpl*)iter, e_out);
}

VertexId get_edge_src_id(GraphHandle graph, struct Edge* e) { return e->src; }

VertexId get_edge_dst_id(GraphHandle graph, struct Edge* e) { return e->dst; }

static void parse_edge_id(GraphHandle graph, htap_impl::EID_TYPE eid,
                          htap_impl::FRAG_ID_TYPE* fid, LabelId* label,
                          int64_t* offset) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  vineyard::IdParser<htap_impl::EID_TYPE>* parser =
      &(static_cast<htap_impl::GraphHandleImpl*>(graph)->eid_parser);
  *fid = parser->GetFid(eid);
  *label = parser->GetLabelId(eid);
  *offset = parser->GetOffset(eid);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

EdgeId get_edge_id(GraphHandle graph, struct Edge* e) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::FRAG_ID_TYPE partition_id;
  LabelId label;
  int64_t offset;
  parse_edge_id(graph, (htap_impl::EID_TYPE)e->offset, &partition_id, &label,
                &offset);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  return htap_impl::get_edge_id(
      &(static_cast<htap_impl::GraphHandleImpl*>(graph)
            ->fragments[partition_id]),
      label, offset);
}

LabelId get_edge_src_label(GraphHandle graph, struct Edge* e) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  return ((htap_impl::GraphHandleImpl*)graph)->vid_parser.GetLabelId(e->src);
}

LabelId get_edge_dst_label(GraphHandle graph, struct Edge* e) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  return ((htap_impl::GraphHandleImpl*)graph)->vid_parser.GetLabelId(e->dst);
}

LabelId get_edge_label(GraphHandle graph, struct Edge* e) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::GraphHandleImpl* handle =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  return handle->eid_parser.GetLabelId(e->offset) + handle->vertex_label_num;
}

int get_edge_property(GraphHandle graph, struct Edge* e, PropertyId id,
                      Property* p_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::FRAG_ID_TYPE partition_id;
  LabelId label;
  int64_t offset;
  parse_edge_id(graph, (htap_impl::EID_TYPE)e->offset, &partition_id, &label,
                &offset);
  htap_impl::GraphHandleImpl* handle =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  PropertyId transformed_id =
      handle->schema->EdgeEntries()[label].reverse_mapping[id];
  if (transformed_id == -1) {
    return -1;
  }
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  int r = htap_impl::get_edge_property(&(handle->fragments[partition_id]),
                                       label, offset, transformed_id, p_out);
  if (r == 0) {
    p_out->id = id;
  }
  return r;
}

PropertiesIterator get_edge_properties(GraphHandle graph, struct Edge* e) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::FRAG_ID_TYPE partition_id;
  LabelId label;
  int64_t offset;
  parse_edge_id(graph, (htap_impl::EID_TYPE)e->offset, &partition_id, &label,
                &offset);
  PropertiesIterator ret = malloc(sizeof(htap_impl::PropertiesIteratorImpl));
  htap_impl::GraphHandleImpl* handle =
      static_cast<htap_impl::GraphHandleImpl*>(graph);
  ((htap_impl::PropertiesIteratorImpl*)ret)->handle = handle;
  htap_impl::get_edge_properties(&(handle->fragments[partition_id]), label,
                                 offset,
                                 (htap_impl::PropertiesIteratorImpl*)ret);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  return ret;
}

int properties_next(PropertiesIterator iter, Property* p_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  int r = htap_impl::properties_next((htap_impl::PropertiesIteratorImpl*)iter,
                                     p_out);
  if (r == 0) {
    htap_impl::PropertiesIteratorImpl* iter_impl =
        (htap_impl::PropertiesIteratorImpl*)iter;
    if (iter_impl->vertex_or_edge) {
      p_out->id =
          iter_impl->handle->schema->VertexEntries()[iter_impl->label_id]
              .mapping[p_out->id];
    } else {
      p_out->id = iter_impl->handle->schema->EdgeEntries()[iter_impl->label_id]
                      .mapping[p_out->id];
    }
  }
  return r;
}

void free_properties_iterator(PropertiesIterator iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  htap_impl::free_properties_iterator((htap_impl::PropertiesIteratorImpl*)iter);
  free(iter);
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

int get_property_as_bool(Property* property, bool* out) {
  return htap_impl::get_property_as_bool(property, out);
}
int get_property_as_char(Property* property, char* out) {
  return htap_impl::get_property_as_char(property, out);
}
int get_property_as_short(Property* property, int16_t* out) {
  return htap_impl::get_property_as_short(property, out);
}
int get_property_as_int(Property* property, int* out) {
  return htap_impl::get_property_as_int(property, out);
}
int get_property_as_long(Property* property, int64_t* out) {
  return htap_impl::get_property_as_long(property, out);
}
int get_property_as_float(Property* property, float* out) {
  return htap_impl::get_property_as_float(property, out);
}
int get_property_as_double(Property* property, double* out) {
  return htap_impl::get_property_as_double(property, out);
}

int get_property_as_string(Property* property, const char** out, int* out_len) {
  int r = htap_impl::get_property_as_string(property, out, out_len);
  return r;
}
int get_property_as_bytes(Property* property, const char** out, int* out_len) {
  return htap_impl::get_property_as_bytes(property, out, out_len);
}
int get_property_as_int_list(Property* property, const int** out,
                             int* out_len) {
  return htap_impl::get_property_as_int_list(property, out, out_len);
}
int get_property_as_long_list(Property* property, const int64_t** out,
                              int* out_len) {
  return htap_impl::get_property_as_long_list(property, out, out_len);
}
int get_property_as_float_list(Property* property, const float** out,
                               int* out_len) {
  return htap_impl::get_property_as_float_list(property, out, out_len);
}
int get_property_as_double_list(Property* property, const double** out,
                                int* out_len) {
  return htap_impl::get_property_as_double_list(property, out, out_len);
}
int get_property_as_string_list(Property* property, const char*** out,
                                const int** out_len, int* out_num) {
  return htap_impl::get_property_as_string_list(property, out, out_len,
                                                out_num);
}

void free_property(Property* property) {}

Schema get_schema(GraphHandle graph) {
#ifndef NDEBUG
  LOG(INFO) << "rust ffi call: " << __FUNCTION__;
#endif
  return ((htap_impl::GraphHandleImpl*)graph)->schema;
}

// *out_num为string的个数
// (*out_len)[i]为第i个string的长度
// (*out)[i]为第i个string的其实地址

PartitionId get_partition_id(GraphHandle graph, VertexId v) {
#ifndef NDEBUG
  LOG(INFO) << "enter get_partition_id " << v;
#endif
  htap_impl::GraphHandleImpl* casted_graph =
      static_cast<htap_impl::GraphHandleImpl*>(graph);

  auto fid = casted_graph->vid_parser.GetFid((htap_impl::VID_TYPE)v);
  LabelId label_id =
      casted_graph->vid_parser.GetLabelId((htap_impl::VID_TYPE)v);
  htap_impl::VID_TYPE offset =
      casted_graph->vid_parser.GetOffset((htap_impl::VID_TYPE)v);

  if (fid >= casted_graph->fnum || fid < 0 ||
      label_id >= casted_graph->vertex_label_num || label_id < 0) {
    return -1;
  }

  if (offset >= casted_graph->vertex_map->GetInnerVertexSize(fid, label_id) ||
      offset < 0) {
    return -1;
  }

  int channel_id = offset / casted_graph->vertex_chunk_sizes[fid][label_id];

  int partition_id = fid * casted_graph->channel_num + channel_id;

#ifndef NDEBUG
  LOG(INFO) << "get partition id: " << v << " -> " << partition_id;
#endif
  return partition_id;
}

// 如果 key 不存在，返回 -1
// 否则返回0，结果存在 internal_id 和 partition_id 中
int get_vertex_id_from_primary_key(GraphHandle graph, LabelId label_id,
                                   const char* key, VertexId* internal_id,
                                   PartitionId* partition_id) {
#ifndef NDEBUG
  LOG(INFO) << "query on primary key: label_id = " << label_id
            << ", key = " << key;
#endif
  if (label_id < 0) {
     return -1;
  }
  auto handle = ((htap_impl::GraphHandleImpl*)graph);
  htap_impl::OID_TYPE oid = std::stoll(key);
  htap_impl::VID_TYPE gid;
  if (handle->vertex_map->GetGid(label_id, oid, gid)) {
    *internal_id = gid;
    *partition_id = get_partition_id(graph, gid);
#ifndef NDEBUG
    LOG(INFO) << "vertex found: gid = " << gid
              << ", partition_id = " << (*partition_id);
#endif
    return 0;
  } else {
#ifndef NDEBUG
    LOG(INFO) << "vertex not found";
#endif
    return -1;
  }
}

void get_process_partition_list(GraphHandle graph, PartitionId** partition_ids,
                                int* partition_id_size) {
#ifndef NDEBUG
  LOG(INFO) << "enter get_process_partition_list";
#endif
  auto impl = (htap_impl::GraphHandleImpl*)graph;
  *partition_id_size = impl->local_fnum * impl->channel_num;
#ifndef NDEBUG
  LOG(INFO) << "local fnum = " << *partition_id_size;
#endif
  *partition_ids = static_cast<PartitionId*>(
      malloc(sizeof(PartitionId) * impl->local_fnum * impl->channel_num));
  /*
  for (int idx = 0; idx < *partition_id_size; ++idx) {
#ifndef NDEBUG
    LOG(INFO) << "emplace parititon id: " << impl->local_fragments[idx];
#endif
    (*partition_ids)[idx] = impl->local_fragments[idx];
  }
  */
  for (htap_impl::FRAG_ID_TYPE fidx = 0; fidx < impl->local_fnum; ++fidx) {
    for (PartitionId cidx = 0; cidx < impl->channel_num; ++cidx) {
      (*partition_ids)[fidx * impl->channel_num + cidx] =
          impl->local_fragments[fidx] * impl->channel_num + cidx;
    }
  }
}

void free_partition_list(PartitionId* partition_ids) { free(partition_ids); }

#ifdef __cplusplus
}
#endif
