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
#include "htap_ds_impl.h"

#include <cstdlib>
#include <memory>
#include <string>

#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/arrow_fragment_group.h"

namespace htap_impl {

void get_graph_handle(ObjectId id, PartitionId channel_num,
                      GraphHandleImpl* handle) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  auto client = std::make_unique<vineyard::Client>();
  VINEYARD_CHECK_OK(client->Connect());
  LOG(INFO) << "Initialize vineyard client";
  std::shared_ptr<vineyard::ArrowFragmentGroup> fg =
      std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
          client->GetObject(id));
  LOG(INFO) << "Get vineyard object ok: " << fg;
  vineyard::fid_t total_frag_num = fg->total_frag_num();
  LabelId vertex_label_num = fg->vertex_label_num();
  LabelId edge_label_num = fg->edge_label_num();
  LOG(INFO) << "FragGroup: total frag num = " << total_frag_num
            << ", vertex label num = " << vertex_label_num
            << ", edge label num = " << edge_label_num;

  handle->fnum = total_frag_num;
  handle->vid_parser.Init(total_frag_num, vertex_label_num);
  handle->eid_parser.Init(total_frag_num, edge_label_num);
  handle->channel_num = channel_num;
  handle->vertex_label_num = vertex_label_num;
  handle->edge_label_num = edge_label_num;

  handle->local_fnum = 0;
  uint64_t native_instance_id = client->instance_id();
  for (const auto& pair : fg->FragmentLocations()) {
    if (pair.second == native_instance_id) {
      ++handle->local_fnum;
    }
  }
  // FIXME: make the following exception free
  if (handle->local_fnum == 0) {
    handle->local_fragments = NULL;
  } else {
    handle->local_fragments = new FRAG_ID_TYPE[handle->local_fnum];
  }
  FRAG_ID_TYPE local_frag_index = 0;

  for (const auto& pair : fg->FragmentLocations()) {
    if (pair.second == native_instance_id) {
      handle->local_fragments[local_frag_index] = pair.first;
      ++local_frag_index;
    }
  }

  handle->fragments = new FRAGMENT_TYPE[total_frag_num];
  handle->schema = NULL;
  handle->vertex_map = NULL;

  for (const auto& pair : fg->Fragments()) {
    FRAG_ID_TYPE fid = pair.first;
    LOG(INFO) << "fid = " << fid
              << ", instance_id = " << client->instance_id()
              << ", location = " << fg->FragmentLocations().at(fid);
    if (fg->FragmentLocations().at(fid) == client->instance_id()) {
      vineyard::ObjectMeta meta;
      VINEYARD_CHECK_OK(client->GetMetaData(pair.second, meta));
#ifndef NDEBUG
      LOG(INFO) << "begin construct fragment: " << pair.second << ", "
                << meta.GetTypeName();
#endif
      handle->fragments[fid].Construct(meta);

      if (handle->vertex_map == NULL) {
        handle->vertex_map = new VERTEX_MAP_TYPE();
        vineyard::ObjectMeta vm_meta;
        LOG(INFO) << "begin get vertex map: "
                  << handle->fragments[fid].vertex_map_id();
        VINEYARD_CHECK_OK(client->GetMetaData(
            handle->fragments[fid].vertex_map_id(), vm_meta));
#ifndef NDEBUG
        LOG(INFO) << "begin construct vertex map: "
                  << handle->fragments[fid].vertex_map_id();
#endif
        handle->vertex_map->Construct(vm_meta);
#ifndef NDEBUG
        LOG(INFO) << "finish construct vertex map: " << pair.second;
#endif
      }
      if (handle->schema == NULL) {
        auto schema = handle->fragments[fid].schema();
        vineyard::MGPropertyGraphSchema mgschema;
        vineyard::json schema_json;
        schema.ToJSON(schema_json);
        mgschema.FromJSON(schema_json);
        handle->schema =
            new vineyard::MGPropertyGraphSchema(mgschema.TransformToMaxGraph());
      }
    }
  }
  handle->vertex_chunk_sizes =
      static_cast<VID_TYPE**>(malloc(sizeof(VID_TYPE*) * total_frag_num));
  for (vineyard::fid_t i = 0; i < total_frag_num; ++i) {
    handle->vertex_chunk_sizes[i] =
        static_cast<VID_TYPE*>(malloc(sizeof(VID_TYPE) * vertex_label_num));
    for (LabelId j = 0; j < vertex_label_num; ++j) {
      VID_TYPE ivnum = handle->vertex_map->GetInnerVertexSize(i, j);
      handle->vertex_chunk_sizes[i][j] =
          (ivnum + channel_num - 1) / channel_num;
    }
  }
  handle->client = client.release();
  LOG(INFO) << "finish get graph handle: " << id << ", handle = " << handle;
}

void free_graph_handle(GraphHandleImpl* handle) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  if (handle == nullptr) {
    return;
  }
  for (FRAG_ID_TYPE i = 0; i < handle->fnum; ++i) {
    free(handle->vertex_chunk_sizes[i]);
  }
  free(handle->vertex_chunk_sizes);

  delete[] handle->fragments;
  if (handle->local_fragments != NULL) {
    delete[] handle->local_fragments;
    handle->local_fragments = NULL;
  }
  delete handle->client;
  handle->client = NULL;
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

static int get_property_from_table(arrow::Table* table, int64_t row_id,
                                   PropertyId col_id, Property* p_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  std::shared_ptr<arrow::DataType> dt = table->field(col_id)->type();
  std::shared_ptr<arrow::Array> array = table->column(col_id)->chunk(0);
  p_out->id = col_id;
  PodProperties pp;
  pp.long_value = 0;
  if (dt == arrow::boolean()) {
    p_out->type = BOOL;
    pp.bool_value =
        std::dynamic_pointer_cast<arrow::BooleanArray>(array)->Value(row_id);
  } else if (dt == arrow::int8()) {
    p_out->type = CHAR;
    pp.char_value =
        std::dynamic_pointer_cast<arrow::Int8Array>(array)->Value(row_id);
  } else if (dt == arrow::int16()) {
    p_out->type = SHORT;
    pp.int16_value =
        std::dynamic_pointer_cast<arrow::Int16Array>(array)->Value(row_id);
  } else if (dt == arrow::int32()) {
    p_out->type = INT;
    pp.int_value =
        std::dynamic_pointer_cast<arrow::Int32Array>(array)->Value(row_id);
  } else if (dt == arrow::int64()) {
    p_out->type = LONG;
    pp.long_value =
        std::dynamic_pointer_cast<arrow::Int64Array>(array)->Value(row_id);
  } else if (dt == arrow::float32()) {
    p_out->type = FLOAT;
    pp.float_value =
        std::dynamic_pointer_cast<arrow::FloatArray>(array)->Value(row_id);
  } else if (dt == arrow::float64()) {
    p_out->type = DOUBLE;
    pp.double_value =
        std::dynamic_pointer_cast<arrow::DoubleArray>(array)->Value(row_id);
  } else if (dt == arrow::utf8()) {
    p_out->type = STRING;
    auto view =
        std::dynamic_pointer_cast<arrow::StringArray>(array)->GetView(row_id);
    pp.long_value = view.length();
    p_out->data = const_cast<void*>(static_cast<const void*>(view.data()));
  } else if (dt == arrow::large_utf8()) {
    p_out->type = STRING;
    auto view =
        std::dynamic_pointer_cast<arrow::LargeStringArray>(array)->GetView(
            row_id);
    pp.long_value = view.length();
    p_out->data = const_cast<void*>(static_cast<const void*>(view.data()));
  } else {
    LOG(ERROR) << "invalid dt is = " << dt->ToString();
    return -1;
  }
  p_out->len = pp.long_value;
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  return 0;
}

static void get_properties_from_table(std::shared_ptr<arrow::Table> table,
                                      int row_id,
                                      PropertiesIteratorImpl* iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  iter->table = table.get();
  iter->row_id = row_id;
  iter->col_num = table->num_columns();
  iter->col_id = 0;
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

OuterId get_outer_id(FRAGMENT_TYPE* frag, Vertex v) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__ << ": vid = " << (VID_TYPE)v;
#endif
  VERTEX_TYPE vert;
  if (frag->InnerVertexGid2Vertex((VID_TYPE)v, vert)) {
    return frag->GetId(vert);
  } else {
    CHECK(false);
    return OuterId();
  }
}

int get_vertex_property(FRAGMENT_TYPE* frag, Vertex v, PropertyId id,
                        Property* p_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__ << ": id = " << id;
#endif
  VERTEX_TYPE vert;
  if (frag->InnerVertexGid2Vertex((VID_TYPE)v, vert)) {
    LabelId label = frag->vertex_label(vert);
    int64_t offset = frag->vertex_offset(vert);
    std::shared_ptr<arrow::Table> table = frag->vertex_data_table(label);
    return get_property_from_table(table.get(), offset, id, p_out);
  } else {
    return -1;
  }
}

void get_vertex_properties(FRAGMENT_TYPE* frag, Vertex v,
                           PropertiesIteratorImpl* iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__ << ": vid = " << (VID_TYPE)v;
#endif
  VERTEX_TYPE vert;
  if (frag->InnerVertexGid2Vertex((VID_TYPE)v, vert)) {
    LabelId label = frag->vertex_label(vert);
    int64_t offset = frag->vertex_offset(vert);
    std::shared_ptr<arrow::Table> table = frag->vertex_data_table(label);
    iter->vertex_or_edge = true;
    iter->label_id = label;
    get_properties_from_table(table, offset, iter);
  }
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

void get_vertices(FRAGMENT_TYPE* frag, LabelId* label, VertexId* ids, int count,
                  GetVertexIteratorImpl* out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  out->ids = static_cast<VID_TYPE*>(malloc(sizeof(VID_TYPE) * count));
  int cur = 0;
  if (label == NULL) {
    for (int i = 0; i < count; ++i) {
      VERTEX_TYPE vert;
      if (frag->InnerVertexGid2Vertex((VID_TYPE)ids[i], vert)) {
        out->ids[cur] = ids[i];
        ++cur;
      }
    }
  } else {
    if (*label >= 0) {
      for (int i = 0; i < count; ++i) {
        VERTEX_TYPE vert;
        if (frag->InnerVertexGid2Vertex((VID_TYPE)ids[i], vert)) {
          if (frag->vertex_label(vert) == *label) {
            out->ids[cur] = ids[i];
            ++cur;
          }
        }
      }
    }
  }
  if (cur == 0) {
    free(out->ids);
    out->ids = NULL;
  }
  out->index = 0;
  out->count = cur;
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

void free_get_vertex_iterator(GetVertexIteratorImpl* iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  if (iter->ids != NULL) {
    free(iter->ids);
  }
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

int get_vertices_next(GetVertexIteratorImpl* iter, Vertex* v_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  if (iter->index == iter->count) {
    return -1;
  } else {
    *v_out = iter->ids[iter->index];
    ++iter->index;
    return 0;
  }
}

static typename FRAGMENT_TYPE::vertex_range_t get_sub_range(
    const typename FRAGMENT_TYPE::vertex_range_t& super_range,
    VID_TYPE chunk_size, PartitionId channel_id) {
  VID_TYPE super_range_begin = super_range.begin().GetValue();
  VID_TYPE super_range_end = super_range.end().GetValue();
  VID_TYPE sub_range_begin =
      std::min(super_range_begin + chunk_size * channel_id, super_range_end);
  VID_TYPE sub_range_end =
      std::min(sub_range_begin + chunk_size, super_range_end);
  return typename FRAGMENT_TYPE::vertex_range_t(sub_range_begin, sub_range_end);
}

void get_all_vertices(FRAGMENT_TYPE* frag, PartitionId channel_id,
                      const VID_TYPE* chunk_sizes, LabelId* labels,
                      int labels_count, int64_t limit,
                      GetAllVerticesIteratorImpl* out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__ << ", limit = " << limit;
#endif
  if (limit == 0) {
    out->ranges = NULL;
    out->range_id = 0;
    out->range_num = 0;
    out->cur_vertex_id = 0;
    return;
  }

  size_t limit_remaining = limit;
  int range_index = 0;

  if (labels_count == 0 || labels == NULL) {
    labels_count = frag->vertex_label_num();

    out->ranges = static_cast<VERTEX_RANGE_TYPE*>(
        malloc(labels_count * sizeof(VERTEX_RANGE_TYPE)));

    for (int i = 0; i < labels_count; ++i) {
#ifndef NDEBUG
      LOG(INFO) << __FUNCTION__ << ", query vertex label " << i;
#endif
      auto super_range = frag->InnerVertices((LabelId)i);
      auto range = get_sub_range(super_range, chunk_sizes[i], channel_id);

      size_t size = range.size();
      if (size == 0) {
        continue;
      }
      out->ranges[range_index].first = frag->Vertex2Gid(range.begin());
      out->ranges[range_index].second = out->ranges[range_index].first + size;
      if (size >= limit_remaining) {
        out->ranges[range_index].second =
            out->ranges[range_index].first + limit_remaining;
        limit_remaining = 0;
        ++range_index;
        break;
      } else {
        limit_remaining -= size;
        ++range_index;
      }
    }
  } else {
    out->ranges = static_cast<VERTEX_RANGE_TYPE*>(
        malloc(labels_count * sizeof(VERTEX_RANGE_TYPE)));

    for (int i = 0; i < labels_count; ++i) {
      if (labels[i] < 0) {
        continue;
      }
#ifndef NDEBUG
      LOG(INFO) << __FUNCTION__ << ", query vertex label " << labels[i];
#endif
      auto super_range = frag->InnerVertices(labels[i]);
      auto range =
          get_sub_range(super_range, chunk_sizes[labels[i]], channel_id);
      size_t size = range.size();
      if (size == 0) {
        continue;
      }
      out->ranges[range_index].first = frag->Vertex2Gid(range.begin());
      out->ranges[range_index].second = out->ranges[range_index].first + size;
      if (size >= limit_remaining) {
        out->ranges[range_index].second =
            out->ranges[range_index].first + limit_remaining;
        limit_remaining = 0;
        ++range_index;
        break;
      } else {
        limit_remaining -= size;
        ++range_index;
      }
    }
  }

  out->range_id = 0;
  if (range_index == 0) {
    free(out->ranges);
    out->ranges = NULL;
    out->range_num = 0;
    out->cur_vertex_id = 0;
  } else {
    out->range_num = range_index;
    out->cur_vertex_id = out->ranges[0].first;
  }
}

void free_get_all_vertices_iterator(GetAllVerticesIteratorImpl* iter) {
  if (iter->ranges != NULL) {
    free(iter->ranges);
  }
}

int get_all_vertices_next(GetAllVerticesIteratorImpl* iter, Vertex* v_out) {
  while (iter->range_id != iter->range_num &&
         iter->cur_vertex_id == iter->ranges[iter->range_id].second) {
    ++iter->range_id;
    iter->cur_vertex_id = iter->ranges[iter->range_id].first;
  }
  if (iter->range_id == iter->range_num) {
    return -1;
  }
  *v_out = (Vertex)iter->cur_vertex_id;
#ifndef NDEBUG
  LOG(INFO) << "get_all_vertices_next: current vertex id = " << (*v_out);
#endif
  ++iter->cur_vertex_id;
  return 0;
}

EdgeId get_edge_id(FRAGMENT_TYPE* frag, LabelId label, int64_t offset) {
#ifndef NDEBUG
  LOG(INFO) << "enter = " << __FUNCTION__ << ", label = " << label
            << ", offset = " << offset;
#endif
  std::shared_ptr<arrow::Table> edge_table = frag->edge_data_table(label);
  return std::dynamic_pointer_cast<
             typename vineyard::ConvertToArrowType<EdgeId>::ArrayType>(
             edge_table->column(0)->chunk(0))
      ->GetView(offset);
}

int get_edge_property(FRAGMENT_TYPE* frag, LabelId label, int64_t offset,
                      PropertyId id, Property* p_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter = " << __FUNCTION__ << ", label = " << label
            << ", offset = " << offset;
#endif
  std::shared_ptr<arrow::Table> table = frag->edge_data_table(label);
  return get_property_from_table(table.get(), offset, id, p_out);
}

void get_edge_properties(FRAGMENT_TYPE* frag, LabelId label, int64_t offset,
                         PropertiesIteratorImpl* iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter = " << __FUNCTION__ << ", label = " << label
            << ", offset = " << offset;
#endif
  std::shared_ptr<arrow::Table> table = frag->edge_data_table(label);
  iter->vertex_or_edge = false;
  iter->label_id = label;
  get_properties_from_table(table, offset, iter);
}

int properties_next(PropertiesIteratorImpl* iter, Property* p_out) {
  if (iter==nullptr) {
    return -1;
  }
  while (iter->col_id < iter->col_num &&
         iter->table->field(iter->col_id)->type() == arrow::null()) {
    ++iter->col_id;
  }
  if (iter->col_num == iter->col_id) {
    return -1;
  }
  PropertyId col_id = iter->col_id;
  ++iter->col_id;
  return get_property_from_table(iter->table, iter->row_id, col_id, p_out);
}

void free_properties_iterator(PropertiesIteratorImpl* iter) {}

void empty_edge_iterator(EdgeIteratorImpl* iter) {
  iter->list_num = 0;
  iter->list_id = 0;
  iter->cur_edge = NULL;
  iter->lists = NULL;
}

void get_out_edges(FRAGMENT_TYPE* frag,
                   vineyard::IdParser<EID_TYPE>* eid_parser, VertexId src_id,
                   LabelId* labels, int labels_count, int64_t limit,
                   EdgeIteratorImpl* iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  iter->list_id = 0;
  iter->src = src_id;
  iter->eid_parser = eid_parser;
  iter->fragment = frag;
  VERTEX_TYPE vert;
  if (frag->InnerVertexGid2Vertex((VID_TYPE)src_id, vert) && limit != 0) {
    int list_index = 0;
    size_t limit_remaining = limit;
    if (labels == NULL || labels_count == 0) {
      labels_count = frag->edge_label_num();
      iter->lists =
          static_cast<AdjListUnit*>(malloc(sizeof(AdjListUnit) * labels_count));
      for (int i = 0; i < labels_count; ++i) {
        auto adj_list = frag->GetOutgoingAdjList(
            vert, (typename FRAGMENT_TYPE::label_id_t)i);
        iter->lists[list_index].begin = adj_list.begin_unit();
        iter->lists[list_index].end = adj_list.end_unit();
        iter->lists[list_index].label = i;
        size_t size =
            iter->lists[list_index].end - iter->lists[list_index].begin;
        if (size == 0) {
          continue;
        }
        if (size >= limit_remaining) {
          iter->lists[list_index].end =
              iter->lists[list_index].begin + limit_remaining;
          ++list_index;
          limit_remaining = 0;
          break;
        } else {
          limit_remaining -= size;
          ++list_index;
        }
      }
    } else {
      iter->lists =
          static_cast<AdjListUnit*>(malloc(sizeof(AdjListUnit) * labels_count));
      for (int i = 0; i < labels_count; ++i) {
        if (labels[i] < 0) {
          continue;
        }
        auto adj_list = frag->GetOutgoingAdjList(
            vert, (typename FRAGMENT_TYPE::label_id_t)labels[i]);
        iter->lists[list_index].begin = adj_list.begin_unit();
        iter->lists[list_index].end = adj_list.end_unit();
        iter->lists[list_index].label = labels[i];
        size_t size =
            iter->lists[list_index].end - iter->lists[list_index].begin;
        if (size == 0) {
          continue;
        }
        if (size >= limit_remaining) {
          iter->lists[list_index].end =
              iter->lists[list_index].begin + limit_remaining;
          ++list_index;
          limit_remaining = 0;
          break;
        } else {
          limit_remaining -= size;
          ++list_index;
        }
      }
    }
    if (list_index == 0) {
      free(iter->lists);
      iter->lists = NULL;
      iter->list_num = 0;
      iter->cur_edge = NULL;
    } else {
      iter->list_num = list_index;
      iter->cur_edge = iter->lists[0].begin;
    }
  } else {
    iter->lists = NULL;
    iter->list_num = 0;
    iter->cur_edge = NULL;
  }
#ifndef NDEBUG
  LOG(INFO) << "exit " << __FUNCTION__;
#endif
}

int out_edge_next(EdgeIteratorImpl* iter, Edge* e_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  while (iter->list_id != iter->list_num &&
         iter->cur_edge == iter->lists[iter->list_id].end) {
    ++iter->list_id;
    iter->cur_edge = iter->lists[iter->list_id].begin;
  }
  if (iter->list_id == iter->list_num) {
    return -1;
  }
  e_out->src = iter->src;
  e_out->dst = iter->fragment->Vertex2Gid(VERTEX_TYPE(iter->cur_edge->vid));
  e_out->offset = iter->eid_parser->GenerateId(iter->fragment->fid(),
                                               iter->lists[iter->list_id].label,
                                               iter->cur_edge->eid);
  ++iter->cur_edge;
#ifndef NDEBUG
  LOG(INFO) << "exit " << __FUNCTION__;
#endif
  return 0;
}

void get_in_edges(FRAGMENT_TYPE* frag, vineyard::IdParser<EID_TYPE>* eid_parser,
                  VertexId dst_id, LabelId* labels, int labels_count,
                  int64_t limit, EdgeIteratorImpl* iter) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  iter->list_id = 0;
  iter->src = dst_id;
  iter->eid_parser = eid_parser;
  iter->fragment = frag;
  VERTEX_TYPE vert;
  if (frag->InnerVertexGid2Vertex((VID_TYPE)dst_id, vert) && limit != 0) {
    int list_index = 0;
    size_t limit_remaining = limit;
    if (labels == NULL || labels_count == 0) {
      labels_count = frag->edge_label_num();
      iter->lists =
          static_cast<AdjListUnit*>(malloc(sizeof(AdjListUnit) * labels_count));
      for (int i = 0; i < labels_count; ++i) {
        auto adj_list = frag->GetIncomingAdjList(
            vert, (typename FRAGMENT_TYPE::label_id_t)i);
        iter->lists[list_index].begin = adj_list.begin_unit();
        iter->lists[list_index].end = adj_list.end_unit();
        iter->lists[list_index].label = i;
        size_t size =
            iter->lists[list_index].end - iter->lists[list_index].begin;
        if (size == 0) {
          continue;
        }
        if (size >= limit_remaining) {
          iter->lists[list_index].end =
              iter->lists[list_index].begin + limit_remaining;
          ++list_index;
          limit_remaining = 0;
          break;
        } else {
          limit_remaining -= size;
          ++list_index;
        }
      }
    } else {
      iter->lists =
          static_cast<AdjListUnit*>(malloc(sizeof(AdjListUnit) * labels_count));
      for (int i = 0; i < labels_count; ++i) {
        if (labels[i] < 0) {
          continue;
        }
        auto adj_list = frag->GetIncomingAdjList(
            vert, (typename FRAGMENT_TYPE::label_id_t)labels[i]);
        iter->lists[list_index].begin = adj_list.begin_unit();
        iter->lists[list_index].end = adj_list.end_unit();
        iter->lists[list_index].label = labels[i];
        size_t size =
            iter->lists[list_index].end - iter->lists[list_index].begin;
        if (size == 0) {
          continue;
        }
        if (size >= limit_remaining) {
          iter->lists[list_index].end =
              iter->lists[list_index].begin + limit_remaining;
          ++list_index;
          limit_remaining = 0;
          break;
        } else {
          limit_remaining -= size;
          ++list_index;
        }
      }
    }
    if (list_index == 0) {
      free(iter->lists);
      iter->lists = NULL;
      iter->list_num = 0;
      iter->cur_edge = NULL;
    } else {
      iter->list_num = list_index;
      iter->cur_edge = iter->lists[0].begin;
    }
  } else {
    iter->lists = NULL;
    iter->list_num = 0;
    iter->cur_edge = NULL;
  }
#ifndef NDEBUG
  LOG(INFO) << "exit " << __FUNCTION__;
#endif
}

int in_edge_next(EdgeIteratorImpl* iter, Edge* e_out) {
#ifndef NDEBUG
  LOG(INFO) << "exit " << __FUNCTION__;
#endif
  while (iter->list_id != iter->list_num &&
         iter->cur_edge == iter->lists[iter->list_id].end) {
    ++iter->list_id;
    iter->cur_edge = iter->lists[iter->list_id].begin;
  }
  if (iter->list_id == iter->list_num) {
    return -1;
  }
  e_out->dst = iter->src;
  e_out->src = iter->fragment->Vertex2Gid(VERTEX_TYPE(iter->cur_edge->vid));
  e_out->offset = iter->eid_parser->GenerateId(iter->fragment->fid(),
                                               iter->lists[iter->list_id].label,
                                               iter->cur_edge->eid);
  ++iter->cur_edge;
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
  return 0;
}

void get_all_edges(FRAGMENT_TYPE* frag, PartitionId channel_id,
                   const VID_TYPE* chunk_sizes,
                   vineyard::IdParser<EID_TYPE>* eid_parser, LabelId* labels,
                   int labels_count, int64_t limit,
                   GetAllEdgesIteratorImpl* out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
  LOG(INFO) << "edge label count " << labels_count;
  for (int i = 0; i < labels_count; i++) {
    LOG(INFO) << "index " << i << " label value " << labels[i];
  }
#endif

  out->fragment = frag;
  out->e_labels = static_cast<LabelId*>(malloc(sizeof(LabelId) * labels_count));
  out->eid_parser = eid_parser;
  memcpy(out->e_labels, labels, sizeof(LabelId) * labels_count);
  out->e_labels_count = labels_count;

  out->chunk_sizes = chunk_sizes;
  out->channel_id = channel_id;

  out->cur_v_label = 0;
  auto super_range = frag->InnerVertices(out->cur_v_label);
  auto range =
      get_sub_range(super_range, chunk_sizes[out->cur_v_label], channel_id);
  while (range.size() == 0) {
    out->cur_v_label += 1;
    if (out->cur_v_label >= static_cast<int>(frag->vertex_label_num())) {
      empty_edge_iterator(&out->ei);
      return;
    }
    super_range = frag->InnerVertices(out->cur_v_label);
    range =
        get_sub_range(super_range, chunk_sizes[out->cur_v_label], channel_id);
  }

  out->cur_range.first = frag->Vertex2Gid(range.begin());
  out->cur_range.second = out->cur_range.first + range.size();

  get_out_edges(frag, eid_parser, out->cur_range.first, out->e_labels,
                out->e_labels_count, limit, &out->ei);

  out->index = 0;
  out->limit = limit;
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

int get_all_edges_next(GetAllEdgesIteratorImpl* iter, Edge* e_out) {
#ifndef NDEBUG
  LOG(INFO) << "enter " << __FUNCTION__;
#endif
  if (iter->cur_v_label >=
      static_cast<int>(iter->fragment->vertex_label_num())) {
    return -1;
  }
  if (iter->index == iter->limit) {
#ifndef NDEBUG
    LOG(INFO) << "finish " << __FUNCTION__ << " with reaching limit";
#endif
    return -1;
  }

  while (true) {
    if (out_edge_next(&iter->ei, e_out) == 0) {
#ifndef NDEBUG
      LOG(INFO) << "finish " << __FUNCTION__ << " no eout";
#endif
      ++iter->index;
      return 0;
    }

    VID_TYPE cur_vid = iter->ei.src + 1;
    if (cur_vid == iter->cur_range.second) {
      ++iter->cur_v_label;
      typename FRAGMENT_TYPE::vertex_range_t super_range, range;
      while (iter->cur_v_label < iter->fragment->vertex_label_num()) {
        super_range = iter->fragment->InnerVertices(iter->cur_v_label);
        range = get_sub_range(super_range, iter->chunk_sizes[iter->cur_v_label],
                              iter->channel_id);
        if (range.size() == 0) {
          ++iter->cur_v_label;
        } else {
          break;
        }
      }
      if (iter->cur_v_label == iter->fragment->vertex_label_num()) {
#ifndef NDEBUG
        LOG(INFO) << "finish " << __FUNCTION__ << " no extra v label";
#endif
        return -1;
      }
      iter->cur_range.first = iter->fragment->Vertex2Gid(range.begin());
      iter->cur_range.second = iter->cur_range.first + range.size();
      cur_vid = iter->cur_range.first;
    }

    free_edge_iterator(&iter->ei);
    get_out_edges(iter->fragment, iter->eid_parser, cur_vid, iter->e_labels,
                  iter->e_labels_count, iter->limit - iter->index, &iter->ei);
  }
#ifndef NDEBUG
  LOG(INFO) << "finish " << __FUNCTION__;
#endif
}

void free_edge_iterator(EdgeIteratorImpl* iter) {
  if (iter->lists != NULL) {
    free(iter->lists);
    iter->lists = NULL;
  }
}

void free_get_all_edges_iterator(GetAllEdgesIteratorImpl* iter) {
  if (iter->e_labels != NULL) {
    free(iter->e_labels);
    iter->e_labels = NULL;
  }
  free_edge_iterator(&iter->ei);
}

int get_property_as_bool(Property* property, bool* out) {
  if (property->type != BOOL) {
    return -1;
  }
  PodProperties pp;
  pp.long_value = property->len;
  *out = pp.bool_value;
  return 0;
}

int get_property_as_char(Property* property, char* out) {
  if (property->type != CHAR) {
    return -1;
  }
  PodProperties pp;
  pp.long_value = property->len;
  *out = pp.char_value;
  return 0;
}

int get_property_as_short(Property* property, int16_t* out) {
  if (property->type != SHORT) {
    return -1;
  }
  PodProperties pp;
  pp.long_value = property->len;
  *out = pp.int16_value;
  return 0;
}

int get_property_as_int(Property* property, int* out) {
  if (property->type != INT) {
    return -1;
  }
  PodProperties pp;
  pp.long_value = property->len;
  *out = pp.int_value;
  return 0;
}

int get_property_as_long(Property* property, int64_t* out) {
  if (property->type != LONG) {
    return -1;
  }
  *out = property->len;
  return 0;
}

int get_property_as_float(Property* property, float* out) {
  if (property->type != FLOAT) {
    return -1;
  }
  // in principle, the following is undefined behavior per standard
  // however, gcc (our toolchain) guarantees it to work as gcc extension
  // (https://gcc.gnu.org/bugs/#nonbugs)
  // TODO: fix it to be more compliant
  PodProperties pp;
  pp.long_value = property->len;
  *out = pp.float_value;
  return 0;
}

int get_property_as_double(Property* property, double* out) {
  if (property->type != DOUBLE) {
    return -1;
  }
  PodProperties pp;
  pp.long_value = property->len;
  *out = pp.double_value;
  return 0;
}

int get_property_as_string(Property* property, const char** out, int* out_len) {
  if (property->type != STRING) {
    return -1;
  }
  *out = static_cast<char*>(property->data);
  *out_len = property->len;
  return 0;
}

int get_property_as_bytes(Property* property, const char** out, int* out_len) {
  if (property->type != BYTES) {
    return -1;
  }
  *out = static_cast<char*>(property->data);
  *out_len = property->len;
  return 0;
}

int get_property_as_int_list(Property* property, const int** out,
                             int* out_len) {
  if (property->type != INT_LIST) {
    return -1;
  }
  return -1;  // FIXME
}

int get_property_as_long_list(Property* property, const int64_t** out,
                              int* out_len) {
  if (property->type != LONG_LIST) {
    return -1;
  }
  return -1;  // FIXME
}

int get_property_as_float_list(Property* property, const float** out,
                               int* out_len) {
  if (property->type != FLOAT_LIST) {
    return -1;
  }
  return -1;  // FIXME
}

int get_property_as_double_list(Property* property, const double** out,
                                int* out_len) {
  if (property->type != DOUBLE_LIST) {
    return -1;
  }
  return -1;  // FIXME
}

int get_property_as_string_list(Property* property, const char*** out,
                                const int** out_len, int* out_num) {
  if (property->type != STRING_LIST) {
    return -1;
  }
  return -1;  // FIXME
}

void free_property(Property* property) {}

}  // namespace htap_impl

