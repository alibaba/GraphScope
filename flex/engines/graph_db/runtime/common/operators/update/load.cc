/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "flex/engines/graph_db/runtime/common/operators/update/load.h"

namespace gs {
namespace runtime {

bl::result<WriteContext> Load::load_single_edge(
    GraphInsertInterface& graph, WriteContext&& ctxs, label_t src_label_id,
    label_t dst_label_id, label_t edge_label_id, PropertyType& src_pk_type,
    PropertyType& dst_pk_type, PropertyType& edge_prop_type, int src_index,
    int dst_index, int prop_index) {
  // grape::EmptyType
  if (edge_prop_type == PropertyType::kEmpty) {
    auto& src = ctxs.get(src_index);
    auto& dst = ctxs.get(dst_index);
    for (int i = 0; i < ctxs.row_num(); i++) {
      graph.AddEdge(src_label_id, src.get(i).to_any(src_pk_type), dst_label_id,
                    dst.get(i).to_any(dst_pk_type), edge_label_id, Any());
    }
  } else {
    auto& src = ctxs.get(src_index);
    auto& dst = ctxs.get(dst_index);
    auto& prop = ctxs.get(prop_index);
    for (int i = 0; i < ctxs.row_num(); i++) {
      graph.AddEdge(src_label_id, src.get(i).to_any(src_pk_type), dst_label_id,
                    dst.get(i).to_any(dst_pk_type), edge_label_id,
                    prop.get(i).to_any(edge_prop_type));
    }
  }
  return ctxs;
}

bl::result<WriteContext> Load::load_single_vertex(
    GraphInsertInterface& graph, WriteContext&& ctxs, label_t label,
    PropertyType& pk_type, int id_col, const std::vector<int>& properties,
    const std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                                 PropertyType, PropertyType, int, int, int>>&
        edges) {
  int row_num = ctxs.row_num();
  auto& id_c = ctxs.get(id_col);

  auto& prop_types = graph.schema().get_vertex_properties(label);

  for (int row = 0; row < row_num; ++row) {
    const auto& id = id_c.get(row).to_any(pk_type);
    std::vector<Any> props;
    for (size_t j = 0; j < properties.size(); ++j) {
      props.push_back(ctxs.get(properties[j]).get(row).to_any(prop_types[j]));
    }
    graph.AddVertex(label, id, props);
  }

  for (const auto& edge : edges) {
    label_t src_label_id, dst_label_id, edge_label_id;
    PropertyType src_pk_type, dst_pk_type, edge_prop_type;
    int src_index, dst_index, prop_index;
    std::tie(src_label_id, dst_label_id, edge_label_id, src_pk_type,
             dst_pk_type, edge_prop_type, src_index, dst_index, prop_index) =
        edge;
    load_single_edge(graph, std::move(ctxs), src_label_id, dst_label_id,
                     edge_label_id, src_pk_type, dst_pk_type, edge_prop_type,
                     src_index, dst_index, prop_index);
  }
  return ctxs;
}

bl::result<WriteContext> Load::load(
    GraphInsertInterface& graph, WriteContext&& ctxs,
    const std::vector<std::tuple<label_t, int, PropertyType, std::vector<int>>>&
        vertex_mappings,
    const std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                                 PropertyType, PropertyType, int, int, int>>&
        edge_mappings) {
  int row_num = ctxs.row_num();
  for (const auto& vertex_mapping : vertex_mappings) {
    auto [label, id_col, pk_type, properties] = vertex_mapping;
    auto id_c = ctxs.get(id_col);
    auto& prop_types = graph.schema().get_vertex_properties(label);
    for (int row = 0; row < row_num; ++row) {
      const auto& id = id_c.get(row).to_any(pk_type);
      std::vector<Any> props;
      for (size_t j = 0; j < properties.size(); ++j) {
        props.push_back(ctxs.get(properties[j]).get(row).to_any(prop_types[j]));
      }
      graph.AddVertex(label, id, props);
    }
  }
  for (const auto& edge_mapping : edge_mappings) {
    label_t src_label_id, dst_label_id, edge_label_id;
    PropertyType src_pk_type, dst_pk_type, edge_prop_type;
    int src_index, dst_index, prop_index;
    std::tie(src_label_id, dst_label_id, edge_label_id, src_pk_type,
             dst_pk_type, edge_prop_type, src_index, dst_index, prop_index) =
        edge_mapping;
    load_single_edge(graph, std::move(ctxs), src_label_id, dst_label_id,
                     edge_label_id, src_pk_type, dst_pk_type, edge_prop_type,
                     src_index, dst_index, prop_index);
  }
  return ctxs;
}

}  // namespace runtime
}  // namespace gs
