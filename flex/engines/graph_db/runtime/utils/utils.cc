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
#include "flex/engines/graph_db/runtime/utils/utils.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

VOpt parse_opt(const physical::GetV_VOpt& opt) {
  if (opt == physical::GetV_VOpt::GetV_VOpt_START) {
    return VOpt::kStart;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_END) {
    return VOpt::kEnd;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_OTHER) {
    return VOpt::kOther;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_BOTH) {
    return VOpt::kBoth;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_ITSELF) {
    return VOpt::kItself;
  } else {
    LOG(FATAL) << "unexpected GetV::Opt";
    return VOpt::kItself;
  }
}

Direction parse_direction(const physical::EdgeExpand_Direction& dir) {
  if (dir == physical::EdgeExpand_Direction_OUT) {
    return Direction::kOut;
  } else if (dir == physical::EdgeExpand_Direction_IN) {
    return Direction::kIn;
  } else if (dir == physical::EdgeExpand_Direction_BOTH) {
    return Direction::kBoth;
  }
  LOG(FATAL) << "not support..." << dir;
  return Direction::kOut;
}

std::vector<label_t> parse_tables(const algebra::QueryParams& query_params) {
  std::vector<label_t> tables;
  int tn = query_params.tables_size();
  for (int i = 0; i < tn; ++i) {
    const common::NameOrId& table = query_params.tables(i);
    tables.push_back(static_cast<label_t>(table.id()));
  }
  return tables;
}

std::vector<LabelTriplet> parse_label_triplets(
    const physical::PhysicalOpr_MetaData& meta) {
  std::vector<LabelTriplet> labels;
  if (meta.has_type()) {
    const common::IrDataType& t = meta.type();
    if (t.has_graph_type()) {
      const common::GraphDataType& gt = t.graph_type();
      if (gt.element_opt() == common::GraphDataType_GraphElementOpt::
                                  GraphDataType_GraphElementOpt_EDGE) {
        int label_num = gt.graph_data_type_size();
        for (int label_i = 0; label_i < label_num; ++label_i) {
          const common::GraphDataType_GraphElementLabel& gdt =
              gt.graph_data_type(label_i).label();
          labels.emplace_back(static_cast<label_t>(gdt.src_label().value()),
                              static_cast<label_t>(gdt.dst_label().value()),
                              static_cast<label_t>(gdt.label()));
        }
      }
    }
  }
  return labels;
}

template <typename T>
bool vertex_property_topN_impl(bool asc, size_t limit,
                               const std::shared_ptr<IVertexColumn>& col,
                               const GraphReadInterface& graph,
                               const std::string& prop_name,
                               std::vector<size_t>& offsets) {
  std::vector<GraphReadInterface::vertex_column_t<T>> property_columns;
  label_t label_num = graph.schema().vertex_label_num();
  for (label_t i = 0; i < label_num; ++i) {
    property_columns.emplace_back(graph.GetVertexColumn<T>(i, prop_name));
  }
  bool success = true;
  if (asc) {
    TopNGenerator<T, TopNAscCmp<T>> gen(limit);
    foreach_vertex(*col, [&](size_t idx, label_t label, vid_t v) {
      if (!property_columns[label].is_null()) {
        gen.push(property_columns[label].get_view(v), idx);
      } else {
        success = false;
      }
    });
    if (success) {
      gen.generate_indices(offsets);
    }
  } else {
    TopNGenerator<T, TopNDescCmp<T>> gen(limit);
    foreach_vertex(*col, [&](size_t idx, label_t label, vid_t v) {
      if (!property_columns[label].is_null()) {
        gen.push(property_columns[label].get_view(v), idx);
      } else {
        success = false;
      }
    });
    if (success) {
      gen.generate_indices(offsets);
    }
  }

  return success;
}

template <typename T>
bool vertex_id_topN_impl(bool asc, size_t limit,
                         const std::shared_ptr<IVertexColumn>& col,
                         const GraphReadInterface& graph,
                         std::vector<size_t>& offsets) {
  if (asc) {
    TopNGenerator<T, TopNAscCmp<T>> gen(limit);
    foreach_vertex(*col, [&](size_t idx, label_t label, vid_t v) {
      auto oid = AnyConverter<T>::from_any(graph.GetVertexId(label, v));
      gen.push(oid, idx);
    });
    gen.generate_indices(offsets);
  } else {
    TopNGenerator<T, TopNDescCmp<T>> gen(limit);
    foreach_vertex(*col, [&](size_t idx, label_t label, vid_t v) {
      auto oid = AnyConverter<T>::from_any(graph.GetVertexId(label, v));
      gen.push(oid, idx);
    });
    gen.generate_indices(offsets);
  }
  return true;
}

bool vertex_id_topN(bool asc, size_t limit,
                    const std::shared_ptr<IVertexColumn>& col,
                    const GraphReadInterface& graph,
                    std::vector<size_t>& offsets) {
  if (col->get_labels_set().size() != 1) {
    return false;
  }
  auto& vec =
      graph.schema().get_vertex_primary_key(*col->get_labels_set().begin());
  if (vec.size() != 1) {
    return false;
  }
  auto type = std::get<0>(vec[0]);
  if (type == PropertyType::Int64()) {
    return vertex_id_topN_impl<int64_t>(asc, limit, col, graph, offsets);
  } else if (type == PropertyType::StringView()) {
    return vertex_id_topN_impl<std::string_view>(asc, limit, col, graph,
                                                 offsets);
  } else if (type == PropertyType::Int32()) {
    return vertex_id_topN_impl<int32_t>(asc, limit, col, graph, offsets);
  } else {
    return false;
  }
}

bool vertex_property_topN(bool asc, size_t limit,
                          const std::shared_ptr<IVertexColumn>& col,
                          const GraphReadInterface& graph,
                          const std::string& prop_name,
                          std::vector<size_t>& offsets) {
  std::vector<PropertyType> prop_types;
  const auto& labels = col->get_labels_set();
  for (auto l : labels) {
    const auto& prop_names = graph.schema().get_vertex_property_names(l);
    int prop_names_size = prop_names.size();
    for (int prop_id = 0; prop_id < prop_names_size; ++prop_id) {
      if (prop_names[prop_id] == prop_name) {
        prop_types.push_back(graph.schema().get_vertex_properties(l)[prop_id]);
        break;
      }
    }
  }
  if (prop_types.size() != labels.size()) {
    return false;
  }
  for (size_t k = 1; k < prop_types.size(); ++k) {
    if (prop_types[k] != prop_types[0]) {
      LOG(INFO) << "multiple types...";
      return false;
    }
  }
  if (prop_types[0] == PropertyType::Date()) {
    return vertex_property_topN_impl<Date>(asc, limit, col, graph, prop_name,
                                           offsets);
  } else if (prop_types[0] == PropertyType::Int32()) {
    return vertex_property_topN_impl<int>(asc, limit, col, graph, prop_name,
                                          offsets);
  } else if (prop_types[0] == PropertyType::Int64()) {
    return vertex_property_topN_impl<int64_t>(asc, limit, col, graph, prop_name,
                                              offsets);
  } else if (prop_types[0] == PropertyType::String()) {
    return vertex_property_topN_impl<std::string_view>(asc, limit, col, graph,
                                                       prop_name, offsets);
  } else if (prop_types[0] == PropertyType::Day()) {
    return vertex_property_topN_impl<Day>(asc, limit, col, graph, prop_name,
                                          offsets);
  } else {
    LOG(INFO) << "prop type not support..." << prop_types[0];
    return false;
  }
}

}  // namespace runtime

}  // namespace gs
