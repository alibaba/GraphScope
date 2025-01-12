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
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
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

std::shared_ptr<IContextColumnBuilder> create_column_builder(RTAnyType type) {
  switch (type) {
  case RTAnyType::kI64Value:
    return std::make_shared<ValueColumnBuilder<int64_t>>();
  case RTAnyType::kStringValue:
    return std::make_shared<ValueColumnBuilder<std::string_view>>();
  case RTAnyType::kVertex:
    return std::make_shared<MLVertexColumnBuilder>();
  case RTAnyType::kI32Value:
    return std::make_shared<ValueColumnBuilder<int32_t>>();
  case RTAnyType::kDate32:
    return std::make_shared<ValueColumnBuilder<Day>>();
  case RTAnyType::kTimestamp:
    return std::make_shared<ValueColumnBuilder<Date>>();
  case RTAnyType::kU64Value:
    return std::make_shared<ValueColumnBuilder<uint64_t>>();
  case RTAnyType::kBoolValue:
    // fix me
    return std::make_shared<ValueColumnBuilder<bool>>();
  case RTAnyType::kEdge:
    return std::make_shared<BDMLEdgeColumnBuilder>();
  case RTAnyType::kStringSetValue:
    return std::make_shared<ValueColumnBuilder<std::set<std::string>>>();
  default:
    LOG(FATAL) << "unsupport type: " << static_cast<int>(type);
    break;
  }
  return nullptr;
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
  for (auto l : col->get_labels_set()) {
    const auto& prop_names = graph.schema().get_vertex_property_names(l);
    int prop_names_size = prop_names.size();
    for (int prop_id = 0; prop_id < prop_names_size; ++prop_id) {
      if (prop_names[prop_id] == prop_name) {
        prop_types.push_back(graph.schema().get_vertex_properties(l)[prop_id]);
        break;
      }
    }
  }
  if (prop_types.empty()) {
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

std::shared_ptr<IContextColumn> build_optional_column_beta(const Expr& expr,
                                                           size_t row_num) {
  switch (expr.type()) {
  case RTAnyType::kI64Value: {
    OptionalValueColumnBuilder<int64_t> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      auto v = expr.eval_path(i, 0);
      if (v.is_null()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(v.as_int64(), true);
      }
    }

    return builder.finish();
  } break;
  case RTAnyType::kI32Value: {
    OptionalValueColumnBuilder<int> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      auto v = expr.eval_path(i, 0);
      if (v.is_null()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(v.as_int32(), true);
      }
    }

    return builder.finish();
  } break;
  case RTAnyType::kF64Value: {
    OptionalValueColumnBuilder<double> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      auto v = expr.eval_path(i, 0);
      if (v.is_null()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(v.as_double(), true);
      }
    }

    return builder.finish();
  } break;
  case RTAnyType::kMap: {
    auto builder = expr.builder();
    for (size_t i = 0; i < row_num; ++i) {
      builder->push_back_elem(expr.eval_path(i, 0));
    }
    return builder->finish();
  } break;
  case RTAnyType::kTuple: {
    OptionalValueColumnBuilder<Tuple> builder;
    for (size_t i = 0; i < row_num; ++i) {
      auto v = expr.eval_path(i, 0);
      if (v.is_null()) {
        builder.push_back_null();
      } else {
        builder.push_back_elem(v);
      }
    }
    return builder.finish();
  } break;
  default: {
    LOG(FATAL) << "not support" << static_cast<int>(expr.type());
    break;
  }
  }
  return nullptr;
}

std::shared_ptr<IContextColumn> build_column_beta(const Expr& expr,
                                                  size_t row_num) {
  if (expr.is_optional()) {
    return build_optional_column_beta(expr, row_num);
  }
  switch (expr.type()) {
  case RTAnyType::kI64Value: {
    ValueColumnBuilder<int64_t> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_int64());
    }
    return builder.finish();
  } break;
  case RTAnyType::kStringValue: {
    ValueColumnBuilder<std::string_view> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(std::string(expr.eval_path(i).as_string()));
    }
    return builder.finish();
  } break;
  case RTAnyType::kDate32: {
    ValueColumnBuilder<Day> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_date32());
    }
    return builder.finish();
  } break;
  case RTAnyType::kTimestamp: {
    ValueColumnBuilder<Date> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_timestamp());
    }
  } break;
  case RTAnyType::kVertex: {
    MLVertexColumnBuilder builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_vertex(expr.eval_path(i).as_vertex());
    }

    return builder.finish();
  } break;
  case RTAnyType::kI32Value: {
    ValueColumnBuilder<int> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_int32());
    }

    return builder.finish();
  } break;
  case RTAnyType::kF64Value: {
    ValueColumnBuilder<double> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_double());
    }
    return builder.finish();
  } break;
  case RTAnyType::kEdge: {
    BDMLEdgeColumnBuilder builder;
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_elem(expr.eval_path(i));
    }
    return builder.finish();
  }
  case RTAnyType::kTuple: {
    if (expr.is_optional()) {
      OptionalValueColumnBuilder<Tuple> builder;
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i);
        if (v.is_null()) {
          builder.push_back_null();
        } else {
          builder.push_back_elem(v);
        }
      }
      return builder.finish();
    } else {
      ValueColumnBuilder<Tuple> builder;
      for (size_t i = 0; i < row_num; ++i) {
        builder.push_back_elem(expr.eval_path(i));
      }
      return builder.finish();
    }
  }
  case RTAnyType::kList: {
    auto builder = expr.builder();
    for (size_t i = 0; i < row_num; ++i) {
      builder->push_back_elem(expr.eval_path(i));
    }
    // set impls
    auto& list_builder = dynamic_cast<ListValueColumnBuilderBase&>(*builder);
    if (!list_builder.impls_has_been_set()) {
      list_builder.set_list_impls(expr.get_list_impls());
    }
    return builder->finish();
  }
  case RTAnyType::kMap: {
    auto builder = expr.builder();
    for (size_t i = 0; i < row_num; ++i) {
      builder->push_back_elem(expr.eval_path(i));
    }
    return builder->finish();
  }
  default:
    LOG(FATAL) << "not support - " << static_cast<int>(expr.type());
    break;
  }

  return nullptr;
}

}  // namespace runtime

}  // namespace gs
