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

Direction parse_direction(const physical::EdgeExpand_Direction& dir) {
  if (dir == physical::EdgeExpand_Direction_OUT) {
    return Direction::kOut;
  } else if (dir == physical::EdgeExpand_Direction_IN) {
    return Direction::kIn;
  } else if (dir == physical::EdgeExpand_Direction_BOTH) {
    return Direction::kBoth;
  }
  LOG(FATAL) << "not support...";
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

std::shared_ptr<IContextColumn> create_column(
    const common::IrDataType& data_type) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType:
    LOG(FATAL) << "not support";
    break;
  case common::IrDataType::kGraphType: {
    const common::GraphDataType& graph_data_type = data_type.graph_type();
    common::GraphDataType_GraphElementOpt elem_opt =
        graph_data_type.element_opt();
    int label_num = graph_data_type.graph_data_type_size();
    if (elem_opt == common::GraphDataType_GraphElementOpt::
                        GraphDataType_GraphElementOpt_VERTEX) {
      if (label_num == 1) {
        label_t v_label = static_cast<label_t>(
            graph_data_type.graph_data_type(0).label().label());
        return std::make_shared<SLVertexColumn>(v_label);
      } else if (label_num > 1) {
        return std::make_shared<MLVertexColumn>();
      } else {
        LOG(FATAL) << "unexpected type";
      }
    } else if (elem_opt == common::GraphDataType_GraphElementOpt::
                               GraphDataType_GraphElementOpt_EDGE) {
      LOG(FATAL) << "unexpected type";
    } else {
      LOG(FATAL) << "unexpected type";
    }
  } break;
  default:
    LOG(FATAL) << "unexpected type";
    break;
  }
  return nullptr;
}

std::shared_ptr<IContextColumn> create_column_beta(RTAnyType type) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<ValueColumn<int64_t>>();
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<ValueColumn<std::string_view>>();
  case RTAnyType::RTAnyTypeImpl::kVertex:
    return std::make_shared<MLVertexColumn>();
  default:
    LOG(FATAL) << "unsupport type: " << static_cast<int>(type.type_enum_);
    break;
  }
  return nullptr;
}

std::shared_ptr<IContextColumnBuilder> create_column_builder(RTAnyType type) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<ValueColumnBuilder<int64_t>>();
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<ValueColumnBuilder<std::string_view>>();
  case RTAnyType::RTAnyTypeImpl::kVertex:
    return std::make_shared<MLVertexColumnBuilder>();
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return std::make_shared<ValueColumnBuilder<int32_t>>();
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return std::make_shared<ValueColumnBuilder<Date>>();
  case RTAnyType::RTAnyTypeImpl::kU64Value:
    return std::make_shared<ValueColumnBuilder<uint64_t>>();
  case RTAnyType::RTAnyTypeImpl::kBoolValue:
    // fix me
    return std::make_shared<ValueColumnBuilder<bool>>();
  case RTAnyType::RTAnyTypeImpl::kEdge:
    return std::make_shared<BDMLEdgeColumnBuilder>();
  case RTAnyType::RTAnyTypeImpl::kStringSetValue:
    return std::make_shared<ValueColumnBuilder<std::set<std::string>>>();
  default:
    LOG(FATAL) << "unsupport type: " << static_cast<int>(type.type_enum_);
    break;
  }
  return nullptr;
}

std::shared_ptr<IContextColumn> build_optional_column(
    const common::IrDataType& data_type, const Expr& expr, size_t row_num) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    switch (data_type.data_type()) {
    case common::DataType::INT64: {
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
    case common::DataType::INT32: {
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
    case common::DataType::DOUBLE: {
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
    case common::DataType::BOOLEAN: {
      OptionalValueColumnBuilder<bool> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i, 0);
        if (v.is_null()) {
          builder.push_back_null();
        } else {
          builder.push_back_opt(v.as_bool(), true);
        }
      }

      return builder.finish();
    } break;
    case common::DataType::STRING: {
      OptionalValueColumnBuilder<std::string_view> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i, 0);
        if (v.is_null()) {
          builder.push_back_null();
        } else {
          builder.push_back_opt(std::string(v.as_string()), true);
        }
      }

      return builder.finish();
    } break;
    case common::DataType::TIMESTAMP: {
      OptionalValueColumnBuilder<Date> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i, 0);
        if (v.is_null()) {
          builder.push_back_null();
        } else {
          builder.push_back_opt(v.as_date32(), true);
        }
      }

      return builder.finish();
    } break;

    default: {
      LOG(FATAL) << "not support"
                 << common::DataType_Name(data_type.data_type());
      break;
    }
    }
  }
  case common::IrDataType::TYPE_NOT_SET: {
    return build_column_beta(expr, row_num);
  } break;
  default: {
    LOG(FATAL) << "not support" << data_type.DebugString();
    break;
  }
  }
  return nullptr;
}

std::shared_ptr<IContextColumn> build_column(
    const common::IrDataType& data_type, const Expr& expr, size_t row_num) {
  if (expr.is_optional()) {
    return build_optional_column(data_type, expr, row_num);
  }
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    switch (data_type.data_type()) {
    case common::DataType::INT64: {
      ValueColumnBuilder<int64_t> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_int64();
        builder.push_back_opt(v);
      }

      return builder.finish();
    } break;
    case common::DataType::INT32: {
      ValueColumnBuilder<int> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_int32();
        builder.push_back_opt(v);
      }

      return builder.finish();
    } break;
    case common::DataType::STRING: {
      ValueColumnBuilder<std::string_view> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_string();
        builder.push_back_opt(std::string(v));
      }

      return builder.finish();
    } break;
    case common::DataType::DATE32: {
      ValueColumnBuilder<Date> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_date32();
        builder.push_back_opt(v);
      }

      return builder.finish();
    } break;
    case common::DataType::STRING_ARRAY: {
      ValueColumnBuilder<std::set<std::string>> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        const auto& v = expr.eval_path(i).as_string_set();
        builder.push_back_opt(v);
      }

      return builder.finish();
    } break;
    case common::DataType::TIMESTAMP: {
      ValueColumnBuilder<Date> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_date32();
        builder.push_back_opt(v);
      }

      return builder.finish();
    } break;
    case common::DataType::BOOLEAN: {
      ValueColumnBuilder<bool> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_bool();
        builder.push_back_opt(v);
      }
      return builder.finish();
    } break;
    case common::DataType::DOUBLE: {
      ValueColumnBuilder<double> builder;
      builder.reserve(row_num);
      for (size_t i = 0; i < row_num; ++i) {
        auto v = expr.eval_path(i).as_double();
        builder.push_back_opt(v);
      }
      return builder.finish();
    } break;
    default: {
      LOG(FATAL) << "not support: "
                 << common::DataType_Name(data_type.data_type());
    }
    }
  } break;
  case common::IrDataType::kGraphType: {
    const common::GraphDataType& graph_data_type = data_type.graph_type();
    common::GraphDataType_GraphElementOpt elem_opt =
        graph_data_type.element_opt();
    int label_num = graph_data_type.graph_data_type_size();
    if (elem_opt == common::GraphDataType_GraphElementOpt::
                        GraphDataType_GraphElementOpt_VERTEX) {
      if (label_num == 1) {
        label_t v_label = static_cast<label_t>(
            graph_data_type.graph_data_type(0).label().label());
        SLVertexColumnBuilder builder(v_label);
        builder.reserve(row_num);
        for (size_t i = 0; i < row_num; ++i) {
          builder.push_back_opt(expr.eval_path(i).as_vertex().second);
        }

        return builder.finish();
      } else if (label_num > 1) {
        MLVertexColumnBuilder builder;
        builder.reserve(row_num);
        for (size_t i = 0; i < row_num; ++i) {
          builder.push_back_vertex(expr.eval_path(i).as_vertex());
        }

        return builder.finish();
      } else {
        LOG(FATAL) << "unexpected type";
      }
    } else if (elem_opt == common::GraphDataType_GraphElementOpt::
                               GraphDataType_GraphElementOpt_EDGE) {
      // LOG(FATAL) << "unexpected type";
      BDMLEdgeColumnBuilder builder;
      for (size_t i = 0; i < row_num; ++i) {
        builder.push_back_elem(expr.eval_path(i));
      }
      return builder.finish();
    } else {
      LOG(FATAL) << "unexpected type";
    }
  } break;
  case common::IrDataType::TYPE_NOT_SET: {
    return build_column_beta(expr, row_num);
  } break;
  default:
    LOG(FATAL) << "unexpected type"
               << common::DataType_Name(data_type.data_type());
    break;
  }

  return nullptr;
}

std::shared_ptr<IContextColumn> build_optional_column_beta(const Expr& expr,
                                                           size_t row_num) {
  switch (expr.type().type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value: {
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
  case RTAnyType::RTAnyTypeImpl::kI32Value: {
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
  case RTAnyType::RTAnyTypeImpl::kF64Value: {
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
  case RTAnyType::RTAnyTypeImpl::kMap: {
    auto builder = expr.builder();
    for (size_t i = 0; i < row_num; ++i) {
      builder->push_back_elem(expr.eval_path(i, 0));
    }
    return builder->finish();
  } break;
  default: {
    LOG(FATAL) << "not support";
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
  switch (expr.type().type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value: {
    ValueColumnBuilder<int64_t> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_int64());
    }

    return builder.finish();
  } break;
  case RTAnyType::RTAnyTypeImpl::kStringValue: {
    ValueColumnBuilder<std::string_view> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(std::string(expr.eval_path(i).as_string()));
    }

    return builder.finish();
  } break;
  case RTAnyType::RTAnyTypeImpl::kDate32: {
    ValueColumnBuilder<Date> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_date32());
    }

    return builder.finish();
  } break;
  case RTAnyType::RTAnyTypeImpl::kVertex: {
    MLVertexColumnBuilder builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_vertex(expr.eval_path(i).as_vertex());
    }

    return builder.finish();
  } break;
  case RTAnyType::RTAnyTypeImpl::kI32Value: {
    ValueColumnBuilder<int> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_int32());
    }

    return builder.finish();
  } break;
  case RTAnyType::RTAnyTypeImpl::kF64Value: {
    ValueColumnBuilder<double> builder;
    builder.reserve(row_num);
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_opt(expr.eval_path(i).as_double());
    }
    return builder.finish();
  } break;
  case RTAnyType::RTAnyTypeImpl::kEdge: {
    BDMLEdgeColumnBuilder builder;
    for (size_t i = 0; i < row_num; ++i) {
      builder.push_back_elem(expr.eval_path(i));
    }
    return builder.finish();
  }
  case RTAnyType::RTAnyTypeImpl::kTuple: {
    if (expr.type().null_able_) {
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
  case RTAnyType::RTAnyTypeImpl::kList: {
    auto builder = expr.builder();
    for (size_t i = 0; i < row_num; ++i) {
      builder->push_back_elem(expr.eval_path(i));
    }
    return builder->finish();
  }
  case RTAnyType::RTAnyTypeImpl::kMap: {
    auto builder = expr.builder();
    for (size_t i = 0; i < row_num; ++i) {
      builder->push_back_elem(expr.eval_path(i));
    }
    return builder->finish();
  }
  default:
    LOG(FATAL) << "not support - " << static_cast<int>(expr.type().type_enum_);
    break;
  }

  return nullptr;
}

}  // namespace runtime

}  // namespace gs
