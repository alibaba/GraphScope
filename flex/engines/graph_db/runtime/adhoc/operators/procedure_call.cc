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

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"
#include "flex/proto_generated_gie/algebra.pb.h"

namespace gs {
namespace runtime {

std::shared_ptr<IContextColumn> any_vec_to_column(
    const std::vector<RTAny>& any_vec) {
  if (any_vec.empty()) {
    return nullptr;
  }
  auto first = any_vec[0].type();
  if (first == RTAnyType::kBoolValue) {
    ValueColumnBuilder<bool> builder;
    for (auto& any : any_vec) {
      builder.push_back_opt(any.as_bool());
    }
    return builder.finish();
  } else if (first == RTAnyType::kI32Value) {
    ValueColumnBuilder<int32_t> builder;
    for (auto& any : any_vec) {
      builder.push_back_opt(any.as_int32());
    }
    return builder.finish();
  } else if (first == RTAnyType::kI64Value) {
    ValueColumnBuilder<int64_t> builder;
    for (auto& any : any_vec) {
      builder.push_back_opt(any.as_int64());
    }
    return builder.finish();
  } else if (first == RTAnyType::kF64Value) {
    ValueColumnBuilder<double> builder;
    for (auto& any : any_vec) {
      builder.push_back_opt(any.as_double());
    }
    return builder.finish();
  } else if (first == RTAnyType::kStringValue) {
    ValueColumnBuilder<std::string_view> builder;
    for (auto& any : any_vec) {
      builder.push_back_elem(any);
    }
    return builder.finish();
  } else {
    LOG(FATAL) << "Unsupported RTAny type: "
               << static_cast<int>(first.type_enum_);
  }
}

RTAny vertex_to_rt_any(const results::Vertex& vertex) {
  auto label_id = vertex.label().id();
  auto label_id_vid = decode_unique_vertex_id(vertex.id());
  CHECK(label_id == label_id_vid.first) << "Inconsistent label id.";
  return RTAny::from_vertex(label_id, label_id_vid.second);
}

RTAny edge_to_rt_any(const results::Edge& edge) {
  LOG(FATAL) << "Not implemented.";
  return RTAny();
}

RTAny object_to_rt_any(const common::Value& val) {
  if (val.item_case() == common::Value::kBoolean) {
    return RTAny::from_bool(val.boolean());
  } else if (val.item_case() == common::Value::kI32) {
    return RTAny::from_int32(val.i32());
  } else if (val.item_case() == common::Value::kI64) {
    return RTAny::from_int64(val.i64());
  } else if (val.item_case() == common::Value::kF64) {
    return RTAny::from_double(val.f64());
  } else if (val.item_case() == common::Value::kStr) {
    return RTAny::from_string(val.str());
  } else {
    LOG(FATAL) << "Unsupported value type: " << val.item_case();
  }
}

RTAny element_to_rt_any(const results::Element& element) {
  if (element.inner_case() == results::Element::kVertex) {
    return vertex_to_rt_any(element.vertex());
  } else if (element.inner_case() == results::Element::kEdge) {
    return edge_to_rt_any(element.edge());
  } else if (element.inner_case() == results::Element::kObject) {
    return object_to_rt_any(element.object());
  } else {
    LOG(FATAL) << "Unsupported element type: " << element.inner_case();
  }
}

RTAny collection_to_rt_any(const results::Collection& collection) {
  std::vector<RTAny> values;
  for (const auto& element : collection.collection()) {
    values.push_back(element_to_rt_any(element));
  }
  return RTAny::from_tuple(std::move(values));
}

RTAny map_to_rt_any(const results::KeyValues& map) {
  LOG(FATAL) << "Not implemented.";
}

RTAny column_to_rt_any(const results::Column& column) {
  auto& entry = column.entry();
  if (entry.has_element()) {
    return element_to_rt_any(entry.element());
  } else if (entry.has_collection()) {
    return collection_to_rt_any(entry.collection());
  } else {
    return map_to_rt_any(entry.map());
  }
}

std::vector<RTAny> result_to_rt_any(const results::Results& result) {
  auto& record = result.record();
  if (record.columns_size() == 0) {
    LOG(WARNING) << "Empty result.";
    return {};
  } else {
    std::vector<RTAny> tuple;
    for (int32_t i = 0; i < record.columns_size(); ++i) {
      tuple.push_back(column_to_rt_any(record.columns(i)));
    }
    return tuple;
  }
}

std::pair<std::vector<std::shared_ptr<IContextColumn>>, std::vector<size_t>>
collective_result_vec_to_column(
    int32_t expect_col_num,
    const std::vector<results::CollectiveResults>& collective_results_vec) {
  std::vector<size_t> offsets;
  offsets.push_back(0);
  size_t record_cnt = 0;
  for (size_t i = 0; i < collective_results_vec.size(); ++i) {
    record_cnt += collective_results_vec[i].results_size();
    offsets.push_back(record_cnt);
  }
  LOG(INFO) << "record_cnt: " << record_cnt;
  std::vector<std::vector<RTAny>> any_vec(expect_col_num);
  for (size_t i = 0; i < collective_results_vec.size(); ++i) {
    for (int32_t j = 0; j < collective_results_vec[i].results_size(); ++j) {
      auto tuple = result_to_rt_any(collective_results_vec[i].results(j));
      CHECK(tuple.size() == (size_t) expect_col_num)
          << "Inconsistent column number.";
      for (int32_t k = 0; k < expect_col_num; ++k) {
        any_vec[k].push_back(tuple[k]);
      }
    }
  }
  std::vector<std::shared_ptr<IContextColumn>> columns;
  for (int32_t i = 0; i < expect_col_num; ++i) {
    columns.push_back(any_vec_to_column(any_vec[i]));
  }
  return std::make_pair(columns, offsets);
}

procedure::Query fill_in_query(const procedure::Query& query,
                               const Context& ctx, size_t idx) {
  procedure::Query real_query;
  real_query.mutable_query_name()->CopyFrom(query.query_name());
  for (auto& param : query.arguments()) {
    auto argument = real_query.add_arguments();
    if (param.value_case() == procedure::Argument::kVar) {
      auto& var = param.var();
      auto tag = var.tag().id();
      auto col = ctx.get(tag);
      if (col == nullptr) {
        LOG(ERROR) << "Tag not found: " << tag;
        continue;
      }
      auto val = col->get_elem(idx);
      auto const_value = argument->mutable_const_();
      if (val.type() == gs::runtime::RTAnyType::kVertex) {
        LOG(FATAL) << "Not implemented yet";
      } else if (val.type() == gs::runtime::RTAnyType::kEdge) {
        LOG(FATAL) << "Not implemented yet";
      } else if (val.type() == gs::runtime::RTAnyType::kI64Value) {
        const_value->set_i64(val.as_int64());
      } else if (val.type() == gs::runtime::RTAnyType::kI32Value) {
        const_value->set_i32(val.as_int32());
      } else if (val.type() == gs::runtime::RTAnyType::kStringValue) {
        const_value->set_str(std::string(val.as_string()));
      } else {
        LOG(ERROR) << "Unsupported type: "
                   << static_cast<int32_t>(val.type().type_enum_);
      }
    } else {
      argument->CopyFrom(param);
    }
  }
  return real_query;
}

/**
 * @brief Evaluate the ProcedureCall operator.
 * The ProcedureCall operator is used to call a stored procedure, which is
 * already registered in the system. The return value of the stored procedure is
 * a result::CollectiveResults object, we need to convert it to a Column, and
 * append to the current context.
 *
 *
 * @param opr The ProcedureCall operator.
 * @param txn The read transaction.
 * @param ctx The input context.
 *
 * @return bl::result<Context> The output context.
 *
 *
 */
bl::result<Context> eval_procedure_call(std::vector<int32_t> aliases,
                                        const physical::ProcedureCall& opr,
                                        const ReadTransaction& txn,
                                        Context&& ctx) {
  auto& query = opr.query();
  auto& proc_name = query.query_name();

  if (proc_name.item_case() == common::NameOrId::kName) {
    // TODO: avoid calling graphDB here.
    auto& db = gs::GraphDB::get();
    //------------------------------------------
    // FIXME: sess id is hard coded to 0
    auto& sess = db.GetSession(0);

    AppBase* app = sess.GetApp(proc_name.name());
    if (!app) {
      RETURN_BAD_REQUEST_ERROR("Stored procedure not found: " +
                               proc_name.name());
    }
    ReadAppBase* read_app = dynamic_cast<ReadAppBase*>(app);
    if (!app) {
      RETURN_BAD_REQUEST_ERROR("Stored procedure is not a read procedure: " +
                               proc_name.name());
    }

    std::vector<results::CollectiveResults> results;
    // Iterate over current context.
    for (size_t i = 0; i < ctx.row_num(); ++i) {
      // Call the procedure.
      // Use real values from the context to replace the placeholders in the
      // query.
      auto real_query = fill_in_query(query, ctx, i);
      // We need to serialize the protobuf-based arguments to the input format
      // that a cypher procedure can accept.
      auto query_str = real_query.SerializeAsString();
      // append CYPHER_PROTO as the last byte as input_format
      query_str.push_back(static_cast<char>(
          GraphDBSession::InputFormat::kCypherProtoProcedure));
      std::vector<char> buffer;
      Encoder encoder(buffer);
      Decoder decoder(query_str.data(), query_str.size());
      if (!read_app->run(sess, decoder, encoder)) {
        RETURN_CALL_PROCEDURE_ERROR("Failed to call procedure: ");
      }
      // Decode the result from the encoder.
      Decoder result_decoder(buffer.data(), buffer.size());
      if (result_decoder.size() < 4) {
        LOG(ERROR) << "Unexpected result size: " << result_decoder.size();
        RETURN_CALL_PROCEDURE_ERROR("Unexpected result size");
      }
      std::string collective_results_str(result_decoder.get_string());
      results::CollectiveResults collective_results;
      if (!collective_results.ParseFromString(collective_results_str)) {
        LOG(ERROR) << "Failed to parse CollectiveResults";
        RETURN_CALL_PROCEDURE_ERROR("Failed to parse procedure's result");
      }
      results.push_back(collective_results);
    }

    auto column_and_offsets =
        collective_result_vec_to_column(aliases.size(), results);
    auto& columns = column_and_offsets.first;
    auto& offsets = column_and_offsets.second;
    if (columns.size() != aliases.size()) {
      LOG(ERROR) << "Column size mismatch: " << columns.size() << " vs "
                 << aliases.size();
      RETURN_CALL_PROCEDURE_ERROR("Column size mismatch");
    }
    LOG(INFO) << "Procedure call returns " << columns.size() << " columns";
    if (columns.size() >= 1) {
      ctx.set_with_reshuffle(aliases[0], columns[0], offsets);
    }
    for (size_t i = 1; i < columns.size(); ++i) {
      ctx.set(aliases[i], columns[i]);
    }
    return std::move(ctx);
  } else {
    LOG(ERROR) << "Currently only support calling stored procedure by name";
    RETURN_UNSUPPORTED_ERROR(
        "Currently only support calling stored procedure by name");
  }
}

}  // namespace runtime
}  // namespace gs
