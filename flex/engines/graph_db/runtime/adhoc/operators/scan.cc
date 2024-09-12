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

#include "flex/engines/graph_db/runtime/common/operators/scan.h"
#include "flex/engines/graph_db/runtime/adhoc/expr_impl.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
namespace gs {

namespace runtime {

static bool is_find_vertex(const physical::Scan& scan_opr,
                           const std::map<std::string, std::string>& params,
                           label_t& label, int64_t& vertex_id, int& alias,
                           bool& scan_oid) {
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    return false;
  }
  if (scan_opr.has_alias()) {
    alias = scan_opr.alias().value();
  } else {
    alias = -1;
  }

  if (!scan_opr.has_params()) {
    return false;
  }
  const algebra::QueryParams& p = scan_opr.params();
  if (p.tables_size() != 1) {
    return false;
  }
  if (p.has_predicate()) {
    return false;
  }
  const common::NameOrId& table = p.tables(0);
  label = static_cast<label_t>(table.id());

  if (!scan_opr.has_idx_predicate()) {
    return false;
  }
  const algebra::IndexPredicate& predicate = scan_opr.idx_predicate();
  if (predicate.or_predicates_size() != 1) {
    return false;
  }
  if (predicate.or_predicates(0).predicates_size() != 1) {
    return false;
  }
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  if (!triplet.has_key()) {
    return false;
  }
  auto key = triplet.key();
  if (key.has_key()) {
    scan_oid = true;
  } else if (key.has_id()) {
    scan_oid = false;
  } else {
    LOG(FATAL) << "unexpected key case";
  }

  if (triplet.cmp() != common::Logical::EQ) {
    return false;
  }

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    if (triplet.const_().item_case() == common::Value::kI32) {
      vertex_id = triplet.const_().i32();
    } else if (triplet.const_().item_case() == common::Value::kI64) {
      vertex_id = triplet.const_().i64();
    } else {
      return false;
    }
  } break;
  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    const common::DynamicParam& p = triplet.param();
    std::string name = p.name();
    std::string value = params.at(name);
    vertex_id = std::stoll(value);
  } break;
  default: {
    return false;
  } break;
  }

  return true;
}

static bl::result<Context> scan_vertices_expr_impl(
    bool scan_oid, const std::vector<Any>& input_ids,
    const ReadTransaction& txn, const ScanParams& scan_params,
    std::unique_ptr<ExprBase> expr) {
  if (scan_oid) {
    return Scan::filter_oids(
        txn, scan_params,
        [&expr, input_ids](label_t label, vid_t vid) {
          return expr->eval_vertex(label, vid, 0).as_bool();
        },
        input_ids);
  } else {
    std::vector<int64_t> gids;
    for (size_t i = 0; i < input_ids.size(); i++) {
      if (input_ids[i].type != PropertyType::Int64()) {
        RETURN_BAD_REQUEST_ERROR("Expect int64 type for global id");
      }
      gids.push_back(input_ids[i].AsInt64());
    }
    return Scan::filter_gids(
        txn, scan_params,
        [&expr, input_ids](label_t label, vid_t vid) {
          return expr->eval_vertex(label, vid, 0).as_bool();
        },
        gids);
  }
}

static bl::result<Context> scan_vertices_no_expr_impl(
    bool scan_oid, const std::vector<Any>& input_ids,
    const ReadTransaction& txn, const ScanParams& scan_params) {
  if (scan_oid) {
    return Scan::filter_oids(
        txn, scan_params, [](label_t label, vid_t vid) { return true; },
        input_ids);
  } else {
    std::vector<int64_t> gids;
    for (size_t i = 0; i < input_ids.size(); i++) {
      if (input_ids[i].type != PropertyType::Int64()) {
        RETURN_BAD_REQUEST_ERROR("Expect int64 type for global id");
      }
      gids.push_back(input_ids[i].AsInt64());
    }
    return Scan::filter_gids(
        txn, scan_params, [](label_t, vid_t) { return true; }, gids);
  }
}

bool parse_idx_predicate(const algebra::IndexPredicate& predicate,
                         const std::map<std::string, std::string>& params,
                         std::vector<Any>& oids, bool& scan_oid) {
  // todo unsupported cases.
  if (predicate.or_predicates_size() != 1) {
    return false;
  }
  // todo unsupported cases.
  if (predicate.or_predicates(0).predicates_size() != 1) {
    return false;
  }
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  if (!triplet.has_key()) {
    return false;
  }
  auto key = triplet.key();
  if (key.has_key()) {
    scan_oid = true;
  } else if (key.has_id()) {
    scan_oid = false;
  } else {
    LOG(FATAL) << "unexpected key case";
  }
  if (triplet.cmp() != common::Logical::EQ && triplet.cmp() != common::WITHIN) {
    return false;
  }

  if (triplet.value_case() ==
      algebra::IndexPredicate_Triplet::ValueCase::kConst) {
    if (triplet.const_().item_case() == common::Value::kI32) {
      oids.emplace_back(triplet.const_().i32());
    } else if (triplet.const_().item_case() == common::Value::kI64) {
      oids.emplace_back(triplet.const_().i64());
    } else if (triplet.const_().item_case() == common::Value::kI64Array) {
      const auto& arr = triplet.const_().i64_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        oids.emplace_back(arr.item(i));
      }

    } else if (triplet.const_().item_case() == common::Value::kStr) {
      std::string value = triplet.const_().str();
      oids.emplace_back(Any::From(value));
    } else if (triplet.const_().item_case() == common::Value::kStrArray) {
      const auto& arr = triplet.const_().str_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        oids.emplace_back(Any::From(arr.item(i)));
      }
    } else {
      return false;
    }
  } else if (triplet.value_case() ==
             algebra::IndexPredicate_Triplet::ValueCase::kParam) {
    const common::DynamicParam& p = triplet.param();
    if (p.data_type().type_case() == common::IrDataType::TypeCase::kDataType) {
      auto dt = p.data_type().data_type();
      if (dt == common::DataType::INT64) {
        std::string name = p.name();
        std::string value = params.at(name);
        int64_t v = std::stoll(value);
        oids.emplace_back(v);
      } else if (dt == common::DataType::STRING) {
        std::string name = p.name();
        std::string value = params.at(name);
        oids.emplace_back(Any::From(value));
      } else if (dt == common::DataType::INT32) {
        std::string name = p.name();
        std::string value = params.at(name);
        int32_t v = std::stoi(value);
        oids.emplace_back(v);
      } else {
        LOG(FATAL) << "unsupported primary key type" << dt;
        return false;
      }
    }
  }
  return true;
}

bl::result<Context> eval_scan(
    const physical::Scan& scan_opr, const ReadTransaction& txn,
    const std::map<std::string, std::string>& params) {
  label_t label;
  int64_t vertex_id;
  int alias;

  bool scan_oid;
  if (is_find_vertex(scan_opr, params, label, vertex_id, alias, scan_oid)) {
    return Scan::find_vertex_with_id(txn, label, vertex_id, alias, scan_oid);
  }

  const auto& opt = scan_opr.scan_opt();
  if (opt == physical::Scan::VERTEX) {
    ScanParams scan_params;
    if (scan_opr.has_alias()) {
      scan_params.alias = scan_opr.alias().value();
    } else {
      scan_params.alias = -1;
    }
    CHECK(scan_opr.has_params());
    bool has_other_type_oid = false;
    const auto& scan_opr_params = scan_opr.params();
    for (const auto& table : scan_opr_params.tables()) {
      // exclude invalid vertex label id
      if (txn.schema().vertex_label_num() <= table.id()) {
        continue;
      }
      scan_params.tables.push_back(table.id());
      const auto& pks = txn.schema().get_vertex_primary_key(table.id());
      if (pks.size() > 1) {
        LOG(ERROR) << "only support one primary key";
        RETURN_UNSUPPORTED_ERROR("only support one primary key");
      }
      auto [type, _, __] = pks[0];
      if (type != PropertyType::kInt64) {
        has_other_type_oid = true;
      }
    }

    // implicit type conversion will happen when oid is int64_t
    if (!has_other_type_oid && scan_opr.has_idx_predicate()) {
      if (scan_opr.has_idx_predicate() && scan_opr_params.has_predicate()) {
        Context ctx;
        auto expr = parse_expression(
            txn, ctx, params, scan_opr_params.predicate(), VarType::kVertexVar);
        std::vector<Any> oids{};
        if (!parse_idx_predicate(scan_opr.idx_predicate(), params, oids,
                                 scan_oid)) {
          LOG(ERROR) << "parse idx predicate failed";
          RETURN_UNSUPPORTED_ERROR("parse idx predicate failed");
        }
        std::vector<Any> valid_oids;
        // In this case, we expect the type consistent with pk
        for (const auto& oid : oids) {
          if (oid.type == PropertyType::Int64()) {
            valid_oids.push_back(oid);
          } else if (oid.type == PropertyType::Int32()) {
            valid_oids.push_back(Any::From<int64_t>(oid.AsInt32()));
          } else {
            LOG(ERROR) << "Expect int64 type for global id, but got: "
                       << oid.type;
            RETURN_BAD_REQUEST_ERROR(
                "Expect int64 type for global id, but got: " +
                oid.type.ToString());
          }
        }
        return scan_vertices_expr_impl(scan_oid, valid_oids, txn, scan_params,
                                       std::move(expr));
      }

      if (scan_opr.has_idx_predicate()) {
        std::vector<Any> oids{};
        if (!parse_idx_predicate(scan_opr.idx_predicate(), params, oids,
                                 scan_oid)) {
          LOG(ERROR) << "parse idx predicate failed: "
                     << scan_opr.DebugString();
          RETURN_UNSUPPORTED_ERROR("parse idx predicate failed");
        }
        return scan_vertices_no_expr_impl(scan_oid, oids, txn, scan_params);
      }
    } else if (scan_opr.has_idx_predicate()) {
      if (scan_opr.has_idx_predicate() && scan_opr_params.has_predicate()) {
        Context ctx;
        auto expr = parse_expression(
            txn, ctx, params, scan_opr_params.predicate(), VarType::kVertexVar);
        std::vector<Any> oids{};
        if (!parse_idx_predicate(scan_opr.idx_predicate(), params, oids,
                                 scan_oid)) {
          LOG(ERROR) << "parse idx predicate failed: "
                     << scan_opr.DebugString();
          RETURN_UNSUPPORTED_ERROR("parse idx predicate failed");
        }
        return scan_vertices_expr_impl(scan_oid, oids, txn, scan_params,
                                       std::move(expr));
      }

      if (scan_opr.has_idx_predicate()) {
        std::vector<Any> oids{};
        if (!parse_idx_predicate(scan_opr.idx_predicate(), params, oids,
                                 scan_oid)) {
          LOG(ERROR) << "parse idx predicate failed: "
                     << scan_opr.DebugString();
          RETURN_UNSUPPORTED_ERROR("parse idx predicate failed");
        }
        return scan_vertices_no_expr_impl(scan_oid, oids, txn, scan_params);
      }
    }

    if (scan_opr_params.has_predicate()) {
      Context ctx;
      auto expr = parse_expression(
          txn, ctx, params, scan_opr_params.predicate(), VarType::kVertexVar);
      if (expr->is_optional()) {
        return Scan::scan_vertex(
            txn, scan_params, [&expr](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, 0, 0).as_bool();
            });
      } else {
        return Scan::scan_vertex(
            txn, scan_params, [&expr](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, 0).as_bool();
            });
      }
    }

    if ((!scan_opr.has_idx_predicate()) && (!scan_opr_params.has_predicate())) {
      return Scan::scan_vertex(txn, scan_params,
                               [](label_t, vid_t) { return true; });
    }
  } else {
    LOG(ERROR) << "unsupport scan option " << scan_opr.DebugString()
               << " we only support scan vertex currently";
    RETURN_UNSUPPORTED_ERROR("unsupport scan option " + scan_opr.DebugString());
  }
  LOG(ERROR) << "unsupport scan option " << scan_opr.DebugString()
             << " we only support scan vertex currently";
  RETURN_UNSUPPORTED_ERROR("unsupport scan option " + scan_opr.DebugString());
  return Context();
}

}  // namespace runtime

}  // namespace gs