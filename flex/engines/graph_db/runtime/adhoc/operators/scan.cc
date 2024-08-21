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
      LOG(FATAL) << "unexpected value case" << triplet.const_().item_case();
    }
  } break;
  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    const common::DynamicParam& p = triplet.param();
    std::string name = p.name();
    std::string value = params.at(name);
    vertex_id = std::stoll(value);
  } break;
  default: {
    LOG(FATAL) << "unexpected value case";
  } break;
  }

  return true;
}

bool parse_idx_predicate(const algebra::IndexPredicate& predicate,
                         const std::map<std::string, std::string>& params,
                         std::vector<int64_t>& oids, bool& scan_oid) {
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
  // const common::Property& key = triplet.key();
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

    } else {
      LOG(FATAL) << "unexpected value case" << triplet.const_().item_case();
    }
  } else if (triplet.value_case() ==
             algebra::IndexPredicate_Triplet::ValueCase::kParam) {
    const common::DynamicParam& p = triplet.param();
    std::string name = p.name();
    std::string value = params.at(name);
    oids.emplace_back(std::stoll(value));
  }
  return true;
}

Context eval_scan(const physical::Scan& scan_opr, const ReadTransaction& txn,
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
    const auto& scan_opr_params = scan_opr.params();
    for (const auto& table : scan_opr_params.tables()) {
      scan_params.tables.push_back(table.id());
    }

    if (scan_opr.has_idx_predicate() && scan_opr_params.has_predicate()) {
      Context ctx;
      auto expr = parse_expression(
          txn, ctx, params, scan_opr_params.predicate(), VarType::kVertexVar);
      std::vector<int64_t> oids{};
      CHECK(parse_idx_predicate(scan_opr.idx_predicate(), params, oids,
                                scan_oid));
      if (scan_oid) {
        return Scan::filter_oids(
            txn, scan_params,
            [&expr, oids](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, 0).as_bool();
            },
            oids);
      } else {
        return Scan::filter_gids(
            txn, scan_params,
            [&expr, oids](label_t label, vid_t vid) {
              return expr->eval_vertex(label, vid, 0).as_bool();
            },
            oids);
      }
    }

    if (scan_opr.has_idx_predicate()) {
      std::vector<int64_t> oids{};
      CHECK(parse_idx_predicate(scan_opr.idx_predicate(), params, oids,
                                scan_oid));

      if (scan_oid) {
        return Scan::filter_oids(
            txn, scan_params,
            [&txn, oids](label_t label, vid_t vid) { return true; }, oids);
      } else {
        return Scan::filter_gids(
            txn, scan_params, [](label_t, vid_t) { return true; }, oids);
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
  }
  LOG(FATAL) << "not support";
  return Context();
}

}  // namespace runtime

}  // namespace gs