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
                           label_t& label, int64_t& vertex_id, int& alias) {
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

  if (triplet.cmp() != common::Logical::EQ) {
    return false;
  }

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    vertex_id = triplet.const_().i64();
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
                         int64_t& oid) {
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
  // const common::Property& key = triplet.key();
  if (triplet.cmp() != common::Logical::EQ) {
    return false;
  }

  if (triplet.value_case() ==
      algebra::IndexPredicate_Triplet::ValueCase::kConst) {
    oid = triplet.const_().i64();
  } else if (triplet.value_case() ==
             algebra::IndexPredicate_Triplet::ValueCase::kParam) {
    const common::DynamicParam& p = triplet.param();
    std::string name = p.name();
    std::string value = params.at(name);
    oid = std::stoll(value);
  }
  return true;
}

Context eval_scan(const physical::Scan& scan_opr, const ReadTransaction& txn,
                  const std::map<std::string, std::string>& params) {
  label_t label;
  int64_t vertex_id;
  int alias;
  if (is_find_vertex(scan_opr, params, label, vertex_id, alias)) {
    return Scan::find_vertex(txn, label, vertex_id, alias);
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

    if (scan_opr.has_idx_predicate()) {
      int64_t oid{};
      CHECK(parse_idx_predicate(scan_opr.idx_predicate(), params, oid));
      return Scan::scan_vertex(
          txn, scan_params, [&txn, oid](label_t label, vid_t vid) {
            return txn.GetVertexId(label, vid).AsInt64() == oid;
          });
    }

    if (scan_opr.has_idx_predicate() && scan_opr_params.has_predicate()) {
      Context ctx;
      auto expr = parse_expression(
          txn, ctx, params, scan_opr_params.predicate(), VarType::kVertexVar);
      int64_t oid{};
      CHECK(parse_idx_predicate(scan_opr.idx_predicate(), params, oid));
      return Scan::scan_vertex(
          txn, scan_params, [&expr, &txn, oid](label_t label, vid_t vid) {
            return txn.GetVertexId(label, vid).AsInt64() == oid &&
                   expr->eval_vertex(label, vid, 0).as_bool();
          });
    }

    if (scan_opr_params.has_predicate()) {
      Context ctx;
      auto expr = parse_expression(
          txn, ctx, params, scan_opr_params.predicate(), VarType::kVertexVar);
      return Scan::scan_vertex(
          txn, scan_params, [&expr](label_t label, vid_t vid) {
            return expr->eval_vertex(label, vid, 0).as_bool();
          });
    }

    if ((!scan_opr.has_idx_predicate()) && (!scan_opr_params.has_predicate())) {
      return Scan::scan_vertex(txn, scan_params,
                               [](label_t, vid_t) { return true; });
    }
  }
  LOG(FATAL) << "AAAAA";
  return Context();
}

}  // namespace runtime

}  // namespace gs