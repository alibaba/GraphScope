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

#include "flex/engines/graph_db/runtime/common/operators/path_expand.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

namespace gs {

namespace runtime {

bl::result<Context> eval_path_expand_v(
    const physical::PathExpand& opr, const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params,
    const physical::PhysicalOpr_MetaData& meta, int alias) {
  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(opr.path_opt() ==
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);
  if (opr.result_opt() !=
      physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V) {
    //    LOG(FATAL) << "not support";
  }
  CHECK(!opr.is_optional());

  Direction dir = parse_direction(opr.base().edge_expand().direction());
  CHECK(!opr.base().edge_expand().is_optional());
  const algebra::QueryParams& query_params = opr.base().edge_expand().params();
  PathExpandParams pep;
  pep.alias = alias;
  pep.dir = dir;
  pep.hop_lower = opr.hop_range().lower();
  pep.hop_upper = opr.hop_range().upper();
  for (size_t ci = 0; ci < ctx.col_num(); ++ci) {
    if (ctx.get(ci) != nullptr) {
      pep.keep_cols.insert(ci);
    }
  }
  pep.start_tag = start_tag;
  pep.labels = parse_label_triplets(meta);
  if (opr.base().edge_expand().expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      LOG(ERROR) << "path expand vertex with predicate is not supported";
      RETURN_UNSUPPORTED_ERROR(
          "path expand vertex with predicate is not supported");
    } else {
      return PathExpand::edge_expand_v(txn, std::move(ctx), pep);
    }
  } else {
    LOG(ERROR) << "Currently only support edge expand to vertex";
    RETURN_UNSUPPORTED_ERROR("Currently only support edge expand to vertex");
  }

  return ctx;
}

bl::result<Context> eval_path_expand_p(
    const physical::PathExpand& opr, const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params,
    const physical::PhysicalOpr_MetaData& meta, int alias) {
  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(opr.path_opt() ==
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);

  CHECK(!opr.is_optional());

  Direction dir = parse_direction(opr.base().edge_expand().direction());
  CHECK(!opr.base().edge_expand().is_optional());
  const algebra::QueryParams& query_params = opr.base().edge_expand().params();
  PathExpandParams pep;
  pep.alias = alias;
  pep.dir = dir;
  pep.hop_lower = opr.hop_range().lower();
  pep.hop_upper = opr.hop_range().upper();
  for (size_t ci = 0; ci < ctx.col_num(); ++ci) {
    if (ctx.get(ci) != nullptr) {
      pep.keep_cols.insert(ci);
    }
  }
  pep.start_tag = start_tag;
  pep.labels = parse_label_triplets(meta);

  if (query_params.has_predicate()) {
    LOG(ERROR) << "Currently can not support predicate in path expand";
    RETURN_UNSUPPORTED_ERROR(
        "Currently can not support predicate in path expand");
  } else {
    return PathExpand::edge_expand_p(txn, std::move(ctx), pep);
  }

  return ctx;
}

}  // namespace runtime

}  // namespace gs