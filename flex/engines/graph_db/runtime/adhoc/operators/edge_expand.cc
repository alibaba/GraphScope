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

#include "flex/engines/graph_db/runtime/common/operators/edge_expand.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

bl::result<Context> eval_edge_expand(
    const physical::EdgeExpand& opr, const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params,
    const physical::PhysicalOpr_MetaData& meta) {
  int v_tag;
  if (!opr.has_v_tag()) {
    v_tag = -1;
  } else {
    v_tag = opr.v_tag().value();
  }

  Direction dir = parse_direction(opr.direction());
  bool is_optional = opr.is_optional();
  CHECK(!is_optional);

  CHECK(opr.has_params());
  const algebra::QueryParams& query_params = opr.params();

  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }

  if (opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      LOG(ERROR) << "edge expand vertex with predicate is not supported";
      RETURN_UNSUPPORTED_ERROR(
          "edge expand vertex with predicate is not supported");
    } else {
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;
      return EdgeExpand::expand_vertex_without_predicate(txn, std::move(ctx),
                                                         eep);
    }
  } else if (opr.expand_opt() ==
             physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (query_params.has_predicate()) {
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;

      GeneralEdgePredicate pred(txn, ctx, params, query_params.predicate());

      return EdgeExpand::expand_edge(txn, std::move(ctx), eep, pred);
    } else {
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;

      return EdgeExpand::expand_edge_without_predicate(txn, std::move(ctx),
                                                       eep);
    }
  } else {
    LOG(ERROR) << "EdgeExpand with expand_opt: " << opr.expand_opt()
               << " is "
                  "not supported";
    RETURN_UNSUPPORTED_ERROR(
        "EdgeExpand with expand_opt is not supported: " +
        std::to_string(static_cast<int>(opr.expand_opt())));
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs