#include "flex/engines/graph_db/runtime/common/operators/edge_expand.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

Context eval_edge_expand(const physical::EdgeExpand& opr,
                         const ReadTransaction& txn, Context&& ctx,
                         const std::map<std::string, std::string>& params,
                         const physical::PhysicalOpr_MetaData& meta) {
  CHECK(opr.has_v_tag());
  int v_tag = opr.v_tag().value();
  LOG(INFO) << opr.DebugString();

  Direction dir = parse_direction(opr.direction());
  // parse optional
  bool is_optional = opr.is_optional();
  // if (is_optional) {
  //   LOG(INFO) << "optional edge expand";
  // } else {
  //   LOG(INFO) << "not optional edge expand";
  // }
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
      LOG(FATAL) << "not support";
    } else {
      EdgeExpandParams eep;
      eep.v_tag = v_tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;
      LOG(INFO) << "expand vertex without predicate";
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
    LOG(FATAL) << "not support";
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs