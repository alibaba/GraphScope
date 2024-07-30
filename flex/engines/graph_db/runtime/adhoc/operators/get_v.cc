#include "flex/engines/graph_db/runtime/common/operators/get_v.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

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

Context eval_get_v(const physical::GetV& opr, const ReadTransaction& txn,
                   Context&& ctx,
                   const std::map<std::string, std::string>& params) {
  int tag = -1;
  if (opr.has_tag()) {
    tag = opr.tag().value();
  }
  VOpt opt = parse_opt(opr.opt());
  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }

  if (opr.has_params()) {
    const algebra::QueryParams& query_params = opr.params();
    GetVParams p;
    p.opt = opt;
    p.tag = tag;
    p.tables = parse_tables(query_params);
    p.alias = alias;
    if (query_params.has_predicate()) {
      GeneralVertexPredicate pred(txn, ctx, params, query_params.predicate());

      if (opt == VOpt::kItself) {
        return GetV::get_vertex_from_vertices(txn, std::move(ctx), p, pred);
      } else if (opt == VOpt::kEnd || opt == VOpt::kStart) {
        return GetV::get_vertex_from_edges(txn, std::move(ctx), p, pred);
      }
    } else {
      if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
        auto ret = GetV::get_vertex_from_edges(txn, std::move(ctx), p,
                                               DummyVertexPredicate());
        LOG(INFO) << "GetV::get_vertex_from_edges" << ret.col_num();
        return ret;
      }
    }
  }

  LOG(FATAL) << "not support";
  return ctx;
}

}  // namespace runtime

}  // namespace gs