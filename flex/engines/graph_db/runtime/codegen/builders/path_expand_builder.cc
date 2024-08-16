#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class PathExpandPBuilder {
 public:
  PathExpandPBuilder(BuildingContext& context) : context_(context) {};

  std::string Build(const physical::PathExpand& opr,
                    const physical::PhysicalOpr_MetaData& meta) {
    CHECK(opr.has_start_tag()) << "PathExpand must have start tag";
    int start_tag = opr.start_tag().value();
    CHECK(opr.path_opt() ==
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);
    CHECK(!opr.is_optional());
    Direction dir = parse_direction(opr.base().edge_expand().direction());
    CHECK(!opr.base().edge_expand().is_optional());
    const algebra::QueryParams& query_params =
        opr.base().edge_expand().params();
    PathExpandParams pep;
    int alias = -1;
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    context_.set_alias(alias, ContextColumnType::kPath, RTAnyType::kPath);
    pep.alias = alias;
    pep.dir = dir;
    pep.hop_lower = opr.hop_range().lower();
    pep.hop_upper = opr.hop_range().upper();
    pep.start_tag = start_tag;
    pep.labels = parse_label_triplets(meta);
    if (query_params.has_predicate()) {
      LOG(FATAL) << "not support" << query_params.DebugString();
    } else {
      std::string ss;
      auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
      ss += "auto ";
      ss += (nxt_ctx + " = PathExpand::edge_path_p(txn, std::move(" + cur_ctx +
             "), " + pep.to_string() + ");\n");
      return ss;
    }
  }

  BuildingContext& context_;
};

std::string build_path_expand_p(BuildingContext& context,
                                const physical::PathExpand& opr,
                                const physical::PhysicalOpr_MetaData& meta) {
  PathExpandPBuilder builder(context);
  return builder.Build(opr, meta);
}

class PathExpandVBuilder {
 public:
  PathExpandVBuilder(BuildingContext& context) : context_(context) {};
  PathExpandVBuilder& Alias(int alias) {
    alias_ = alias;
    context_.set_alias(alias_, ContextColumnType::kVertex, RTAnyType::kVertex);
    return *this;
  }

  std::string Build(const physical::PathExpand& opr,
                    const physical::PhysicalOpr_MetaData& meta) {
    CHECK(opr.has_start_tag()) << "PathExpand must have start tag";
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
    const algebra::QueryParams& query_params =
        opr.base().edge_expand().params();
    PathExpandParams pep;
    pep.alias = alias_;
    pep.dir = dir;
    pep.hop_lower = opr.hop_range().lower();
    pep.hop_upper = opr.hop_range().upper();
    pep.start_tag = start_tag;
    pep.labels = parse_label_triplets(meta);
    if (opr.base().edge_expand().expand_opt() ==
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      if (query_params.has_predicate()) {
        LOG(FATAL) << "not support";
      } else {
        std::string ss;
        auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
        ss += "auto ";
        ss += (nxt_ctx + " = PathExpand::edge_path_v(txn, std::move(" +
               cur_ctx + "), " + pep.to_string() + ");\n");
        return ss;
      }
    } else {
      LOG(FATAL) << "not support";
    }
  }

 private:
  BuildingContext& context_;
  int alias_;
};

std::string build_path_expand_v(BuildingContext& context,
                                const physical::PathExpand& opr,
                                const physical::PhysicalOpr_MetaData& meta,
                                int alias) {
  PathExpandVBuilder builder(context);
  return builder.Alias(alias).Build(opr, meta);
}

}  // namespace runtime
}  // namespace gs
