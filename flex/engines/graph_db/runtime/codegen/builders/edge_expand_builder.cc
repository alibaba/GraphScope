#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class EdgeExpandBuilder {
 public:
  EdgeExpandBuilder(BuildingContext& context) : context_(context) {};

  std::string Build(const physical::EdgeExpand& opr,
                    const physical::PhysicalOpr_MetaData& meta) {
    int tag = -1;
    if (opr.has_v_tag()) {
      tag = opr.v_tag().value();
    }
    Direction dir = parse_direction(opr.direction());
    CHECK(!opr.is_optional());

    CHECK(opr.has_params());
    const auto& params = opr.params();
    int alias = -1;
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }

    if (opr.expand_opt() ==
        physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      context_.set_alias(alias, ContextColumnType::kVertex, RTAnyType::kVertex);
      if (params.has_predicate()) {
        LOG(FATAL) << "not support" << params.DebugString();
      } else {
        EdgeExpandParams eep;
        eep.v_tag = tag;
        eep.labels = parse_label_triplets(meta);
        eep.dir = dir;
        eep.alias = alias;
        auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
        std::string ss;
        ss += "auto ";
        ss += nxt_ctx +
              " = EdgeExpand::expand_vertex_without_predicate(txn, std::move(" +
              cur_ctx + "), " + eep.to_string() + ");\n";
        return ss;
      }
    } else if (opr.expand_opt() ==
               physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      EdgeExpandParams eep;
      eep.v_tag = tag;
      eep.labels = parse_label_triplets(meta);
      eep.dir = dir;
      eep.alias = alias;
      context_.set_alias(alias, ContextColumnType::kEdge, RTAnyType::kEdge);
      if (params.has_predicate()) {
        std::string ss;
        auto [expr_name, expr_str, type] =
            build_expr(context_, params.predicate(), VarType::kEdgeVar);
        auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
        ss += expr_str;
        ss += "auto ";
        ss += nxt_ctx + " = EdgeExpand::expand_edge(txn, std::move(" + cur_ctx +
              "), " + eep.to_string() + ", EdgePredicate(";
        ss += expr_name;
        ss += "));\n";
        return ss;
      } else {
        auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
        std::string ss;
        ss += "auto ";
        ss += nxt_ctx +
              " = EdgeExpand::expand_edge_without_predicate(txn, std::move(" +
              cur_ctx + "), " + eep.to_string() + ");\n";
        return ss;
      }
    }

    LOG(FATAL) << "not support" << opr.DebugString();

    return "";
  }

  BuildingContext& context_;
};

std::string build_edge_expand(BuildingContext& context,
                              const physical::EdgeExpand& opr,
                              const physical::PhysicalOpr_MetaData& meta) {
  EdgeExpandBuilder builder(context);
  return builder.Build(opr, meta);
}
}  // namespace runtime
}  // namespace gs