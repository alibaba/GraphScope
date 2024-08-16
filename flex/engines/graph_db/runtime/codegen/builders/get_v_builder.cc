#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class GetVBuilder {
 public:
  GetVBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const physical::GetV& opr) {
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
      const auto& params = opr.params();
      GetVParams get_v_params;
      get_v_params.tag = tag;
      get_v_params.opt = opt;
      get_v_params.alias = alias;
      context_.set_alias(alias, ContextColumnType::kVertex, RTAnyType::kVertex);
      get_v_params.tables = parse_tables(params);
      if (params.has_predicate()) {
        std::string ss{};
        auto [expr_name, expr_str] = build_expr(context_, params.predicate());
        ss += expr_str;
        if (opt == VOpt::kItself) {
          auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
          ss += "auto ";
          ss += (cur_ctx + " = GetV::get_vertex_from_vertices(txn, std::move(" +
                 cur_ctx + "), " + get_v_params.toString() + ", " + expr_name +
                 ");\n");
          return ss;
        } else if (opt == VOpt::kEnd || opt == VOpt::kStart) {
          auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
          ss += "auto ";
          ss += (nxt_ctx + " = GetV::get_vertex_from_edges(txn, std::move(" +
                 cur_ctx + "), " + get_v_params.toString() + ", " + expr_name +
                 ");\n");
          return ss;
        }

      } else {
        if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
          std::string ss;
          auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
          ss += "auto ";
          ss += (nxt_ctx + " = GetV::get_vertex_from_edges(txn, std::move(" +
                 cur_ctx + "), " + get_v_params.toString() +
                 ", [](size_t){\nreturn true;});\n ");
          return ss;
        }
      }
    }
    LOG(FATAL) << "not support";
    return "";
  }
  BuildingContext& context_;
};
std::string build_get_v(BuildingContext& context, const physical::GetV& opr) {
  return GetVBuilder(context).Build(opr);
}
}  // namespace runtime
}  // namespace gs