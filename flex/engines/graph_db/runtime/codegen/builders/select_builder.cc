#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class SelectBuilder {
 public:
  SelectBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const algebra::Select& opr) {
    auto [expr_name, expr_str] =
        build_expr(context_, opr.predicate(), VarType::kPathVar);
    std::string ss;
    auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
    ss += "auto ";
    ss += (nxt_ctx + " = Select::select(" + cur_ctx + ", PathPredicate(" +
           expr_name + "));\n");
    return ss;
  }
  BuildingContext& context_;
};
std::string build_select(BuildingContext& context, const algebra::Select& opr) {
  SelectBuilder builder(context);
  return builder.Build(opr);
}
}  // namespace runtime
}  // namespace gs