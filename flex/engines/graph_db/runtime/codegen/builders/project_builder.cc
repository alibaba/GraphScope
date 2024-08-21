#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class ProjectBuilder {
 public:
  ProjectBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const physical::Project& opr) {
    bool is_append = opr.is_append();
    BuildingContext context;
    if (is_append) {
      context = context_;
    }
    int mappings_num = opr.mappings_size();
    std::string ss;
    std::string mapping_str;
    for (int i = 0; i < mappings_num; i++) {
      const auto& mapping = opr.mappings(i);
      auto [expr_name, expr_str, type] = build_expr(context_, mapping.expr());
      ss += expr_str;
      int alias = -1;
      if (mapping.has_alias()) {
        alias = mapping.alias().value();
      }
      if (type == RTAnyType::kVertex) {
        context.set_alias(alias, ContextColumnType::kVertex, type);
      } else if (type == RTAnyType::kEdge) {
        context.set_alias(alias, ContextColumnType::kEdge, type);
      } else if (type == RTAnyType::kPath) {
        context.set_alias(alias, ContextColumnType::kPath, type);
      } else {
        context.set_alias(alias, ContextColumnType::kValue, type);
      }
      mapping_str +=
          "ProjectExpr(" + expr_name + ", " + std::to_string(alias) + ")";
      if (i != mappings_num - 1) {
        mapping_str += ", ";
      }
    }
    auto [cur_ctx, nxt_ctx] = context.GetCurAndNextCtxName();
    ss += "auto " + nxt_ctx + " = Project::project(std::move(" + cur_ctx +
          "), std::make_tuple(" + mapping_str + "), " +
          (is_append ? "true" : "false") + ");\n";
    return ss;
  }

 private:
  BuildingContext& context_;
};

std::string build_project(BuildingContext& context,
                          const physical::Project& opr) {
  ProjectBuilder builder(context);
  return builder.Build(opr);
}
}  // namespace runtime
}  // namespace gs