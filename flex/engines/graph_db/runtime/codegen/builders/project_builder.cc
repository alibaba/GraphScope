#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class ProjectBuilder {
 public:
  ProjectBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const physical::Project& opr) {
    bool is_append = opr.is_append();
    int mappings_num = opr.mappings_size();
    for (int i = 0; i < mappings_num; i++) {
      const auto& mapping = opr.mappings(i);
      auto [expr_name, expr_str, type] = build_expr(context_, mapping.expr());
    }
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