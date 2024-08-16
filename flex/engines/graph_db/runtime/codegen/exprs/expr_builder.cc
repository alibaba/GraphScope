#include "flex/engines/graph_db/runtime/codegen/exprs/expr_builder.h"

namespace gs {
namespace runtime {
// expr name, expr string
std::pair<std::string, std::string> build_expr(BuildingContext& context,
                                               const common::Expression& expr,
                                               VarType var_type) {
  ExprBuilder builder(context);
  return builder.varType(var_type).Build(expr);
}
}  // namespace runtime
}  // namespace gs