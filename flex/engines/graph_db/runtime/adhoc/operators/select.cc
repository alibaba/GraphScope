#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

Context eval_select(const algebra::Select& opr, const ReadTransaction& txn,
                    Context&& ctx,
                    const std::map<std::string, std::string>& params) {
  Expr expr(txn, ctx, params, opr.predicate(), VarType::kPathVar);
  std::vector<size_t> offsets;
  size_t row_num = ctx.row_num();
  for (size_t i = 0; i < row_num; ++i) {
    if (expr.eval_path(i).as_bool()) {
      offsets.push_back(i);
    }
  }

  ctx.reshuffle(offsets);
  return ctx;
}

}  // namespace runtime

}  // namespace gs