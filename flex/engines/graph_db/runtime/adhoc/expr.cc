#include "flex/engines/graph_db/runtime/adhoc/expr.h"

namespace gs {

namespace runtime {

Expr::Expr(const ReadTransaction& txn, const Context& ctx,
           const std::map<std::string, std::string>& params,
           const common::Expression& expr, VarType var_type) {
  expr_ = parse_expression(txn, ctx, params, expr, var_type);
}

RTAny Expr::eval_path(size_t idx) const {
  RTAny ret = expr_->eval_path(idx);
  return ret;
}

RTAny Expr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  return expr_->eval_vertex(label, v, idx);
}
RTAny Expr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                      const Any& data, size_t idx) const {
  return expr_->eval_edge(label, src, dst, data, idx);
}

RTAnyType Expr::type() const { return expr_->type(); }

}  // namespace runtime

}  // namespace gs