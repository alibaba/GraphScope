#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class OrderByBuilder {
 public:
  OrderByBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const algebra::OrderBy& opr) {
    int lower = 0;
    int upper = std::numeric_limits<int>::max();
    if (opr.has_limit()) {
      lower = std::max(lower, static_cast<int>(opr.limit().lower()));
      upper = std::min(upper, static_cast<int>(opr.limit().upper()));
    }
    int keys_num = opr.pairs_size();
    std::string ss;
    std::string cmp;
    for (int i = 0; i < keys_num; ++i) {
      const auto& pair = opr.pairs(i);
      auto [name, str, type] =
          var_pb_2_str(context_, pair.key(), VarType::kPathVar);
      ss += str + "\n";
      if (pair.order() ==
          algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC) {
        cmp = name + ".typed_eval_path(i) < " + name + ".typed_eval_path(j)";

      } else if (pair.order() == algebra::OrderBy_OrderingPair_Order::
                                     OrderBy_OrderingPair_Order_DESC) {
        cmp = "(" + name + ".typed_eval_path(j) < " + name +
              ".typed_eval_path(i))";
      } else {
        LOG(ERROR) << "Unknown order type";
      }
      cmp += " || ";
    }
    cmp += " (i < j) ";
    auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
    ss += nxt_ctx + " = OrderBy::order_by_with_limit(txn, " + cur_ctx + ", " +
          "[&](size_t i, size_t j){ \nreturn " + cmp + ";}" + ", " +
          std::to_string(lower) + ", " + std::to_string(upper) + ");\n";
    return ss;
  }

 private:
  BuildingContext& context_;
};

std::string build_order_by(BuildingContext& context,
                           const algebra::OrderBy& opr) {
  OrderByBuilder builder(context);
  return builder.Build(opr);
}
}  // namespace runtime
}  // namespace gs