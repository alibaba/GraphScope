#include <limits>

#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/operators/order_by.h"

#include "flex/proto_generated_gie/algebra.pb.h"

namespace gs {

namespace runtime {

class GeneralComparer {
 public:
  GeneralComparer() : keys_num_(0) {}
  ~GeneralComparer() {}

  void add_keys(Var&& key, bool asc) {
    keys_.emplace_back(std::move(key));
    order_.push_back(asc);
    ++keys_num_;
  }

  bool operator()(size_t lhs, size_t rhs) const {
    for (size_t k = 0; k < keys_num_; ++k) {
      auto& v = keys_[k];
      auto asc = order_[k];
      RTAny lhs_val = v.get(lhs);
      RTAny rhs_val = v.get(rhs);
      if (lhs_val < rhs_val) {
        return asc;
      } else if (rhs_val < lhs_val) {
        return !asc;
      }
    }

    return lhs < rhs;
  }

 private:
  std::vector<Var> keys_;
  std::vector<bool> order_;
  size_t keys_num_;
};

Context eval_order_by(const algebra::OrderBy& opr, const ReadTransaction& txn,
                      Context&& ctx) {
  int lower = 0;
  int upper = std::numeric_limits<int>::max();
  if (opr.has_limit()) {
    lower = std::max(lower, static_cast<int>(opr.limit().lower()));
    upper = std::min(upper, static_cast<int>(opr.limit().upper()));
  }

  GeneralComparer cmp;
  int keys_num = opr.pairs_size();
  LOG(INFO) << opr.DebugString();
  LOG(INFO) << keys_num << " keys num\n";
  for (int i = 0; i < keys_num; ++i) {
    const algebra::OrderBy_OrderingPair& pair = opr.pairs(i);
    Var v(txn, ctx, pair.key(), VarType::kPathVar);
    CHECK(pair.order() == algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_ASC ||
          pair.order() == algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_DESC);
    bool order =
        pair.order() ==
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
    cmp.add_keys(std::move(v), order);
  }
  LOG(INFO) << "OrderBy: keys_num = " << keys_num;

  OrderBy::order_by_with_limit<GeneralComparer>(txn, ctx, cmp, lower, upper);

  LOG(INFO) << "OrderBy: done";

  return ctx;
}

}  // namespace runtime

}  // namespace gs