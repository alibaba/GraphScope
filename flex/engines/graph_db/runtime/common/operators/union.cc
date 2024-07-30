#include "flex/engines/graph_db/runtime/common/operators/union.h"

namespace gs {

namespace runtime {

Context Union::union_op(Context&& ctx1, Context&& ctx2) {
  CHECK(ctx1.col_num() == ctx2.col_num());
  size_t col_num = ctx1.col_num();
  Context ret;
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    if (ctx1.get(col_i) == nullptr) {
      CHECK(ctx2.get(col_i) == nullptr);
      continue;
    }
    ret.set(col_i, ctx1.get(col_i)->union_col(ctx2.get(col_i)));
  }
  return ret;
}

}  // namespace runtime

}  // namespace gs
