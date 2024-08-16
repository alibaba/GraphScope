#include "flex/engines/graph_db/runtime/common/operators/limit.h"

namespace gs {
namespace runtime {
Context Limit::limit(Context&& ctx, size_t lower, size_t upper) {
  if (upper > ctx.row_num()) {
    upper = ctx.row_num() - 1;
  }
  if (lower < 0) {
    lower = 0;
  }
  if (lower >= upper) {
    return Context();
  }
  ctx.slice(lower, upper);
  return ctx;
}
}  // namespace runtime
}  // namespace gs
