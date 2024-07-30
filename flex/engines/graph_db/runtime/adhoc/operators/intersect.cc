#include "flex/engines/graph_db/runtime/common/operators/intersect.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"

namespace gs {
namespace runtime {

Context eval_intersect(const ReadTransaction& txn,
                       const physical::Intersect& opr,
                       std::vector<Context>&& ctxs) {
  int32_t key = opr.key();
  if (ctxs.size() == 1) {
    return std::move(ctxs[0]);
  }
  return Intersect::intersect(std::move(ctxs), key);
}

}  // namespace runtime
}  // namespace gs
