#ifndef RUNTIME_COMMON_OPERATORS_SELECT_H_
#define RUNTIME_COMMON_OPERATORS_SELECT_H_

#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Select {
 public:
  template <typename PRED_T>
  static void select(Context& ctx, const PRED_T& pred) {
    size_t row_num = ctx.row_num();
    std::vector<size_t> offsets;
    for (size_t k = 0; k < row_num; ++k) {
      if (pred(k)) {
        offsets.push_back(k);
      }
    }

    ctx.reshuffle(offsets);
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_SELECT_H_