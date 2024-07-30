#ifndef RUNTIME_COMMON_OPERATORS_INTERSECT_H_
#define RUNTIME_COMMON_OPERATORS_INTERSECT_H_

#include <tuple>
#include <vector>

#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Intersect {
 public:
  static Context intersect(Context&& ctx,
                           std::vector<std::tuple<Context, int, int>>&& ctxs,
                           int alias);

  static Context intersect(std::vector<Context>&& ctxs, int key);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_INTERSECT_H_