#ifndef RUNTIME_COMMON_OPERATORS_LIMIT_H_
#define RUNTIME_COMMON_OPERATORS_LIMIT_H_
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {
namespace runtime {
class Limit {
 public:
  static Context limit(Context&& ctx, size_t lower, size_t upper);
};
}  // namespace runtime
}  // namespace gs
#endif