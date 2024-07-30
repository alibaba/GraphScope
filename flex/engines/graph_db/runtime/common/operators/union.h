#ifndef RUNTIME_COMMON_OPERATORS_UNION_H_
#define RUNTIME_COMMON_OPERATORS_UNION_H_

#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Union {
 public:
  static Context union_op(Context&& ctx1, Context&& ctx2);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_UNION_H_