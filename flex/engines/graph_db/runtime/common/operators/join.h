#ifndef RUNTIME_COMMON_OPERATORS_JOIN_H_
#define RUNTIME_COMMON_OPERATORS_JOIN_H_

#include <vector>
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {
namespace runtime {

struct JoinParams {
  std::vector<int> left_columns;
  std::vector<int> right_columns;
  JoinKind join_type;
};

class Join {
 public:
  static Context join(Context&& ctx, Context&& ctx2, const JoinParams& params);
};
}  // namespace runtime
}  // namespace gs

#endif  // COMMON_OPERATORS_JOIN_H_