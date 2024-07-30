#include "flex/engines/graph_db/runtime/common/operators/join.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {
namespace runtime {
Context eval_join(const physical::Join& opr, Context&& ctx, Context&& ctx2) {
  JoinParams p;
  auto left_keys = opr.left_keys();
  for (int i = 0; i < left_keys.size(); i++) {
    if (!left_keys.Get(i).has_tag()) {
      LOG(FATAL) << "left_keys should have tag";
    }
    p.left_columns.push_back(left_keys.Get(i).tag().id());
  }
  auto right_keys = opr.right_keys();
  for (int i = 0; i < right_keys.size(); i++) {
    if (!right_keys.Get(i).has_tag()) {
      LOG(FATAL) << "right_keys should have tag";
    }
    p.right_columns.push_back(right_keys.Get(i).tag().id());
  }
  switch (opr.join_kind()) {
  case physical::Join_JoinKind::Join_JoinKind_INNER:
    p.join_type = JoinKind::kInnerJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_SEMI:
    p.join_type = JoinKind::kSemiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_ANTI:
    p.join_type = JoinKind::kAntiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
    p.join_type = JoinKind::kLeftOuterJoin;
    break;
  default:
    LOG(FATAL) << "unsupported join kind" << opr.join_kind();
  }
  return Join::join(std::move(ctx), std::move(ctx2), p);
}
}  // namespace runtime
}  // namespace gs