#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class JoinBuilder {
 public:
  std::string join_kind_2_str(const physical::Join_JoinKind& kind) {
    switch (kind) {
    case physical::Join_JoinKind::Join_JoinKind_INNER:
      return "JoinKind::kInnerJoin";
    case physical::Join_JoinKind::Join_JoinKind_SEMI:
      return "JoinKind::kSemiJoin";
    case physical::Join_JoinKind::Join_JoinKind_ANTI:
      return "JoinKind::kAntiJoin";
    case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
      return "JoinKind::kLeftOuterJoin";
    default:
      LOG(FATAL) << "unsupported join kind" << kind;
    }
    return "";
  }
  JoinBuilder(BuildingContext& context_left, BuildingContext& context_right,
              BuildingContext& context)
      : context_left_(context_left),
        context_right_(context_right),
        context_(context) {};
  std::string Build(const physical::Join& opr) {
    std::string ss;
    const auto& left_keys = opr.left_keys();
    std::string left_keys_str = "";
    for (int i = 0; i < left_keys.size(); ++i) {
      auto [expr_name, expr_str, type] =
          var_pb_2_str(context_left_, left_keys[i], VarType::kPathVar);
      ss += expr_str;
      left_keys_str += "JoinKey(" + expr_name + ", " +
                       std::to_string(left_keys[i].tag().id()) + ")";
      if (i != left_keys.size() - 1) {
        left_keys_str += ", ";
      }
    }

    const auto& right_keys = opr.right_keys();

    std::string right_keys_str = "";

    for (int i = 0; i < right_keys.size(); ++i) {
      auto [expr_name, expr_str, type] =
          var_pb_2_str(context_right_, right_keys[i], VarType::kPathVar);
      ss += expr_str;
      right_keys_str += "JoinKey(" + expr_name + ", " +
                        std::to_string(right_keys[i].tag().id()) + ")";
      if (i != right_keys.size() - 1) {
        right_keys_str += ", ";
      }
    }
    auto left_cur_ctx = context_left_.GetCurCtxName();
    auto right_cur_ctx = context_right_.GetCurCtxName();
    auto cur_ctx = context_.GetCurCtxName();

    ss += "auto ";
    ss += cur_ctx + " = Join::join(std::move(" + left_cur_ctx +
          "), std::move(" + right_cur_ctx + "), " +
          join_kind_2_str(opr.join_kind()) + ", std::make_tuple(" +
          left_keys_str + "), std::make_tuple(" + right_keys_str + "));\n";
    // TODO: update types of context
    return ss;
  }
  BuildingContext& context_left_;
  BuildingContext& context_right_;
  BuildingContext& context_;
};

std::string build_join(BuildingContext& context_left,
                       BuildingContext& context_right, BuildingContext& context,
                       const physical::Join& opr) {
  JoinBuilder builder(context_left, context_right, context);
  return builder.Build(opr);
}
}  // namespace runtime
}  // namespace gs