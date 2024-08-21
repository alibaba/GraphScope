#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class GroupByBuilder {
 public:
  std::string agg_kind_2_str(const physical::GroupBy_AggFunc::Aggregate& v) {
    switch (v) {
    case physical::GroupBy_AggFunc::SUM:
      return "AggrKind::kSum";
    case physical::GroupBy_AggFunc::MIN:
      return "AggrKind::kMin";
    case physical::GroupBy_AggFunc::MAX:
      return "AggrKind::kMax";
    case physical::GroupBy_AggFunc::COUNT:
      return "AggrKind::kCount";
    case physical::GroupBy_AggFunc::COUNT_DISTINCT:
      return "AggrKind::kCountDistinct";
    case physical::GroupBy_AggFunc::TO_SET:
      return "AggrKind::kToSet";
    case physical::GroupBy_AggFunc::FIRST:
      return "AggrKind::kFirst";
    case physical::GroupBy_AggFunc::TO_LIST:
      return "AggrKind::kToList";
    case physical::GroupBy_AggFunc::AVG:
      return "AggrKind::kAvg";
    default:
      LOG(FATAL) << "unsupported aggregate kind" << v;
    }
    return "Unknown";
  }

  std::string agg_func_2_str(const physical::GroupBy_AggFunc& opr) {
    int alias = -1;
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    int var_num = opr.vars_size();
    CHECK(var_num == 1) << "only support one variable in aggregate function";
    const auto& var = opr.vars(0);
    auto [expr_name, expr_str, type] =
        var_pb_2_str(context_, var, VarType::kPathVar);
    return "AggrFunc(" + agg_kind_2_str(opr.aggregate()) + ", " + expr_name +
           ", " + std::to_string(alias) + ")";
  }
  GroupByBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const physical::GroupBy& opr) {
    int mappings_num = opr.mappings_size();
    std::string ss{};
    std::string keys{};

    for (int i = 0; i < mappings_num; i++) {
      const auto& mapping = opr.mappings(i);
      auto [expr_name, expr_str, type] =
          var_pb_2_str(context_, mapping.key(), VarType::kPathVar);
      ss += expr_str + "\n";
      int alias = -1;
      if (mapping.has_alias()) {
        alias = mapping.alias().value();
      }
      keys += "AggrKey(" + expr_name + ", " + std::to_string(alias) + ")";
      if (i != mappings_num - 1) {
        keys += ", ";
      }
    }
    std::string func_str{};
    int function_num = opr.functions_size();
    for (int i = 0; i < function_num; i++) {
      const auto& function = opr.functions(i);
      func_str += agg_func_2_str(function);
      if (i + 1 != function_num) {
        func_str += ", ";
      }

      // function.aggregate()
    }
    auto [cur_ctx, nxt_ctx] = context_.GetCurAndNextCtxName();
    ss += "auto " + nxt_ctx + " = GroupBy::group_by(std::move(" + cur_ctx +
          "), std::make_tuple(" + keys + "), std::make_tuple(" + func_str +
          "));\n";
  }

 private:
  BuildingContext& context_;
};

std::string build_group_by(BuildingContext& context,
                           const physical::GroupBy& opr) {
  GroupByBuilder builder(context);
  return builder.Build(opr);
}
}  // namespace runtime
}  // namespace gs
