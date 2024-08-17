#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class DedupBuilder {
 public:
  DedupBuilder(BuildingContext& context) : context_(context) {}

  std::string Build(const algebra::Dedup& opr) {
    int keys_num = opr.keys_size();
    std::vector<std::string> exprs;
    std::string ss;
    for (int i = 0; i < keys_num; ++i) {
      const auto& key = opr.keys(i);
      auto [expr_name, expr_str, type] =
          var_pb_2_str(context_, key, VarType::kPathVar);
      ss += expr_str;
      exprs.push_back(expr_name);
    }
    auto [cur_ctx, next_ctx] = context_.GetCurAndNextCtxName();
    ss += "auto ";
    ss += next_ctx + "Dedup::dedup(txn, " + cur_ctx + ", {";
    for (int i = 0; i < keys_num; ++i) {
      ss += "[&, " + exprs[i] + "]" +
            "(size_t i) {\nreturn exprs[i].typed_eval_path(i);}\n";
      if (i != keys_num - 1) {
        ss += ", ";
      }
    }
    ss += "});\n";
    return ss;
  }

 private:
  BuildingContext& context_;
};

std::string build_dedup(BuildingContext& context, const algebra::Dedup& opr) {
  DedupBuilder builder(context);
  return builder.Build(opr);
}
}  // namespace runtime
}  // namespace gs