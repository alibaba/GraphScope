#ifndef FOLD_BUILDER_H
#define FOLD_BUILDER_H

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/op_builder/expr_builder.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

#include "flex/codegen/op_builder/group_by_builder.h"

namespace gs {

std::pair<std::string, std::string> gen_agg_var_and_code_for_fold(
    BuildingContext& ctx, const physical::GroupBy::AggFunc& agg_func) {
  auto agg_func_name = agg_func_pb_2_str(agg_func.aggregate());
  auto cur_var_name = ctx.GetNextAggFuncName();
  std::vector<int32_t> in_tags;
  std::vector<std::string> in_prop_names;
  std::vector<std::string> in_prop_types;
  int32_t res_alias = agg_func.alias().value();
  auto real_res_alias = ctx.CreateOrGetTagInd(res_alias);
  auto& vars = agg_func.vars();
  for (auto i = 0; i < vars.size(); ++i) {
    auto& var = vars[i];
    auto raw_tag_id = var.tag().id();
    in_tags.push_back(ctx.GetTagInd(raw_tag_id));
    LOG(INFO) << "var " << i << " tag id " << raw_tag_id << " real tag id "
              << in_tags[i];
    if (var.has_property()) {
      auto var_prop = var.property();
      if (var_prop.item_case() == common::Property::kId) {
        // IdKey
        LOG(INFO) << "aggregate on internal id";
        in_prop_names.push_back("None");
        in_prop_types.push_back(EMPTY_TYPE);
      } else {
        LOG(INFO) << "aggregate on property " << var_prop.key().name();
        in_prop_names.push_back(var.property().key().name());
        in_prop_types.push_back(
            common_data_type_pb_2_str(var.node_type().data_type()));
      }
    } else {
      // var has no property, which means internal id.
      LOG(INFO) << "aggregate on internal id";
      in_prop_names.push_back("None");
      in_prop_types.push_back(EMPTY_TYPE);
    }
  }
  std::stringstream ss;
  ss << "auto " << cur_var_name << " = " << make_agg_prop_name;
  ss << "<";
  ss << real_res_alias << ", ";
  ss << agg_func_name << ", ";
  CHECK(in_prop_names.size() > 0);
  for (auto i = 0; i < in_prop_types.size() - 1; ++i) {
    ss << in_prop_types[i] << ", ";
  }
  ss << in_prop_types[in_prop_types.size() - 1];
  ss << ">";
  ss << "(";
  {
    // propnames
    ss << "{";
    for (auto i = 0; i < in_prop_names.size() - 1; ++i) {
      ss << add_quote(in_prop_names[i]) << ", ";
    }
    ss << add_quote(in_prop_names[in_prop_names.size() - 1]);
    ss << "}, ";
  }
  {
    // input tags
    ss << "std::integer_sequence<int32_t, ";
    for (auto i = 0; i < in_tags.size() - 1; ++i) {
      ss << in_tags[i] << ", ";
    }
    ss << in_tags[in_tags.size() - 1];
    ss << ">{}";
  }
  ss << ");" << std::endl;
  return std::make_pair(cur_var_name, ss.str());
}

// i.e. group without key.
class FoldOpBuilder {
 public:
  FoldOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  FoldOpBuilder& AddAggFunc(const physical::GroupBy::AggFunc& agg_func) {
    std::string agg_fun_var_name, agg_fun_code;
    std::tie(agg_fun_var_name, agg_fun_code) =
        gen_agg_var_and_code_for_fold(ctx_, agg_func);
    agg_func_name_and_code.emplace_back(agg_fun_var_name, agg_fun_code);
    return *this;
  }

  std::vector<std::string> Build() const {
    CHECK(agg_func_name_and_code.size() > 0);
    std::string fold_opt_var_name = ctx_.GetNextGroupOptName();
    std::string fold_opt_code;
    {
      std::stringstream ss;
      ss << "auto " << fold_opt_var_name << " = "
         << "gs::make_fold_opt(";
      for (auto i = 0; i < agg_func_name_and_code.size() - 1; ++i) {
        ss << "std::move(" << agg_func_name_and_code[i].first << "), ";
      }
      ss << "std::move("
         << agg_func_name_and_code[agg_func_name_and_code.size() - 1].first
         << ")";
      ss << ");" << std::endl;
      fold_opt_code = ss.str();
    }
    LOG(INFO) << "fold_opt_code: " << fold_opt_code;

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();

    std::string cur_ctx_code;
    {
      std::stringstream ss;
      ss << "auto " << next_ctx_name << " = Engine::GroupByWithoutKey";
      ss << "(";
      ss << ctx_.TimeStampVar() << ", ";
      ss << ctx_.GraphVar() << ", ";
      ss << "std::move(" << prev_ctx_name << "), ";
      ss << "std::move(" << fold_opt_var_name << ")";
      ss << ");" << std::endl;
      cur_ctx_code = ss.str();
    }
    std::vector<std::string> res;
    for (auto i = 0; i < agg_func_name_and_code.size(); ++i) {
      res.push_back(agg_func_name_and_code[i].second);
    }
    res.push_back(fold_opt_code);
    res.push_back(cur_ctx_code);
    return res;
  }

 private:
  BuildingContext& ctx_;
  std::vector<std::pair<std::string, std::string>> agg_func_name_and_code;
};

static std::vector<std::string> BuildGroupWithoutKeyOp(
    BuildingContext& ctx, const physical::GroupBy& group_by_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  CHECK(group_by_pb.mappings_size() == 0);
  FoldOpBuilder fold_op_builder(ctx);
  for (auto i = 0; i < group_by_pb.functions_size(); ++i) {
    fold_op_builder.AddAggFunc(group_by_pb.functions(i));
  }
  return fold_op_builder.Build();
}
}  // namespace gs

#endif  // FOLD_BUILDER_H