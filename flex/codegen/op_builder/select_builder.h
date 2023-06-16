#ifndef SELECT_BUILDER_H
#define SELECT_BUILDER_H

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

#include "flex/codegen/op_builder/expr_builder.h"

namespace gs {
class SelectOpBuilder {
 public:
  SelectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  SelectOpBuilder& expr_name(const std::string expr_name) {
    expr_name_ = expr_name;
    return *this;
  }

  std::string Build() const {
    std::stringstream ss;
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    ss << "auto " << next_ctx_name << _ASSIGN_STR_ << "Engine::Select("
       << ctx_.TimeStampVar() << ", " << ctx_.GraphVar() << ", "
       << "std::move(" << prev_ctx_name << "), std::move(" << expr_name_
       << "));" << std::endl;
    return ss.str();
  }

 private:
  BuildingContext& ctx_;
  std::string expr_name_;
};

// return expression code and select op code(including expr calling)
static std::pair<std::string, std::string> BuildSelectOp(
    BuildingContext& ctx, const algebra::Select& select_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  if (!select_pb.has_predicate()) {
    throw std::runtime_error("Select expression is not set");
  }
  ExprBuilder expr_builder(ctx);
  auto& expr_oprs = select_pb.predicate().operators();
  expr_builder.AddAllExprOpr(expr_oprs);

  std::string func_name, func_code;
  std::vector<codegen::ParamConst> func_call_params;
  std::vector<std::string> tag_props;
  common::DataType unused_expr_ret_type;
  std::tie(func_name, func_call_params, tag_props, func_code,
           unused_expr_ret_type) = expr_builder.Build();
  LOG(INFO) << "func_name: " << func_name;
  LOG(INFO) << "func_code: " << func_code;
  for (auto i = 0; i < func_call_params.size(); ++i) {
    LOG(INFO) << "func_call_params: " << i << ", "
              << data_type_2_string(func_call_params[i].type)
              << func_call_params[i].var_name << ", ";
  }
  for (auto i = 0; i < tag_props.size(); ++i) {
    LOG(INFO) << "tag_props: " << i << ", " << tag_props[i];
  }
  // add func_call_params to ctx's param const;
  for (auto i = 0; i < func_call_params.size(); ++i) {
    ctx.AddParameterVar(func_call_params[i]);
  }

  ctx.AddExprCode(func_code);

  std::string expr_call_code;
  std::string expr_val_name;
  {
    std::stringstream ss;
    expr_val_name = ctx.GetNextExprVarName();
    ss << _4_SPACES << func_name << " " << expr_val_name;
    ss << "(";
    for (auto i = 0; i < func_call_params.size(); ++i) {
      ss << func_call_params[i].var_name << ",";
    }
    for (auto i = 0; i < tag_props.size() - 1; ++i) {
      ss << tag_props[i] << ",";
    }
    if (tag_props.size() > 0) {
      ss << tag_props[tag_props.size() - 1];
    }
    ss << ");" << std::endl;
    expr_call_code = ss.str();
  }

  ////build select op code
  SelectOpBuilder select_builder(ctx);
  select_builder.expr_name(expr_val_name);
  std::string select_op_code = select_builder.Build();
  return std::make_pair(expr_call_code, select_op_code);
}

}  // namespace gs

#endif  // SELECT_BUILDER_H