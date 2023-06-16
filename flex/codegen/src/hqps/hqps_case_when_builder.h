/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef CODEGEN_SRC_HQPS_HQPS_CASE_WHEN_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_CASE_WHEN_BUILDER_H_

#include <string>
#include <tuple>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"

namespace gs {

class CaseWhenBuilder : public ExprBuilder {
 public:
  using base_t = ExprBuilder;
  using ret_t = std::tuple<std::string, std::vector<codegen::ParamConst>,
                           std::vector<std::pair<int32_t, std::string>>,
                           std::string, common::DataType>;
  CaseWhenBuilder(BuildingContext& ctx)
      : base_t(ctx),
        ret_type_(common::DataType::DataType_INT_MIN_SENTINEL_DO_NOT_USE_) {
    VLOG(10) << "try to build: " << base_t::class_name_;
  }

  CaseWhenBuilder& when_then_exprs(
      const google::protobuf::RepeatedPtrField<common::Case::WhenThen>&
          when_expr) {
    VLOG(10) << "Got when then exprs of size: " << when_expr.size();

    // Basiclly, each when_then is a if then.
    for (auto& when_then_expr : when_expr) {
      auto& when_val = when_then_expr.when_expression();
      auto& the_result_expr = when_then_expr.then_result_expression();
      if (when_val.operators_size() == 0) {
        throw std::runtime_error("when expression is empty");
      }
      when_then_expr_impl_general(when_val, the_result_expr);
    }
    return *this;
  }

  // Since we process else_expr at last, we can do this return.
  CaseWhenBuilder& else_expr(const common::Expression& else_exr) {
    if (else_exr.operators_size() == 0) {
      throw std::runtime_error("else expression is empty");
    }

    {
      auto& else_oprs = else_exr.operators();
      auto expr_code = build_sub_expr(else_oprs);

      std::stringstream ss;
      ss << "return (";
      ss << expr_code;
      ss << ");" << std::endl;
      else_code_ = ss.str();  // assign to member.
    }
    VLOG(10) << "Finish else expr: " << else_code_;
    return *this;
  }

  CaseWhenBuilder& return_type(common::DataType ret_type) {
    ret_type_ = ret_type;
    return *this;
  }

  ret_t Build() const override {
    for (auto i = 0; i < construct_params_.size(); ++i) {
      ctx_.AddParameterVar(construct_params_[i]);
    }

    VLOG(10) << "Enter express building";
    std::string constructor_param_str, field_init_code_str,
        func_call_template_typename_str, func_call_params_str,
        func_call_impl_str, private_filed_str;
    constructor_param_str = get_constructor_params_str();
    field_init_code_str = get_field_init_code_str();
    func_call_template_typename_str = get_func_call_typename_str();

    func_call_params_str = get_func_call_params_str();
    // the func_call impl is overrided
    func_call_impl_str = get_func_call_impl_str();
    private_filed_str = get_private_filed_str();

    boost::format formater(EXPR_BUILDER_TEMPLATE_STR);
    formater % class_name_ % constructor_param_str % field_init_code_str %
        func_call_template_typename_str % func_call_params_str %
        func_call_impl_str % private_filed_str;

    std::string str = formater.str();

    return std::make_tuple(
        class_name_, construct_params_, tag_selectors_, str,
        common::DataType::DataType_INT_MIN_SENTINEL_DO_NOT_USE_);
  }

 protected:
  void when_then_expr_impl_general(const common::Expression& when_val,
                                   const common::Expression& the_result_expr) {
    if (when_val.operators_size() != 1) {
      throw std::runtime_error("when expression can only one");
    }
    // can only be var or dynamic param
    auto& when_opr = when_val.operators(0);
    if (when_opr.item_case() != common::ExprOpr::kConst &&
        when_opr.item_case() != common::ExprOpr::kParam) {
      throw std::runtime_error("when expression can only be const or param");
    }

    std::string when_key;
    {
      if (when_opr.item_case() == common::ExprOpr::kConst) {
        auto& const_val = when_opr.const_();
        when_key = value_pb_to_str(const_val);
      } else {
        auto& param = when_opr.param();
        auto param_node_type = when_opr.node_type();
        auto param_const =
            param_const_pb_to_param_const(param, param_node_type);
        VLOG(10) << "receive param const: " << param.DebugString();
        when_key = param_const.var_name + "_";  // TODO: fix hack
        construct_params_.push_back(param_const);
      }
    }

    // then result expression
    std::string then_result_expr_code;
    {
      auto& else_oprs = the_result_expr.operators();
      auto expr_code = build_sub_expr(else_oprs);
      std::stringstream ss;
      ss << "return (";
      ss << expr_code;
      ss << ");" << std::endl;
      then_result_expr_code = ss.str();
    }

    // concatenate case when into a if else.
    {
      std::stringstream ss;
      ss << "if (" << when_key << ") {" << std::endl;
      ss << then_result_expr_code;
      ss << "}" << std::endl;
      auto tmp_res = ss.str();
      VLOG(10) << "WhenThen expr: " << tmp_res;
      when_then_codes_.emplace_back(std::move(tmp_res));
    }
  }

  std::string get_func_call_impl_str() const override {
    std::stringstream ss;
    for (int i = 0; i < when_then_codes_.size(); ++i) {
      ss << when_then_codes_[i] << std::endl;
    }
    ss << else_code_ << std::endl;
    return ss.str();
  }

  // For each when then expr, we need to build a sub expr.
  std::string build_sub_expr(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& oprs) {
    ExprBuilder expr_builder(ctx_, cur_var_id_, true);
    expr_builder.AddAllExprOpr(oprs);
    auto& expr_nodes = expr_builder.GetExprNodes();
    auto& tag_props = expr_builder.GetTagSelectors();
    auto& func_call_vars = expr_builder.GetFuncCallVars();
    auto& param_consts = expr_builder.GetConstructParams();
    // save the tag props and param const to us.
    for (auto tag_prop : tag_props) {
      tag_selectors_.push_back(tag_prop);
    }
    for (auto param_const : param_consts) {
      construct_params_.push_back(param_const);
    }
    for (auto func_call_var : func_call_vars) {
      func_call_vars_.push_back(func_call_var);
    }
    VLOG(10) << "Inc var id from " << cur_var_id_ << " to "
             << expr_builder.GetCurVarId();
    cur_var_id_ = expr_builder.GetCurVarId();

    std::stringstream ss;
    for (auto& expr_node : expr_nodes) {
      ss << expr_node << " ";
    }
    return ss.str();
  }

  std::string input_expr_code_;

  std::vector<std::string> when_then_codes_;
  std::string else_code_;
  common::DataType ret_type_;
};

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_CASE_WHEN_BUILDER_H_