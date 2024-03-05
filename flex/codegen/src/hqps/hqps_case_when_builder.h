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
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"

#include <boost/format.hpp>

namespace gs {

// can be multiple when then exprs
static constexpr const char* CASE_WHEN_EXPR_TEMPLATE_STR =
    "if (%1%){\n"
    "   return %2%;\n"
    "}\n";

static constexpr const char* ELSE_EXPR_TEMPLATE_STR = "return %1%;\n";

class CaseWhenBuilder : public ExprBuilder {
 public:
  using base_t = ExprBuilder;
  using ret_t = std::tuple<std::string, std::vector<codegen::ParamConst>,
                           std::vector<std::pair<int32_t, std::string>>,
                           std::string, std::vector<common::DataType>>;
  CaseWhenBuilder(BuildingContext& ctx) : base_t(ctx) {
    VLOG(10) << "try to build: " << base_t::class_name_;
  }

  CaseWhenBuilder& when_then_exprs(
      const google::protobuf::RepeatedPtrField<common::Case::WhenThen>&
          when_expr) {
    VLOG(10) << "Got when then exprs of size: " << when_expr.size();

    // Basically, each when_then is a if then.
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
    VLOG(10) << "Building else expr of size: " << else_exr.DebugString();

    {
      auto& else_oprs = else_exr.operators();

      auto expr_code = build_sub_expr(else_oprs, true);

      boost::format formater(ELSE_EXPR_TEMPLATE_STR);
      formater % expr_code;
      else_code_ = formater.str();
    }
    VLOG(10) << "Finish else expr: " << else_code_;
    return *this;
  }

  CaseWhenBuilder& return_type(common::DataType ret_type) {
    res_data_type_.emplace_back(ret_type);
    return *this;
  }

  ret_t Build() const override {
    for (size_t i = 0; i < construct_params_.size(); ++i) {
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
    // the func_call impl is overridden
    func_call_impl_str = get_func_call_impl_str();
    private_filed_str = get_private_filed_str();

    boost::format formater(EXPR_BUILDER_TEMPLATE_STR);
    auto ret_type_str = common_data_type_pb_2_str(res_data_type_);
    formater % class_name_ % ret_type_str % constructor_param_str %
        field_init_code_str % func_call_template_typename_str % "auto" %
        func_call_params_str % func_call_impl_str % private_filed_str;

    std::string str = formater.str();

    return std::make_tuple(
        class_name_, construct_params_, tag_selectors_, str,
        std::vector{common::DataType::DataType_INT_MIN_SENTINEL_DO_NOT_USE_});
  }

 protected:
  bool try_to_find_none(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& oprs) {
    for (auto& opr : oprs) {
      if (opr.item_case() == common::ExprOpr::kConst) {
        auto& const_pb = opr.const_();
        if (const_pb.item_case() == common::Value::kNone) {
          return true;
        }
      }
    }
    return false;
  }

  void when_then_expr_impl_general(const common::Expression& when_val,
                                   const common::Expression& the_result_expr) {
    // build when expr from sub_expr
    std::string when_key_code;
    {
      auto& when_oprs = when_val.operators();
      when_key_code = build_sub_expr(when_oprs, false);
    }

    // then result expression
    std::string then_result_expr_code;
    {
      auto& else_oprs = the_result_expr.operators();
      if (try_to_find_none(else_oprs) && else_oprs.size() == 1) {
        then_result_expr_code = "NullRecordCreator<result_t>::GetNull()";
      } else {
        then_result_expr_code = build_sub_expr(else_oprs, false);
      }
    }

    // concatenate case when into a if else.
    {
      boost::format formater(CASE_WHEN_EXPR_TEMPLATE_STR);
      formater % when_key_code % then_result_expr_code;
      auto tmp_res = formater.str();
      VLOG(10) << "WhenThen expr: " << tmp_res;
      when_then_codes_.emplace_back(std::move(tmp_res));
    }
  }

  std::string get_func_call_impl_str() const override {
    std::stringstream ss;
    for (size_t i = 0; i < when_then_codes_.size(); ++i) {
      ss << when_then_codes_[i] << std::endl;
    }
    ss << else_code_ << std::endl;
    return ss.str();
  }

  // For each when then expr, we need to build a sub expr.
  // if set_ret_type is true, add return data types to res_data_types
  std::string build_sub_expr(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& oprs,
      bool set_ret_type) {
    ExprBuilder expr_builder(ctx_, true);
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
    if (set_ret_type) {
      for (int32_t i = 0; i < oprs.size(); ++i) {
        auto cur_opr = oprs[i];
        auto node_type = cur_opr.node_type();
        if (node_type.type_case() == common::IrDataType::kDataType) {
          auto data_type = node_type.data_type();
          res_data_type_.emplace_back(data_type);
        } else if (node_type.type_case() == common::IrDataType::TYPE_NOT_SET) {
          // if node_type is not set, try to find all expr_opr's node type and
          // emplace_back to res_data_type_
          if (cur_opr.item_case() == common::ExprOpr::kVars) {
            auto& vars = cur_opr.vars();
            for (auto j = 0; j < vars.keys_size(); ++j) {
              auto var = vars.keys(j);
              auto var_node_type = var.node_type();
              CHECK(var_node_type.type_case() == common::IrDataType::kDataType);
              res_data_type_.emplace_back(var_node_type.data_type());
            }
          } else if (cur_opr.item_case() == common::ExprOpr::kConst) {
            auto const_pb = cur_opr.const_();
            res_data_type_.emplace_back(common_value_2_data_type(const_pb));
          } else {
            LOG(FATAL) << "Only support vars now" << cur_opr.DebugString();
          }
        } else {
          LOG(FATAL) << "Can only accept data type" << node_type.DebugString();
        }
      }
    }
    for (auto func_call_var : func_call_vars) {
      func_call_vars_.push_back(func_call_var);
    }

    std::stringstream ss;
    for (auto& expr_node : expr_nodes) {
      ss << expr_node << " ";
    }
    return ss.str();
  }

  std::string input_expr_code_;

  std::vector<std::string> when_then_codes_;
  std::string else_code_;
};

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_CASE_WHEN_BUILDER_H_