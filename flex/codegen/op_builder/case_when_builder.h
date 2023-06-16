#ifndef CASE_WHEN_BUILDER_H
#define CASE_WHEN_BUILDER_H

#include <string>
#include <tuple>
#include <vector>

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/codegen_utils.h"
#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"

#include "flex/codegen/op_builder/expr_builder.h"

namespace gs {

class CaseWhenBuilder : public ExprBuilder {
 public:
  // expr_class_name, expr_code, dynamic_params, tag_props, return_type
  static constexpr const char* SWITCH_KEY = "key";
  using base_t = ExprBuilder;
  using ret_t =
      std::tuple<std::string, std::vector<codegen::ParamConst>,
                 std::vector<std::string>, std::string, common::DataType>;
  CaseWhenBuilder(BuildingContext& ctx)
      : base_t(ctx),
        ret_type_(common::DataType::DataType_INT_MIN_SENTINEL_DO_NOT_USE_) {
    LOG(INFO) << "try to build: " << base_t::class_name_;
  }

  CaseWhenBuilder& input_expr(const common::Expression& input_expr) {
    if (input_expr.operators_size() == 0) {
      LOG(INFO) << "No input expression is provided";
      return *this;
    }
    auto& oprs = input_expr.operators();

    // build a expression and evaluate.
    // we just use the builder, BUT never realy build it.
    std::string expr_code = build_sub_expr(oprs);

    input_val_name_ = SWITCH_KEY;
    // auto key = (var0 + 1 > 0);
    {
      std::stringstream ss;
      ss << "auto " << input_val_name_ << " = (";
      ss << expr_code;
      ss << ");" << std::endl;
      input_expr_code_ = ss.str();
    }

    LOG(INFO) << "After input expr, we have tag props: "
              << tag_prop_strs_.size()
              << " param consts: " << construct_params_.size()
              << " func call vars: " << func_call_vars_.size();
    return *this;
  }

  CaseWhenBuilder& when_then_exprs(
      const google::protobuf::RepeatedPtrField<common::Case::WhenThen>&
          when_expr) {
    LOG(INFO) << "Got when then exprs of size: " << when_expr.size();

    // Basiclly, each when_then is a if then.
    for (auto& when_then_expr : when_expr) {
      auto& when_val = when_then_expr.when_expression();
      auto& the_result_expr = when_then_expr.then_result_expression();
      if (when_val.operators_size() == 0) {
        throw std::runtime_error("when expression is empty");
      }
      if (input_val_name_.empty()) {
        when_then_expr_impl_simple(when_val, the_result_expr);
      } else {
        when_then_expr_impl_general(when_val, the_result_expr);
        // presented, then input_val_name can only be boolean or a single value.
      }
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
      else_code_ = ss.str();
    }
    LOG(INFO) << "Finish else expr: " << else_code_;
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

    LOG(INFO) << "Start building case when expr";
    std::stringstream ss;
    start_class(ss);
    add_constructor(ss);
    // add function call is different from the base expr builder.
    add_func_call_case_when(ss);
    add_tag_prop_getter(ss);
    // private member
    add_private_member(ss);
    // end class
    end_class(ss);

    std::string str = ss.str();

    // return std::make_tuple(class_name_, construct_params_, tag_prop_strs_,
    // str);
    return std::make_tuple(class_name_, construct_params_, tag_prop_strs_, str,
                           ret_type_);
  }

 private:
  void add_func_call_case_when(std::stringstream& ss) const {
    if (constains_vertex_id(func_call_vars_)) {
      ss << _4_SPACES << "template <typename vertex_id_t>" << std::endl;
    }
    ss << _4_SPACES << "inline auto operator()";
    ss << "(";
    if (func_call_vars_.size() > 0) {
      for (auto i = 0; i < func_call_vars_.size() - 1; ++i) {
        ss << data_type_2_string(func_call_vars_[i].type) << " "
           << base_t::EXPR_OPERATOR_CALL_VAR_NAME << i << ",";
      }
      ss << data_type_2_string(func_call_vars_.back().type) << " "
         << base_t::EXPR_OPERATOR_CALL_VAR_NAME << func_call_vars_.size() - 1;
    }
    ss << ") const {" << std::endl;

    {
      // add switch key
      ss << _8_SPACES << input_expr_code_ << std::endl;
    }
    {
      for (auto& str : when_then_codes_) {
        ss << str << std::endl;
      }
      ss << else_code_ << std::endl;
    }
    ss << _4_SPACES << "}" << std::endl;
  }

  void when_then_expr_impl_simple(const common::Expression& when_val,
                                  const common::Expression& the_result_expr) {
    std::string when_key, result_code;
    // In this case, when_key must be bool value.
    { when_key = build_sub_expr(when_val.operators()); }
    {
      auto expr_code = build_sub_expr(the_result_expr.operators());

      std::stringstream ss;
      ss << "return (";
      ss << expr_code;
      ss << ")";
      result_code = ss.str();
    }

    // Concatenate
    {
      std::stringstream ss;
      ss << _8_SPACES << "if (" << when_key << ") { " << std::endl;
      ss << _8_SPACES << "    " << result_code << std::endl;
      ss << " }" << std::endl;
    }
  }

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
        LOG(INFO) << "receive param const: " << param.DebugString();
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
      ss << "if (" << input_val_name_ << " == " << when_key << ") {"
         << std::endl;
      ss << then_result_expr_code;
      ss << "}" << std::endl;
      auto tmp_res = ss.str();
      LOG(INFO) << "WhenThen expr: " << tmp_res;
      when_then_codes_.emplace_back(std::move(tmp_res));
    }
  }

  // build subexpr
  std::string build_sub_expr(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& oprs) {
    ExprBuilder expr_builder(ctx_, cur_var_id_, true);
    expr_builder.AddAllExprOpr(oprs);
    auto& expr_nodes = expr_builder.GetExprNodes();
    auto& tag_props = expr_builder.GetTagPropertyStrs();
    auto& func_call_vars = expr_builder.GetFuncCallVars();
    auto& param_consts = expr_builder.GetConstructParams();
    // save the tag props and param const to us.
    for (auto tag_prop : tag_props) {
      tag_prop_strs_.push_back(tag_prop);
    }
    for (auto param_const : param_consts) {
      construct_params_.push_back(param_const);
    }
    for (auto func_call_var : func_call_vars) {
      func_call_vars_.push_back(func_call_var);
    }
    LOG(INFO) << "Inc var id from " << cur_var_id_ << " to "
              << expr_builder.GetCurVarId();
    cur_var_id_ = expr_builder.GetCurVarId();

    std::stringstream ss;
    for (auto& expr_node : expr_nodes) {
      ss << expr_node << " ";
    }
    return ss.str();
  }

  std::string input_val_name_;  // input expression valuation result name
  std::string input_expr_code_;

  std::vector<std::string> when_then_codes_;
  std::string else_code_;
  common::DataType ret_type_;
};

}  // namespace gs

#endif  // CASE_WHEN_BUILDER_H