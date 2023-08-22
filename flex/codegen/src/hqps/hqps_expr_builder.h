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
#ifndef CODEGEN_SRC_HQPS_HQPS_EXPR_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_EXPR_BUILDER_H_

#include <stack>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"

#include <boost/format.hpp>

namespace gs {

// 1: expression class name
// 2: constructor params, as string
// 3: construction implement, concatenated as string
// 4: operator call params, as string
// 5: operator call implement, concatenated as string
// 6: private members, concatenated as string
static constexpr const char* EXPR_BUILDER_TEMPLATE_STR =
    "struct %1% {\n"
    "  public: \n"
    "   using result_t = %2%;\n"
    "   %1%(%3%) %4% {}\n"
    "   %5%\n"
    "   inline %6% operator()(%7%) const {\n"
    "     %8%\n"
    "   }\n"
    "  private:\n"
    "    %9%\n"
    "};\n";

// The input variable can have property or not, if property is not present, we
// take that as a IdKey
static std::pair<int32_t, std::string> variable_to_tag_id_property_selector(
    BuildingContext& ctx, const common::Variable& var) {
  int tag_id = -1;
  if (var.has_tag()) {
    tag_id = var.tag().id();
  }
  int real_tag_ind = ctx.GetTagInd(tag_id);
  if (var.has_property()) {
    std::string prop_name = var.property().key().name();
    std::string prop_type = data_type_2_string(
        common_data_type_pb_2_data_type(var.node_type().data_type()));
    boost::format formater(PROPERTY_SELECTOR);
    formater % prop_type % prop_name;

    return std::make_pair(real_tag_ind, formater.str());
  } else {
    // if variable has no property, we assume it means get the innerIdProperty
    // there are two cases:
    // 0: vertex, but the node type is passed as all property and types.
    // 1: collection, just take the value;
    std::string prop_type;
    if (var.node_type().type_case() == common::IrDataType::kDataType) {
      prop_type = data_type_2_string(
          common_data_type_pb_2_data_type(var.node_type().data_type()));
    } else {
      prop_type = GRAPE_EMPTY_TYPE;
    }
    boost::format formater(PROPERTY_SELECTOR);
    formater % prop_type % "None";

    return std::make_pair(real_tag_ind, formater.str());
  }
}

static std::string logical_to_str(const common::Logical& logical) {
  switch (logical) {
  case common::Logical::AND:
    return "&&";
  case common::Logical::OR:
    return "||";
  case common::Logical::NOT:
    return "!";
  case common::Logical::EQ:
    return "==";
  case common::Logical::NE:
    return "!=";
  case common::Logical::GT:
    return ">";
  case common::Logical::GE:
    return ">=";
  case common::Logical::LT:
    return "<";
  case common::Logical::LE:
    return "<=";
  case common::Logical::WITHIN:
    return "< WithIn > ";
  default:
    throw std::runtime_error("unknown logical");
  }
}

std::string i64_array_pb_to_str(const common::I64Array& array) {
  auto size = array.item_size();
  std::stringstream ss;
  ss << "std::array<int64_t," << size << ">{";
  for (int i = 0; i < size; ++i) {
    ss << array.item(i);
    if (i + 1 != size) {
      ss << ",";
    }
  }
  ss << "}";
  return ss.str();
}

// i32_array_pb_to_str
std::string i32_array_pb_to_str(const common::I32Array& array) {
  auto size = array.item_size();
  std::stringstream ss;
  ss << "std::array<int32_t," << size << ">{";
  for (int i = 0; i < size; ++i) {
    ss << array.item(i);
    if (i + 1 != size) {
      ss << ",";
    }
  }
  ss << "}";
  return ss.str();
}

static std::string value_pb_to_str(const common::Value& value) {
  switch (value.item_case()) {
  case common::Value::kI32:
    return std::to_string(value.i32());
  case common::Value::kI64:
    return std::to_string(value.i64());
  case common::Value::kF64:
    return std::to_string(value.f64());
  case common::Value::kStr:
    return with_quote(value.str());
  case common::Value::kBoolean:
    return value.boolean() ? "true" : "false";
  case common::Value::kI32Array:
    return i32_array_pb_to_str(value.i32_array());
  case common::Value::kI64Array:
    return i64_array_pb_to_str(value.i64_array());
  case common::Value::kNone:
    return NONE_LITERAL;
  default:
    throw std::runtime_error("unknown value type" + value.DebugString());
  }
}

bool constains_vertex_id(const std::vector<codegen::ParamConst>& params) {
  for (auto& param : params) {
    if (param.type == codegen::DataType::kVertexId) {
      return true;
    }
  }
  return false;
}

// Simlutate the calculation of expression, return the result data type.
// convert to prefix expression

/*Build a expression struct from expression*/
class ExprBuilder {
 protected:
  static constexpr const char* EXPR_OPERATOR_CALL_VAR_NAME = "var";

 public:
  ExprBuilder(BuildingContext& ctx, int var_id = 0, bool no_build = false)
      : ctx_(ctx),
        cur_var_id_(var_id),
        res_data_type_(
            common::DataType::DataType_INT_MIN_SENTINEL_DO_NOT_USE_) {
    if (!no_build) {
      // no build indicates whether we will use this builder as a helper.
      // If set to true, we will not let queryClassName and next_expr_name
      // increase.
      class_name_ = ctx_.GetQueryClassName() + ctx_.GetNextExprName();
    }
  }

  void set_return_type(common::DataType data_type) {
    res_data_type_ = data_type;
  }

  void AddAllExprOpr(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& expr_ops) {
    // we currently don't support filter with label keys!
    // If we meet label keys just ignore.
    auto size = expr_ops.size();
    VLOG(10) << "Adding expr of size: " << size;
    for (auto i = 0; i < size;) {
      auto expr = expr_ops[i];
      if (expr.has_var() && expr.var().property().has_label()) {
        VLOG(10) << "Found label in expr, skip this check";
        int j = i;
        for (; j < size; ++j) {
          if (expr_ops[j].item_case() == common::ExprOpr::kBrace &&
              expr_ops[j].brace() ==
                  common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
            VLOG(10) << "Found right brace at ind: " << j
                     << ", started at: " << i;
            AddExprOpr(std::string("true"));
            AddExprOpr(expr_ops[j]);
            i = j + 1;
            break;
          }
        }
        if (j == size) {
          LOG(WARNING) << "no right brace found" << j << "size: " << size;
          i = j;
        }
      } else {
        AddExprOpr(expr_ops[i]);
        ++i;
      }
    }
  }

  void AddExprOpr(const std::string expr_str) {
    expr_nodes_.emplace_back(expr_str);
  }

  // visit each expr opr.
  void AddExprOpr(const common::ExprOpr& opr) {
    switch (opr.item_case()) {
    case common::ExprOpr::kBrace: {
      auto brace = opr.brace();
      if (brace == common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
        expr_nodes_.emplace_back("(");
      } else if (brace == common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        expr_nodes_.emplace_back(")");
      } else {
        throw std::runtime_error("unknown brace");
      }
      break;
    }

    case common::ExprOpr::kConst: {
      VLOG(10) << "Got const: " << opr.const_().DebugString();
      auto str = value_pb_to_str(opr.const_());
      VLOG(10) << "Got const: " << str;
      expr_nodes_.emplace_back(std::move(str));
      break;
    }

    case common::ExprOpr::kVar: {
      auto& var = opr.var();
      auto param_const = variable_to_param_const(var, ctx_);
      // for each variable, we need add the variable to func_call_vars_.
      // and also set a expr node for it. which is unique.
      func_call_vars_.push_back(param_const);
      expr_nodes_.emplace_back(std::string(EXPR_OPERATOR_CALL_VAR_NAME) +
                               std::to_string(cur_var_id_++));

      // expr_nodes_.emplace_back(param_const.var_name);
      // convert a variable to a tag property,
      // gs::NamedProperty<gs::Int64>{"prop1"}, saved for later use.
      tag_selectors_.emplace_back(
          variable_to_tag_id_property_selector(ctx_, var));
      VLOG(10) << "Got var: " << var.DebugString();
      break;
    }

    case common::ExprOpr::kLogical: {
      auto logical = opr.logical();
      auto str = logical_to_str(logical);
      VLOG(10) << "Got expr opt logical: " << str;
      expr_nodes_.emplace_back(std::move(str));
      break;
    }

    // todo: use dynamic param
    case common::ExprOpr::kParam: {
      auto param_const_pb = opr.param();
      auto param_node_type = opr.node_type();
      auto param_const =
          param_const_pb_to_param_const(param_const_pb, param_node_type);
      VLOG(10) << "receive param const: " << param_const_pb.DebugString();
      construct_params_.push_back(param_const);
      expr_nodes_.emplace_back(param_const.var_name + "_");
      break;
    }

    case common::ExprOpr::kArith: {
      auto arith = opr.arith();
      auto str = arith_to_str(arith);
      VLOG(10) << "Got expr opt arith: " << str;
      expr_nodes_.emplace_back(std::move(str));
      break;
    }

    default:
      LOG(WARNING) << "not recognized expr opr: " << opr.DebugString();
      throw std::runtime_error("not recognized expr opr");
    }
  }

  // get expr nodes
  const std::vector<std::string>& GetExprNodes() const { return expr_nodes_; }

  // get func call vars
  const std::vector<codegen::ParamConst>& GetFuncCallVars() const {
    return func_call_vars_;
  }

  // get tag property strs
  const std::vector<std::pair<int32_t, std::string>>& GetTagSelectors() const {
    return tag_selectors_;
  }

  // get construct params
  const std::vector<codegen::ParamConst>& GetConstructParams() const {
    return construct_params_;
  }

  int32_t GetCurVarId() const { return cur_var_id_; }

  // 0: function name
  // 1: function call params,
  // 2: tag_property
  // 3. function code
  // 4. return type
  virtual std::tuple<std::string, std::vector<codegen::ParamConst>,
                     std::vector<std::pair<int32_t, std::string>>, std::string,
                     common::DataType>
  Build() const {
    // Insert param vars to context.
    for (auto i = 0; i < construct_params_.size(); ++i) {
      ctx_.AddParameterVar(construct_params_[i]);
    }

    std::string constructor_param_str, field_init_code_str,
        func_call_template_typename_str, func_call_params_str,
        func_call_impl_str, private_filed_str;
    constructor_param_str = get_constructor_params_str();
    field_init_code_str = get_field_init_code_str();
    func_call_template_typename_str = get_func_call_typename_str();
    func_call_params_str = get_func_call_params_str();
    func_call_impl_str = get_func_call_impl_str();
    private_filed_str = get_private_filed_str();
    VLOG(10) << "Finish preparing code blocks";

    boost::format formater(EXPR_BUILDER_TEMPLATE_STR);
    formater % class_name_ % common_data_type_pb_2_str(res_data_type_) %
        constructor_param_str % field_init_code_str %
        func_call_template_typename_str % "auto" % func_call_params_str %
        func_call_impl_str % private_filed_str;

    std::string str = formater.str();

    return std::make_tuple(class_name_, construct_params_, tag_selectors_, str,
                           res_data_type_);
  }

  bool empty() const { return expr_nodes_.empty(); }

 protected:
  // return the concatenated string of constructor's input params
  std::string get_constructor_params_str() const {
    std::stringstream ss;
    for (int i = 0; i < construct_params_.size(); ++i) {
      ss << data_type_2_string(construct_params_[i].type) << " "
         << construct_params_[i].var_name;
      if (i != construct_params_.size() - 1) {
        ss << ",";
      }
    }
    return ss.str();
  }

  std::string get_field_init_code_str() const {
    std::stringstream ss;
    if (!construct_params_.empty()) {
      ss << ":";
    }
    for (int i = 0; i < construct_params_.size(); ++i) {
      ss << construct_params_[i].var_name << "_"
         << "(" << construct_params_[i].var_name << ")";
      if (i != construct_params_.size() - 1) {
        ss << ",";
      }
    }
    return ss.str();
  }

  std::string get_func_call_typename_str() const {
    std::string typename_template = "";
    if (constains_vertex_id(func_call_vars_)) {
      typename_template = "template <typename vertex_id_t>";
    }
    return typename_template;
  }

  std::string get_func_call_params_str() const {
    std::stringstream ss;
    for (int i = 0; i < func_call_vars_.size(); ++i) {
      ss << data_type_2_string(func_call_vars_[i].type) << " "
         << EXPR_OPERATOR_CALL_VAR_NAME << i;
      if (i != func_call_vars_.size() - 1) {
        ss << ",";
      }
    }
    return ss.str();
  }

  virtual std::string get_func_call_impl_str() const {
    std::stringstream ss;
    ss << "return ";
    for (auto i = 0; i < expr_nodes_.size(); ++i) {
      ss << expr_nodes_[i] << " ";
    }
    ss << ";";
    return ss.str();
  }

  std::string get_private_filed_str() const {
    std::stringstream ss;
    for (auto i = 0; i < construct_params_.size(); ++i) {
      ss << data_type_2_string(construct_params_[i].type) << " "
         << construct_params_[i].var_name << "_;" << std::endl;
    }
    return ss.str();
  }

  // this corresponding to the input params.
  std::vector<codegen::ParamConst> construct_params_;
  // input var list of function call
  std::vector<codegen::ParamConst> func_call_vars_;
  // we shall also keep the private member too, use {var};
  std::vector<std::pair<int32_t, std::string>>
      tag_selectors_;  // gs::NamedProperty<int64_t>({"creationDate"})
  // component of expression
  std::vector<std::string> expr_nodes_;
  BuildingContext& ctx_;
  int cur_var_id_;
  common::DataType res_data_type_;

  std::string class_name_;
};
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_EXPR_BUILDER_H_