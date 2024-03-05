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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_EXPR_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_EXPR_BUILDER_H_

#include <map>
#include <stack>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {
namespace pegasus {
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

bool contains_vertex_id(const std::vector<codegen::ParamConst>& params) {
  for (auto& param : params) {
    if (param.type == codegen::DataType::kVertexId) {
      return true;
    }
  }
  return false;
}

/*Build a expression struct from expression*/
class ExprBuilder {
 protected:
  static constexpr const char* EXPR_OPERATOR_CALL_VAR_NAME = "var";

 public:
  ExprBuilder(BuildingContext& ctx, int var_id = 0, bool no_build = false,
              int cur_var_start = 0)
      : ctx_(ctx),
        cur_var_start_(cur_var_start),
        cur_var_id_(var_id),
        cur_case_id_(0) {
    if (no_build) {
      // no build indicates whether we will use this builder as a helper.
      // If set to true, we will not let queryClassName and next_expr_name
      // increase.
      class_name_ = ctx_.GetQueryClassName() + ctx_.GetNextExprName();
    }
  }

  void AddAllExprOpr(
      const google::protobuf::RepeatedPtrField<common::ExprOpr>& expr_ops) {
    // we currently don't support filter with label keys!
    // If we meet label keys just ignore.
    auto size = expr_ops.size();
    VLOG(10) << "Adding expr of size: " << size;
    for (int32_t i = 0; i < size;) {
      auto expr = expr_ops[i];
      if (expr.has_var() && expr.var().property().has_label()) {
        VLOG(10) << "Found label in expr, skip this check";
        // try to find next right brace
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
    VLOG(10) << "Added expr of size: " << size;
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
        VLOG(10) << "left brace";
        expr_nodes_.emplace_back("(");
      } else if (brace == common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        VLOG(10) << "right brace";
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
      VLOG(10) << "Got var";
      auto& var = opr.var();
      auto param_const = variable_to_param_const(var, ctx_);
      // for each variable, we need add the variable to func_call_vars_.
      // and also set a expr node for it. which is unique.
      func_call_vars_.push_back(param_const);
      if (!var.has_tag()) {
        func_call_tags_.push_back(-1);
      } else {
        func_call_tags_.push_back(var.tag().id());
      }
      expr_nodes_.emplace_back(std::string(EXPR_OPERATOR_CALL_VAR_NAME) +
                               std::to_string(cur_var_id_++));

      // expr_nodes_.emplace_back(param_const.var_name);
      // convert a variable to a tag property,
      // gs::NamedProperty<gs::Int64>{"prop1"}, saved for later use.
      // tag_prop_strs_.emplace_back(variable_to_named_property(ctx_, var));
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
      ctx_.AddParameterVar(param_const);
      expr_nodes_.emplace_back(param_const.var_name);
      break;
    }

    case common::ExprOpr::kArith: {
      auto arith = opr.arith();
      auto str = arith_to_str(arith);
      VLOG(10) << "Got expr opt arith: " << str;
      expr_nodes_.emplace_back(std::move(str));
      break;
    }

    case common::ExprOpr::kCase: {
      int32_t when_then_size = opr.case_().when_then_expressions_size();
      std::stringstream case_ss;
      std::stringstream var_ss, when_then_ss;
      case_ss << "{\n";
      int32_t cur_var_num = 0;
      std::unordered_set<int32_t> tag_used;
      for (int32_t i = 0; i < when_then_size; i++) {
        auto when_then = opr.case_().when_then_expressions(i);
        auto when_expr = when_then.when_expression();
        auto then_expr = when_then.then_result_expression();
        {
          auto when_builder = ExprBuilder(ctx_, cur_var_num);
          when_builder.AddAllExprOpr(when_expr.operators());
          std::string predicate_expr;
          std::vector<std::string> var_names;
          std::vector<int32_t> var_tags;
          std::vector<codegen::ParamConst> properties;
          std::vector<std::string> case_exprs;
          std::tie(predicate_expr, var_names, var_tags, properties,
                   case_exprs) = when_builder.BuildRust();
          cur_var_num += var_names.size();
          if (i != 0) {
            when_then_ss << "} else ";
          }
          when_then_ss << "if " << predicate_expr << "{\n";
          write_var_expr(var_ss, var_names, var_tags, properties, tag_used);
        }

        {
          auto then_builder = ExprBuilder(ctx_, cur_var_num);
          then_builder.AddAllExprOpr(then_expr.operators());
          std::string predicate_expr;
          std::vector<std::string> var_names;
          std::vector<int32_t> var_tags;
          std::vector<codegen::ParamConst> properties;
          std::vector<std::string> case_exprs;
          std::tie(predicate_expr, var_names, var_tags, properties,
                   case_exprs) = then_builder.BuildRust();
          when_then_ss << predicate_expr << "\n";
          write_var_expr(var_ss, var_names, var_tags, properties, tag_used);
        }
      }
      when_then_ss << "}";
      auto else_expr = opr.case_().else_result_expression();
      {
        auto else_builder = ExprBuilder(ctx_, cur_var_num);
        else_builder.AddAllExprOpr(else_expr.operators());
        std::string predicate_expr;
        std::vector<std::string> var_names;
        std::vector<int32_t> var_tags;
        std::vector<codegen::ParamConst> properties;
        std::vector<std::string> case_exprs;
        std::tie(predicate_expr, var_names, var_tags, properties, case_exprs) =
            else_builder.BuildRust();
        when_then_ss << " else {\n" << predicate_expr << "\n}\n";
        write_var_expr(var_ss, var_names, var_tags, properties, tag_used);
        when_then_ss << "};\n";
      }
      std::string case_name = "case_" + std::to_string(cur_case_id_);
      case_exprs_.push_back("let " + case_name + " = {\n" + var_ss.str() +
                            when_then_ss.str());
      expr_nodes_.emplace_back(case_name);
      cur_case_id_++;
      break;
    }

    default:
      LOG(FATAL) << "not recognized expr opr: " << opr.DebugString();
      break;
    }
  }

  // get expr nodes
  const std::vector<std::string>& GetExprNodes() const { return expr_nodes_; }

  // get func call vars
  const std::vector<codegen::ParamConst>& GetFuncCallVars() const {
    return func_call_vars_;
  }

  // get tag property strs
  const std::vector<std::string>& GetTagPropertyStrs() const {
    return tag_prop_strs_;
  }

  // get construct params
  const std::vector<codegen::ParamConst>& GetConstructParams() const {
    return construct_params_;
  }

  int32_t GetCurVarId() const { return cur_var_id_; }

  bool empty() const { return expr_nodes_.empty(); }

  std::tuple<std::string, std::vector<std::string>, std::vector<int32_t>,
             std::vector<codegen::ParamConst>, std::vector<std::string>>
  BuildRust() const {
    std::stringstream expr_ss;

    for (size_t i = 0; i < expr_nodes_.size(); ++i) {
      expr_ss << expr_nodes_[i] << " ";
    }
    std::string predicate_expr = expr_ss.str();

    std::vector<std::string> var_names;
    std::vector<codegen::ParamConst> properties;
    if (func_call_vars_.size() > 0) {
      for (size_t i = 0; i < func_call_vars_.size(); ++i) {
        var_names.push_back(std::string(EXPR_OPERATOR_CALL_VAR_NAME) +
                            std::to_string(cur_var_start_ + i));
        if (func_call_vars_[i].var_name.find("var") == 0) {
          codegen::ParamConst empty;
          empty.var_name = "none";
          properties.push_back(empty);
        } else {
          properties.push_back(func_call_vars_[i]);
        }
      }
    }

    return std::make_tuple(predicate_expr, var_names, func_call_tags_,
                           properties, case_exprs_);
  }

 protected:
  void start_class(std::stringstream& ss) const {
    ss << "template <";
    for (size_t i = 0; i < tag_prop_strs_.size() - 1; ++i) {
      ss << "typename TAG_PROP_" << i << ", ";
    }
    ss << "typename TAG_PROP_" << tag_prop_strs_.size() - 1 << ">";
    ss << std::endl;
    ss << "struct " << class_name_ << " {" << std::endl;
    ss << "  using tag_prop_t = std::tuple<";
    for (size_t i = 0; i < tag_prop_strs_.size() - 1; ++i) {
      ss << "TAG_PROP_" << i << ", ";
    }
    ss << "TAG_PROP_" << tag_prop_strs_.size() - 1 << ">;" << std::endl;
  }

  void end_class(std::stringstream& ss) const { ss << "};"; }

  void add_constructor(std::stringstream& ss) const {
    ss << _4_SPACES << class_name_ << "(";
    {
      // params
      // if (construct_params_.size() >= 1) {
      for (size_t i = 0; i < construct_params_.size(); ++i) {
        ss << data_type_2_string(construct_params_[i].type) << " "
           << construct_params_[i].var_name << ", ";
      }

      // }
      // tag_props
      CHECK(tag_prop_strs_.size() > 0);
      for (size_t i = 0; i < tag_prop_strs_.size() - 1; ++i) {
        ss << "TAG_PROP_" << i << "&& prop_" << i << ", ";
      }
      ss << "TAG_PROP_" << tag_prop_strs_.size() - 1 << "&& prop_"
         << tag_prop_strs_.size() - 1;
      ss << ")";
    }
    {
      // constructor's code.
      ss << " : ";
      if (construct_params_.size() >= 1) {
        for (size_t i = 0; i < construct_params_.size() - 1; ++i) {
          ss << construct_params_[i].var_name << "_"
             << "(" << construct_params_[i].var_name << "), ";
        }
        ss << construct_params_.back().var_name << "_"
           << "(" << construct_params_.back().var_name << ")";
        ss << ",";
      }

      for (size_t i = 0; i < tag_prop_strs_.size() - 1; ++i) {
        ss << "prop_" << i << "_(std::move(prop_" << i << ")),";
      }
      ss << "prop_" << tag_prop_strs_.size() - 1 << "_("
         << "std::move(prop_" << tag_prop_strs_.size() - 1 << "))";
    }

    ss << "{}" << std::endl;
  }

  void add_func_call(std::stringstream& ss) const {
    // for function call, there can be vertex_id_t as input param, which depends
    // on vertex_id type. so we need to template typename.
    if (contains_vertex_id(func_call_vars_)) {
      ss << _4_SPACES << "template <typename vertex_id_t>" << std::endl;
    }
    ss << _4_SPACES << "inline auto operator()";
    ss << "(";
    if (func_call_vars_.size() > 0) {
      for (size_t i = 0; i + 1 < func_call_vars_.size(); ++i) {
        ss << data_type_2_string(func_call_vars_[i].type) << " "
           << EXPR_OPERATOR_CALL_VAR_NAME << i << ",";
      }
      ss << data_type_2_string(func_call_vars_.back().type) << " "
         << EXPR_OPERATOR_CALL_VAR_NAME << func_call_vars_.size() - 1;
    }
    ss << ") const {" << std::endl;
    ss << _8_SPACES << "return ";
    for (size_t i = 0; i < expr_nodes_.size(); ++i) {
      ss << expr_nodes_[i] << " ";
    }
    ss << ";" << std::endl;
    ss << _4_SPACES << "}" << std::endl;
  }

  void add_private_member(std::stringstream& ss) const {
    ss << _4_SPACES << "private:" << std::endl;
    for (size_t i = 0; i < construct_params_.size(); ++i) {
      ss << _8_SPACES << data_type_2_string(construct_params_[i].type) << " "
         << construct_params_[i].var_name << "_;";
      ss << std::endl;
    }
    for (size_t i = 0; i < tag_prop_strs_.size(); ++i) {
      ss << _8_SPACES << "TAG_PROP_" << i << " prop_" << i << "_;";
      ss << std::endl;
    }
  }

  void write_var_expr(std::stringstream& ss,
                      std::vector<std::string>& var_names,
                      std::vector<int32_t>& var_tags,
                      std::vector<codegen::ParamConst>& properties,
                      std::unordered_set<int32_t>& tag_used) {
    std::unordered_set<int32_t> tags_set;
    for (auto var_tag : var_tags) {
      if (tag_used.find(var_tag) == tag_used.end()) {
        tags_set.insert(var_tag);
      }
    }
    for (auto tag : tags_set) {
      int32_t var_index = 0;
      std::pair<int32_t, std::vector<int32_t>> input_type;
      if (tag != -1) {
        var_index = ctx_.GetAliasIndex(tag);
        input_type = ctx_.GetAliasType(tag);
      } else {
        input_type = ctx_.GetHeadType();
      }
      ss << "let vertex_id" << tag + 1 << " = CSR.get_internal_id(i"
         << var_index << " as usize);\n";
      VLOG(10) << "Get input alias type, index " << var_index << " label size "
               << input_type.second.size();
      if (input_type.first == 0 && input_type.second.size() > 1) {
        ss << "let vertex_label" << tag + 1
           << " = LDBCVertexParser::<usize>::get_label_id(i" << var_index
           << " as usize);\n";
      }
    }

    for (size_t i = 0; i < var_names.size(); i++) {
      int32_t var_tag = var_tags[i];
      std::pair<int32_t, std::vector<int32_t>> input_type;
      if (var_tag != -1) {
        input_type = ctx_.GetAliasType(var_tag);
      } else {
        input_type = ctx_.GetHeadType();
      }
      ss << "let " << var_names[i] << " = ";
      if (input_type.first == 0 && input_type.second.size() > 1) {
        for (size_t j = 0; j < input_type.second.size(); j++) {
          if (j != 0) {
            ss << "} else ";
          }
          if (j != input_type.second.size() - 1) {
            ss << "if vertex_label" << var_tag + 1
               << " == " << input_type.second[j] << "{\n";
          } else {
            ss << "{\n";
          }
          ss << properties[i].var_name << "_" << input_type.second[j]
             << "[vertex_id" << var_tag + 1 << "]\n";
        }
        ss << "};\n";
      } else {
        ss << properties[i].var_name << "_" << input_type.second[0]
           << "[vertex_id" << var_tag + 1 << "];\n";
      }
    }
  }

  // this corresponding to the input params.
  std::vector<codegen::ParamConst> construct_params_;
  // input var list of function call
  std::vector<codegen::ParamConst> func_call_vars_;
  // tag used in input var list
  std::vector<int32_t> func_call_tags_;
  // we shall also keep the private member too, use {var};
  std::vector<std::string>
      tag_prop_strs_;  // gs::NamedProperty<int64_t>({"creationDate"})
  std::vector<std::string> case_exprs_;
  // component of expression
  std::vector<std::string> expr_nodes_;
  BuildingContext& ctx_;
  int cur_var_start_;
  int cur_var_id_;
  int cur_case_id_;

  std::string class_name_;
};
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_EXPR_BUILDER_H_