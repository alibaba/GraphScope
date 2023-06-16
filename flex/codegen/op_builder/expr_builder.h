#ifndef EXPR_BUILDER_H
#define EXPR_BUILDER_H

#include <stack>
#include <string>
#include <vector>

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/codegen_utils.h"
#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"

namespace gs {

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

static codegen::ParamConst variable_to_param_cost(const common::Variable& var,
                                                  BuildingContext& ctx) {
  codegen::ParamConst param_const;
  if (var.has_property()) {
    // TODO property can be id,label,len,all,etc.
    param_const.var_name = var.property().key().name();
    param_const.type =
        common_data_type_pb_2_data_type(var.node_type().data_type());
  } else {
    LOG(INFO) << "got param const on IdKey";
    param_const.var_name = ctx.GetNextVarName();
    param_const.type = codegen::DataType::kVertexId;
  }

  return param_const;
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
      : ctx_(ctx), cur_var_id_(var_id) {
    if (!no_build) {
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
    LOG(INFO) << "Adding expr of size: " << size;
    for (auto i = 0; i < size;) {
      auto expr = expr_ops[i];
      if (expr.has_var() && expr.var().property().has_label()) {
        LOG(INFO) << "Found label in expr, skip this check";
        // try to find next right brace
        int j = i;
        for (; j < size; ++j) {
          if (expr_ops[j].item_case() == common::ExprOpr::kBrace &&
              expr_ops[j].brace() ==
                  common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
            LOG(INFO) << "Found right brace at ind: " << j
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
    LOG(INFO) << "Added expr of size: " << size;
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
        LOG(INFO) << "left brace";
        expr_nodes_.emplace_back("(");
      } else if (brace == common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        LOG(INFO) << "right brace";
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
      LOG(INFO) << "Got var";
      auto& var = opr.var();
      auto param_const = variable_to_param_cost(var, ctx_);
      // for each variable, we need add the variable to func_call_vars_.
      // and also set a expr node for it. which is unique.
      func_call_vars_.push_back(param_const);
      expr_nodes_.emplace_back(std::string(EXPR_OPERATOR_CALL_VAR_NAME) +
                               std::to_string(cur_var_id_++));

      // expr_nodes_.emplace_back(param_const.var_name);
      // convert a variable to a tag property,
      // gs::NamedProperty<gs::Int64>{"prop1"}, saved for later use.
      tag_prop_strs_.emplace_back(variable_to_named_property(ctx_, var));
      VLOG(10) << "Got var: " << var.DebugString();
      break;
    }

    case common::ExprOpr::kLogical: {
      auto logical = opr.logical();
      auto str = logical_to_str(logical);
      LOG(INFO) << "Got expr opt logical: " << str;
      expr_nodes_.emplace_back(std::move(str));
      break;
    }

    // todo: use dynamic param
    case common::ExprOpr::kParam: {
      auto param_const_pb = opr.param();
      auto param_node_type = opr.node_type();
      auto param_const =
          param_const_pb_to_param_const(param_const_pb, param_node_type);
      LOG(INFO) << "receive param const: " << param_const_pb.DebugString();
      construct_params_.push_back(param_const);
      expr_nodes_.emplace_back(param_const.var_name + "_");
      break;
    }

    case common::ExprOpr::kArith: {
      auto arith = opr.arith();
      auto str = arith_to_str(arith);
      LOG(INFO) << "Got expr opt arith: " << str;
      expr_nodes_.emplace_back(std::move(str));
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

  // 0: function name
  // 1: function call params,
  // 2: tag_property
  // 3. function code
  // 4. return type
  virtual std::tuple<std::string, std::vector<codegen::ParamConst>,
                     std::vector<std::string>, std::string, common::DataType>
  Build() const {
    // Insert param vars to context.
    for (auto i = 0; i < construct_params_.size(); ++i) {
      ctx_.AddParameterVar(construct_params_[i]);
    }

    LOG(INFO) << "Enter express building";
    std::stringstream ss;
    // CHECK(tag_prop_strs_.size() == 1) << "only support one tag property now";

    // start class with struct
    start_class(ss);
    // constructor
    add_constructor(ss);
    // function call
    add_func_call(ss);
    add_tag_prop_getter(ss);
    // private member
    add_private_member(ss);
    // end class
    end_class(ss);

    std::string str = ss.str();

    return std::make_tuple(
        class_name_, construct_params_, tag_prop_strs_, str,
        common::DataType::DataType_INT_MIN_SENTINEL_DO_NOT_USE_);
  }

  bool empty() const { return expr_nodes_.empty(); }

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
    if (constains_vertex_id(func_call_vars_)) {
      ss << _4_SPACES << "template <typename vertex_id_t>" << std::endl;
    }
    ss << _4_SPACES << "inline auto operator()";
    ss << "(";
    if (func_call_vars_.size() > 0) {
      for (auto i = 0; i < func_call_vars_.size() - 1; ++i) {
        ss << data_type_2_string(func_call_vars_[i].type) << " "
           << EXPR_OPERATOR_CALL_VAR_NAME << i << ",";
      }
      ss << data_type_2_string(func_call_vars_.back().type) << " "
         << EXPR_OPERATOR_CALL_VAR_NAME << func_call_vars_.size() - 1;
    }
    ss << ") const {" << std::endl;
    ss << _8_SPACES << "return ";
    for (auto i = 0; i < expr_nodes_.size(); ++i) {
      ss << expr_nodes_[i] << " ";
    }
    ss << ";" << std::endl;
    ss << _4_SPACES << "}" << std::endl;
  }

  void add_tag_prop_getter(std::stringstream& ss) const {
    ss << _4_SPACES << "inline auto Properties() const {" << std::endl;
    ss << _8_SPACES << "return std::make_tuple(";

    for (auto i = 0; i < tag_prop_strs_.size() - 1; ++i) {
      ss << "prop_" << i << "_"
         << ",";
    }
    ss << "prop_" << tag_prop_strs_.size() - 1 << "_"
       << ");" << std::endl;
    ss << _4_SPACES << "}";
    ss << std::endl;
  }

  void add_private_member(std::stringstream& ss) const {
    ss << _4_SPACES << "private:" << std::endl;
    for (auto i = 0; i < construct_params_.size(); ++i) {
      ss << _8_SPACES << data_type_2_string(construct_params_[i].type) << " "
         << construct_params_[i].var_name << "_;";
      ss << std::endl;
    }
    for (auto i = 0; i < tag_prop_strs_.size(); ++i) {
      ss << _8_SPACES << "TAG_PROP_" << i << " prop_" << i << "_;";
      ss << std::endl;
    }
  }

  // this corresponding to the input params.
  std::vector<codegen::ParamConst> construct_params_;
  // input var list of function call
  std::vector<codegen::ParamConst> func_call_vars_;
  // we shall also keep the private member too, use {var};
  std::vector<std::string>
      tag_prop_strs_;  // gs::NamedProperty<int64_t>({"creationDate"})
  // component of expression
  std::vector<std::string> expr_nodes_;
  BuildingContext& ctx_;
  int cur_var_id_;

  std::string class_name_;
};
}  // namespace gs

#endif  // EXPR_BUILDER_H