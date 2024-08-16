#ifndef RUNTIME_CODEGEN_EXPRS_EXPR_BUILDER_H
#define RUNTIME_CODEGEN_EXPRS_EXPR_BUILDER_H
#include <stack>

#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
#include "flex/engines/graph_db/runtime/codegen/exprs/expr_utils.h"

namespace gs {
namespace runtime {
class ExprBuilder {
 public:
  ExprBuilder(BuildingContext& context)
      : context_(context), var_type_(VarType::kPathVar) {}

  std::pair<std::string, RTAnyType> buildExpression(
      std::stack<common::ExprOpr>& opr_stack, std::string& ss) {
    auto opr = opr_stack.top();
    opr_stack.pop();
    switch (opr.item_case()) {
    case common::ExprOpr::kConst: {
      auto value = opr.const_();
      auto [str, expr_name, type] = value_pb_2_str(context_, value);
      ss += str + "\n";
      return {expr_name, type};
    }
    case common::ExprOpr::kVar: {
      auto var = opr.var();
      auto [str, expr_name, type] = var_pb_2_str(context_, var, var_type_);
      ss += str + "\n";
      return {expr_name, type};
    }
    case common::ExprOpr::kParam: {
      auto param = opr.param();
      auto [str, expr_name, type] = param_pb_2_str(context_, param);
      ss += str + "\n";
      return {expr_name, type};
    }
    case common::ExprOpr::kExtract: {
      LOG(FATAL) << "not support";
    }
    case common::ExprOpr::kCase: {
      LOG(FATAL) << "not support";
    }
    case common::ExprOpr::kMap: {
      LOG(FATAL) << "not support";
    }
    case common::ExprOpr::kLogical: {
      switch (opr.logical()) {
      case common::Logical::AND:
      case common::Logical::OR: {
        auto [left, left_type] = buildExpression(opr_stack, ss);
        auto [right, right_type] = buildExpression(opr_stack, ss);
        auto expr_name = context_.GetNextExprName();
        ss += "BinaryOpExpr ";
        ss += expr_name + "(" + left + ", " + right + ", " +
              logical_2_str(opr.logical()) + "());\n";
        return {expr_name, RTAnyType::kBoolValue};
      }
      case common::Logical::NOT: {
        auto [left, left_type] = buildExpression(opr_stack, ss);
        auto expr_name = context_.GetNextExprName();
        ss += "UnaryOpExpr ";

        ss += expr_name + " (" + left + ", NotOp());\n";
        return {expr_name, RTAnyType::kBoolValue};
      }
      case common::Logical::EQ:
      case common::Logical::NE:
      case common::Logical::GE:
      case common::Logical::GT:
      case common::Logical::LT:
      case common::Logical::LE: {
        auto [left, left_type] = buildExpression(opr_stack, ss);
        auto [right, right_type] = buildExpression(opr_stack, ss);
        auto expr_name = context_.GetNextExprName();
        ss += "BinaryOpExpr ";
        ss += expr_name + "(" + left + ", " + right + ", " +
              logical_2_str(opr.logical()) + "<" + type2str(left_type) +
              ">());\n";
        return {expr_name, RTAnyType::kBoolValue};
      }
      case common::Logical::WITHIN: {
        auto [left, left_type] = buildExpression(opr_stack, ss);
        auto right = opr_stack.top();
        opr_stack.pop();
        auto arr = array_2_str(right.const_(), left_type);
        auto expr_name = context_.GetNextExprName();

        ss += "WithInExpr ";
        ss += expr_name + "(" + left + ", " + arr + ");\n";
        return {expr_name, RTAnyType::kBoolValue};
      }

      default:
        LOG(FATAL) << "Unsupported logical operator: " << opr.logical();
        break;
      }
    }
    case common::ExprOpr::kArith: {
      switch (opr.arith()) {
      case common::Arithmetic::ADD:
      case common::Arithmetic::SUB:
      case common::Arithmetic::MUL:
      case common::Arithmetic::DIV:
      case common::Arithmetic::MOD: {
        auto [left, left_type] = buildExpression(opr_stack, ss);
        auto [right, right_type] = buildExpression(opr_stack, ss);
        auto expr_name = context_.GetNextExprName();
        ss += "BinaryOpExpr ";
        ss += expr_name + " (" + left + ", " + right + ", " +
              arith_2_str(opr.arith()) + "<" + type2str(left_type) + ">);\n";
        return {expr_name, left_type};
      }
      default:
        LOG(FATAL) << "Unsupported arithmetic operator: " << opr.arith();
        break;
      }
    }
    case common::ExprOpr::kVars: {
      auto op = opr.vars();
      auto expr_name = context_.GetNextExprName();
      ss += "TupleExpr ";
      ss += expr_name + "(";
      for (int i = 0; i < op.keys_size(); ++i) {
        auto key = op.keys(i);
        auto [str, expr_name, type] = var_pb_2_str(context_, key, var_type_);
        ss += expr_name;
        if (i != op.keys_size() - 1) {
          ss += ", ";
        } else {
          ss += ");\n";
        }
      }
      return {expr_name, RTAnyType::kTuple};
    }
    default:
      LOG(FATAL) << "Unsupported operator type: " << opr.item_case();
      break;
    }
    return {"", RTAnyType::kUnknown};
  }
  static inline int get_priority(const common::ExprOpr& opr) {
    switch (opr.item_case()) {
    case common::ExprOpr::kBrace: {
      return 17;
    }
    case common::ExprOpr::kExtract: {
      return 2;
    }
    case common::ExprOpr::kLogical: {
      switch (opr.logical()) {
      case common::Logical::AND:
        return 11;
      case common::Logical::OR:
        return 12;
      case common::Logical::NOT:
        return 2;
      case common::Logical::WITHIN:
      case common::Logical::WITHOUT:
        return 2;
      case common::Logical::EQ:
      case common::Logical::NE:
        return 7;
      case common::Logical::GE:
      case common::Logical::GT:
      case common::Logical::LT:
      case common::Logical::LE:
        return 6;
      case common::Logical::REGEX:
        return 2;
      default:
        return 16;
      }
    }
    case common::ExprOpr::kArith: {
      switch (opr.arith()) {
      case common::Arithmetic::ADD:
      case common::Arithmetic::SUB:
        return 4;
      case common::Arithmetic::MUL:
      case common::Arithmetic::DIV:
      case common::Arithmetic::MOD:
        return 3;
      default:
        return 16;
      }
    }
    default:
      return 16;
    }
    return 16;
  }
  // expr name, expr string
  std::pair<std::string, std::string> parse_expression_impl(
      const common::Expression& expr) {
    std::stack<common::ExprOpr> opr_stack;
    std::stack<common::ExprOpr> opr_stack2;
    const auto& oprs = expr.operators();
    for (auto it = oprs.rbegin(); it != oprs.rend(); ++it) {
      switch ((*it).item_case()) {
      case common::ExprOpr::kBrace: {
        auto brace = (*it).brace();
        if (brace == common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
          while (!opr_stack.empty() &&
                 opr_stack.top().item_case() != common::ExprOpr::kBrace) {
            opr_stack2.push(opr_stack.top());
            opr_stack.pop();
          }
          CHECK(!opr_stack.empty());
          opr_stack.pop();
        } else if (brace == common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
          opr_stack.push(*it);
        }
        break;
      }
      case common::ExprOpr::kConst:
      case common::ExprOpr::kVar:
      case common::ExprOpr::kParam:
      case common::ExprOpr::kVars:
      case common::ExprOpr::kExtract:
      case common::ExprOpr::kCase:
      case common::ExprOpr::kMap: {
        opr_stack2.push(*it);
        break;
      }
      case common::ExprOpr::kArith:
      case common::ExprOpr::kLogical: {
        if ((*it).logical() == common::Logical::NOT ||
            (*it).logical() == common::Logical::ISNULL) {
          opr_stack2.push(*it);
          break;
        }
        while (!opr_stack.empty() &&
               get_priority(opr_stack.top()) <= get_priority(*it)) {
          opr_stack2.push(opr_stack.top());
          opr_stack.pop();
        }
        opr_stack.push(*it);
        break;
      }
      default:
        LOG(FATAL) << "Unsupported operator type: " << (*it).DebugString();
        break;
      }
    }
    while (!opr_stack.empty()) {
      opr_stack2.push(opr_stack.top());
      opr_stack.pop();
    }
    std::string ss;
    auto [name, type] = buildExpression(opr_stack2, ss);
    return {name, ss};
  }

  ExprBuilder& varType(VarType var_type) {
    var_type_ = var_type;
    return *this;
  }

  std::pair<std::string, std::string> Build(const common::Expression& expr) {
    return parse_expression_impl(expr);
  }
  BuildingContext& context_;
  VarType var_type_;
};
// expr name, expr string
std::pair<std::string, std::string> build_expr(
    BuildingContext& context, const common::Expression& expr,
    VarType var_type = VarType::kPathVar);

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_CODEGEN_EXPRS_EXPR_BUILDER_H
