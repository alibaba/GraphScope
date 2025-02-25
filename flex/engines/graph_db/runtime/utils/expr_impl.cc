/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/runtime/utils/expr_impl.h"
#include <regex>
#include <stack>

namespace gs {

namespace runtime {

RTAny VariableExpr::eval_path(size_t idx) const { return var_.get(idx); }
RTAny VariableExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  return var_.get_vertex(label, v, idx);
}
RTAny VariableExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, size_t idx) const {
  return var_.get_edge(label, src, dst, data, idx);
}

RTAny VariableExpr::eval_path(size_t idx, int) const {
  return var_.get(idx, 0);
}

RTAny VariableExpr::eval_vertex(label_t label, vid_t v, size_t idx, int) const {
  return var_.get_vertex(label, v, idx, 0);
}

RTAny VariableExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, size_t idx, int) const {
  return var_.get_edge(label, src, dst, data, idx, 0);
}

RTAnyType VariableExpr::type() const { return var_.type(); }

LogicalExpr::LogicalExpr(std::unique_ptr<ExprBase>&& lhs,
                         std::unique_ptr<ExprBase>&& rhs, common::Logical logic)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), logic_(logic) {
  switch (logic) {
  case common::Logical::LT: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs < rhs; };
    break;
  }
  case common::Logical::GT: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return rhs < lhs; };
    break;
  }
  case common::Logical::GE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(lhs < rhs); };
    break;
  }
  case common::Logical::LE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(rhs < lhs); };
    break;
  }
  case common::Logical::EQ: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs == rhs; };
    break;
  }
  case common::Logical::NE: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return !(lhs == rhs); };
    break;
  }
  case common::Logical::AND: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      return lhs.as_bool() && rhs.as_bool();
    };
    break;
  }
  case common::Logical::OR: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      return lhs.as_bool() || rhs.as_bool();
    };
    break;
  }
  case common::Logical::REGEX: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) {
      auto lhs_str = std::string(lhs.as_string());
      auto rhs_str = std::string(rhs.as_string());
      return std::regex_match(lhs_str, std::regex(rhs_str));
    };
    break;
  }
  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(logic);
    break;
  }
  }
}

RTAny LogicalExpr::eval_path(size_t idx) const {
  return RTAny::from_bool(op_(lhs_->eval_path(idx), rhs_->eval_path(idx)));
}

RTAny LogicalExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  return RTAny::from_bool(
      op_(lhs_->eval_vertex(label, v, idx), rhs_->eval_vertex(label, v, idx)));
}

RTAny LogicalExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                             const Any& data, size_t idx) const {
  return RTAny::from_bool(op_(lhs_->eval_edge(label, src, dst, data, idx),
                              rhs_->eval_edge(label, src, dst, data, idx)));
}

RTAnyType LogicalExpr::type() const { return RTAnyType::kBoolValue; }

UnaryLogicalExpr::UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr,
                                   common::Logical logic)
    : expr_(std::move(expr)), logic_(logic) {}

RTAny UnaryLogicalExpr::eval_path(size_t idx) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_path(idx).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_path(idx, 0).type() ==
                            RTAnyType::kNull);
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_path(size_t idx, int) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_path(idx, 0).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_path(idx, 0).type() ==
                            RTAnyType::kNull);
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(!expr_->eval_vertex(label, v, idx).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(expr_->eval_vertex(label, v, idx, 0).is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAny UnaryLogicalExpr::eval_edge(const LabelTriplet& label, vid_t src,
                                  vid_t dst, const Any& data,
                                  size_t idx) const {
  if (logic_ == common::Logical::NOT) {
    return RTAny::from_bool(
        !expr_->eval_edge(label, src, dst, data, idx).as_bool());
  } else if (logic_ == common::Logical::ISNULL) {
    return RTAny::from_bool(
        expr_->eval_edge(label, src, dst, data, idx, 0).is_null());
  }
  LOG(FATAL) << "not support" << static_cast<int>(logic_);
  return RTAny::from_bool(false);
}

RTAnyType UnaryLogicalExpr::type() const { return RTAnyType::kBoolValue; }

ArithExpr::ArithExpr(std::unique_ptr<ExprBase>&& lhs,
                     std::unique_ptr<ExprBase>&& rhs, common::Arithmetic arith)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)), arith_(arith) {
  switch (arith_) {
  case common::Arithmetic::ADD: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs + rhs; };
    break;
  }
  case common::Arithmetic::SUB: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs - rhs; };
    break;
  }
  case common::Arithmetic::DIV: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs / rhs; };
    break;
  }
  case common::Arithmetic::MOD: {
    op_ = [](const RTAny& lhs, const RTAny& rhs) { return lhs % rhs; };
    break;
  }

  default: {
    LOG(FATAL) << "not support..." << static_cast<int>(arith);
    break;
  }
  }
}

RTAny ArithExpr::eval_path(size_t idx) const {
  return op_(lhs_->eval_path(idx), rhs_->eval_path(idx));
}

RTAny ArithExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  return op_(lhs_->eval_vertex(label, v, idx),
             rhs_->eval_vertex(label, v, idx));
}

RTAny ArithExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, size_t idx) const {
  return op_(lhs_->eval_edge(label, src, dst, data, idx),
             rhs_->eval_edge(label, src, dst, data, idx));
}

RTAnyType ArithExpr::type() const {
  if (lhs_->type() == RTAnyType::kF64Value ||
      rhs_->type() == RTAnyType::kF64Value) {
    return RTAnyType::kF64Value;
  }
  if (lhs_->type() == RTAnyType::kI64Value ||
      rhs_->type() == RTAnyType::kI64Value) {
    return RTAnyType::kI64Value;
  }
  return lhs_->type();
}

DateMinusExpr::DateMinusExpr(std::unique_ptr<ExprBase>&& lhs,
                             std::unique_ptr<ExprBase>&& rhs)
    : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

RTAny DateMinusExpr::eval_path(size_t idx) const {
  auto lhs = lhs_->eval_path(idx).as_timestamp();
  auto rhs = rhs_->eval_path(idx).as_timestamp();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAny DateMinusExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  auto lhs = lhs_->eval_vertex(label, v, idx).as_timestamp();
  auto rhs = rhs_->eval_vertex(label, v, idx).as_timestamp();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAny DateMinusExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                               const Any& data, size_t idx) const {
  auto lhs = lhs_->eval_edge(label, src, dst, data, idx).as_timestamp();
  auto rhs = rhs_->eval_edge(label, src, dst, data, idx).as_timestamp();
  return RTAny::from_int64(lhs.milli_second - rhs.milli_second);
}

RTAnyType DateMinusExpr::type() const { return RTAnyType::kI64Value; }

ConstExpr::ConstExpr(const RTAny& val) : val_(val) {
  if (val_.type() == RTAnyType::kStringValue) {
    s = val_.as_string();
    val_ = RTAny::from_string(s);
  }
}
RTAny ConstExpr::eval_path(size_t idx) const { return val_; }
RTAny ConstExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  return val_;
}
RTAny ConstExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, size_t idx) const {
  return val_;
}

RTAnyType ConstExpr::type() const { return val_.type(); }

static int32_t extract_year(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_year + 1900;
}

static int32_t extract_month(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_mon + 1;
}

static int32_t extract_day(int64_t ms) {
  auto micro_second = ms / 1000;
  struct tm tm;
  gmtime_r((time_t*) (&micro_second), &tm);
  return tm.tm_mday;
}

int32_t extract_time_from_milli_second(int64_t ms, common::Extract extract) {
  if (extract.interval() == common::Extract::YEAR) {
    return extract_year(ms);
  } else if (extract.interval() == common::Extract::MONTH) {
    return extract_month(ms);
  } else if (extract.interval() == common::Extract::DAY) {
    return extract_day(ms);
  } else {
    LOG(FATAL) << "not support";
  }
  return 0;
}

CaseWhenExpr::CaseWhenExpr(
    std::vector<std::pair<std::unique_ptr<ExprBase>,
                          std::unique_ptr<ExprBase>>>&& when_then_exprs,
    std::unique_ptr<ExprBase>&& else_expr)
    : when_then_exprs_(std::move(when_then_exprs)),
      else_expr_(std::move(else_expr)) {}

RTAny CaseWhenExpr::eval_path(size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_path(idx).as_bool()) {
      return pair.second->eval_path(idx);
    }
  }
  return else_expr_->eval_path(idx);
}

RTAny CaseWhenExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_vertex(label, v, idx).as_bool()) {
      return pair.second->eval_vertex(label, v, idx);
    }
  }
  return else_expr_->eval_vertex(label, v, idx);
}

RTAny CaseWhenExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                              const Any& data, size_t idx) const {
  for (auto& pair : when_then_exprs_) {
    if (pair.first->eval_edge(label, src, dst, data, idx).as_bool()) {
      return pair.second->eval_edge(label, src, dst, data, idx);
    }
  }
  return else_expr_->eval_edge(label, src, dst, data, idx);
}

RTAnyType CaseWhenExpr::type() const {
  RTAnyType type(RTAnyType::kNull);
  if (when_then_exprs_.size() > 0) {
    if (when_then_exprs_[0].second->type() != RTAnyType::kNull) {
      type = when_then_exprs_[0].second->type();
    }
  }
  if (else_expr_->type() != RTAnyType::kNull) {
    type = else_expr_->type();
  }
  return type;
}

TupleExpr::TupleExpr(const Context& ctx,
                     std::vector<std::unique_ptr<ExprBase>>&& exprs)
    : ctx_(ctx), exprs_(std::move(exprs)) {}

RTAny TupleExpr::eval_path(size_t idx) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_path(idx));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  ctx_.value_collection->emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

RTAny TupleExpr::eval_vertex(label_t label, vid_t v, size_t idx) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_vertex(label, v, idx));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  ctx_.value_collection->emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

RTAny TupleExpr::eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& data, size_t idx) const {
  std::vector<RTAny> ret;
  for (auto& expr : exprs_) {
    ret.push_back(expr->eval_edge(label, src, dst, data, idx));
  }
  auto tup = Tuple::make_generic_tuple_impl(std::move(ret));
  Tuple t(tup.get());
  ctx_.value_collection->emplace_back(std::move(tup));
  return RTAny::from_tuple(t);
}

RTAnyType TupleExpr::type() const { return RTAnyType::kTuple; }

static RTAny parse_const_value(const common::Value& val) {
  switch (val.item_case()) {
  case common::Value::kI32:
    return RTAny::from_int32(val.i32());
  case common::Value::kStr:
    return RTAny::from_string(val.str());
  case common::Value::kI64:
    return RTAny::from_int64(val.i64());
  case common::Value::kBoolean:
    return RTAny::from_bool(val.boolean());
  case common::Value::kNone:
    return RTAny(RTAnyType::kNull);
  case common::Value::kF64:
    return RTAny::from_double(val.f64());
  default:
    LOG(FATAL) << "not support for " << val.item_case();
  }
  return RTAny();
}

template <size_t N, size_t I, typename... Args>
struct TypedTupleBuilder {
  std::unique_ptr<ExprBase> build_typed_tuple(
      const Context& ctx, std::array<std::unique_ptr<ExprBase>, N>&& exprs) {
    switch (exprs[I - 1]->type()) {
    case RTAnyType::kI32Value:
      return TypedTupleBuilder<N, I - 1, int, Args...>().build_typed_tuple(
          ctx, std::move(exprs));
    case RTAnyType::kI64Value:
      return TypedTupleBuilder<N, I - 1, int64_t, Args...>().build_typed_tuple(
          ctx, std::move(exprs));
    case RTAnyType::kF64Value:
      return TypedTupleBuilder<N, I - 1, double, Args...>().build_typed_tuple(
          ctx, std::move(exprs));
    case RTAnyType::kStringValue:
      return TypedTupleBuilder<N, I - 1, std::string_view, Args...>()
          .build_typed_tuple(ctx, std::move(exprs));
    default: {
      std::vector<std::unique_ptr<ExprBase>> exprs_vec;
      for (auto& expr : exprs) {
        exprs_vec.emplace_back(std::move(expr));
      }
      return std::make_unique<TupleExpr>(ctx, std::move(exprs_vec));
    }
    }
  }
};

template <size_t N, typename... Args>
struct TypedTupleBuilder<N, 0, Args...> {
  std::unique_ptr<ExprBase> build_typed_tuple(
      const Context& ctx, std::array<std::unique_ptr<ExprBase>, N>&& exprs) {
    return std::make_unique<TypedTupleExpr<Args...>>(ctx, std::move(exprs));
  }
};

static RTAny parse_param(const common::DynamicParam& param,
                         const std::map<std::string, std::string>& input) {
  if (param.data_type().type_case() ==
      common::IrDataType::TypeCase::kDataType) {
    auto type = parse_from_ir_data_type(param.data_type());

    const std::string& name = param.name();
    if (type == RTAnyType::kDate32) {
      Day val = Day(std::stoll(input.at(name)));
      return RTAny::from_date32(val);
    } else if (type == RTAnyType::kStringValue) {
      const std::string& val = input.at(name);
      return RTAny::from_string(val);
    } else if (type == RTAnyType::kI32Value) {
      int val = std::stoi(input.at(name));
      return RTAny::from_int32(val);
    } else if (type == RTAnyType::kI64Value) {
      int64_t val = std::stoll(input.at(name));
      return RTAny::from_int64(val);
    } else if (type == RTAnyType::kTimestamp) {
      Date val = Date(std::stoll(input.at(name)));
      return RTAny::from_timestamp(val);
    } else if (type == RTAnyType::kF64Value) {
      double val = std::stod(input.at(name));
      return RTAny::from_double(val);
    }

    LOG(FATAL) << "not support type: " << param.DebugString();
  }
  LOG(FATAL) << "graph data type not expected....";
  return RTAny();
}

static inline int get_proiority(const common::ExprOpr& opr) {
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
    case common::Logical::WITHIN:
    case common::Logical::WITHOUT:
    case common::Logical::REGEX:
      return 2;
    case common::Logical::EQ:
    case common::Logical::NE:
      return 7;
    case common::Logical::GE:
    case common::Logical::GT:
    case common::Logical::LT:
    case common::Logical::LE:
      return 6;
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
  case common::ExprOpr::kDateTimeMinus:
    return 4;
  default:
    return 16;
  }
  return 16;
}

template <typename GraphInterface>
static std::unique_ptr<ExprBase> parse_expression_impl(
    const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type);

template <typename GraphInterface>
static std::unique_ptr<ExprBase> build_expr(
    const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    std::stack<common::ExprOpr>& opr_stack, VarType var_type) {
  while (!opr_stack.empty()) {
    auto opr = opr_stack.top();
    opr_stack.pop();
    switch (opr.item_case()) {
    case common::ExprOpr::kConst: {
      if (opr.const_().item_case() == common::Value::kStr) {
        const std::string& str = opr.const_().str();
        return std::make_unique<ConstExpr>(RTAny::from_string(str));
      }
      return std::make_unique<ConstExpr>(parse_const_value(opr.const_()));
    }
    case common::ExprOpr::kParam: {
      return std::make_unique<ConstExpr>(parse_param(opr.param(), params));
    }
    case common::ExprOpr::kVar: {
      return std::make_unique<VariableExpr>(graph, ctx, opr.var(), var_type);
    }
    case common::ExprOpr::kLogical: {
      if (opr.logical() == common::Logical::WITHIN) {
        auto lhs = opr_stack.top();
        opr_stack.pop();
        auto rhs = opr_stack.top();
        opr_stack.pop();
        assert(lhs.has_var());
        if (rhs.has_const_()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          if (key->type() == RTAnyType::kI64Value) {
            return std::make_unique<WithInExpr<int64_t>>(ctx, std::move(key),
                                                         rhs.const_());
          } else if (key->type() == RTAnyType::kU64Value) {
            return std::make_unique<WithInExpr<uint64_t>>(ctx, std::move(key),
                                                          rhs.const_());
          } else if (key->type() == RTAnyType::kI32Value) {
            return std::make_unique<WithInExpr<int32_t>>(ctx, std::move(key),
                                                         rhs.const_());
          } else if (key->type() == RTAnyType::kStringValue) {
            return std::make_unique<WithInExpr<std::string>>(
                ctx, std::move(key), rhs.const_());
          } else {
            LOG(FATAL) << "not support";
          }
        } else if (rhs.has_var()) {
          auto key =
              std::make_unique<VariableExpr>(graph, ctx, lhs.var(), var_type);
          if (key->type() == RTAnyType::kVertex) {
            auto val =
                std::make_unique<VariableExpr>(graph, ctx, rhs.var(), var_type);
            if (val->type() == RTAnyType::kList) {
              return std::make_unique<VertexWithInListExpr>(ctx, std::move(key),
                                                            std::move(val));
            } else if (val->type() == RTAnyType::kSet) {
              return std::make_unique<VertexWithInSetExpr>(ctx, std::move(key),
                                                           std::move(val));
            } else {
              LOG(FATAL) << "not support";
            }
          }

        } else {
          LOG(FATAL) << "not support" << rhs.DebugString();
        }
      } else if (opr.logical() == common::Logical::NOT ||
                 opr.logical() == common::Logical::ISNULL) {
        auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
        return std::make_unique<UnaryLogicalExpr>(std::move(lhs),
                                                  opr.logical());
      } else {
        auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
        auto rhs = build_expr(graph, ctx, params, opr_stack, var_type);
        return std::make_unique<LogicalExpr>(std::move(lhs), std::move(rhs),
                                             opr.logical());
      }
      break;
    }
    case common::ExprOpr::kArith: {
      auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
      auto rhs = build_expr(graph, ctx, params, opr_stack, var_type);
      return std::make_unique<ArithExpr>(std::move(lhs), std::move(rhs),
                                         opr.arith());
    }
    case common::ExprOpr::kCase: {
      auto op = opr.case_();
      size_t len = op.when_then_expressions_size();
      std::vector<
          std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
          when_then_exprs;
      for (size_t i = 0; i < len; ++i) {
        auto when_expr = op.when_then_expressions(i).when_expression();
        auto then_expr = op.when_then_expressions(i).then_result_expression();
        when_then_exprs.emplace_back(
            parse_expression_impl(graph, ctx, params, when_expr, var_type),
            parse_expression_impl(graph, ctx, params, then_expr, var_type));
      }
      auto else_expr = parse_expression_impl(
          graph, ctx, params, op.else_result_expression(), var_type);
      return std::make_unique<CaseWhenExpr>(std::move(when_then_exprs),
                                            std::move(else_expr));
    }
    case common::ExprOpr::kExtract: {
      auto hs = build_expr(graph, ctx, params, opr_stack, var_type);
      if (hs->type() == RTAnyType::kI64Value) {
        return std::make_unique<ExtractExpr<int64_t>>(std::move(hs),
                                                      opr.extract());
      } else if (hs->type() == RTAnyType::kDate32) {
        return std::make_unique<ExtractExpr<Day>>(std::move(hs), opr.extract());
      } else if (hs->type() == RTAnyType::kTimestamp) {
        return std::make_unique<ExtractExpr<Date>>(std::move(hs),
                                                   opr.extract());
      } else {
        LOG(FATAL) << "not support" << static_cast<int>(hs->type());
      }
    }
    case common::ExprOpr::kVars: {
      auto op = opr.vars();

      if (op.keys_size() == 3) {
        std::array<std::unique_ptr<ExprBase>, 3> exprs;
        for (int i = 0; i < op.keys_size(); ++i) {
          exprs[i] =
              std::make_unique<VariableExpr>(graph, ctx, op.keys(i), var_type);
        }
        return TypedTupleBuilder<3, 3>().build_typed_tuple(ctx,
                                                           std::move(exprs));
      } else if (op.keys_size() == 2) {
        std::array<std::unique_ptr<ExprBase>, 2> exprs;
        for (int i = 0; i < op.keys_size(); ++i) {
          exprs[i] =
              std::make_unique<VariableExpr>(graph, ctx, op.keys(i), var_type);
        }
        return TypedTupleBuilder<2, 2>().build_typed_tuple(ctx,
                                                           std::move(exprs));
      }

      std::vector<std::unique_ptr<ExprBase>> exprs;
      for (int i = 0; i < op.keys_size(); ++i) {
        exprs.push_back(
            std::make_unique<VariableExpr>(graph, ctx, op.keys(i), var_type));
      }
      return std::make_unique<TupleExpr>(ctx, std::move(exprs));
    }
    case common::ExprOpr::kMap: {
      auto op = opr.map();
      std::vector<RTAny> keys_vec;
      std::vector<std::unique_ptr<ExprBase>> exprs;
      for (int i = 0; i < op.key_vals_size(); ++i) {
        auto key = op.key_vals(i).key();
        auto val = op.key_vals(i).val();
        auto any = parse_const_value(key);
        keys_vec.push_back(any);
        exprs.emplace_back(
            std::make_unique<VariableExpr>(graph, ctx, val,
                                           var_type));  // just for parse
      }
      if (exprs.size() > 0) {
        return std::make_unique<MapExpr>(ctx, std::move(keys_vec),
                                         std::move(exprs));
      }
      LOG(FATAL) << "not support" << opr.DebugString();
    }
    case common::ExprOpr::kUdfFunc: {
      auto op = opr.udf_func();
      std::string name = op.name();
      auto expr =
          parse_expression_impl(graph, ctx, params, op.parameters(0), var_type);
      if (name == "gs.function.relationships") {
        return std::make_unique<RelationshipsExpr>(ctx, std::move(expr));
      } else if (name == "gs.function.nodes") {
        return std::make_unique<NodesExpr>(ctx, std::move(expr));
      } else if (name == "gs.function.startNode") {
        return std::make_unique<StartNodeExpr>(std::move(expr));
      } else if (name == "gs.function.endNode") {
        return std::make_unique<EndNodeExpr>(std::move(expr));
      } else if (name == "gs.function.toFloat") {
        return std::make_unique<ToFloatExpr>(std::move(expr));
      } else if (name == "gs.function.concat") {
        auto expr2 = parse_expression_impl(graph, ctx, params, op.parameters(1),
                                           var_type);
        return std::make_unique<StrConcatExpr>(ctx, std::move(expr),
                                               std::move(expr2));
      } else if (name == "gs.function.listSize") {
        return std::make_unique<StrListSizeExpr>(std::move(expr));
      } else {
        LOG(FATAL) << "not support udf" << opr.DebugString();
      }
    }
    case common::ExprOpr::kDateTimeMinus: {
      auto lhs = build_expr(graph, ctx, params, opr_stack, var_type);
      auto rhs = build_expr(graph, ctx, params, opr_stack, var_type);
      return std::make_unique<DateMinusExpr>(std::move(lhs), std::move(rhs));
    }
    default:
      LOG(FATAL) << "not support" << opr.DebugString();
      break;
    }
  }
  return nullptr;
}

template <typename GraphInterface>
static std::unique_ptr<ExprBase> parse_expression_impl(
    const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type) {
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
        assert(!opr_stack.empty());
        opr_stack.pop();
      } else if (brace == common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        opr_stack.emplace(*it);
      }
      break;
    }
    case common::ExprOpr::kConst:
    case common::ExprOpr::kVar:
    case common::ExprOpr::kParam:
    case common::ExprOpr::kVars: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kArith:
    case common::ExprOpr::kLogical:
    case common::ExprOpr::kDateTimeMinus: {
      // unary operator
      if ((*it).logical() == common::Logical::NOT ||
          (*it).logical() == common::Logical::ISNULL) {
        opr_stack2.push(*it);
        break;
      }

      while (!opr_stack.empty() &&
             get_proiority(opr_stack.top()) <= get_proiority(*it)) {
        opr_stack2.push(opr_stack.top());
        opr_stack.pop();
      }
      opr_stack.push(*it);
      break;
    }
    case common::ExprOpr::kExtract: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kCase: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kMap: {
      opr_stack2.push(*it);
      break;
    }
    case common::ExprOpr::kUdfFunc: {
      opr_stack2.push(*it);
      break;
    }

    default: {
      LOG(FATAL) << "not support" << (*it).DebugString();
      break;
    }
    }
  }
  while (!opr_stack.empty()) {
    opr_stack2.push(opr_stack.top());
    opr_stack.pop();
  }
  return build_expr(graph, ctx, params, opr_stack2, var_type);
}

template <typename GraphInterface>
std::unique_ptr<ExprBase> parse_expression(
    const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type) {
  return parse_expression_impl(graph, ctx, params, expr, var_type);
}

template std::unique_ptr<ExprBase> parse_expression<GraphReadInterface>(
    const GraphReadInterface&, const Context&,
    const std::map<std::string, std::string>&, const common::Expression&,
    VarType);
template std::unique_ptr<ExprBase> parse_expression<GraphUpdateInterface>(
    const GraphUpdateInterface&, const Context&,
    const std::map<std::string, std::string>&, const common::Expression&,
    VarType);

}  // namespace runtime

}  // namespace gs
