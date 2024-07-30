#ifndef RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_
#define RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_

#include "flex/proto_generated_gie/expr.pb.h"

#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {

class ExprBase {
 public:
  virtual RTAny eval_path(size_t idx) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx) const = 0;
  virtual RTAnyType type() const = 0;
  virtual RTAny eval_path(size_t idx, int) const { return eval_path(idx); }
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const {
    return eval_vertex(label, v, idx);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, int) const {
    return eval_edge(label, src, dst, data, idx);
  }
  virtual std::shared_ptr<IContextColumnBuilder> builder() const {
    LOG(FATAL) << "not implemented";
    return nullptr;
  }

  virtual ~ExprBase() = default;
};

class ConstTrueExpr : public ExprBase {
 public:
  RTAny eval_path(size_t idx) const override { return RTAny::from_bool(true); }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_bool(true);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_bool(true);
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }
};

class ConstFalseExpr : public ExprBase {
 public:
  RTAny eval_path(size_t idx) const override { return RTAny::from_bool(false); }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_bool(false);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_bool(false);
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }
};

class LabelWithInExpr : public ExprBase {
 public:
  LabelWithInExpr(const ReadTransaction& txn, const Context& ctx,
                  const common::Variable& var, const common::Value& array) {
    CHECK(array.item_case() == common::Value::kI64Array);
    CHECK(var.has_property());
    CHECK(var.property().has_label());
    size_t len = array.i64_array().item_size();
    for (size_t idx = 0; idx < len; ++idx) {
      labels_.push_back(array.i64_array().item(idx));
    }
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not implemented";
    return RTAny::from_bool(false);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    for (auto l : labels_) {
      if (l == label) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "not implemented";
    return RTAny::from_bool(false);
  }
  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  std::vector<int> labels_;
};

class VariableExpr : public ExprBase {
 public:
  VariableExpr(const ReadTransaction& txn, const Context& ctx,
               const common::Variable& pb, VarType var_type);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;
  RTAnyType type() const override;

  RTAny eval_path(size_t idx, int) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, int) const override;

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return var_.builder();
  }

 private:
  Var var_;
};

class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, common::Logical logic);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> expr_;
  common::Logical logic_;
};
class LogicalExpr : public ExprBase {
 public:
  LogicalExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
              common::Logical logic);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  common::Logical logic_;
};

class ExtractExpr : public ExprBase {
 public:
  ExtractExpr(std::unique_ptr<ExprBase>&& expr, const common::Extract& extract);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> expr_;
  const common::Extract extract_;
};
class ArithExpr : public ExprBase {
 public:
  ArithExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
            common::Arithmetic arith);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  common::Arithmetic arith_;
};

class ConstExpr : public ExprBase {
 public:
  ConstExpr(const RTAny& val);
  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  RTAny val_;
  std::string s;
};

class CaseWhenExpr : public ExprBase {
 public:
  CaseWhenExpr(
      std::vector<std::pair<std::unique_ptr<ExprBase>,
                            std::unique_ptr<ExprBase>>>&& when_then_exprs,
      std::unique_ptr<ExprBase>&& else_expr);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
      when_then_exprs_;
  std::unique_ptr<ExprBase> else_expr_;
};

class TupleExpr : public ExprBase {
 public:
  TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs);

  RTAny eval_path(size_t idx) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

std::unique_ptr<ExprBase> parse_expression(
    const ReadTransaction& txn, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_