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

  virtual bool is_optional() const { return false; }

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

template <typename T>
class WithInExpr : public ExprBase {
 public:
  WithInExpr(const ReadTransaction& txn, const Context& ctx,
             std::unique_ptr<ExprBase>&& key, const common::Value& array)
      : key_(std::move(key)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      CHECK(array.item_case() == common::Value::kI64Array);
      size_t len = array.i64_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.i64_array().item(idx));
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      CHECK(array.item_case() == common::Value::kI32Array);
      size_t len = array.i32_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.i32_array().item(idx));
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      CHECK(array.item_case() == common::Value::kStrArray);
      size_t len = array.str_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.str_array().item(idx));
      }
    } else {
      LOG(FATAL) << "not implemented";
    }
  }

  RTAny eval_path(size_t idx) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_path(idx).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(key_->eval_path(idx));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_path(size_t idx, int) const override {
    auto any_val = key_->eval_path(idx, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_vertex(label, v, idx).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(key_->eval_vertex(label, v, idx));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    auto any_val = key_->eval_vertex(label, v, idx, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val =
          std::string(key_->eval_edge(label, src, dst, data, idx).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(
          key_->eval_edge(label, src, dst, data, idx));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
    return RTAny::from_bool(false);
  }
  RTAnyType type() const override { return RTAnyType::kBoolValue; }
  bool is_optional() const override { return key_->is_optional(); }

  std::unique_ptr<ExprBase> key_;
  std::vector<T> container_;
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

  bool is_optional() const override { return var_.is_optional(); }

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

  bool is_optional() const override { return expr_->is_optional(); }

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

  RTAny eval_path(size_t idx, int) const override {
    if (lhs_->eval_path(idx, 0).is_null() ||
        rhs_->eval_path(idx, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    if (lhs_->eval_vertex(label, v, idx, 0).is_null() ||
        rhs_->eval_vertex(label, v, idx, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, int) const override {
    LOG(FATAL) << "not implemented";
  }

  RTAnyType type() const override;

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

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

class MapExpr : public ExprBase {
 public:
  MapExpr(std::vector<std::string>&& keys,
          std::vector<std::unique_ptr<ExprBase>>&& values)
      : keys(std::move(keys)), value_exprs(std::move(values)) {
    CHECK(keys.size() == values.size());
  }

  RTAny eval_path(size_t idx) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx));
    }
    values.emplace_back(ret);
    size_t id = values.size() - 1;
    auto map_impl = MapImpl::make_map_impl(&keys, &values[id]);
    auto map = Map::make_map(map_impl);
    return RTAny::from_map(map);
  }

  RTAny eval_path(size_t idx, int) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, 0));
    }
    values.emplace_back(ret);
    size_t id = values.size() - 1;
    auto map_impl = MapImpl::make_map_impl(&keys, &values[id]);
    auto map = Map::make_map(map_impl);
    return RTAny::from_map(map);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kMap; }

  bool is_optional() const override {
    for (auto& expr : value_exprs) {
      if (expr->is_optional()) {
        return true;
      }
    }
    return false;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    auto builder = std::make_shared<MapValueColumnBuilder>();
    builder->set_keys(keys);
    return std::dynamic_pointer_cast<IContextColumnBuilder>(builder);
  }

 private:
  std::vector<std::string> keys;
  std::vector<std::unique_ptr<ExprBase>> value_exprs;
  mutable std::vector<std::vector<RTAny>> values;
};
std::unique_ptr<ExprBase> parse_expression(
    const ReadTransaction& txn, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_EXPR_IMPL_H_