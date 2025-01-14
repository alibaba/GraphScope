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

#ifndef RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_
#define RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_

#include "flex/proto_generated_gie/expr.pb.h"

#include "flex/engines/graph_db/runtime/common/rt_any.h"
#include "flex/engines/graph_db/runtime/utils/var.h"

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
  virtual std::vector<std::shared_ptr<ListImplBase>> get_list_impls() const {
    LOG(FATAL) << "not implemented";
  }
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

class VertexWithInSetExpr : public ExprBase {
 public:
  VertexWithInSetExpr(const GraphReadInterface& graph, const Context& ctx,
                      std::unique_ptr<ExprBase>&& key,
                      std::unique_ptr<ExprBase>&& val_set)
      : key_(std::move(key)), val_set_(std::move(val_set)) {
    assert(key_->type() == RTAnyType::kVertex);
    assert(val_set_->type() == RTAnyType::kSet);
  }
  RTAny eval_path(size_t idx) const override {
    auto key = key_->eval_path(idx).as_vertex();
    auto set = val_set_->eval_path(idx).as_set();
    assert(set.impl_ != nullptr);
    auto ptr = dynamic_cast<SetImpl<VertexRecord>*>(set.impl_);
    assert(ptr != nullptr);
    return RTAny::from_bool(ptr->exists(key));
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    auto key = key_->eval_vertex(label, v, idx).as_vertex();
    auto set = val_set_->eval_vertex(label, v, idx).as_set();
    return RTAny::from_bool(
        dynamic_cast<SetImpl<VertexRecord>*>(set.impl_)->exists(key));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    auto key = key_->eval_edge(label, src, dst, data, idx).as_vertex();
    auto set = val_set_->eval_edge(label, src, dst, data, idx).as_set();
    return RTAny::from_bool(
        dynamic_cast<SetImpl<VertexRecord>*>(set.impl_)->exists(key));
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  bool is_optional() const override { return key_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_set_;
};
class VertexWithInListExpr : public ExprBase {
 public:
  VertexWithInListExpr(const GraphReadInterface& graph, const Context& ctx,
                       std::unique_ptr<ExprBase>&& key,
                       std::unique_ptr<ExprBase>&& val_list)
      : key_(std::move(key)), val_list_(std::move(val_list)) {
    assert(key_->type() == RTAnyType::kVertex);
    assert(val_list_->type() == RTAnyType::kList);
  }

  RTAny eval_path(size_t idx) const override {
    auto key = key_->eval_path(idx).as_vertex();
    auto list = val_list_->eval_path(idx).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    auto key = key_->eval_vertex(label, v, idx).as_vertex();
    auto list = val_list_->eval_vertex(label, v, idx).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    auto key = key_->eval_edge(label, src, dst, data, idx).as_vertex();
    auto list = val_list_->eval_edge(label, src, dst, data, idx).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAnyType type() const override { return RTAnyType::kBoolValue; }

  bool is_optional() const override { return key_->is_optional(); }
  std::unique_ptr<ExprBase> key_;
  std::unique_ptr<ExprBase> val_list_;
};

template <typename T>
class WithInExpr : public ExprBase {
 public:
  WithInExpr(const GraphReadInterface& graph, const Context& ctx,
             std::unique_ptr<ExprBase>&& key, const common::Value& array)
      : key_(std::move(key)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      assert(array.item_case() == common::Value::kI64Array);
      size_t len = array.i64_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.i64_array().item(idx));
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      assert(array.item_case() == common::Value::kI32Array);
      size_t len = array.i32_array().item_size();
      for (size_t idx = 0; idx < len; ++idx) {
        container_.push_back(array.i32_array().item(idx));
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(array.item_case() == common::Value::kStrArray);
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
  VariableExpr(const GraphReadInterface& graph, const Context& ctx,
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

  RTAny eval_path(size_t idx, int) const override;
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
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_path(idx, 0).is_null()) {
        flag |= lhs_->eval_path(idx, 0).as_bool();
      }
      if (!rhs_->eval_path(idx, 0).is_null()) {
        flag |= rhs_->eval_path(idx, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }

    if (lhs_->eval_path(idx, 0).is_null() ||
        rhs_->eval_path(idx, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_vertex(label, v, idx, 0).is_null()) {
        flag |= lhs_->eval_vertex(label, v, idx, 0).as_bool();
      }
      if (!rhs_->eval_vertex(label, v, idx, 0).is_null()) {
        flag |= rhs_->eval_vertex(label, v, idx, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }
    if (lhs_->eval_vertex(label, v, idx, 0).is_null() ||
        rhs_->eval_vertex(label, v, idx, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, int) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }

  RTAnyType type() const override;

  bool is_optional() const override {
    return lhs_->is_optional() || rhs_->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  std::function<bool(RTAny, RTAny)> op_;
  common::Logical logic_;
};

int32_t extract_time_from_milli_second(int64_t ms, common::Extract extract);

template <typename T>
class ExtractExpr : public ExprBase {
 public:
  ExtractExpr(std::unique_ptr<ExprBase>&& expr, const common::Extract& extract)
      : expr_(std::move(expr)), extract_(extract) {}
  int32_t eval_impl(const RTAny& val) const {
    if constexpr (std::is_same_v<T, int64_t>) {
      return extract_time_from_milli_second(val.as_int64(), extract_);
    } else if constexpr (std::is_same_v<T, Date>) {
      return extract_time_from_milli_second(val.as_timestamp().milli_second,
                                            extract_);

    } else if constexpr (std::is_same_v<T, Day>) {
      if (extract_.interval() == common::Extract::DAY) {
        return val.as_date32().day();
      } else if (extract_.interval() == common::Extract::MONTH) {
        return val.as_date32().month();
      } else if (extract_.interval() == common::Extract::YEAR) {
        return val.as_date32().year();
      }
    }
    LOG(FATAL) << "not support" << extract_.DebugString();
    return 0;
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_int32(eval_impl(expr_->eval_path(idx)));
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_int32(eval_impl(expr_->eval_vertex(label, v, idx)));
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_int32(
        eval_impl(expr_->eval_edge(label, src, dst, data, idx)));
  }

  RTAnyType type() const override { return RTAnyType::kI32Value; }

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
  std::function<RTAny(RTAny, RTAny)> op_;
  common::Arithmetic arith_;
};

class DateMinusExpr : public ExprBase {
 public:
  DateMinusExpr(std::unique_ptr<ExprBase>&& lhs,
                std::unique_ptr<ExprBase>&& rhs);

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

  bool is_optional() const override { return val_.is_null(); }

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

  bool is_optional() const override {
    for (auto& expr_pair : when_then_exprs_) {
      if (expr_pair.first->is_optional() || expr_pair.second->is_optional()) {
        return true;
      }
    }
    return else_expr_->is_optional();
  }

 private:
  std::vector<std::pair<std::unique_ptr<ExprBase>, std::unique_ptr<ExprBase>>>
      when_then_exprs_;
  std::unique_ptr<ExprBase> else_expr_;
};
/**
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
};*/

template <typename... Args>
class TypedTupleExpr : public ExprBase {
 public:
  TypedTupleExpr(std::array<std::unique_ptr<ExprBase>, sizeof...(Args)>&& exprs)
      : exprs_(std::move(exprs)) {
    assert(exprs.size() == sizeof...(Args));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_path_impl(std::index_sequence<Is...>,
                                     size_t idx) const {
    return std::make_tuple(
        TypedConverter<Args>::to_typed(exprs_[Is]->eval_path(idx))...);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_tuple(
        eval_path_impl(std::index_sequence_for<Args...>(), idx));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_vertex_impl(std::index_sequence<Is...>,
                                       label_t label, vid_t v,
                                       size_t idx) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_vertex(label, v, idx))...);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_tuple(
        eval_vertex_impl(std::index_sequence_for<Args...>(), label, v, idx));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_edge_impl(std::index_sequence<Is...>,
                                     const LabelTriplet& label, vid_t src,
                                     vid_t dst, const Any& data,
                                     size_t idx) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_edge(label, src, dst, data, idx))...);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_tuple(eval_edge_impl(std::index_sequence_for<Args...>(),
                                            label, src, dst, data, idx));
  }

  RTAnyType type() const override { return RTAnyType::kTuple; }

 private:
  std::array<std::unique_ptr<ExprBase>, sizeof...(Args)> exprs_;
};

class MapExpr : public ExprBase {
 public:
  MapExpr(std::vector<std::string>&& keys,
          std::vector<std::unique_ptr<ExprBase>>&& values)
      : keys(std::move(keys)), value_exprs(std::move(values)) {
    assert(keys.size() == values.size());
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

class ListExprBase : public ExprBase {
 public:
  ListExprBase() = default;
  RTAnyType type() const override { return RTAnyType::kList; }
};
class RelationshipsExpr : public ListExprBase {
 public:
  RelationshipsExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx) const override {
    assert(args->type() == RTAnyType::kPath);
    auto path = args->eval_path(idx).as_path();
    auto rels = path.relationships();
    auto ptr = ListImpl<Relation>::make_list_impl(std::move(rels));
    impls.push_back(ptr);
    return RTAny::from_list(List::make_list(ptr));
  }

  RTAny eval_path(size_t idx, int) const override {
    auto path = args->eval_path(idx, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::make_shared<ListValueColumnBuilder<Relation>>();
  }
  std::vector<std::shared_ptr<ListImplBase>> get_list_impls() const override {
    return impls;
  }

 private:
  std::unique_ptr<ExprBase> args;
  mutable std::vector<std::shared_ptr<ListImplBase>> impls;
};

class NodesExpr : public ListExprBase {
 public:
  NodesExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx) const override {
    assert(args->type() == RTAnyType::kPath);
    auto path = args->eval_path(idx).as_path();
    auto nodes = path.nodes();
    auto ptr = ListImpl<VertexRecord>::make_list_impl(std::move(nodes));
    impls.push_back(ptr);
    return RTAny::from_list(List::make_list(ptr));
  }

  RTAny eval_path(size_t idx, int) const override {
    auto path = args->eval_path(idx, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::make_shared<ListValueColumnBuilder<VertexRecord>>();
  }

  std::vector<std::shared_ptr<ListImplBase>> get_list_impls() const override {
    return impls;
  }

 private:
  std::unique_ptr<ExprBase> args;
  mutable std::vector<std::shared_ptr<ListImplBase>> impls;
};

class StartNodeExpr : public ExprBase {
 public:
  StartNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx).as_relation();
    auto node = path.start_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, int) const override {
    auto path = args->eval_path(idx, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }

  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class EndNodeExpr : public ExprBase {
 public:
  EndNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx).as_relation();
    auto node = path.end_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, int) const override {
    auto path = args->eval_path(idx, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class ToFloatExpr : public ExprBase {
 public:
  static double to_double(const RTAny& val) {
    if (val.type() == RTAnyType::kI64Value) {
      return static_cast<double>(val.as_int64());
    } else if (val.type() == RTAnyType::kI32Value) {
      return static_cast<double>(val.as_int32());
    } else if (val.type() == RTAnyType::kF64Value) {
      return val.as_double();
    } else {
      LOG(FATAL) << "invalid type";
    }
  }

  ToFloatExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx) const override {
    auto val = args->eval_path(idx);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_path(size_t idx, int) const override {
    auto val = args->eval_path(idx, 0);
    if (val.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    auto val = args->eval_vertex(label, v, idx);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    auto val = args->eval_edge(label, src, dst, data, idx);
    return RTAny::from_double(to_double(val));
  }

  RTAnyType type() const override { return RTAnyType::kF64Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

std::unique_ptr<ExprBase> parse_expression(
    const GraphReadInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_