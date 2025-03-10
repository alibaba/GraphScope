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
  virtual RTAny eval_path(size_t idx, Arena&) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                            Arena&) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, Arena&) const = 0;
  virtual RTAnyType type() const = 0;
  virtual RTAny eval_path(size_t idx, Arena& arena, int) const {
    return eval_path(idx, arena);
  }
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                            int) const {
    return eval_vertex(label, v, idx, arena);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, Arena& arena,
                          int) const {
    return eval_edge(label, src, dst, data, idx, arena);
  }

  virtual bool is_optional() const { return false; }

  virtual RTAnyType elem_type() const { return RTAnyType::kEmpty; }

  virtual ~ExprBase() = default;
};

class VertexWithInSetExpr : public ExprBase {
 public:
  VertexWithInSetExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
                      std::unique_ptr<ExprBase>&& val_set)
      : key_(std::move(key)), val_set_(std::move(val_set)) {
    assert(key_->type() == RTAnyType::kVertex);
    assert(val_set_->type() == RTAnyType::kSet);
  }
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto key = key_->eval_path(idx, arena).as_vertex();
    auto set = val_set_->eval_path(idx, arena).as_set();
    assert(set.impl_ != nullptr);
    auto ptr = dynamic_cast<SetImpl<VertexRecord>*>(set.impl_);
    assert(ptr != nullptr);
    return RTAny::from_bool(ptr->exists(key));
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto key = key_->eval_vertex(label, v, idx, arena).as_vertex();
    auto set = val_set_->eval_vertex(label, v, idx, arena).as_set();
    return RTAny::from_bool(
        dynamic_cast<SetImpl<VertexRecord>*>(set.impl_)->exists(key));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto key = key_->eval_edge(label, src, dst, data, idx, arena).as_vertex();
    auto set = val_set_->eval_edge(label, src, dst, data, idx, arena).as_set();
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
  VertexWithInListExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
                       std::unique_ptr<ExprBase>&& val_list)
      : key_(std::move(key)), val_list_(std::move(val_list)) {
    assert(key_->type() == RTAnyType::kVertex);
    assert(val_list_->type() == RTAnyType::kList);
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto key = key_->eval_path(idx, arena).as_vertex();
    auto list = val_list_->eval_path(idx, arena).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto key = key_->eval_vertex(label, v, idx, arena).as_vertex();
    auto list = val_list_->eval_vertex(label, v, idx, arena).as_list();
    for (size_t i = 0; i < list.size(); i++) {
      if (list.get(i).as_vertex() == key) {
        return RTAny::from_bool(true);
      }
    }
    return RTAny::from_bool(false);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto key = key_->eval_edge(label, src, dst, data, idx, arena).as_vertex();
    auto list =
        val_list_->eval_edge(label, src, dst, data, idx, arena).as_list();
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

#define PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(dst_vector_name, array_name) \
  size_t len = array_name.item_size();                                   \
  for (size_t idx = 0; idx < len; ++idx) {                               \
    dst_vector_name.push_back(array_name.item(idx));                     \
  }

template <typename T>
class WithInExpr : public ExprBase {
 public:
  WithInExpr(const Context& ctx, std::unique_ptr<ExprBase>&& key,
             const common::Value& array)
      : key_(std::move(key)) {
    if constexpr (std::is_same_v<T, int64_t>) {
      if (array.item_case() == common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        // TODO(zhanglei,lexiao): We should support more types here, and if type
        // conversion fails, we should return an error.
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      if (array.item_case() == common::Value::kI64Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int64_t array";
      }
    } else if constexpr (std::is_same_v<T, int32_t>) {
      if (array.item_case() == common::Value::kI32Array) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i32_array());
      } else if constexpr (std::is_same_v<T, int64_t>) {
        PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.i64_array());
      } else {
        LOG(INFO) << "Could not convert array with type " << array.item_case()
                  << " to int32_t array";
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(array.item_case() == common::Value::kStrArray);
      PARSER_COMMON_VALUE_ARRAY_TO_VECTOR(container_, array.str_array());
    } else {
      LOG(FATAL) << "not implemented";
    }
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(key_->eval_path(idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(key_->eval_path(idx, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto any_val = key_->eval_path(idx, arena, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx, arena);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val =
          std::string(key_->eval_vertex(label, v, idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val =
          TypedConverter<T>::to_typed(key_->eval_vertex(label, v, idx, arena));
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    }
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                    int) const override {
    auto any_val = key_->eval_vertex(label, v, idx, arena, 0);
    if (any_val.is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx, arena);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    if constexpr (std::is_same_v<T, std::string>) {
      auto val = std::string(
          key_->eval_edge(label, src, dst, data, idx, arena).as_string());
      return RTAny::from_bool(std::find(container_.begin(), container_.end(),
                                        val) != container_.end());
    } else {
      auto val = TypedConverter<T>::to_typed(
          key_->eval_edge(label, src, dst, data, idx, arena));
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
  template <typename GraphInterface>
  VariableExpr(const GraphInterface& graph, const Context& ctx,
               const common::Variable& pb, VarType var_type)
      : var_(graph, ctx, pb, var_type) {}

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;
  RTAnyType type() const override;

  RTAny eval_path(size_t idx, Arena&, int) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&,
                    int) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&, int) const override;

  bool is_optional() const override { return var_.is_optional(); }

 private:
  Var var_;
};

class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& expr, common::Logical logic);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAny eval_path(size_t idx, Arena&, int) const override;
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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_path(idx, arena, 0).is_null()) {
        flag |= lhs_->eval_path(idx, arena, 0).as_bool();
      }
      if (!rhs_->eval_path(idx, arena, 0).is_null()) {
        flag |= rhs_->eval_path(idx, arena, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }

    if (lhs_->eval_path(idx, arena, 0).is_null() ||
        rhs_->eval_path(idx, arena, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_path(idx, arena);
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena& arena,
                    int) const override {
    if (logic_ == common::Logical::OR) {
      bool flag = false;
      if (!lhs_->eval_vertex(label, v, idx, arena, 0).is_null()) {
        flag |= lhs_->eval_vertex(label, v, idx, arena, 0).as_bool();
      }
      if (!rhs_->eval_vertex(label, v, idx, arena, 0).is_null()) {
        flag |= rhs_->eval_vertex(label, v, idx, arena, 0).as_bool();
      }
      return RTAny::from_bool(flag);
    }
    if (lhs_->eval_vertex(label, v, idx, arena, 0).is_null() ||
        rhs_->eval_vertex(label, v, idx, arena, 0).is_null()) {
      return RTAny::from_bool(false);
    }
    return eval_vertex(label, v, idx, arena);
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&, int) const override {
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

  RTAny eval_path(size_t idx, Arena& arena) const override {
    return RTAny::from_int32(eval_impl(expr_->eval_path(idx, arena)));
  }
  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    return RTAny::from_int32(
        eval_impl(expr_->eval_vertex(label, v, idx, arena)));
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    return RTAny::from_int32(
        eval_impl(expr_->eval_edge(label, src, dst, data, idx, arena)));
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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
};

class ConstExpr : public ExprBase {
 public:
  ConstExpr(const RTAny& val);
  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

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

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

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

class TupleExpr : public ExprBase {
 public:
  TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs);

  RTAny eval_path(size_t idx, Arena&) const override;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override;

  RTAnyType type() const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> exprs_;
};

template <typename... Args>
class TypedTupleExpr : public ExprBase {
 public:
  TypedTupleExpr(std::array<std::unique_ptr<ExprBase>, sizeof...(Args)>&& exprs)
      : exprs_(std::move(exprs)) {
    assert(exprs.size() == sizeof...(Args));
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_path_impl(std::index_sequence<Is...>, size_t idx,
                                     Arena& arena) const {
    return std::make_tuple(
        TypedConverter<Args>::to_typed(exprs_[Is]->eval_path(idx, arena))...);
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto tup = eval_path_impl(std::index_sequence_for<Args...>(), idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_vertex_impl(std::index_sequence<Is...>,
                                       label_t label, vid_t v, size_t idx,
                                       Arena& arena) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_vertex(label, v, idx, arena))...);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto tup = eval_vertex_impl(std::index_sequence_for<Args...>(), label, v,
                                idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  template <std::size_t... Is>
  std::tuple<Args...> eval_edge_impl(std::index_sequence<Is...>,
                                     const LabelTriplet& label, vid_t src,
                                     vid_t dst, const Any& data, size_t idx,
                                     Arena& arena) const {
    return std::make_tuple(TypedConverter<Args>::to_typed(
        exprs_[Is]->eval_edge(label, src, dst, data, idx, arena))...);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto tup = eval_edge_impl(std::index_sequence_for<Args...>(), label, src,
                              dst, data, idx, arena);
    auto t = Tuple::make_tuple_impl(std::move(tup));
    Tuple ret(t.get());
    arena.emplace_back(std::move(t));
    return RTAny::from_tuple(ret);
  }

  RTAnyType type() const override { return RTAnyType::kTuple; }

 private:
  std::array<std::unique_ptr<ExprBase>, sizeof...(Args)> exprs_;
};

class MapExpr : public ExprBase {
 public:
  MapExpr(std::vector<RTAny>&& keys,
          std::vector<std::unique_ptr<ExprBase>>&& values)
      : keys(std::move(keys)), value_exprs(std::move(values)) {
    assert(keys.size() == values.size());
  }

  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, arena));
    }
    auto map_impl = MapImpl::make_map_impl(keys, ret);
    auto map = Map::make_map(map_impl.get());
    arena.emplace_back(std::move(map_impl));
    return RTAny::from_map(map);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    std::vector<RTAny> ret;
    for (size_t i = 0; i < keys.size(); i++) {
      ret.push_back(value_exprs[i]->eval_path(idx, arena, 0));
    }
    auto map_impl = MapImpl::make_map_impl(keys, ret);
    auto map = Map::make_map(map_impl.get());
    arena.emplace_back(std::move(map_impl));
    return RTAny::from_map(map);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "not implemented";
    return RTAny();
  }
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
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

 private:
  std::vector<RTAny> keys;
  std::vector<std::unique_ptr<ExprBase>> value_exprs;
};

class ListExprBase : public ExprBase {
 public:
  ListExprBase() = default;
  RTAnyType type() const override { return RTAnyType::kList; }
};
class RelationshipsExpr : public ListExprBase {
 public:
  RelationshipsExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kPath);
    auto path = args->eval_path(idx, arena).as_path();
    auto rels = path.relationships();
    auto ptr = ListImpl<Relation>::make_list_impl(std::move(rels));
    List rel_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(rel_list);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  RTAnyType elem_type() const override { return RTAnyType::kRelation; }

 private:
  std::unique_ptr<ExprBase> args;
};

class NodesExpr : public ListExprBase {
 public:
  NodesExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kPath);
    auto path = args->eval_path(idx, arena).as_path();
    auto nodes = path.nodes();
    auto ptr = ListImpl<VertexRecord>::make_list_impl(std::move(nodes));
    List node_list(ptr.get());
    arena.emplace_back(std::move(ptr));
    return RTAny::from_list(node_list);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  bool is_optional() const override { return args->is_optional(); }

  RTAnyType elem_type() const override { return RTAnyType::kVertex; }

 private:
  std::unique_ptr<ExprBase> args;
};

class StartNodeExpr : public ExprBase {
 public:
  StartNodeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx, arena).as_relation();
    auto node = path.start_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
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
  RTAny eval_path(size_t idx, Arena& arena) const override {
    assert(args->type() == RTAnyType::kRelation);
    auto path = args->eval_path(idx, arena).as_relation();
    auto node = path.end_node();
    return RTAny::from_vertex(node);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto path = args->eval_path(idx, arena, 0);
    if (path.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, Arena&) const override {
    LOG(FATAL) << "should not be called";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena&) const override {
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
  RTAny eval_path(size_t idx, Arena& arena) const override {
    auto val = args->eval_path(idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto val = args->eval_path(idx, arena, 0);
    if (val.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto val = args->eval_vertex(label, v, idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto val = args->eval_edge(label, src, dst, data, idx, arena);
    return RTAny::from_double(to_double(val));
  }

  RTAnyType type() const override { return RTAnyType::kF64Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  std::unique_ptr<ExprBase> args;
};

class StrConcatExpr : public ExprBase {
 public:
  StrConcatExpr(std::unique_ptr<ExprBase>&& lhs,
                std::unique_ptr<ExprBase>&& rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}
  RTAny eval_path(size_t idx, Arena& arena) const override {
    std::string ret = std::string(lhs->eval_path(idx, arena).as_string()) +
                      ";" + std::string(rhs->eval_path(idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    if (lhs->eval_path(idx, arena, 0).is_null() ||
        rhs->eval_path(idx, arena, 0).is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    std::string ret =
        std::string(lhs->eval_vertex(label, v, idx, arena).as_string()) + ";" +
        std::string(rhs->eval_vertex(label, v, idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    std::string ret =
        std::string(
            lhs->eval_edge(label, src, dst, data, idx, arena).as_string()) +
        ";" +
        std::string(
            rhs->eval_edge(label, src, dst, data, idx, arena).as_string());
    auto ptr = StringImpl::make_string_impl(ret);
    auto sv = ptr->str_view();
    arena.emplace_back(std::move(ptr));

    return RTAny::from_string(sv);
  }

  RTAnyType type() const override { return RTAnyType::kStringValue; }
  bool is_optional() const override {
    return lhs->is_optional() || rhs->is_optional();
  }

 private:
  std::unique_ptr<ExprBase> lhs;
  std::unique_ptr<ExprBase> rhs;
};

class StrListSizeExpr : public ExprBase {
 public:
  StrListSizeExpr(std::unique_ptr<ExprBase>&& args) : args(std::move(args)) {}

  RTAny eval_path(size_t idx, Arena& arena) const override {
    CHECK(args->type() == RTAnyType::kStringValue);
    auto str_list = args->eval_path(idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAny eval_path(size_t idx, Arena& arena, int) const override {
    auto list = args->eval_path(idx, arena, 0);
    if (list.is_null()) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx, arena);
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx,
                    Arena& arena) const override {
    auto str_list = args->eval_vertex(label, v, idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, Arena& arena) const override {
    auto str_list =
        args->eval_edge(label, src, dst, data, idx, arena).as_string();
    return RTAny::from_int32(_size(str_list));
  }

  RTAnyType type() const override { return RTAnyType::kI32Value; }
  bool is_optional() const override { return args->is_optional(); }

 private:
  int32_t _size(const std::string_view& sv) const {
    if (sv.empty()) {
      return 0;
    }
    int64_t ret = 1;
    for (auto c : sv) {
      if (c == ';') {
        ++ret;
      }
    }
    return ret;
  }
  std::unique_ptr<ExprBase> args;
};

template <typename GraphInterface>
std::unique_ptr<ExprBase> parse_expression(
    const GraphInterface& graph, const Context& ctx,
    const std::map<std::string, std::string>& params,
    const common::Expression& expr, VarType var_type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_UTILS_RUNTIME_EXPR_IMPL_H_