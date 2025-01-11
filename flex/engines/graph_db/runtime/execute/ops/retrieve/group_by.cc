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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/group_by.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/group_by.h"

namespace gs {
namespace runtime {
namespace ops {

static AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v) {
  if (v == physical::GroupBy_AggFunc::SUM) {
    return AggrKind::kSum;
  } else if (v == physical::GroupBy_AggFunc::MIN) {
    return AggrKind::kMin;
  } else if (v == physical::GroupBy_AggFunc::MAX) {
    return AggrKind::kMax;
  } else if (v == physical::GroupBy_AggFunc::COUNT) {
    return AggrKind::kCount;
  } else if (v == physical::GroupBy_AggFunc::COUNT_DISTINCT) {
    return AggrKind::kCountDistinct;
  } else if (v == physical::GroupBy_AggFunc::TO_SET) {
    return AggrKind::kToSet;
  } else if (v == physical::GroupBy_AggFunc::FIRST) {
    return AggrKind::kFirst;
  } else if (v == physical::GroupBy_AggFunc::TO_LIST) {
    return AggrKind::kToList;
  } else if (v == physical::GroupBy_AggFunc::AVG) {
    return AggrKind::kAvg;
  } else {
    LOG(FATAL) << "unsupport" << static_cast<int>(v);
    return AggrKind::kSum;
  }
}

class GroupByOpr : public IReadOperator {
 public:
  GroupByOpr(std::function<std::unique_ptr<KeyBase>(const GraphReadInterface&,
                                                    const Context&)>&& key_fun,
             std::vector<std::function<std::unique_ptr<ReducerBase>(
                 const GraphReadInterface&, const Context&)>>&& aggrs)
      : key_fun_(std::move(key_fun)), aggrs_(std::move(aggrs)) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    auto key = key_fun_(graph, ctx);
    std::vector<std::unique_ptr<ReducerBase>> reducers;
    for (auto& aggr : aggrs_) {
      reducers.push_back(aggr(graph, ctx));
    }
    return GroupBy::group_by(graph, std::move(ctx), std::move(key),
                             std::move(reducers));
  }

 private:
  std::function<std::unique_ptr<KeyBase>(const GraphReadInterface&,
                                         const Context&)>
      key_fun_;
  std::vector<std::function<std::unique_ptr<ReducerBase>(
      const GraphReadInterface&, const Context&)>>
      aggrs_;
};

struct SLVertexWrapper {
  using V = vid_t;
  SLVertexWrapper(const SLVertexColumn& column) : column(column) {}
  V operator()(size_t idx) const { return column.vertices()[idx]; }
  const SLVertexColumn& column;
};

struct SLVertexWrapperBeta {
  using V = VertexRecord;
  SLVertexWrapperBeta(const SLVertexColumn& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_vertex(idx); }
  const SLVertexColumn& column;
};

template <typename VERTEX_COL>
struct MLVertexWrapper {
  using V = VertexRecord;
  MLVertexWrapper(const VERTEX_COL& vertex) : vertex(vertex) {}
  V operator()(size_t idx) const { return vertex.get_vertex(idx); }
  const VERTEX_COL& vertex;
};
template <typename T>
struct ValueWrapper {
  using V = T;
  ValueWrapper(const ValueColumn<T>& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_value(idx); }
  const ValueColumn<T>& column;
};

struct ColumnWrapper {
  using V = RTAny;
  ColumnWrapper(const IContextColumn& column) : column(column) {}
  V operator()(size_t idx) const { return column.get_elem(idx); }
  const IContextColumn& column;
};

template <typename... EXPR>
struct KeyExpr {
  std::tuple<EXPR...> exprs;
  KeyExpr(std::tuple<EXPR...>&& exprs) : exprs(std::move(exprs)) {}
  using V = std::tuple<typename EXPR::V...>;
  V operator()(size_t idx) const {
    return std::apply(
        [idx](auto&&... expr) { return std::make_tuple(expr(idx)...); }, exprs);
  }
};

template <size_t I, typename... EXPR>
struct _KeyBuilder {
  static std::unique_ptr<KeyBase> make_sp_key(
      const Context& ctx, const std::vector<std::pair<int, int>>& tag_alias,
      std::tuple<EXPR...>&& exprs) {
    if constexpr (I == 0) {
      KeyExpr<EXPR...> key(std::move(exprs));
      return std::make_unique<Key<decltype(key)>>(std::move(key), tag_alias);
    } else {
      auto [head, tail] = tag_alias[I - 1];
      auto col = ctx.get(head);
      if (col->is_optional()) {
        return nullptr;
      }
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
          SLVertexWrapper wrapper(
              *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, SLVertexWrapper, EXPR...>::make_sp_key(
              ctx, tag_alias, std::move(new_exprs));
        } else if (vertex_col->vertex_column_type() ==
                   VertexColumnType::kMultiple) {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
          MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(
              *typed_vertex_col);
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1,
                             MLVertexWrapper<decltype(*typed_vertex_col)>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));
        } else {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
          MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(
              *typed_vertex_col);
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1,
                             MLVertexWrapper<decltype(*typed_vertex_col)>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));
        }

      } else if (col->column_type() == ContextColumnType::kValue) {
        if (col->elem_type() == RTAnyType::kI64Value) {
          ValueWrapper<int64_t> wrapper(
              *dynamic_cast<const ValueColumn<int64_t>*>(col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, ValueWrapper<int64_t>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));
        } else if (col->elem_type() == RTAnyType::kI32Value) {
          ValueWrapper<int32_t> wrapper(
              *dynamic_cast<const ValueColumn<int32_t>*>(col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, ValueWrapper<int32_t>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));
        } else if (col->elem_type() == RTAnyType::kStringValue) {
          ValueWrapper<std::string_view> wrapper(
              *dynamic_cast<const ValueColumn<std::string_view>*>(col.get()));
          auto new_exprs = std::tuple_cat(std::make_tuple(std::move(wrapper)),
                                          std::move(exprs));
          return _KeyBuilder<I - 1, ValueWrapper<std::string_view>,
                             EXPR...>::make_sp_key(ctx, tag_alias,
                                                   std::move(new_exprs));

        } else {
          return nullptr;
        }
      }
    }
    return nullptr;
  }
};
template <size_t I>
struct KeyBuilder {
  static std::unique_ptr<KeyBase> make_sp_key(
      const Context& ctx, const std::vector<std::pair<int, int>>& tag_alias) {
    if (I != tag_alias.size()) {
      return nullptr;
    }
    auto col = ctx.get(tag_alias[I - 1].first);
    if (col->column_type() == ContextColumnType::kVertex) {
      auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
      if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
        SLVertexWrapper wrapper(
            *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
        auto new_exprs = std::make_tuple<SLVertexWrapper>(std::move(wrapper));
        return _KeyBuilder<I - 1, SLVertexWrapper>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      } else if (vertex_col->vertex_column_type() ==
                 VertexColumnType::kMultiple) {
        auto typed_vertex_col =
            std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
        MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(*typed_vertex_col);
        auto new_exprs =
            std::make_tuple<MLVertexWrapper<decltype(*typed_vertex_col)>>(
                std::move(wrapper));
        return _KeyBuilder<I - 1,
                           MLVertexWrapper<decltype(*typed_vertex_col)>>::
            make_sp_key(ctx, tag_alias, std::move(new_exprs));
      } else {
        auto typed_vertex_col =
            std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
        MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(*typed_vertex_col);
        auto new_exprs =
            std::make_tuple<MLVertexWrapper<decltype(*typed_vertex_col)>>(
                std::move(wrapper));
        return _KeyBuilder<I - 1,
                           MLVertexWrapper<decltype(*typed_vertex_col)>>::
            make_sp_key(ctx, tag_alias, std::move(new_exprs));
      }
    } else if (col->column_type() == ContextColumnType::kValue) {
      if (col->elem_type() == RTAnyType::kI64Value) {
        ValueWrapper<int64_t> wrapper(
            *dynamic_cast<const ValueColumn<int64_t>*>(col.get()));
        auto new_exprs =
            std::make_tuple<ValueWrapper<int64_t>>(std::move(wrapper));
        return _KeyBuilder<I - 1, ValueWrapper<int64_t>>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      } else if (col->elem_type() == RTAnyType::kI32Value) {
        ValueWrapper<int32_t> wrapper(
            *dynamic_cast<const ValueColumn<int32_t>*>(col.get()));
        auto new_exprs =
            std::make_tuple<ValueWrapper<int32_t>>(std::move(wrapper));
        return _KeyBuilder<I - 1, ValueWrapper<int32_t>>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      } else if (col->elem_type() == RTAnyType::kStringValue) {
        ValueWrapper<std::string_view> wrapper(
            *dynamic_cast<const ValueColumn<std::string_view>*>(col.get()));
        auto new_exprs =
            std::make_tuple<ValueWrapper<std::string_view>>(std::move(wrapper));
        return _KeyBuilder<I - 1, ValueWrapper<std::string_view>>::make_sp_key(
            ctx, tag_alias, std::move(new_exprs));
      } else {
        return nullptr;
      }
    }
    return nullptr;
  }
};

struct VarWrapper {
  RTAny operator()(size_t idx) const { return vars.get(idx); }
  VarWrapper(Var&& vars) : vars(std::move(vars)) {}
  Var vars;
};

template <typename T>
struct TypedVarWrapper {
  using V = T;
  T operator()(size_t idx) const {
    auto v = vars.get(idx);
    return TypedConverter<T>::to_typed(v);
  }
  TypedVarWrapper(Var&& vars) : vars(std::move(vars)) {}
  Var vars;
};

template <typename T>
struct OptionalTypedVarWrapper {
  using V = T;
  std::optional<T> operator()(size_t idx) const {
    auto v = vars.get(idx, 0);
    if (v.is_null()) {
      return std::nullopt;
    }
    return TypedConverter<T>::to_typed(v);
  }
  OptionalTypedVarWrapper(Var&& vars) : vars(std::move(vars)) {}
  Var vars;
};

template <typename EXPR, bool IS_OPTIONAL, typename Enable = void>
struct SumReducer;

template <typename EXPR, bool IS_OPTIONAL>
struct SumReducer<
    EXPR, IS_OPTIONAL,
    std::enable_if_t<std::is_arithmetic<typename EXPR::V>::value>> {
  EXPR expr;
  using V = typename EXPR::V;
  SumReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& sum) const {
    if constexpr (!IS_OPTIONAL) {
      sum = expr(group[0]);
      for (size_t i = 1; i < group.size(); ++i) {
        sum += expr(group[i]);
      }
      return true;
    } else {
      for (size_t i = 0; i < group.size(); ++i) {
        auto v = expr(group[i]);
        if (v.has_value()) {
          sum = v.value();
          i++;
          for (; i < group.size(); ++i) {
            auto v = expr(group[i]);
            if (v.has_value()) {
              sum += v.value();
            }
          }
          return true;
        }
      }
      return false;
    }
  }
};

template <typename T, typename = void>
struct is_hashable : std::false_type {};

template <typename T>
struct is_hashable<
    T, std::void_t<decltype(std::declval<std::hash<T>>()(std::declval<T>()))>>
    : std::true_type {};

template <typename EXPR, bool IS_OPTIONAL>
struct CountDistinctReducer {
  EXPR expr;
  using V = int64_t;
  using T = typename EXPR::V;

  CountDistinctReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (is_hashable<T>::value) {
      if constexpr (!IS_OPTIONAL) {
        std::unordered_set<T> set;
        for (auto idx : group) {
          set.insert(expr(idx));
        }
        val = set.size();
        return true;
      } else {
        std::unordered_set<T> set;
        for (auto idx : group) {
          auto v = expr(idx);
          if (v.has_value()) {
            set.insert(v.value());
          }
        }
        val = set.size();
        return true;
      }
    } else {
      if constexpr (!IS_OPTIONAL) {
        std::set<T> set;
        for (auto idx : group) {
          set.insert(expr(idx));
        }
        val = set.size();
        return true;
      } else {
        std::set<T> set;
        for (auto idx : group) {
          auto v = expr(idx);
          if (v.has_value()) {
            set.insert(v.value());
          }
        }
        val = set.size();
        return true;
      }
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct CountReducer {
  EXPR expr;
  using V = int64_t;

  CountReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = group.size();
      return true;
    } else {
      val = 0;
      for (auto idx : group) {
        if (expr(idx).has_value()) {
          val += 1;
        }
      }
      return true;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct MinReducer {
  EXPR expr;

  using V = typename EXPR::V;
  MinReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = expr(group[0]);
      for (size_t i = 1; i < group.size(); ++i) {
        val = std::min(val, expr(group[i]));
      }
      return true;
    } else {
      for (size_t i = 0; i < group.size(); ++i) {
        auto v = expr(group[i]);
        if (v.has_value()) {
          val = v.value();
          ++i;
          for (; i < group.size(); ++i) {
            auto v = expr(group[i]);
            if (v.has_value()) {
              val = std::min(val, v.value());
            }
          }
          return true;
        }
      }
      return false;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct MaxReducer {
  EXPR expr;

  using V = typename EXPR::V;
  MaxReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = expr(group[0]);
      for (size_t i = 1; i < group.size(); ++i) {
        val = std::max(val, expr(group[i]));
      }
      return true;
    } else {
      for (size_t i = 0; i < group.size(); ++i) {
        auto v = expr(group[i]);
        if (v.has_value()) {
          val = v.value();
          ++i;
          for (; i < group.size(); ++i) {
            auto v = expr(group[i]);
            if (v.has_value()) {
              val = std::max(val, v.value());
            }
          }
          return true;
        }
      }
      return false;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct FirstReducer {
  EXPR expr;
  using V = typename EXPR::V;
  FirstReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    if constexpr (!IS_OPTIONAL) {
      val = expr(group[0]);
      return true;
    } else {
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          val = v.value();
          return true;
        }
      }
      return false;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct ToSetReducer {
  EXPR expr;
  using V = std::set<typename EXPR::V>;
  ToSetReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& val) const {
    val.clear();
    if constexpr (!IS_OPTIONAL) {
      for (auto idx : group) {
        val.insert(expr(idx));
      }
      return true;
    } else {
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          val.insert(v.value());
        }
      }
      return true;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL>
struct ToListReducer {
  EXPR expr;

  using V = std::vector<typename EXPR::V>;
  ToListReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& list) const {
    list.clear();
    if constexpr (!IS_OPTIONAL) {
      for (auto idx : group) {
        list.push_back(expr(idx));
      }
      return true;
    } else {
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          list.push_back(v.value());
        }
      }
      return true;
    }
  }
};

template <typename EXPR, bool IS_OPTIONAL, typename Enable = void>
struct AvgReducer;

template <typename EXPR, bool IS_OPTIONAL>
struct AvgReducer<
    EXPR, IS_OPTIONAL,
    std::enable_if_t<std::is_arithmetic<typename EXPR::V>::value>> {
  EXPR expr;
  using V = typename EXPR::V;
  AvgReducer(EXPR&& expr) : expr(std::move(expr)) {}
  bool operator()(const std::vector<size_t>& group, V& avg) const {
    if constexpr (!IS_OPTIONAL) {
      avg = 0;
      for (auto idx : group) {
        avg += expr(idx);
      }
      avg = avg / group.size();
      return true;
    } else {
      avg = 0;
      size_t count = 0;
      for (auto idx : group) {
        auto v = expr(idx);
        if (v.has_value()) {
          avg += v.value();
          count += 1;
        }
      }
      if (count == 0) {
        return false;
      }
      avg = avg / count;
      return true;
    }
  }
};

template <typename T>
struct SetCollector {
  void init(size_t size) { builder.reserve(size); }
  void collect(std::set<T>&& val) {
    auto set = builder.allocate_set();
    auto set_impl = dynamic_cast<SetImpl<T>*>(set.impl_);
    for (auto& v : val) {
      set_impl->insert(v);
    }
    builder.push_back_opt(set);
  }
  auto get() { return builder.finish(); }
  SetValueColumnBuilder<T> builder;
};

template <>
struct SetCollector<std::string_view> {
  void init(size_t size) { builder.reserve(size); }
  void collect(std::set<std::string_view>&& val) {
    std::set<std::string> set;
    for (auto& s : val) {
      set.insert(std::string(s));
    }

    builder.push_back_opt(std::move(set));
  }
  auto get() { return builder.finish(); }
  ValueColumnBuilder<std::set<std::string>> builder;
};

template <typename T>
struct ValueCollector {
  void init(size_t size) { builder.reserve(size); }
  void collect(T&& val) { builder.push_back_opt(std::move(val)); }
  auto get() { return builder.finish(); }
  ValueColumnBuilder<T> builder;
};

struct VertexCollector {
  void init(size_t size) { builder.reserve(size); }
  void collect(VertexRecord&& val) { builder.push_back_vertex(std::move(val)); }
  auto get() { return builder.finish(); }
  MLVertexColumnBuilder builder;
};

template <typename T>
struct ListCollector {
  void init(size_t size) { builder.reserve(size); }
  void collect(std::vector<T>&& val) {
    auto impl = ListImpl<T>::make_list_impl(std::move(val));
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  auto get() {
    builder.set_list_impls(impls);
    return builder.finish();
  }
  std::vector<std::shared_ptr<ListImplBase>> impls;
  ListValueColumnBuilder<T> builder;
};

template <>
struct ListCollector<std::string_view> {
  void init(size_t size) { builder.reserve(size); }
  void collect(std::vector<std::string_view>&& val) {
    std::vector<std::string> vec;
    vec.reserve(val.size());
    for (auto& s : val) {
      vec.push_back(std::string(s));
    }
    auto impl = ListImpl<std::string_view>::make_list_impl(std::move(vec));
    auto list = List::make_list(impl);
    impls.emplace_back(impl);
    builder.push_back_opt(list);
  }
  auto get() {
    builder.set_list_impls(impls);
    return builder.finish();
  }
  std::vector<std::shared_ptr<ListImplBase>> impls;
  ListValueColumnBuilder<std::string> builder;
};

template <typename EXPR, bool IS_OPTIONAL>
std::unique_ptr<ReducerBase> _make_reducer(EXPR&& expr, AggrKind kind,
                                           int alias) {
  switch (kind) {
  case AggrKind::kSum: {
    if constexpr (std::is_arithmetic<typename EXPR::V>::value) {
      SumReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
      ValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "unsupport" << static_cast<int>(kind);
      return nullptr;
    }
  }
  case AggrKind::kCountDistinct: {
    CountDistinctReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    ValueCollector<int64_t> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kCount: {
    CountReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    ValueCollector<int64_t> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kMin: {
    MinReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    ValueCollector<typename EXPR::V> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kMax: {
    MaxReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    ValueCollector<typename EXPR::V> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kFirst: {
    FirstReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    if constexpr (std::is_same<typename EXPR::V, VertexRecord>::value) {
      VertexCollector collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      ValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    }
  }
  case AggrKind::kToSet: {
    ToSetReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    SetCollector<typename EXPR::V> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kToList: {
    ToListReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
    ListCollector<typename EXPR::V> collector;
    return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
        std::move(r), std::move(collector), alias);
  }
  case AggrKind::kAvg: {
    if constexpr (std::is_arithmetic<typename EXPR::V>::value) {
      AvgReducer<EXPR, IS_OPTIONAL> r(std::move(expr));
      ValueCollector<typename EXPR::V> collector;
      return std::make_unique<Reducer<decltype(r), decltype(collector)>>(
          std::move(r), std::move(collector), alias);
    } else {
      LOG(FATAL) << "unsupport" << static_cast<int>(kind);
      return nullptr;
    }
  }
  default:
    LOG(FATAL) << "unsupport" << static_cast<int>(kind);
    return nullptr;
  };
}

template <typename T>
std::unique_ptr<ReducerBase> make_reducer(const GraphReadInterface& graph,
                                          const Context& ctx, Var&& var,
                                          AggrKind kind, int alias) {
  if (var.is_optional()) {
    OptionalTypedVarWrapper<T> wrapper(std::move(var));
    return _make_reducer<decltype(wrapper), true>(std::move(wrapper), kind,
                                                  alias);
  } else {
    TypedVarWrapper<T> wrapper(std::move(var));
    return _make_reducer<decltype(wrapper), false>(std::move(wrapper), kind,
                                                   alias);
  }
}
std::unique_ptr<ReducerBase> make_reducer(const GraphReadInterface& graph,
                                          const Context& ctx,
                                          const common::Variable& var,
                                          AggrKind kind, int alias) {
  if (!var.has_property() && var.has_tag()) {
    int tag = var.has_tag() ? var.tag().id() : -1;
    auto col = ctx.get(tag);
    if (!col->is_optional()) {
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
          SLVertexWrapperBeta wrapper(
              *dynamic_cast<const SLVertexColumn*>(vertex_col.get()));
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        } else if (vertex_col->vertex_column_type() ==
                   VertexColumnType::kMultiple) {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
          MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(
              *typed_vertex_col);
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        } else {
          auto typed_vertex_col =
              std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
          MLVertexWrapper<decltype(*typed_vertex_col)> wrapper(
              *typed_vertex_col);
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        }
      } else if (col->column_type() == ContextColumnType::kValue) {
        if (col->elem_type() == RTAnyType::kI64Value) {
          ValueWrapper<int64_t> wrapper(
              *dynamic_cast<const ValueColumn<int64_t>*>(col.get()));
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        } else if (col->elem_type() == RTAnyType::kI32Value) {
          ValueWrapper<int32_t> wrapper(
              *dynamic_cast<const ValueColumn<int32_t>*>(col.get()));
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        } else if (col->elem_type() == RTAnyType::kStringValue) {
          ValueWrapper<std::string_view> wrapper(
              *dynamic_cast<const ValueColumn<std::string_view>*>(col.get()));
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        } else if (col->elem_type() == RTAnyType::kTimestamp) {
          ValueWrapper<Date> wrapper(
              *dynamic_cast<const ValueColumn<Date>*>(col.get()));
          return _make_reducer<decltype(wrapper), false>(std::move(wrapper),
                                                         kind, alias);
        }
      }
    }
  }
  Var var_(graph, ctx, var, VarType::kPathVar);
  if (var_.type() == RTAnyType::kI32Value) {
    return make_reducer<int32_t>(graph, ctx, std::move(var_), kind, alias);
  } else if (var_.type() == RTAnyType::kI64Value) {
    return make_reducer<int64_t>(graph, ctx, std::move(var_), kind, alias);
  } else if (var_.type() == RTAnyType::kF64Value) {
    return make_reducer<double>(graph, ctx, std::move(var_), kind, alias);
  } else if (var_.type() == RTAnyType::kStringValue) {
    return make_reducer<std::string_view>(graph, ctx, std::move(var_), kind,
                                          alias);
  } else if (var_.type() == RTAnyType::kTimestamp) {
    return make_reducer<Date>(graph, ctx, std::move(var_), kind, alias);
  } else if (var_.type() == RTAnyType::kVertex) {
    return make_reducer<VertexRecord>(graph, ctx, std::move(var_), kind, alias);
  } else if (var_.type() == RTAnyType::kTuple) {
    return make_reducer<Tuple>(graph, ctx, std::move(var_), kind, alias);
  } else {
    LOG(FATAL) << "unsupport" << static_cast<int>(var_.type());
    return nullptr;
  }
}
std::pair<std::unique_ptr<IReadOperator>, ContextMeta> GroupByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int mappings_num = plan.plan(op_idx).opr().group_by().mappings_size();
  int func_num = plan.plan(op_idx).opr().group_by().functions_size();
  ContextMeta meta;
  for (int i = 0; i < mappings_num; ++i) {
    auto& key = plan.plan(op_idx).opr().group_by().mappings(i);
    if (key.has_alias()) {
      meta.set(key.alias().value());
    } else {
      meta.set(-1);
    }
  }
  for (int i = 0; i < func_num; ++i) {
    auto& func = plan.plan(op_idx).opr().group_by().functions(i);
    if (func.has_alias()) {
      meta.set(func.alias().value());
    } else {
      meta.set(-1);
    }
  }

  auto opr = plan.plan(op_idx).opr().group_by();
  std::vector<std::pair<int, int>> mappings;
  std::vector<common::Variable> vars;

  for (int i = 0; i < mappings_num; ++i) {
    auto& key = opr.mappings(i);
    CHECK(key.has_key());
    CHECK(key.has_alias());
    CHECK(!key.key().has_property());
    int tag = key.key().has_tag() ? key.key().tag().id() : -1;
    int alias = key.has_alias() ? key.alias().value() : -1;
    mappings.emplace_back(tag, alias);
    vars.emplace_back(key.key());
  }
  auto make_key_func = [mappings = std::move(mappings), vars = std::move(vars)](
                           const GraphReadInterface& graph,
                           const Context& ctx) -> std::unique_ptr<KeyBase> {
    std::unique_ptr<KeyBase> key = nullptr;
    if (mappings.size() == 3) {
      key = KeyBuilder<3>::make_sp_key(ctx, mappings);
    } else if (mappings.size() == 2) {
      key = KeyBuilder<2>::make_sp_key(ctx, mappings);
    } else if (mappings.size() == 1) {
      key = KeyBuilder<1>::make_sp_key(ctx, mappings);
    }
    if (key == nullptr) {
      std::vector<VarWrapper> key_vars;

      for (const auto& var : vars) {
        Var var_(graph, ctx, var, VarType::kPathVar);
        key_vars.emplace_back(VarWrapper(std::move(var_)));
      }
      key = std::make_unique<GKey<VarWrapper>>(std::move(key_vars), mappings);
      // make_general_key(graph, ctx, mappings);
    }
    return key;
  };

  std::vector<std::function<std::unique_ptr<ReducerBase>(
      const GraphReadInterface&, const Context&)>>
      reduces;
  for (int i = 0; i < func_num; ++i) {
    auto& func = opr.functions(i);
    auto aggr_kind = parse_aggregate(func.aggregate());
    CHECK(func.vars_size() == 1);
    auto& var = func.vars(0);

    int alias = func.has_alias() ? func.alias().value() : -1;
    reduces.emplace_back(
        [alias, aggr_kind, var](
            const GraphReadInterface& graph,
            const Context& ctx) -> std::unique_ptr<ReducerBase> {
          return make_reducer(graph, ctx, var, aggr_kind, alias);
        });
  }
  return std::make_pair(std::make_unique<GroupByOpr>(std::move(make_key_func),
                                                     std::move(reduces)),
                        meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs