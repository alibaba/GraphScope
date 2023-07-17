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
#ifndef ENGINES_HQPS_ENGINE_UTILS_OPERATOR_UTILS_H_
#define ENGINES_HQPS_ENGINE_UTILS_OPERATOR_UTILS_H_

#include <string>
#include <tuple>

#include "grape/types.h"

namespace gs {

// define a header to make input_col_id easy to understand
#define INPUT_COL_ID(x) (x)
#define LAST_COL -1

#define PROJ_TO_NEW false
#define PROJ_TO_APPEND true

// Indicator flag for appending the result column to Context.
enum class AppendOpt {
  Persist = 0,  // persistently store the result column in Context.
  Temp = 1,  // temporally store the result column, will be replaced by the next
             // column.
  Replace = 2,  // replace the last column in Context.
};

template <AppendOpt opt, int32_t old_alias, typename... PREV_COL>
struct ResultColId {
  static constexpr int32_t res_alias =
      opt == AppendOpt::Temp
          ? -1
          : (opt == AppendOpt::Replace
                 ? old_alias
                 : (old_alias == -1 ? (int32_t) sizeof...(PREV_COL)
                                    : old_alias + 1));
};

template <AppendOpt opt, int32_t old_alias>
struct ResultColId<opt, old_alias, grape::EmptyType> {
  static constexpr int32_t res_alias =
      opt == AppendOpt::Temp
          ? -1
          : (opt == AppendOpt::Replace ? old_alias
                                       : (old_alias == -1 ? 0 : old_alias + 1));
};

template <typename T>
struct PropertySelector {
  using prop_t = T;
  std::string prop_name_;
  PropertySelector(std::string prop_name) : prop_name_(std::move(prop_name)) {}
  PropertySelector() = default;
};

using InternalIdSelector = PropertySelector<grape::EmptyType>;

// @brief Mapping a vertex/edge to new data with expr& selector.
// @tparam EXPR
// @tparam ...SELECTOR
template <typename EXPR, typename SELECTOR_TUPLE, int32_t... in_col_id>
struct MultiMapper {
  EXPR expr_;
  SELECTOR_TUPLE selectors_;
  MultiMapper(EXPR&& expr, SELECTOR_TUPLE&& selectors)
      : expr_(std::move(expr)), selectors_(std::move(selectors)) {}
};

// Mapping the data selected by selector identically.
template <int in_col_id, typename SELECTOR>
struct IdentityMapper {
  SELECTOR selector_;
  IdentityMapper(SELECTOR&& selector) : selector_(std::move(selector)) {}
  IdentityMapper() = default;
};

template <typename EXPR, typename... SELECTOR>
struct Filter {
  using expr_t = EXPR;

  EXPR expr_;
  std::tuple<SELECTOR...> selectors_;
  Filter() = default;
  Filter(EXPR&& expr, std::tuple<SELECTOR...>&& selectors)
      : expr_(std::move(expr)), selectors_(std::move(selectors)) {}
};

template <int... in_col_id, typename EXPR, typename... SELECTOR>
auto make_mapper_with_expr(EXPR&& expr, SELECTOR&&... selector) {
  return MultiMapper<EXPR, std::tuple<SELECTOR...>, in_col_id...>(
      std::move(expr), std::make_tuple(selector...));
}

template <int in_col_id, typename SELECTOR>
auto make_mapper_with_variable(SELECTOR&& selector) {
  return IdentityMapper<in_col_id, SELECTOR>(std::move(selector));
}

// makeFilter
template <typename EXPR, typename... SELECTOR>
auto make_filter(EXPR&& expr, SELECTOR&&... selectors) {
  return Filter<EXPR, SELECTOR...>(std::move(expr),
                                   std::make_tuple(std::move(selectors)...));
}

///////////////////////////////for group by////////////////////////////
template <int _col_id, typename T>
struct GroupKey {
  using selector_t = PropertySelector<T>;
  static constexpr int col_id = _col_id;
  PropertySelector<T> selector_;
  GroupKey(PropertySelector<T>&& selector) : selector_(std::move(selector)) {}
  GroupKey() = default;
};

enum class AggFunc {
  SUM = 0,
  MIN = 1,
  MAX = 2,
  COUNT = 3,
  COUNT_DISTINCT = 4,
  TO_LIST = 5,
  TO_SET = 6,
  AVG = 7,
  FIRST = 8,
};

// Get the return type of this aggregation.
template <AggFunc agg_func, typename T>
struct AggFuncReturnValue {
  // default return t;
  using return_t = T;
};

template <typename T>
struct AggFuncReturnValue<AggFunc::COUNT, T> {
  using return_t = size_t;
};

template <typename T>
struct AggFuncReturnValue<AggFunc::COUNT_DISTINCT, T> {
  using return_t = size_t;
};

// for grouping values, for which key, and to which alias, applying which
// agg_func.
// col_ind: the index of property which we will use.

template <AggFunc _agg_func, typename Selectors, typename TagIds>
struct AggregateProp;

template <AggFunc _agg_func, typename... T, int... Is>
struct AggregateProp<_agg_func, std::tuple<PropertySelector<T>...>,
                     std::integer_sequence<int32_t, Is...>> {
  static_assert(sizeof...(Is) == sizeof...(T));
  static constexpr AggFunc agg_func = _agg_func;
  static constexpr size_t num_vars = sizeof...(T);
  std::tuple<PropertySelector<T>...> selectors_;

  AggregateProp(std::tuple<PropertySelector<T>...>&& selectors)
      : selectors_(std::move(selectors)) {}
};

template <AggFunc _agg_func, typename... T, int... Is>
auto make_aggregate_prop(std::tuple<PropertySelector<T>...>&& selectors,
                         std::integer_sequence<int32_t, Is...>) {
  return AggregateProp<_agg_func, std::tuple<PropertySelector<T>...>,
                       std::integer_sequence<int32_t, Is...>>(
      std::move(selectors));
}

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_UTILS_OPERATOR_UTILS_H_