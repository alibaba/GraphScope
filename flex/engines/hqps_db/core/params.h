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
#ifndef ENGINES_HQPS_ENGINE_PARAMS_H_
#define ENGINES_HQPS_ENGINE_PARAMS_H_

#include <time.h>
#include <climits>
#include <iostream>
#include <string>
// #include "grape/grape.h"
#include "flex/engines/hqps_db/core/utils/hqps_type.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"

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

template <typename... T>
using PropNameArray =
    std::array<std::string, std::tuple_size_v<std::tuple<T...>>>;

template <typename T>
struct PropTupleArray;

template <typename... T>
struct PropTupleArray<std::tuple<T...>> {
  using prop_name_array_t = PropNameArray<T...>;
};

template <>
struct PropTupleArray<grape::EmptyType> {
  using prop_name_array_t = PropNameArray<grape::EmptyType>;
};

template <typename T>
using PropTupleArrayT = typename PropTupleArray<T>::prop_name_array_t;

template <typename T, int _tag_id = -1>
struct NamedProperty {
  using prop_t = T;
  static constexpr int tag_id = _tag_id;
  // using data_tuple_t = std::tuple<T...>;

  // PropNameArray<T...> names;
  std::string name;
  NamedProperty() = default;
  NamedProperty(std::string&& n) : name(std::move(n)){};
  NamedProperty(const std::string& n) : name(n){};
};

template <int _tag_id = -1>
struct InnerIdProperty {
  static constexpr int tag_id = _tag_id;
  InnerIdProperty() = default;
};

// Denote the length of a path
struct LengthKey {
  using length_data_type = int32_t;
};

inline bool operator<(const LabelKey& lhs, const LabelKey& rhs) {
  return lhs.label_id < rhs.label_id;
}

inline bool operator==(const LabelKey& lhs, const LabelKey& rhs) {
  return lhs.label_id == rhs.label_id;
}

inline bool operator!=(const LabelKey& lhs, const LabelKey& rhs) {
  return lhs.label_id != rhs.label_id;
}

inline bool operator>(const LabelKey& lhs, const LabelKey& rhs) {
  return lhs.label_id > rhs.label_id;
}

// overload hash_value for LabelKey
inline std::size_t hash_value(const LabelKey& key) {
  return std::hash<int32_t>()(key.label_id);
}

// static constexpr size_t dist_col = 0;

using groot_prop_label_t = std::string;

struct Range {
  Range() : start_(0), limit_(INT_MAX) {}
  Range(size_t s, size_t l) : start_(s), limit_(l) {}
  size_t start_;
  size_t limit_;
};
// Sort
enum SortOrder {
  Shuffle = 0,  // random order
  ASC = 1,      // increasing order.
  DESC = 2,     // descending order
};

//@.name
//@a.name
template <SortOrder sort_order_, int tag, typename T>
struct OrderingPropPair {
  using prop_t = T;
  static constexpr int tag_id = tag;
  // static constexpr size_t col_id = col;
  static constexpr SortOrder sort_order = sort_order_;
  std::string name;
  OrderingPropPair(std::string n) : name(n) {}
};

// The query pay load for ordering.
template <typename... ORDER_PAIR>
struct SortOrderOpt {
  std::tuple<ORDER_PAIR...> ordering_pairs_;
  Range range_;
  // SORT_FUNC sort_func_;
  // sort_func_(std::move(sort_func)),

  SortOrderOpt(Range&& range, ORDER_PAIR&&... tuple)
      : range_(std::move(range)), ordering_pairs_{tuple...} {}
};

template <typename... ORDER_PAIR>
auto make_sort_opt(Range&& range, ORDER_PAIR&&... pairs) {
  return SortOrderOpt<ORDER_PAIR...>(std::move(range),
                                     std::forward<ORDER_PAIR>(pairs)...);
}

enum JoinKind {
  Semi = 0,
  InnerJoin = 1,
  AntiJoin = 2,
  LeftOuterJoin = 3,
};

enum Direction { Out = 0, In = 1, Both = 2 };
enum VOpt {
  Start = 0,   // The start vertex of current expanded edge.
  End = 1,     // the ending vertex of this expanding.
  Other = 2,   // the other vertices.
  Both_V = 3,  // both side
  Itself = 4,  //  Get vertex from vertex set
};

enum PathOpt {
  Arbitrary = 0,  // can be duplicated path
  Simple = 1,     // a single path which contains no duplicated value.
};

enum ResultOpt {
  EndV = 0,  // Get the end vertex of path. i.e. [3],[4]
  AllV = 1,  // Get all the vertex on path. i.e. [1,2,3],[1,2,4]
};

enum Interval {
  YEAR = 0,
  MONTH = 1,
  DAY = 2,
  HOUR = 3,
  MINUTE = 4,
  SECOND = 5,
};

template <Interval interval>
struct DateTimeExtractor;

// Extract Year, month, day, hour, minute, second from Date

template <>
struct DateTimeExtractor<Interval::YEAR> {
  static int32_t extract(const Date& date) {
    auto micro_second = date.milli_second / 1000;
    struct tm tm;
    gmtime_r((time_t*) (&micro_second), &tm);
    return tm.tm_year + 1900;
  }
};

template <>
struct DateTimeExtractor<Interval::MONTH> {
  static int32_t extract(const Date& date) {
    auto micro_second = date.milli_second / 1000;
    struct tm tm;
    gmtime_r((time_t*) (&micro_second), &tm);
    return tm.tm_mon + 1;
  }
};

template <>
struct DateTimeExtractor<Interval::DAY> {
  static int32_t extract(const Date& date) {
    auto micro_second = date.milli_second / 1000;
    struct tm tm;
    gmtime_r((time_t*) (&micro_second), &tm);
    return tm.tm_mday;
  }
};

template <>
struct DateTimeExtractor<Interval::HOUR> {
  static int32_t extract(const Date& date) {
    auto micro_second = date.milli_second / 1000;
    struct tm tm;
    gmtime_r((time_t*) (&micro_second), &tm);
    return tm.tm_hour;
  }
};

template <>
struct DateTimeExtractor<Interval::MINUTE> {
  static int32_t extract(const Date& date) {
    auto micro_second = date.milli_second / 1000;
    struct tm tm;
    gmtime_r((time_t*) (&micro_second), &tm);
    return tm.tm_min;
  }
};

template <>
struct DateTimeExtractor<Interval::SECOND> {
  static int32_t extract(const Date& date) {
    auto micro_second = date.milli_second / 1000;
    struct tm tm;
    gmtime_r((time_t*) (&micro_second), &tm);
    return tm.tm_sec;
  }
};

struct TruePredicate {
  template <typename T>
  bool operator()(T& t) const {
    return true;
  }

  bool operator()() const { return true; }
};

struct TrueFilter {
  TruePredicate expr_;
};

template <typename T>
struct IsTruePredicate : std::false_type {};

template <>
struct IsTruePredicate<TruePredicate> : std::true_type {};

struct FalsePredicate {
  template <typename T>
  bool operator()(T& t) {
    return false;
  }
};

////////////////////////EdgeExpand Params//////////////////////
// EdgeExpandMsg
// can use for both edgeExpandE and edgeExpandV
template <typename LabelT, typename EDGE_FILTER_FUNC, typename... SELECTOR>
struct EdgeExpandOpt {
  EdgeExpandOpt(Direction dir, LabelT edge_label, LabelT other_label,
                Filter<EDGE_FILTER_FUNC, SELECTOR...>&& edge_filter)
      : dir_(dir),
        edge_label_(edge_label),
        other_label_(std::move(other_label)),
        edge_filter_(std::move(edge_filter)) {}

  Direction dir_;
  LabelT edge_label_;
  LabelT other_label_;  // There might be multiple dst labels.
  Filter<EDGE_FILTER_FUNC, SELECTOR...> edge_filter_;
};

// EdgeExpand to vertices with multiple edge triplet.
// The edge triplet are in the form of <src_label, dst_label, edge_label>
template <typename LabelT, typename EDGE_FILTER_FUNC>
struct EdgeExpandVMultiTripletOpt {
  EdgeExpandVMultiTripletOpt(
      Direction dir, std::vector<std::array<LabelT, 3>>&& edge_label_triplets,
      EDGE_FILTER_FUNC&& edge_filter)
      : direction_(dir),
        edge_label_triplets_(std::move(edge_label_triplets)),
        edge_filter_(std::move(edge_filter)) {}

  Direction direction_;
  std::vector<std::array<LabelT, 3>> edge_label_triplets_;
  EDGE_FILTER_FUNC edge_filter_;
};

template <typename LabelT, typename EDGE_FILTER_FUNC, typename Selectors,
          typename... T>
struct EdgeExpandEOpt;

template <size_t num_labels, typename LabelT, typename EDGE_FILTER_FUNC,
          typename Selectors, typename... T>
struct EdgeExpandEMultiLabelOpt;

template <typename LabelT, typename EDGE_FILTER_FUNC, typename... SELECTOR,
          typename... T>
struct EdgeExpandEOpt<LabelT, EDGE_FILTER_FUNC, std::tuple<SELECTOR...>, T...> {
  EdgeExpandEOpt(PropNameArray<T...>&& prop_names, Direction dir,
                 LabelT edge_label, LabelT other_label,
                 Filter<EDGE_FILTER_FUNC, SELECTOR...>&& edge_filter)
      : prop_names_(std::move(prop_names)),
        dir_(dir),
        edge_label_(edge_label),
        other_label_(std::move(other_label)),
        edge_filter_(std::move(edge_filter)) {}

  EdgeExpandEOpt(Direction dir, LabelT edge_label, LabelT other_label,
                 EDGE_FILTER_FUNC&& edge_filter)
      : dir_(dir),
        edge_label_(edge_label),
        other_label_(std::move(other_label)),
        edge_filter_(std::move(edge_filter)) {}

  PropNameArray<T...> prop_names_;
  Direction dir_;
  LabelT edge_label_;
  LabelT other_label_;  // There might be multiple dst labels.
  Filter<EDGE_FILTER_FUNC, SELECTOR...> edge_filter_;
};

template <size_t num_labels, typename LabelT, typename EDGE_FILTER_FUNC,
          typename... SELECTOR, typename... T>
struct EdgeExpandEMultiLabelOpt<num_labels, LabelT, EDGE_FILTER_FUNC,
                                std::tuple<SELECTOR...>, T...> {
  EdgeExpandEMultiLabelOpt(PropNameArray<T...>&& prop_names, Direction dir,
                           LabelT edge_label,
                           std::array<LabelT, num_labels> other_label,
                           Filter<EDGE_FILTER_FUNC, SELECTOR...>&& edge_filter)
      : prop_names_(std::move(prop_names)),
        dir_(dir),
        edge_label_(edge_label),
        other_label_(std::move(other_label)),
        edge_filter_(std::move(edge_filter)) {}

  EdgeExpandEMultiLabelOpt(Direction dir, LabelT edge_label,
                           std::array<LabelT, num_labels> other_label,
                           Filter<EDGE_FILTER_FUNC, SELECTOR...>&& edge_filter)
      : dir_(dir),
        edge_label_(edge_label),
        other_label_(std::move(other_label)),
        edge_filter_(std::move(edge_filter)) {}

  PropNameArray<T...> prop_names_;
  Direction dir_;
  LabelT edge_label_;
  std::array<LabelT, num_labels> other_label_;
  EDGE_FILTER_FUNC edge_filter_;
};

// EdgeExpandE with multiple edge triplet pairs.
template <size_t num_pairs, typename LabelT, typename FILTER_T,
          typename... PropTuple>
struct EdgeExpandMultiEOpt {
  EdgeExpandMultiEOpt(
      Direction dir,
      std::array<std::array<LabelT, 3>, num_pairs>&& edge_label_triplets,
      std::tuple<PropNameArray<PropTuple>...>&& prop_names,
      FILTER_T&& edge_filter)
      : dir_(dir),
        edge_label_triplets_(std::move(edge_label_triplets)),
        prop_names_(std::move(prop_names)),
        edge_filter_(std::move(edge_filter)) {}
  Direction dir_;
  std::array<std::array<LabelT, 3>, num_pairs> edge_label_triplets_;
  std::tuple<PropNameArray<PropTuple>...> prop_names_;
  FILTER_T edge_filter_;
};

template <typename... T, typename LabelT, typename EDGE_FILTER_FUNC,
          typename... SELECTOR>
auto make_edge_expande_opt(PropNameArray<T...>&& prop_names, Direction dir,
                           LabelT edge_label, LabelT other_label,
                           Filter<EDGE_FILTER_FUNC, SELECTOR...>&& func) {
  return EdgeExpandEOpt<LabelT, EDGE_FILTER_FUNC, std::tuple<SELECTOR...>,
                        T...>(std::move(prop_names), dir, edge_label,
                              other_label, std::move(func));
}

template <typename... T, typename LabelT>
auto make_edge_expande_opt(PropNameArray<T...>&& prop_names, Direction dir,
                           LabelT edge_label, LabelT other_label) {
  return EdgeExpandEOpt<LabelT, TruePredicate, std::tuple<>, T...>(
      std::move(prop_names), dir, edge_label, other_label,
      Filter<TruePredicate>());
}

template <typename LabelT>
auto make_edge_expande_opt(Direction dir, LabelT edge_label,
                           LabelT other_label) {
  return EdgeExpandEOpt<LabelT, TruePredicate, std::tuple<>>(
      dir, edge_label, other_label, Filter<TruePredicate>());
}

template <typename LabelT, size_t N>
auto make_edge_expande_opt(Direction dir, LabelT edge_label,
                           std::array<LabelT, N> other_labels) {
  return EdgeExpandEMultiLabelOpt<N, LabelT, TruePredicate, std::tuple<>>(
      dir, edge_label, other_labels, Filter<TruePredicate>());
}

// make edge expand with multiple labels
template <typename LabelT, typename FILTER_T, typename... PropTuple>
auto make_edge_expand_multie_opt(
    Direction dir,
    std::array<std::array<LabelT, 3>, sizeof...(PropTuple)>&&
        edge_label_triplets,
    std::tuple<PropTupleArrayT<PropTuple>...>&& prop_names, FILTER_T&& func) {
  return EdgeExpandMultiEOpt<sizeof...(PropTuple), LabelT, FILTER_T,
                             PropTuple...>(dir, std::move(edge_label_triplets),
                                           std::move(prop_names),
                                           std::move(func));
}

template <typename LabelT, typename... PropTuple>
auto make_edge_expand_multie_opt(
    Direction dir,
    std::array<std::array<LabelT, 3>, sizeof...(PropTuple)>&&
        edge_label_triplets,
    std::tuple<PropTupleArrayT<PropTuple>...>&& prop_names) {
  return EdgeExpandMultiEOpt<sizeof...(PropTuple), LabelT,
                             Filter<TruePredicate>, PropTuple...>(
      dir, std::move(edge_label_triplets), std::move(prop_names),
      Filter<TruePredicate>());
}

// Expand with multiple edge triplet pairs. resulting vertices, prop names and
// prop types are not needed.
template <typename LabelT, typename FILTER_T>
auto make_edge_expand_multiv_opt(
    Direction dir, std::vector<std::array<LabelT, 3>>&& edge_label_triplets,
    FILTER_T&& func) {
  return EdgeExpandVMultiTripletOpt<LabelT, FILTER_T>(
      dir, std::move(edge_label_triplets), std::move(func));
}

template <typename LabelT>
auto make_edge_expand_multiv_opt(
    Direction dir, std::vector<std::array<LabelT, 3>>&& edge_label_triplets) {
  return EdgeExpandVMultiTripletOpt<LabelT, Filter<TruePredicate>>(
      dir, std::move(edge_label_triplets), Filter<TruePredicate>());
}

// For edge expand with multiple labels.
template <typename LabelT, size_t num_labels, typename EDGE_FILTER_FUNC>
struct EdgeExpandOptMultiLabel {
  EdgeExpandOptMultiLabel(
      Direction dir, LabelT edge_label,
      std::array<LabelT, num_labels>&& other_label,
      std::array<EDGE_FILTER_FUNC, num_labels>&& edge_filter)
      : direction_(dir),
        edge_label_(edge_label),
        edge_filter_(std::move(edge_filter)),
        other_labels_(std::move(other_label)) {}

  Direction direction_;
  LabelT edge_label_;
  // edge filter func can be apply to every label vertices
  std::array<EDGE_FILTER_FUNC, num_labels> edge_filter_;
  std::array<LabelT, num_labels>
      other_labels_;  // There might be multiple dst labels.
};

template <typename LabelT, size_t num_labels>
auto make_edge_expandv_opt(Direction dir, LabelT edge_label,
                           std::array<LabelT, num_labels>&& other_labels) {
  return EdgeExpandOptMultiLabel(
      dir, edge_label, std::move(other_labels),
      std::array<Filter<TruePredicate>, num_labels>());
}

template <typename LabelT, size_t num_labels, typename FUNC>
auto make_edge_expandv_opt(Direction dir, LabelT edge_label,
                           std::array<LabelT, num_labels>&& other_labels,
                           std::array<FUNC, num_labels>&& func) {
  return EdgeExpandOptMultiLabel(dir, edge_label, std::move(other_labels),
                                 std::move(func));
}

template <typename LabelT>
inline auto make_edge_expandv_opt(Direction dir, LabelT edge_label,
                                  LabelT other_label) {
  return EdgeExpandOpt(dir, edge_label, other_label, Filter<TruePredicate>());
}

template <typename LabelT, typename FUNC_T, typename... SELECTOR>
auto make_edge_expandv_opt(Direction dir, LabelT edge_label, LabelT other_label,
                           Filter<FUNC_T, SELECTOR...>&& func) {
  return EdgeExpandOpt(dir, edge_label, other_label, std::move(func));
}

// Template cannot have to variadic template parameters.
// so we make filter_t as a tuple.
template <typename LabelT, size_t num_labels, typename FILTER_T, typename... T>
struct GetVOpt;

template <typename LabelT, size_t num_labels, typename EXPR,
          typename... SELECTOR, typename... T>
struct GetVOpt<LabelT, num_labels, Filter<EXPR, SELECTOR...>, T...> {
  VOpt v_opt_;
  // label of vertices we need.
  std::array<LabelT, num_labels> v_labels_;
  // columns of vertices we need to fetch.
  Filter<EXPR, SELECTOR...> filter_;
  std::tuple<PropertySelector<T>...> props_;

  GetVOpt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels,
          std::tuple<PropertySelector<T>...>&& props,
          Filter<EXPR, SELECTOR...>&& filter)
      : v_opt_(v_opt),
        v_labels_(std::move(v_labels)),
        props_(std::move(props)),
        filter_(std::move(filter)) {}

  GetVOpt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels,
          std::tuple<PropertySelector<T>...>&& props)
      : v_opt_(v_opt),
        v_labels_(std::move(v_labels)),
        props_(std::move(props)) {}

  GetVOpt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels,
          Filter<EXPR, SELECTOR...>&& filter)
      : v_opt_(v_opt),
        v_labels_(std::move(v_labels)),
        filter_(std::move(filter)) {}

  // Only with v_labels.
  GetVOpt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels)
      : v_opt_(v_opt), v_labels_(std::move(v_labels)) {}
  // it is ok that other members will be initiate to default value.
};

template <typename LabelT, typename EXPR, typename... T>
using SimpleGetVOpt = GetVOpt<LabelT, 1, EXPR, T...>;

template <typename LabelT, typename EXPR>
using SimpleGetVNoPropOpt = GetVOpt<LabelT, 1, EXPR>;

// make get_v opt with labels and props and expr(filters)
template <typename... T, typename LabelT, size_t num_labels, typename EXPR,
          typename... SELECTOR>
auto make_getv_opt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels,
                   std::tuple<PropertySelector<T>...>&& props,
                   Filter<EXPR, SELECTOR...>&& filter) {
  return GetVOpt<LabelT, num_labels, Filter<EXPR, SELECTOR...>, T...>(
      v_opt, std::move(v_labels), std::move(props), std::move(filter));
}

template <typename LabelT, size_t num_labels, typename EXPR,
          typename... SELECTOR>
auto make_getv_opt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels,
                   Filter<EXPR, SELECTOR...>&& filter) {
  return GetVOpt<LabelT, num_labels, Filter<EXPR, SELECTOR...>>(
      v_opt, std::move(v_labels), std::move(filter));
}

// make get_v opt with labels and props.
template <typename... T, typename LabelT, size_t num_labels>
auto make_getv_opt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels,
                   PropNameArray<T...>&& props) {
  return GetVOpt<LabelT, num_labels, Filter<TruePredicate>, T...>(
      v_opt, std::move(v_labels), std::move(props));
}

// make get_v opt with labels.
// template <typename LabelT, size_t num_labels, typename EXPR>
// auto make_getv_opt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels) {
//   return GetVOpt<LabelT, num_labels, EXPR>(v_opt, std::move(v_labels));
// }

// inline auto make_getv_opt(VOpt v_opt, std::string v_label) {
//   return SimpleGetVOpt<std::string, TruePredicate>(
//       v_opt, std::array<std::string, 1>{v_label});
// }

// Path expand with only one dst label, with until condition, and resulting
// vertices.
template <typename LabelT, typename EDGE_FILTER_T, typename VERTEX_FILTER_T,
          typename UNTIL_CONDITION, typename... T>
struct PathExpandOptImpl {
  PathExpandOptImpl(EdgeExpandOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
                    SimpleGetVOpt<LabelT, VERTEX_FILTER_T, T...>&& get_v_opt,
                    Range&& range, UNTIL_CONDITION&& until_condition,
                    PathOpt path_opt = PathOpt::Arbitrary,
                    ResultOpt result_opt = ResultOpt::EndV)
      : edge_expand_opt_(std::move(edge_expand_opt)),
        get_v_opt_(std::move(get_v_opt)),
        range_(std::move(range)),
        until_condition_(std::move(until_condition)),
        path_opt_(path_opt),
        result_opt_(result_opt) {}

  EdgeExpandOpt<LabelT, EDGE_FILTER_T> edge_expand_opt_;
  SimpleGetVOpt<LabelT, VERTEX_FILTER_T, T...> get_v_opt_;
  Range range_;  // Range for result vertices, default is [0,INT_MAX)
  UNTIL_CONDITION until_condition_;
  PathOpt path_opt_;      // Single path or not.
  ResultOpt result_opt_;  // Get all vertices on Path or only ending vertices.
};

// Path expand with only one edge label, but possible multiple dst labels.
template <typename LabelT, size_t num_labels, typename EDGE_FILTER_T,
          size_t get_v_num_labels, typename VERTEX_FILTER_T,
          typename UNTIL_CONDITION, typename... T>
struct PathExpandMultiDstOptImpl {
  PathExpandMultiDstOptImpl(
      EdgeExpandOptMultiLabel<LabelT, num_labels, EDGE_FILTER_T>&&
          edge_expand_opt,
      GetVOpt<LabelT, get_v_num_labels, VERTEX_FILTER_T, T...>&& get_v_opt,
      Range&& range, UNTIL_CONDITION&& until_condition,
      PathOpt path_opt = PathOpt::Arbitrary,
      ResultOpt result_opt = ResultOpt::EndV)
      : edge_expand_opt_(std::move(edge_expand_opt)),
        get_v_opt_(std::move(get_v_opt)),
        range_(std::move(range)),
        until_condition_(std::move(until_condition)),
        path_opt_(path_opt),
        result_opt_(result_opt) {}

  EdgeExpandOptMultiLabel<LabelT, num_labels, EDGE_FILTER_T> edge_expand_opt_;
  GetVOpt<LabelT, get_v_num_labels, VERTEX_FILTER_T, T...> get_v_opt_;
  Range range_;  // Range for result vertices, default is [0,INT_MAX)
  UNTIL_CONDITION until_condition_;
  PathOpt path_opt_;      // Single path or not.
  ResultOpt result_opt_;  // Get all vertices on Path or only ending vertices.
};

// Path expandv with multiple edge triplets. The src vertices can also contain
// many labels
template <typename LabelT, typename EDGE_FILTER_T, size_t get_v_num_labels,
          typename VERTEX_FILTER_T, typename UNTIL_CONDITION, typename... T>
struct PathExpandVMultiTripletOptImpl {
  PathExpandVMultiTripletOptImpl(
      EdgeExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
      GetVOpt<LabelT, get_v_num_labels, VERTEX_FILTER_T, T...>&& get_v_opt,
      Range&& range, UNTIL_CONDITION&& until_condition,
      PathOpt path_opt = PathOpt::Arbitrary,
      ResultOpt result_opt = ResultOpt::EndV)
      : edge_expand_opt_(std::move(edge_expand_opt)),
        get_v_opt_(std::move(get_v_opt)),
        range_(std::move(range)),
        until_condition_(std::move(until_condition)),
        path_opt_(path_opt),
        result_opt_(result_opt) {}

  EdgeExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T> edge_expand_opt_;
  GetVOpt<LabelT, get_v_num_labels, VERTEX_FILTER_T, T...> get_v_opt_;
  Range range_;  // Range for result vertices, default is [0,INT_MAX)
  UNTIL_CONDITION until_condition_;
  PathOpt path_opt_;      // Single path or not.
  ResultOpt result_opt_;  // Get all vertices on Path or only ending vertices.
};

template <typename LabelT, typename EDGE_FILTER_T, typename VERTEX_FILTER_T,
          typename... T>
using PathExpandVOpt = PathExpandOptImpl<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T,
                                         Filter<TruePredicate>, T...>;

template <typename LabelT, size_t num_labels, typename EDGE_FILTER_T,
          size_t get_v_num_labels, typename VERTEX_FILTER_T, typename... T>
using PathExpandVMultiDstOpt =
    PathExpandMultiDstOptImpl<LabelT, num_labels, EDGE_FILTER_T,
                              get_v_num_labels, VERTEX_FILTER_T,
                              Filter<TruePredicate>, T...>;

template <typename LabelT, typename EDGE_FILTER_T, size_t get_v_num_labels,
          typename VERTEX_FILTER_T, typename... T>
using PathExpandVMultiTripletOpt =
    PathExpandVMultiTripletOptImpl<LabelT, EDGE_FILTER_T, get_v_num_labels,
                                   VERTEX_FILTER_T, Filter<TruePredicate>,
                                   T...>;

template <typename LabelT, typename EDGE_FILTER_T, typename VERTEX_FILTER_T>
using PathExpandPOpt = PathExpandOptImpl<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T,
                                         Filter<TruePredicate>>;

// opt used for simple path opt.
template <typename LabelT, typename VERTEX_FILTER_T, typename EDGE_FILTER_T,
          typename UNTIL_CONDITION, typename... T>
using ShortestPathOpt =
    PathExpandOptImpl<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T, UNTIL_CONDITION,
                      T...>;

// make path expand opt with only one dst label.
template <typename LabelT, typename EDGE_FILTER_T, typename VERTEX_FILTER_T,
          typename... T>
auto make_path_expandv_opt(
    EdgeExpandOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
    SimpleGetVOpt<LabelT, VERTEX_FILTER_T, T...>&& get_v_opt, Range&& range,
    PathOpt path_opt = PathOpt::Arbitrary,
    ResultOpt result_opt = ResultOpt::EndV) {
  return PathExpandVOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T, T...>(
      std::move(edge_expand_opt), std::move(get_v_opt), std::move(range),
      Filter<TruePredicate>(), path_opt, result_opt);
}

// make path expand opt with only one edge label, but multiple dst labels.
template <typename LabelT, size_t num_labels, typename EDGE_FILTER_T,
          size_t get_v_num_labels, typename VERTEX_FILTER_T, typename... T>
auto make_path_expandv_opt(
    EdgeExpandOptMultiLabel<LabelT, num_labels, EDGE_FILTER_T>&&
        edge_expand_opt,
    GetVOpt<LabelT, get_v_num_labels, VERTEX_FILTER_T, T...>&& get_v_opt,
    Range&& range, PathOpt path_opt = PathOpt::Arbitrary,
    ResultOpt result_opt = ResultOpt::EndV) {
  return PathExpandVMultiDstOpt<LabelT, num_labels, EDGE_FILTER_T,
                                get_v_num_labels, VERTEX_FILTER_T, T...>(
      std::move(edge_expand_opt), std::move(get_v_opt), std::move(range),
      Filter<TruePredicate>(), path_opt, result_opt);
}

// make path expand opt with multiple edge label triplet.
template <typename LabelT, typename EDGE_FILTER_T, size_t get_v_num_labels,
          typename VERTEX_FILTER_T, typename... T>
auto make_path_expandv_opt(
    EdgeExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
    GetVOpt<LabelT, get_v_num_labels, VERTEX_FILTER_T, T...>&& get_v_opt,
    Range&& range, PathOpt path_opt = PathOpt::Arbitrary,
    ResultOpt result_opt = ResultOpt::EndV) {
  return PathExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T, get_v_num_labels,
                                    VERTEX_FILTER_T, T...>(
      std::move(edge_expand_opt), std::move(get_v_opt), std::move(range),
      Filter<TruePredicate>(), path_opt, result_opt);
}

template <typename LabelT, typename EDGE_FILTER_T, typename VERTEX_FILTER_T>
auto make_path_expandp_opt(
    EdgeExpandOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
    SimpleGetVNoPropOpt<LabelT, VERTEX_FILTER_T>&& get_v_opt, Range&& range,
    PathOpt path_opt = PathOpt::Arbitrary,
    ResultOpt result_opt = ResultOpt::EndV) {
  return PathExpandPOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T>(
      std::move(edge_expand_opt), std::move(get_v_opt), std::move(range),
      Filter<TruePredicate>(), path_opt, result_opt);
}

template <typename LabelT, typename EXPR, typename EDGE_FILTER_T,
          typename UNTIL_CONDITION, typename... SELECTOR, typename... T>
auto make_shortest_path_opt(
    EdgeExpandOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
    SimpleGetVOpt<LabelT, EXPR, T...>&& get_v_opt, Range&& range,
    Filter<UNTIL_CONDITION, SELECTOR...>&& until_condition,
    PathOpt path_opt = PathOpt::Arbitrary,
    ResultOpt result_opt = ResultOpt::EndV) {
  return ShortestPathOpt<LabelT, EXPR, EDGE_FILTER_T,
                         Filter<UNTIL_CONDITION, SELECTOR...>, T...>(
      std::move(edge_expand_opt), std::move(get_v_opt), std::move(range),
      std::move(until_condition), path_opt, result_opt);
}

// Just filter with v_labels.
template <typename LabelT, size_t num_labels>
auto make_getv_opt(VOpt v_opt, std::array<LabelT, num_labels>&& v_labels) {
  return GetVOpt<LabelT, num_labels, Filter<TruePredicate>>(
      v_opt, std::move(v_labels));
}

///////////////////////Group prams////////////////////////////

template <int _tag_id, typename... T>
struct TagProp {
  static constexpr int tag_id = _tag_id;
  PropNameArray<T...> prop_names_;
  using prop_tuple_t = std::tuple<T...>;

  TagProp(PropNameArray<T...>&& prop_names)
      : prop_names_(std::move(prop_names)) {}
};

// tagPropWithAlias
template <int _tag_id, int _res_alias, typename... T>
struct AliasTagProp {
  static constexpr int tag_id = _tag_id;
  static constexpr int res_alias = _res_alias;
  // the property name for projection.
  TagProp<_tag_id, T...> tag_prop_;
  // PropNameArray<T...> prop_names_;
  AliasTagProp(PropNameArray<T...>&& prop_names)
      : tag_prop_{std::move(prop_names)} {}
};

// Alias the property of multiple tags' multiple property.

// For the grouping key, use which property, and alias to what.
template <int _tag_id, int... Is>
struct KeyAlias {
  static constexpr int tag_id = _tag_id;
};

template <int _tag_id, int _res_alias>
struct ProjectSelf {
  static constexpr int tag_id = _tag_id;
  static constexpr int res_alias = _res_alias;
};

// evaluate expression on previous context.
template <int _res_alias, typename RES_T, typename EXPR>
struct ProjectExpr {
  static constexpr int res_alias = _res_alias;
  EXPR expr_;
  ProjectExpr(EXPR&& expr) : expr_(std::move(expr)) {}
};

template <int _res_alias, typename RES_T, typename EXPR>
auto make_project_expr(EXPR&& expr) {
  // get the return type of expr()
  // using RES_T = typename EXPR::result_t;
  return ProjectExpr<_res_alias, RES_T, EXPR>(std::move(expr));
}

template <int _tag_id, int _res_alias, typename... T>
auto make_key_alias_prop(PropNameArray<T...>&& names) {
  return AliasTagProp<_tag_id, _res_alias, T...>(std::move(names));
}

template <typename... _AGGREGATE>
struct FoldOpt {
  using agg_tuple_t = std::tuple<_AGGREGATE...>;
  static constexpr size_t num_agg = sizeof...(_AGGREGATE);
  agg_tuple_t aggregate_;
  FoldOpt(agg_tuple_t&& aggregate) : aggregate_(std::move(aggregate)) {}

  FoldOpt(_AGGREGATE&&... aggregate)
      : aggregate_(std::forward<_AGGREGATE>(aggregate)...) {}
};

template <typename... _AGG_T>
auto make_fold_opt(_AGG_T&&... aggs) {
  return FoldOpt(std::forward<_AGG_T>(aggs)...);
}

// The res_alias of project opt's should be gte 0.
// As we append them on by one after each projection.
template <typename... KEY_ALIAS_PROP>
struct ProjectOpt {
  std::tuple<KEY_ALIAS_PROP...> key_alias_tuple_;
  static constexpr size_t num_proj_cols = sizeof...(KEY_ALIAS_PROP);
  ProjectOpt(KEY_ALIAS_PROP&&... key_aliases)
      : key_alias_tuple_(std::forward<KEY_ALIAS_PROP>(key_aliases)...) {}
};

template <typename... KEY_ALIAS_PROP>
auto make_project_opt(KEY_ALIAS_PROP&&... key_alias) {
  return ProjectOpt(std::forward<KEY_ALIAS_PROP>(key_alias)...);
}

// convert tag_alias_prop to named_property
// only support one type
template <int _tag_id, int _res_alias, typename T>
NamedProperty<T, _tag_id> alias_tag_prop_to_named_property(
    const AliasTagProp<_tag_id, _res_alias, T>& alias_tag_prop) {
  return NamedProperty<T, _tag_id>(alias_tag_prop.tag_prop_.prop_names_[0]);
}

// ShortestPath
/*
message ShortestPathExpand {
  message WeightCal {
    enum Aggregate {
      SUM = 0;
      MAX = 1;
      MIN = 2;
      AVG = 3;
      MUL = 4;
    }
    // This optional expression defines how to calculate the weight on each
edge. In the expression,
    // one can directly write start, end to indicate the start/edge vertices
of the edge.
    // e.g. the expression: "start.value + end.value * weight" defines that
the weight of each edge
    // is calculated by multiplying the edge vertex's value with the edge's
weight and then summing
    // it with the start vertex's value.
    common.Expression weight_each = 1;
    // Define how to aggregate the calculated weight of each edge as the path
weight Aggregate aggregate = 2;
  }
  // A shortest path expansion has a base of path expansion
  PathExpand path_expand = 1;
  // An optional weight calculation function for shortest path. If not
specified, the weight is
  // by default the length of the path.
  WeightCal weight_cal = 2;
}
*/

}  // namespace gs

namespace std {

inline ostream& operator<<(ostream& os, const gs::Dist& g) {
  os << g.dist;
  return os;
}
}  // namespace std

#endif  // ENGINES_HQPS_ENGINE_PARAMS_H_
