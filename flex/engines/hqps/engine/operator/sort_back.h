/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_OPERATOR_SORT_H_
#define GRAPHSCOPE_OPERATOR_SORT_H_

#include <queue>
#include <string>
#include <tuple>

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/engines/hqps/engine/params.h"


namespace gs {

template <typename T>
class CollectionPropVec;

template <>
class CollectionPropVec<grape::EmptyType> {
 public:
  CollectionPropVec() {}

  template <typename IND_ELE_T>
  inline auto GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    return std::get<1>(ind_ele);
  }
};

template <typename T>
class CollectionPropGetter {
 public:
  CollectionPropGetter() {}

  inline auto get_view(const std::tuple<size_t, T>& ele) const {
    return std::make_tuple(std::get<1>(ele));
  }

  inline auto get_view() const {
    return std::make_tuple(std::get<1>(ind_ele_));
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = ind_ele;
  }

 private:
  std::tuple<size_t, T> ind_ele_;
};

template <typename T>
class SingleLabelPropVec {
 public:
  SingleLabelPropVec(std::vector<std::tuple<T>>&& vec) : vec_(std::move(vec)) {}

  template <typename IND_ELE_T>
  inline T GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    size_t ind = std::get<0>(ind_ele);
    // CHECK(ind < vec_.size());
    return std::get<0>(vec_[ind]);
  }

 private:
  std::vector<std::tuple<T>> vec_;
};

template <typename T>
class SingleLabelPropVecV2 {
 public:
  SingleLabelPropVecV2(std::vector<T>&& vec) : vec_(std::move(vec)) {}

  template <typename IND_ELE_T>
  inline T GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    size_t ind = std::get<0>(ind_ele);
    // CHECK(ind < vec_.size());
    return vec_[ind];
  }

 private:
  std::vector<T> vec_;
};

template <int tag_id, typename PROP_GETTER_T, typename IND_ELE_T>
class TwoLabelVertexSetPropGetter {
 public:
  TwoLabelVertexSetPropGetter(std::array<PROP_GETTER_T, 2>&& getters)
      : getters_(std::move(getters)) {}

  inline auto get_view(const IND_ELE_T& ind_ele) const {
    return getters_[std::get<1>(ind_ele)].get_view(std::get<2>(ind_ele));
  }

  inline auto get_view()
      const {  // const std::tuple<size_t, int32_t, VID_T>& ind_ele
    return getters_[std::get<1>(ind_ele_)].get_view(std::get<2>(ind_ele_));
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  IND_ELE_T ind_ele_;
  std::array<PROP_GETTER_T, 2> getters_;
};

template <int tag_id, typename PROP_GETTER_T, typename IND_ELE_T>
class RowVertexSetPropGetter {
 public:
  RowVertexSetPropGetter(PROP_GETTER_T&& getter) : getter_(std::move(getter)) {}

  template <typename VID_T>
  inline auto get_view(const std::tuple<size_t, VID_T>& ind_ele) const {
    return getter_.get_view(std::get<1>(ind_ele));
  }

  inline auto get_view() const {
    return getter_.get_view(std::get<1>(ind_ele_));
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  IND_ELE_T ind_ele_;
  PROP_GETTER_T getter_;
};

template <int tag_id, typename PROP_GETTER_T, typename IND_ELE_T>
class KeyedRowVertexSetPropGetter {
 public:
  KeyedRowVertexSetPropGetter(PROP_GETTER_T&& getter)
      : getter_(std::move(getter)) {}

  template <typename VID_T>
  inline auto get_view(const std::tuple<size_t, VID_T>& ind_ele) const {
    return getter_.get_view(std::get<1>(ind_ele));
  }

  inline auto get_view() const {
    return getter_.get_view(std::get<1>(ind_ele_));
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  IND_ELE_T ind_ele_;
  PROP_GETTER_T getter_;
};

template <size_t Is, typename... T>
class RefPropVec {
 public:
  RefPropVec(const std::vector<std::tuple<T...>>& vec) : vec_(vec) {}

  template <typename IND_ELE_T>
  inline auto GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    size_t ind = std::get<0>(ind_ele);
    // CHECK(ind < vec_.size());
    return std::get<Is>(vec_[ind]);
  }

 private:
  const std::vector<std::tuple<T...>>& vec_;
};

template <size_t Is, typename... T>
auto make_ref_prop_vec(const std::vector<std::tuple<T...>>& vec) {
  return RefPropVec<Is, T...>(vec);
}

class EdgeLabelPropVec {
 public:
  EdgeLabelPropVec() {}

  // std::array<std::vector<std::tuple<T>>, N>&& vec
  //: vec_(std::move(vec))

  template <typename IND_ELE_T>
  inline auto GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    //(FIXME:) CURRENTLY hack implements.
    size_t set_ind = std::get<0>(ind_ele);
    size_t set_inner_ind = std::get<1>(ind_ele);
    // CHECK(set_inner_ind < vec_[set_ind].size());
    // Assumes only one property.
    auto ele = std::get<2>(ind_ele);
    return std::get<0>(std::get<1>(ele)->properties());
  }

 private:
  // std::array<std::vector<std::tuple<T>>, N> vec_;
};

class FlatEdgeLabelPropVec {
 public:
  FlatEdgeLabelPropVec() {}

  // std::array<std::vector<std::tuple<T>>, N>&& vec
  //: vec_(std::move(vec))

  template <typename IND_ELE_T>
  inline auto GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    //(FIXME:) CURRENTLY hack implements.
    auto ele = std::get<1>(ind_ele);
    return std::get<0>(std::get<2>(ele));
  }

 private:
  // std::array<std::vector<std::tuple<T>>, N> vec_;
};

template <size_t N, typename T>
class MultiLabelPropVec {
 public:
  using vec_t = std::vector<std::tuple<T>>;
  MultiLabelPropVec(std::array<vec_t, N>&& array) : array_(std::move(array)) {}

  template <typename IND_ELE_T>
  inline T GetWithIndexEle(const IND_ELE_T& ind_ele) const {
    size_t set_ind = std::get<0>(ind_ele);
    size_t inner_ind = std::get<0>(std::get<1>(ind_ele));
    CHECK(set_ind < N);
    CHECK(inner_ind < array_[set_ind].size());
    return std::get<0>(array_[set_ind][inner_ind]);
  }

 private:
  std::array<vec_t, N> array_;
};

// ele_t is full_ele_t
// template <size_t base_tag, typename ELE_T, typename PROP_VEC_TUPLE,
//           typename ORDER_PAIR_TUPLE, typename SORT_FUNC>
// class GeneralComparator;

// template <size_t base_tag, typename ELE_T, typename... PROP_VEC,
//           typename... ORDER_PAIRS, typename SORT_FUNC>
// class GeneralComparator<base_tag, ELE_T, std::tuple<PROP_VEC...>,
//                         std::tuple<ORDER_PAIRS...>, SORT_FUNC> {
//   static constexpr size_t num_pairs = sizeof...(ORDER_PAIRS);
//   //  using index_ele_tuple_t = std::tuple_element_t<0, ELE_T>;
//   using sort_ele_tuple_t = std::tuple_element_t<1, ELE_T>;
//   static_assert(std::tuple_size_v<sort_ele_tuple_t> == num_pairs);

//  public:
//   GeneralComparator(std::tuple<PROP_VEC...>& prop_vecs_tuple,
//                     std::tuple<ORDER_PAIRS...>& tuple,
//                     const SORT_FUNC& sort_func)
//       : prop_vecs_(prop_vecs_tuple), tuple_(tuple), sort_func_(sort_func) {}

//   template <typename index_ele_tuple_t>
//   sort_ele_tuple_t GetSortTupleFromIndexElement(
//       const index_ele_tuple_t& ind_ele_tuple) const {
//     return get_sort_tuple_from_index_ele(ind_ele_tuple,
//                                          std::make_index_sequence<num_pairs>{});
//   }

//   template <typename index_ele_tuple_t, size_t... Is>
//   auto get_sort_tuple_from_index_ele(const index_ele_tuple_t& index_ele,
//                                      std::index_sequence<Is...>) const {
//     return std::make_tuple(
//         get_sort_tuple_from_index_ele<index_ele_tuple_t, Is>(index_ele)...);
//   }

//   template <typename index_ele_tuple_t, size_t Is>
//   auto get_sort_tuple_from_index_ele(const index_ele_tuple_t& index_ele)
//   const {
//     using order_pair_t = std::tuple_element_t<Is,
//     std::tuple<ORDER_PAIRS...>>; static constexpr SortOrder order =
//     order_pair_t::sort_order; static constexpr int tag_id =
//     order_pair_t::tag_id - (int) base_tag; using prop_t = typename
//     order_pair_t::prop_t; auto l_ind_ele =
//     gs::get_from_tuple<tag_id>(index_ele); if constexpr
//     (std::is_same_v<grape::EmptyType, prop_t>) {
//       return std::get<1>(l_ind_ele);
//     } else {
//       return std::get<Is>(prop_vecs_).GetWithIndexEle(l_ind_ele);
//     }
//   }

//  private:
//   std::tuple<PROP_VEC...> prop_vecs_;
//   std::tuple<ORDER_PAIRS...> tuple_;
//   const SORT_FUNC& sort_func_;
// };

template <int base_tag, typename... ORDER_PAIRS>
struct GeneralComparator {
  static constexpr size_t num_pairs = sizeof...(ORDER_PAIRS);
  const std::tuple<ORDER_PAIRS...>& order_pairs_;
  GeneralComparator(const std::tuple<ORDER_PAIRS...>& order_pairs)
      : order_pairs_(order_pairs) {}

  template <typename... IND_ELE, typename... T, typename... GETTER>
  inline bool operator()(const std::tuple<IND_ELE...>& ele_tuple,
                         const std::tuple<T...>& top_tuple,
                         const std::tuple<GETTER...>& getter) const {
    return compare_impl<0>(ele_tuple, top_tuple, getter);
  }

  template <typename... IND_ELE, typename... GETTER>
  inline auto get_sort_tuple(const std::tuple<IND_ELE...>& ele_tuple,
                             const std::tuple<GETTER...>& getter) const {
    return get_sort_tuple(ele_tuple, getter,
                          std::make_index_sequence<num_pairs>());
  }

  template <typename... IND_ELE, typename... GETTER, size_t... Is>
  inline auto get_sort_tuple(const std::tuple<IND_ELE...>& ele_tuple,
                             const std::tuple<GETTER...>& getter,
                             std::index_sequence<Is...>) const {
    return std::tuple_cat(get_sort_tuple<Is>(ele_tuple, getter)...);
  }

  template <size_t Is, typename... IND_ELE, typename... GETTER>
  inline auto get_sort_tuple(const std::tuple<IND_ELE...>& ele_tuple,
                             const std::tuple<GETTER...>& getter) const {
    static constexpr int tag_id =
        std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::tag_id;
    if constexpr (tag_id == -1) {
      static constexpr int new_tag_id = sizeof...(IND_ELE) - 1;
      return std::get<new_tag_id>(getter).get_view(
          std::get<new_tag_id>(ele_tuple));
    } else {
      static constexpr int new_tag_id = tag_id - base_tag;
      return std::get<new_tag_id>(getter).get_view(
          std::get<new_tag_id>(ele_tuple));
    }
  }

  template <
      size_t Is, typename... IND_ELE, typename... T, typename... GETTER,
      typename std::enable_if<
          (std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::sort_order ==
           SortOrder::ASC)>::type* = nullptr>
  inline bool compare_impl(const std::tuple<IND_ELE...>& ele_tuple,
                           const std::tuple<T...>& top_tuple,
                           const std::tuple<GETTER...>& getters) const {
    auto& getter = std::get<Is>(getters);
    static constexpr int tag_id =
        std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::tag_id;
    if constexpr (tag_id == -1) {
      static constexpr int new_tag_id = sizeof...(IND_ELE) - 1;
      auto data = std::get<0>(
          getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple)));
      if (data < std::get<Is>(top_tuple)) {
        return true;
      }
      if (data > std::get<Is>(top_tuple)) {
        return false;
      }
      if constexpr (Is + 1 < num_pairs) {
        return compare_impl<Is + 1>(ele_tuple, top_tuple, getters);
      } else {
        return true;
      }
    } else {
      static constexpr int new_tag_id = tag_id - base_tag;
      auto data = std::get<0>(
          getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple)));
      if (data < std::get<Is>(top_tuple)) {
        return true;
      }
      if (data > std::get<Is>(top_tuple)) {
        return false;
      }
      if constexpr (Is + 1 < num_pairs) {
        return compare_impl<Is + 1>(ele_tuple, top_tuple, getters);
      } else {
        return true;
      }
    }
  }

  template <
      size_t Is, typename... IND_ELE, typename... T, typename... GETTER,
      typename std::enable_if<
          (std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::sort_order ==
           SortOrder::DESC)>::type* = nullptr>
  inline bool compare_impl(const std::tuple<IND_ELE...>& ele_tuple,
                           const std::tuple<T...>& top_tuple,
                           const std::tuple<GETTER...>& getters) const {
    auto& getter = std::get<Is>(getters);
    static constexpr int tag_id =
        std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::tag_id;
    if constexpr (tag_id == -1) {
      static constexpr int new_tag_id = sizeof...(IND_ELE) - 1;
      auto data = std::get<0>(
          getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple)));
      if (data > std::get<Is>(top_tuple)) {
        return true;
      }
      if (data < std::get<Is>(top_tuple)) {
        return false;
      }
      if constexpr (Is + 1 < num_pairs) {
        return compare_impl<Is + 1>(ele_tuple, top_tuple, getters);
      } else {
        return true;
      }
    } else {
      static constexpr int new_tag_id = tag_id - base_tag;
      auto data = std::get<0>(
          getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple)));
      if (data > std::get<Is>(top_tuple)) {
        return true;
      }
      if (data < std::get<Is>(top_tuple)) {
        return false;
      }
      if constexpr (Is + 1 < num_pairs) {
        return compare_impl<Is + 1>(ele_tuple, top_tuple, getters);
      } else {
        return true;
      }
    }
  }
};

template <typename SET_T, typename PROP_T>
struct PropTypeOfSet;

template <typename SET_T>
struct PropTypeOfSet<SET_T, EntityProperty> {
  using result_value_t = typename SET_T::EntityValueType;
};

template <typename SET_T, typename T>
struct PropTypeOfSet<SET_T, OidProperty<T>> {
  using result_value_t = T;
};

template <typename SET_T, typename T>
struct PropTypeOfSet<SET_T, Property<T>> {
  using result_value_t = T;
};

template <typename CTX_T, typename ORDER_PAIR>
struct ResultTOfContextOrderPair;

// Result of the data type after apply order pair
template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, typename ORDER_PAIR>
struct ResultTOfContextOrderPair<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>, ORDER_PAIR> {
  static constexpr int tag_id = ORDER_PAIR::tag_id;
  static constexpr size_t col_id = ORDER_PAIR::col_id;
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using context_iter_t = typename context_t::iterator;
  // using ctx_node_t =
  // std::remove_reference_t<decltype(std::declval<context_t>().template
  // GetNode<tag_id>())>;
  using data_tuple_t = decltype(std::declval<context_iter_t>().GetAllData());

  using tag_data_tuple_t =
      typename gs::tuple_element<tag_id, data_tuple_t>::type;
  using result_t = typename gs::tuple_element<col_id, tag_data_tuple_t>::type;
};

template <typename GRAPH_INTERFACE>
class SortOp {
 public:
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV_T, typename... ORDER_PAIRS, typename SORT_FUNC,
            typename index_ele_tuple_t =
                typename Context<CTX_HEAD_T, cur_alias, base_tag,
                                 CTX_PREV_T...>::index_ele_tuples_t>
  // typename data_tuple_t = std::tuple<typename
  // CTX_PREV_T::data_tuple_t...,
  //  typename CTX_HEAD_T::data_tuple_t>,
  // typename data_index_ele_tuple_t =
  // std::tuple<data_tuple_t, index_ele_tuple_t>,
  // typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>>
  static auto SortTopK(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>&& ctx,
      std::tuple<ORDER_PAIRS...>&& tuples, size_t limit,
      const SORT_FUNC& sort_func) {
    VLOG(10) << "[SortTopK]: limit: " << limit;
    // Generate tuples from ctx, with required key.

    // Assumes input sets doesn't contains properties.
    // we should compare with select properties, but also hold the index info.
    // auto prop_store_cols =
    //     get_prop_store_cols(tuples, ctx, time_stamp, graph,
    //                         std::make_index_sequence<sizeof...(ORDER_PAIRS)>());
    using ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>;
    using sort_tuple_t = std::tuple<typename ORDER_PAIRS::prop_t...>;
    using full_ele_tuple_t = std::pair<sort_tuple_t, size_t>;

    // using prop_vec_tuple_t = decltype(prop_store_cols);
    // using cmp_t =
    //     GeneralComparator<base_tag, full_ele_tuple_t, prop_vec_tuple_t,
    //                       std::tuple<ORDER_PAIRS...>, SORT_FUNC>;
    // cmp_t cmp(prop_store_cols, tuples, sort_func);

    // VLOG(10) << "[Sort] Finish construct comparator";

    std::priority_queue<full_ele_tuple_t, std::vector<full_ele_tuple_t>,
                        SORT_FUNC>
        pq(sort_func);
    sort_tuple_t empty_tuple;
    sort_tuple_t& top_tuple = empty_tuple;
    double t0 = -grape::GetCurrentTime();
    size_t cnt = 0;
    auto sort_prop_getter_tuple = create_prop_getter_tuple(
        tuples, ctx, graph, time_stamp,
        std::make_index_sequence<sizeof...(ORDER_PAIRS)>());
    GeneralComparator<ctx_t::base_tag_id, ORDER_PAIRS...> comparator(tuples);
    for (auto iter : ctx) {
      auto cur_tuple = iter.GetAllIndexElement();
      if (pq.size() < limit) {
        auto sort_tuple =
            comparator.get_sort_tuple(cur_tuple, sort_prop_getter_tuple);
        pq.emplace(std::make_pair(sort_tuple, cnt));
        top_tuple = pq.top().first;
      } else if (pq.size() == limit) {
        // update prop getter with index_ele.
        //        update_prop_getter(sort_prop_getter_tuple, cur_tuple);
        //       if (sort_func.eval(sort_prop_getter_tuple, top_tuple)) {
        if (comparator(cur_tuple, top_tuple, sort_prop_getter_tuple)) {
          pq.pop();
          auto sort_tuple =
              comparator.get_sort_tuple(cur_tuple, sort_prop_getter_tuple);
          pq.emplace(std::make_pair(sort_tuple, cnt));
          top_tuple = pq.top().first;
        }
      }
      cnt += 1;
    }

    t0 += grape::GetCurrentTime();
    // LOG(INFO) << " sort tuple cost: " << t0;
    // pop out all ele in priority_queue
    double t1 = -grape::GetCurrentTime();
    std::vector<std::pair<size_t, size_t>> inds;
    inds.reserve(pq.size());
    cnt = 0;
    while (!pq.empty()) {
      inds.emplace_back(std::make_pair(cnt, pq.top().second));
      pq.pop();
      cnt += 1;
    }
    sort(inds.begin(), inds.end(),
         [](const auto& a, const auto& b) { return a.second < b.second; });
    std::vector<index_ele_tuple_t> index_eles;
    index_eles.resize(inds.size());
    auto iter2 = ctx.begin();
    cnt = 0;
    size_t inds_ind = 0;
    while (inds_ind < inds.size()) {
      auto pair = inds[inds_ind];
      while (cnt < pair.second) {
        ++iter2;
        ++cnt;
      }
      index_eles[pair.first] = iter2.GetAllIndexElement();
      inds_ind += 1;
    }
    std::reverse(index_eles.begin(), index_eles.end());
    t1 += grape::GetCurrentTime();
    VLOG(10) << "Finish extract top k result, sort tuple time: " << t0
             << ", prepare index ele: " << t1;

    return ctx.Flat(std::move(index_eles));
  }

  template <size_t Is = 0, typename... IND_ELE, typename... GETTER>
  static inline void update_prop_getter(std::tuple<GETTER...>& getters,
                                        std::tuple<IND_ELE...>& ind_eles) {
    std::get<Is>(getters).set_ind_ele(ind_eles);
    if constexpr (Is + 1 < sizeof...(GETTER)) {
      update_prop_getter<Is + 1>(getters, ind_eles);
    }
  }

  // get all ordering pair's required props in tuple of vector.
  template <typename... ORDER_PAIRS, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV_T, size_t... Is>
  static auto get_prop_store_cols(
      std::tuple<ORDER_PAIRS...>& order_pairs,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>& ctx,
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      std::index_sequence<Is...>) {
    return std::make_tuple(get_prop_store_col(std::get<Is>(order_pairs), ctx,
                                              time_stamp, graph)...);
  }

  // get prop vec tor each ordering pair.
  template <typename ORDER_PAIR, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV_T>
  static auto get_prop_store_col(
      ORDER_PAIR& order_pair,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>& ctx,
      int64_t time_stamp, const GRAPH_INTERFACE& graph) {
    auto& node = ctx.template GetNode<ORDER_PAIR::tag_id>();
    return get_prop_store_col(order_pair, node, time_stamp, graph);
  }

  // specialization 0: multi label vertex set.
  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<SET_T::is_multi_label && !SET_T::is_collection &&
                              SET_T::is_vertex_set &&
                              !SET_T::is_general_set>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    auto array_of_vec = get_prop_vec_array<typename ORDER_PAIR::prop_t>(
        order_pair, set, time_stamp, graph, names,
        std::make_index_sequence<SET_T::num_labels>());
    return MultiLabelPropVec(std::move(array_of_vec));
  }

  // template 0.1 general vertex set.
  template <typename ORDER_PAIR, typename SET_T,
            typename std::enable_if<
                !SET_T::is_multi_label && !SET_T::is_collection &&
                SET_T::is_vertex_set && SET_T::is_general_set>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    auto props = get_prop_store_col_impl(order_pair, set, time_stamp, graph);
    return SingleLabelPropVec(std::move(props));
  }

  // specialization 1: single label.
  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<!SET_T::is_multi_label && !SET_T::is_collection &&
                              SET_T::is_vertex_set && !SET_T::is_general_set &&
                              !SET_T::is_two_label_set>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    auto props = get_prop_store_col_impl(order_pair, set, time_stamp, graph);
    return SingleLabelPropVec(std::move(props));
  }

  template <size_t Is, typename prop_t, typename SET_T, typename... T,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  static SingleLabelPropVecV2<prop_t> get_prop_vec_with_set_cache(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      const std::string& query_prop,
      const std::array<std::string, sizeof...(T)>& cur_prop_names,
      const std::vector<std::tuple<T...>>& data_vec) {
    if (cur_prop_names[Is] == query_prop) {
      LOG(INFO) << "Found prop: " << query_prop << "in cache";
      std::vector<prop_t> vec;
      vec.reserve(data_vec.size());
      for (auto i = 0; i < data_vec.size(); ++i) {
        vec.emplace_back(std::get<Is>(data_vec[i]));
      }
      return SingleLabelPropVecV2(std::move(vec));
    } else {
      return get_prop_vec_with_set_cache<Is + 1, prop_t>(
          time_stamp, graph, set, query_prop, cur_prop_names, data_vec);
    }
  }

  template <size_t Is, typename prop_t, typename SET_T, typename... T,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  static SingleLabelPropVecV2<prop_t> get_prop_vec_with_set_cache(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      const std::string& query_prop,
      const std::array<std::string, sizeof...(T)>& cur_prop_names,
      const std::vector<std::tuple<T...>>& data_vec) {
    LOG(INFO) << "prop: " << query_prop << " not found";
    PropNameArray<prop_t> names{query_prop};
    auto props = get_property_tuple_two_label_single<prop_t>(time_stamp, graph,
                                                             set, names);
    // fill builtin props;
    set.fillBuiltinProps(props, names);
    return SingleLabelPropVecV2(std::move(props));
  }

  // for two-label-set
  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<!SET_T::is_multi_label && !SET_T::is_collection &&
                              SET_T::is_vertex_set && !SET_T::is_general_set &&
                              SET_T::is_two_label_set &&
                              (SET_T::num_props > 0)>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    //     auto& data_vec = set.GetDataVec();
    //     return make_ref_prop_vec<ORDER_PAIR::col_id>(data_vec);
    auto& cur_prop_names = set.GetPropNames();
    auto& data_vec = set.GetDataVec();

    return get_prop_vec_with_set_cache<0, typename ORDER_PAIR::prop_t>(
        time_stamp, graph, set, order_pair.name, cur_prop_names, data_vec);
  }

  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<!SET_T::is_multi_label && !SET_T::is_collection &&
                              SET_T::is_vertex_set && !SET_T::is_general_set &&
                              SET_T::is_two_label_set &&
                              (SET_T::num_props == 0)>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    using prop_t = typename ORDER_PAIR::prop_t;
    PropNameArray<prop_t> names{order_pair.name};
    auto props = get_property_tuple_two_label_single<prop_t>(time_stamp, graph,
                                                             set, names);
    // fill builtin props;
    // set.fillBuiltinProps(props, names);
    return SingleLabelPropVecV2(std::move(props));
  }

  // template <
  //     size_t Is, typename ORDER_PAIR, typename VID_T, typename LabelT,
  //     typename... T,
  //     typename std::enable_if<
  //         (Is < TwoLabelVertexSetImpl<VID_T, LabelT,
  //         T...>::num_props)>::type* = nullptr>
  // static auto create_prop_ref_col_for_two_label_set(
  //     ORDER_PAIR& order_pair, TwoLabelVertexSetImpl<VID_T, LabelT, T...>&
  //     set, int64_t time_stamp, const GRAPH_INTERFACE& graph) {
  //   using is_type = std::tuple_element_t<Is, std::tuple<T...>>;
  //   if constexpr (std::is_same_v<is_type, typename ORDER_PAIR::prop_t>) {
  //     if (order_pair.name == set.GetPropNames()[Is]) {
  //       LOG(INFO) << "Found match prop: " << order_pair.name << ", ind: " <<
  //       Is; auto& data_tuple_vec = set.GetDataVec(); return new
  //       RefPropVec<Is, T...>(data_tuple_vec);
  //     }
  //   }
  //   return create_prop_ref_col_for_two_label_set<Is + 1>(order_pair, set,
  //                                                        time_stamp, graph);
  // }

  // template <size_t Is, typename ORDER_PAIR, typename VID_T, typename LabelT,
  //           typename... T,
  //           typename std::enable_if<
  //               (Is >= TwoLabelVertexSetImpl<VID_T, LabelT,
  //               T...>::num_props)>:: type* = nullptr>
  // static auto create_prop_ref_col_for_two_label_set(
  //     ORDER_PAIR& order_pair, TwoLabelVertexSetImpl<VID_T, LabelT, T...>&
  //     set, int64_t time_stamp, const GRAPH_INTERFACE& graph) {
  //   return nullptr;
  // }

  // specialization 2: collection.
  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<!SET_T::is_multi_label && SET_T::is_collection &&
                              !SET_T::is_vertex_set &&
                              !SET_T::is_general_set>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    // auto props = get_prop_store_col_impl(order_pair, set, time_stamp, graph);
    // return SingleLabelPropVec(std::move(props));
    using prop_t = typename ORDER_PAIR::prop_t;
    // grape::EmptyType means we want the element itself.
    return CollectionPropVec<prop_t>();
  }

  // specialization 3: edge set.
  // currenly we can only get edge proproperty from edge set.
  template <typename ORDER_PAIR, typename SET_T,
            typename std::enable_if<
                !SET_T::is_multi_label && !SET_T::is_collection &&
                SET_T::is_edge_set && SET_T::is_multi_src>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    static constexpr size_t num_src_labels = SET_T::num_src_labels;
    // PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    // std::array<std::vector<std::tuple<typename ORDER_PAIR::prop_t>>,
    //            num_src_labels>
    //     tuples;
    // fill builtin props;
    // for (auto i = 0; i < num_src_labels; ++i) {
    // tuples[i].resize(set.NumEdgesFromSrc(i));
    // }
    // set.fillBuiltinProps(tuples, names);
    return EdgeLabelPropVec();
  }

  // specialization 4: flat edge set.
  // currenly we can only get edge proproperty from edge set.
  template <typename ORDER_PAIR, typename SET_T,
            typename std::enable_if<
                !SET_T::is_multi_label && !SET_T::is_collection &&
                SET_T::is_edge_set && !SET_T::is_multi_src>::type* = nullptr>
  static auto get_prop_store_col(ORDER_PAIR& order_pair, SET_T& set,
                                 int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph) {
    // static constexpr size_t num_src_labels = SET_T::num_src_labels;
    // PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    // std::array<std::vector<std::tuple<typename ORDER_PAIR::prop_t>>,
    //            num_src_labels>
    //     tuples;
    // fill builtin props;
    // for (auto i = 0; i < num_src_labels; ++i) {
    // tuples[i].resize(set.NumEdgesFromSrc(i));
    // }
    // set.fillBuiltinProps(tuples, names);
    return FlatEdgeLabelPropVec();
  }

  template <typename T, typename ORDER_PAIR, typename SET_T, size_t... Is>
  static auto get_prop_vec_array(ORDER_PAIR& order_pair, SET_T& set, int64_t ts,
                                 const GRAPH_INTERFACE& graph,
                                 PropNameArray<T>& prop_names,
                                 std::index_sequence<Is...>) {
    return std::array<std::vector<std::tuple<T>>, sizeof...(Is)>{
        get_prop_store_col_impl(order_pair, set.template GetSet<Is>(), ts,
                                graph)...};
  }

  // get one label for each set.
  // If the property is none, we just ignore.
  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<!SET_T::is_multi_label && SET_T::is_vertex_set &&
                              !SET_T::is_general_set &&
                              !SET_T::is_two_label_set>::type* = nullptr>
  static auto get_prop_store_col_impl(ORDER_PAIR& order_pair, SET_T& set,
                                      int64_t time_stamp,
                                      const GRAPH_INTERFACE& graph) {
    PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    auto props =
        graph.template GetVertexPropsFromVid<typename ORDER_PAIR::prop_t>(
            time_stamp, set.GetLabel(), set.GetVertices(), names);
    // fill builtin props;
    set.fillBuiltinProps(props, names);
    return props;
  }

  // get one label for each set.
  // If the property is none, we just ignore.
  // Specialization for general set
  template <
      typename ORDER_PAIR, typename SET_T,
      typename std::enable_if<!SET_T::is_multi_label && SET_T::is_vertex_set &&
                              SET_T::is_general_set>::type* = nullptr>
  static auto get_prop_store_col_impl(ORDER_PAIR& order_pair, SET_T& set,
                                      int64_t time_stamp,
                                      const GRAPH_INTERFACE& graph) {
    PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    auto props = get_property_tuple_general<typename ORDER_PAIR::prop_t>(
        time_stamp, graph, set, names);
    // fill builtin props;
    set.fillBuiltinProps(props, names);
    return props;
  }

  // for two label set, we can use the cached props.
  // we assume the properties has already been cache.
  // template <
  //     typename ORDER_PAIR, typename SET_T,
  //     typename std::enable_if<!SET_T::is_multi_label && SET_T::is_vertex_set
  //     &&
  //                             !SET_T::is_general_set &&
  //                             SET_T::is_two_label_set>::type* = nullptr>
  // static auto get_prop_store_col_impl(ORDER_PAIR& order_pair, SET_T& set,
  //                                     int64_t time_stamp,
  //                                     const GRAPH_INTERFACE& graph) {
  //   // If
  //   PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
  //   auto props = get_property_tuple_two_label<typename ORDER_PAIR::prop_t>(
  //       time_stamp, graph, set, names);
  //   // fill builtin props;
  //   set.fillBuiltinProps(props, names);
  //   return props;
  // }

  template <typename... ORDER_PAIR, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, size_t... Is>
  static auto create_prop_getter_tuple(
      const std::tuple<ORDER_PAIR...>& pairs,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
      const GRAPH_INTERFACE& graph, int64_t time_stamp,
      std::index_sequence<Is...>) {
    return std::make_tuple(create_prop_getter_impl(std::get<Is>(pairs), ctx,
                                                   graph, time_stamp)...);
  }

  template <typename ORDER_PAIR, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV>
  static auto create_prop_getter_impl(
      const ORDER_PAIR& pairs,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
      const GRAPH_INTERFACE& graph, int64_t time_stamp) {
    static constexpr int tag_id = ORDER_PAIR::tag_id;
    auto& set = ctx.template GetNode<tag_id>();
    return create_prop_getter_impl(pairs, set, graph, time_stamp);
  }

  template <typename ORDER_PAIR, typename VID_T, typename... T>
  static auto create_prop_getter_impl(const ORDER_PAIR& ordering_pair,
                                      const RowVertexSet<VID_T, T...>& set,
                                      const GRAPH_INTERFACE& graph,
                                      int64_t time_stamp) {
    using prop_getter_t = typename GRAPH_INTERFACE::template prop_getter_t<
        typename ORDER_PAIR::prop_t>;
    // const std::array<std::string, 2>& labels = set.GetLabels();
    std::string label = set.GetLabel();
    std::array<std::string, 1> names{ordering_pair.name};
    auto getter = graph.template GetPropGetter<typename ORDER_PAIR::prop_t>(
        time_stamp, label, ordering_pair.names);
    return RowVertexSetPropGetter<
        ORDER_PAIR::tag_id, prop_getter_t,
        typename RowVertexSet<VID_T, T...>::index_ele_tuple_t>(
        std::move(getter));
  }

  // return a pair of prop_getter, each for one label.
  template <typename ORDER_PAIR, typename VID_T, typename LabelT, typename... T>
  static auto create_prop_getter_impl(
      const ORDER_PAIR& ordering_pair,
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set,
      const GRAPH_INTERFACE& graph, int64_t time_stamp) {
    using prop_getter_t = typename GRAPH_INTERFACE::template prop_getter_t<
        typename ORDER_PAIR::prop_t>;
    const std::array<std::string, 2>& labels = set.GetLabels();
    std::array<prop_getter_t, 2> prop_getter;
    // std::array<std::string, sizeof...(ORDER_PAIR)> prop_names;
    // set_prop_names(prop_names, ordering_pair);
    std::array<std::string, 1> names{ordering_pair.name};
    for (auto i = 0; i < 2; ++i) {
      prop_getter[i] =
          graph.template GetPropGetter<typename ORDER_PAIR::prop_t>(
              time_stamp, labels[i], names);
    }
    return TwoLabelVertexSetPropGetter<
        ORDER_PAIR::tag_id, prop_getter_t,
        typename TwoLabelVertexSet<VID_T, LabelT, T...>::index_ele_tuple_t>(
        std::move(prop_getter));
  }

  template <typename ORDER_PAIR, typename LabelT, typename KEY_T,
            typename VID_T, typename... T>
  static auto create_prop_getter_impl(
      const ORDER_PAIR& ordering_pair,
      const KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>& set,
      const GRAPH_INTERFACE& graph, int64_t time_stamp) {
    using prop_getter_t = typename GRAPH_INTERFACE::template prop_getter_t<
        typename ORDER_PAIR::prop_t>;
    // const std::array<std::string, 2>& labels = set.GetLabels();
    std::string label = set.GetLabel();
    std::array<std::string, 1> names{ordering_pair.name};
    auto getter = graph.template GetPropGetter<typename ORDER_PAIR::prop_t>(
        time_stamp, label, names);
    return KeyedRowVertexSetPropGetter<
        ORDER_PAIR::tag_id, prop_getter_t,
        typename KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T,
                                       T...>::index_ele_tuple_t>(
        std::move(getter));
  }

  template <typename ORDER_PAIR, typename T>
  static auto create_prop_getter_impl(const ORDER_PAIR& ordering_pair,
                                      const Collection<T>& set,
                                      const GRAPH_INTERFACE& graph,
                                      int64_t time_stamp) {
    CHECK(ordering_pair.name == "None" || ordering_pair.name == "none");
    return CollectionPropGetter<T>();
  }
};

}  // namespace gs

#endif  // GRAPHSCOPE_OPERATOR_SORT_H_
