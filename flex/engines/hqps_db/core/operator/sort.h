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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_SORT_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_SORT_H_

#include <queue>
#include <string>

#include "flex/engines/hqps_db/core/context.h"

#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/general_edge_set.h"

#include "flex/engines/hqps_db/core/utils/props.h"

namespace gs {

template <typename... ORDER_PAIRS>
struct TupleComparator {
  static constexpr size_t num_pairs = sizeof...(ORDER_PAIRS);
  std::tuple<ORDER_PAIRS...> order_pairs_;

  TupleComparator(std::tuple<ORDER_PAIRS...>& order_pairs)
      : order_pairs_(order_pairs) {}

  template <typename TUPLE_T>
  inline bool operator()(const TUPLE_T& left, const TUPLE_T& right) const {
    return compare_impl<0>(left, right);
  }

  template <
      size_t Is, typename TUPLE_T,
      typename std::enable_if<
          (std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::sort_order ==
           SortOrder::ASC)>::type* = nullptr>
  inline bool compare_impl(const TUPLE_T& left, const TUPLE_T& right) const {
    auto lv = std::get<Is>(left);
    auto rv = std::get<Is>(right);
    if (lv < rv) {
      return true;
    }
    if (lv > rv) {
      return false;
    }
    if constexpr (Is + 1 < num_pairs) {
      return compare_impl<Is + 1>(left, right);
    } else {
      return true;
    }
  }

  template <
      size_t Is, typename TUPLE_T,
      typename std::enable_if<
          (std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::sort_order ==
           SortOrder::DESC)>::type* = nullptr>
  inline bool compare_impl(const TUPLE_T& left, const TUPLE_T& right) const {
    auto lv = std::get<Is>(left);
    auto rv = std::get<Is>(right);
    if (lv > rv) {
      return true;
    }
    if (lv < rv) {
      return false;
    }
    if constexpr (Is + 1 < num_pairs) {
      return compare_impl<Is + 1>(left, right);
    } else {
      return true;
    }
  }
};  // namespace gs

template <int base_tag, typename... ORDER_PAIRS>
struct GeneralComparator {
  static constexpr size_t num_pairs = sizeof...(ORDER_PAIRS);
  static constexpr auto num_pair_ind_seq =
      std::make_index_sequence<num_pairs>();
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
                             const std::tuple<GETTER...>& getter,
                             size_t cnt) const {
    return get_sort_tuple(ele_tuple, getter, cnt, num_pair_ind_seq);
  }

  template <typename... IND_ELE, typename... GETTER, size_t... Is>
  inline auto get_sort_tuple(const std::tuple<IND_ELE...>& ele_tuple,
                             const std::tuple<GETTER...>& getter, size_t cnt,
                             std::index_sequence<Is...>) const {
    return std::tuple{get_sort_tuple<Is>(ele_tuple, getter)..., cnt};
  }

  template <size_t Is, typename... IND_ELE, typename... GETTER>
  inline auto get_sort_tuple(const std::tuple<IND_ELE...>& ele_tuple,
                             const std::tuple<GETTER...>& getter) const {
    static constexpr int tag_id =
        std::tuple_element_t<Is, std::tuple<ORDER_PAIRS...>>::tag_id;
    if constexpr (tag_id == -1) {
      static constexpr int new_tag_id = sizeof...(IND_ELE) - 1;
      return std::get<Is>(getter).get_view(std::get<new_tag_id>(ele_tuple));
    } else {
      static constexpr int new_tag_id = tag_id - base_tag;
      return std::get<Is>(getter).get_view(std::get<new_tag_id>(ele_tuple));
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
      auto data = getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple));
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
      auto data = getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple));
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
      auto data = getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple));
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
      auto data = getter.get_view(gs::get_from_tuple<new_tag_id>(ele_tuple));
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
            typename... CTX_PREV_T, typename... ORDER_PAIRS,
            typename index_ele_tuple_t =
                typename Context<CTX_HEAD_T, cur_alias, base_tag,
                                 CTX_PREV_T...>::index_ele_tuples_t>
  static auto SortTopK(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>&& ctx,
      std::tuple<ORDER_PAIRS...>&& tuples, size_t limit) {
    VLOG(10) << "[SortTopK]: limit: " << limit
             << ", input size: " << ctx.GetHead().Size();
    std::apply(
        [](auto&... args) {
          ((LOG(INFO) << "SortTopK: " << args.name << " "), ...);
        },
        tuples);

    // Generate tuples from ctx, with required key.

    using ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>;
    using sort_tuple_t = std::tuple<typename ORDER_PAIRS::prop_t..., size_t>;

    TupleComparator<ORDER_PAIRS...> tuple_sorter(tuples);
    std::priority_queue<sort_tuple_t, std::vector<sort_tuple_t>,
                        TupleComparator<ORDER_PAIRS...>>
        pq(tuple_sorter);
    sort_tuple_t empty_tuple;
    sort_tuple_t& top_tuple = empty_tuple;

    size_t cnt = 0;
    auto sort_prop_getter_tuple = create_prop_getter_tuple(
        tuples, ctx, graph, std::make_index_sequence<sizeof...(ORDER_PAIRS)>());
    LOG(INFO) << "Finish create prop getter tuple.";
    GeneralComparator<ctx_t::base_tag_id, ORDER_PAIRS...> comparator(tuples);

    double t0 = -grape::GetCurrentTime();
    for (auto iter : ctx) {
      auto cur_tuple = iter.GetAllIndexElement();
      if (pq.size() < limit) {
        pq.emplace(
            comparator.get_sort_tuple(cur_tuple, sort_prop_getter_tuple, cnt));
        top_tuple = pq.top();
      } else if (pq.size() == limit) {
        // update prop getter with index_ele.
        if (comparator(cur_tuple, top_tuple, sort_prop_getter_tuple)) {
          pq.pop();
          pq.emplace(comparator.get_sort_tuple(cur_tuple,
                                               sort_prop_getter_tuple, cnt));
          top_tuple = pq.top();
        }
      }
      cnt += 1;
    }

    t0 += grape::GetCurrentTime();
    LOG(INFO) << " sort tuple cost: " << t0;
    // pop out all ele in priority_queue
    double t1 = -grape::GetCurrentTime();
    std::vector<std::pair<size_t, size_t>> inds;
    inds.reserve(pq.size());
    cnt = 0;
    while (!pq.empty()) {
      inds.emplace_back(std::make_pair(cnt, gs::get_from_tuple<-1>(pq.top())));
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
      index_eles[pair.first] = (iter2.GetAllIndexElement());
      inds_ind += 1;
    }
    std::reverse(index_eles.begin(), index_eles.end());
    t1 += grape::GetCurrentTime();
    VLOG(10) << "Finish extract top k result, sort tuple time: " << t0
             << ", prepare index ele: " << t1
             << ", result num: " << index_eles.size();

    return ctx.Flat(std::move(index_eles));
  }

  template <typename... ORDER_PAIR, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, size_t... Is>
  static auto create_prop_getter_tuple(
      const std::tuple<ORDER_PAIR...>& pairs,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
      const GRAPH_INTERFACE& graph, std::index_sequence<Is...>) {
    return std::make_tuple(create_prop_getter_impl_for_order_pair(
        std::get<Is>(pairs), ctx, graph)...);
  }

  template <typename ORDER_PAIR, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
      const GRAPH_INTERFACE& graph) {
    static constexpr int tag_id = ORDER_PAIR::tag_id;
    auto& set = ctx.template GetNode<tag_id>();
    return create_prop_getter_impl<tag_id, typename ORDER_PAIR::prop_t>(
        set, graph, ordering_pair.name);
  }

  // Get property getter for row vertex set, with ordinary properties.
  template <typename ORDER_PAIR, typename LabelT, typename VID_T, typename... T>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair,
      const RowVertexSet<LabelT, VID_T, T...>& set,
      const GRAPH_INTERFACE& graph) {
    return create_prop_getter_impl<ORDER_PAIR::tag_id,
                                   typename ORDER_PAIR::prop_t>(
        set, graph, ordering_pair.name);
  }

  // return a pair of prop_getter, each for one label.
  template <typename ORDER_PAIR, typename VID_T, typename LabelT, typename... T>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair,
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set,
      const GRAPH_INTERFACE& graph) {
    return create_prop_getter_impl<ORDER_PAIR::tag_id,
                                   typename ORDER_PAIR::prop_t>(
        set, graph, ordering_pair.name);
  }

  template <typename ORDER_PAIR, typename LabelT, typename KEY_T,
            typename VID_T, typename... T>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair,
      const KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>& set,
      const GRAPH_INTERFACE& graph) {
    return create_prop_getter_impl<ORDER_PAIR::tag_id,
                                   typename ORDER_PAIR::prop_t>(
        set, graph, ordering_pair.name);
  }

  template <typename ORDER_PAIR, typename VID_T, typename LabelT,
            typename EDATA_T>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair,
      const FlatEdgeSet<VID_T, LabelT, EDATA_T>& set,
      const GRAPH_INTERFACE& graph) {
    return create_prop_getter_impl<ORDER_PAIR::tag_id,
                                   typename ORDER_PAIR::prop_t>(
        set, graph, ordering_pair.name);
  }

  template <typename ORDER_PAIR, size_t N, typename GI, typename VID_T,
            typename LabelT, typename... EDATA_T>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair,
      const GeneralEdgeSet<N, GI, VID_T, LabelT, EDATA_T...>& set,
      const GRAPH_INTERFACE& graph) {
    return create_prop_getter_impl<ORDER_PAIR::tag_id,
                                   typename ORDER_PAIR::prop_t>(
        set, graph, ordering_pair.name);
  }

  template <typename ORDER_PAIR, typename T>
  static auto create_prop_getter_impl_for_order_pair(
      const ORDER_PAIR& ordering_pair, const Collection<T>& set,
      const GRAPH_INTERFACE& graph) {
    if (ordering_pair.name != "None" || ordering_pair.name != "none") {
      throw std::runtime_error("Expect None property getter for Collection.");
    }
    return CollectionPropGetter<ORDER_PAIR::tag_id, T>();
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_SORT_H_
