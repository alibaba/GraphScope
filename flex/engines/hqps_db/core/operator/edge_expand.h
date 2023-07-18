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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_EDGE_EXPAND_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_EDGE_EXPAND_H_

#include <string>
#include <tuple>

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

#include "flex/engines/hqps_db/structures/multi_edge_set/adj_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/general_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/multi_label_dst_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/keyed_row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"
#include "grape/utils/bitset.h"

namespace gs {

// In expand to edges, we just need to keep the reference of vertex set.
template <typename GRAPH_INTERFACE, typename VERTEX_SET_T,
          typename EDGE_FILTER_T>
struct EdgeExpandVState {
  const GRAPH_INTERFACE& graph_;
  const VERTEX_SET_T& cur_vertex_set_;
  Direction direction_;
  typename GRAPH_INTERFACE::label_id_t edge_label_, other_label_;
  size_t limit_;
  EDGE_FILTER_T edge_filter_;

  EdgeExpandVState(const GRAPH_INTERFACE& frag, const VERTEX_SET_T& v_set,
                   Direction direction,
                   typename GRAPH_INTERFACE::label_id_t edge_label,
                   typename GRAPH_INTERFACE::label_id_t other_label,
                   EDGE_FILTER_T&& edge_filter, size_t limit)
      : graph_(frag),
        cur_vertex_set_(v_set),
        direction_(direction),
        edge_label_(edge_label),
        other_label_(other_label),
        edge_filter_(std::move(edge_filter)),
        limit_(limit) {}
};

// In expand to edges, we need to create a new copy of vertex set.
template <typename GRAPH_INTERFACE, typename VERTEX_SET_T,
          typename EDGE_FILTER_T, typename... T>
struct EdgeExpandEState {
  const GRAPH_INTERFACE& graph_;
  VERTEX_SET_T& cur_vertex_set_;
  Direction direction_;
  typename GRAPH_INTERFACE::label_id_t edge_label_, other_label_;
  const PropNameArray<T...>& prop_names_;
  const EDGE_FILTER_T& edge_filter_;
  size_t limit_;

  EdgeExpandEState(const GRAPH_INTERFACE& frag, VERTEX_SET_T& v_set,
                   Direction direction,
                   typename GRAPH_INTERFACE::label_id_t edge_label,
                   typename GRAPH_INTERFACE::label_id_t other_label,
                   const PropNameArray<T...>& prop_names,
                   const EDGE_FILTER_T& edge_filter, size_t limit)
      : graph_(frag),
        cur_vertex_set_(v_set),
        direction_(direction),
        edge_label_(edge_label),
        other_label_(other_label),
        limit_(limit),
        prop_names_(prop_names),
        edge_filter_(edge_filter) {}
};

template <typename GRAPH_INTERFACE, typename VERTEX_SET_T, size_t num_labels,
          typename EDGE_FILTER_T, typename... T>
struct EdgeExpandEMutltiDstState {
  const GRAPH_INTERFACE& graph_;
  VERTEX_SET_T& cur_vertex_set_;
  Direction direction_;
  typename GRAPH_INTERFACE::label_id_t edge_label_;
  std::array<typename GRAPH_INTERFACE::label_id_t, num_labels> other_label_;
  const PropNameArray<T...>& prop_names_;
  const EDGE_FILTER_T& edge_filter_;
  size_t limit_;

  EdgeExpandEMutltiDstState(
      const GRAPH_INTERFACE& frag, VERTEX_SET_T& v_set, Direction direction,
      typename GRAPH_INTERFACE::label_id_t edge_label,
      std::array<typename GRAPH_INTERFACE::label_id_t, num_labels> other_label,
      const PropNameArray<T...>& prop_names, const EDGE_FILTER_T& edge_filter,
      size_t limit)
      : graph_(frag),
        cur_vertex_set_(v_set),
        direction_(direction),
        edge_label_(edge_label),
        other_label_(other_label),
        limit_(limit),
        prop_names_(prop_names),
        edge_filter_(edge_filter) {}
};

template <typename GRAPH_INTERFACE>
class EdgeExpand {
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;

 public:
  /// @brief Directly obtain vertices from edge.
  /// Activation: RowVertexSet, TruePredicate.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <typename... T, typename EDGE_FILTER_T,
            typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>>
  static RES_T EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<label_id_t, vertex_id_t, T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T&& edge_filter, size_t limit = INT_MAX) {
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);
    return EdgeExpandVFromSingleLabel(state);
  }

  /// @brief Directly obtain vertices from keyed row vertex set, via edge.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <typename... T, typename EDGE_FILTER_T,
            typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>>
  static RES_T EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      const KeyedRowVertexSet<label_id_t, vertex_id_t, vertex_id_t, T...>&
          cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T&& edge_filter, size_t limit = INT_MAX) {
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);
    return EdgeExpandVFromSingleLabel(state);
  }

  /// @brief Directly obtain vertices from edge, without property and apply from
  /// multi label set, Activation: MultiLabelVertexSet, TruePredicate.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <
      typename VERTEX_SET_T, typename... SELECTOR,
      typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>,
      typename std::enable_if<VERTEX_SET_T::is_multi_label>::type* = nullptr>
  static RES_T EdgeExpandV(const GRAPH_INTERFACE& graph,
                           const VERTEX_SET_T& cur_vertex_set,
                           Direction direction, label_id_t edge_label,
                           label_id_t other_label,
                           Filter<TruePredicate, SELECTOR...>&& edge_filter,
                           size_t limit = INT_MAX) {
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);

    std::vector<vertex_id_t> vids;
    std::vector<offset_t> offset;
    static constexpr size_t num_src_labels = VERTEX_SET_T::num_labels;
    using nbr_list_array_t = typename GRAPH_INTERFACE::nbr_list_array_t;
    std::vector<nbr_list_array_t> nbr_lists;
    for (auto i = 0; i < num_src_labels; ++i) {
      auto& cur_set = state.cur_vertex_set_.GetSet(i);
      label_id_t src_label, dst_label;
      std::tie(src_label, dst_label) = get_graph_label_pair(
          direction, cur_set.GetLabel(), state.other_label_);
      VLOG(10) << "[EdgeExpandVMultiSrcLabel: from label: "
               << cur_set.GetLabel() << ", other label: " << state.other_label_
               << ",edge label: " << state.edge_label_ << "src: " << src_label
               << ",dst: " << dst_label << ",dire: " << state.direction_;
      auto nbr_list_array = state.graph_.GetOtherVertices(
          src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
          gs::to_string(state.direction_), state.limit_);
      nbr_lists.emplace_back(std::move(nbr_list_array));
    }

    offset.reserve(state.cur_vertex_set_.Size() + 1);
    // first gather size.
    offset.emplace_back(vids.size());
    for (auto iter : state.cur_vertex_set_) {
      auto vid = iter.GetVertex();
      auto cur_set_ind = iter.GetCurInd();
      auto set_inner_ind = iter.GetCurSetInnerInd();
      CHECK(nbr_lists.size() > cur_set_ind);
      CHECK(nbr_lists[cur_set_ind].size() > set_inner_ind);
      auto& cur_array = nbr_lists[cur_set_ind];
      auto cur_nbr_list = cur_array.get(set_inner_ind);
      // VLOG(10) << "vertex: " << vid << ", num nbrs: " << cur_nbr_list.size();

      for (auto nbr : cur_nbr_list) {
        // TODO: use edge_filter to filter.
        vids.emplace_back(nbr.neighbor());
      }
      offset.emplace_back(vids.size());
    }
    VLOG(10) << "vids size: " << vids.size();
    VLOG(10) << "offset: " << gs::to_string(offset);
    vertex_set_t result_set(std::move(vids), state.other_label_);
    auto pair = std::make_pair(std::move(result_set), std::move(offset));
    return pair;
  }

  /// @brief Directly obtain vertices from two label vertex set.
  /// multi label set
  /// Activation: From two label set, TruePredicate.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <
      typename VERTEX_SET_T, typename... SELECTOR,
      typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>,
      typename std::enable_if<VERTEX_SET_T::is_two_label_set>::type* = nullptr>
  static RES_T EdgeExpandV(const GRAPH_INTERFACE& graph,
                           const VERTEX_SET_T& cur_vertex_set,
                           Direction direction, label_id_t edge_label,
                           label_id_t other_label,
                           Filter<TruePredicate, SELECTOR...>&& edge_filter,
                           size_t limit = INT_MAX) {
    VLOG(10) << "[EdgeExpandV] for two label vertex set size: "
             << cur_vertex_set.Size();
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);

    std::vector<vertex_id_t> vids;
    std::vector<offset_t> offset;
    static constexpr size_t num_src_labels = VERTEX_SET_T::num_labels;
    using nbr_list_t = typename GRAPH_INTERFACE::nbr_list_t;
    using nbr_list_array_t = typename GRAPH_INTERFACE::nbr_list_array_t;
    nbr_list_array_t nbr_list_array;
    nbr_list_array.resize(state.cur_vertex_set_.Size());

    for (auto i = 0; i < num_src_labels; ++i) {
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> active_inds;
      std::tie(cur_vids, active_inds) = state.cur_vertex_set_.GetVertices(i);
      label_id_t cur_label = state.cur_vertex_set_.GetLabel(i);
      label_id_t src_label, dst_label;
      std::tie(src_label, dst_label) =
          get_graph_label_pair(direction, cur_label, state.other_label_);

      VLOG(10) << "[EdgeExpandV-TwoLabelSet]: from label: "
               << ",edge label: " << state.edge_label_ << "src: " << src_label
               << ",dst: " << dst_label << ",dire: " << state.direction_;
      auto tmp_nbr_list_array = state.graph_.GetOtherVertices(
          src_label, dst_label, state.edge_label_, cur_vids,
          gs::to_string(state.direction_), state.limit_);
      // nbr_lists.emplace_back(std::move(nbr_list_array));

      CHECK(tmp_nbr_list_array.size() == active_inds.size());
      for (auto i = 0; i < active_inds.size(); ++i) {
        auto dst_ind = active_inds[i];
        CHECK(nbr_list_array.get(dst_ind).size() == 0);
        nbr_list_array.get_vector(dst_ind).swap(
            tmp_nbr_list_array.get_vector(i));
      }
    }
    CHECK(nbr_list_array.size() == state.cur_vertex_set_.Size());

    offset.reserve(state.cur_vertex_set_.Size() + 1);
    // first gather size.
    offset.emplace_back(vids.size());
    for (auto i = 0; i < nbr_list_array.size(); ++i) {
      for (auto nbr : nbr_list_array.get(i)) {
        // TODO: use edge_filter to filter.
        vids.emplace_back(nbr.neighbor());
      }
      offset.emplace_back(vids.size());
    }
    vertex_set_t result_set(std::move(vids), state.other_label_);
    auto pair = std::make_pair(std::move(result_set), std::move(offset));
    return pair;
  }

  /// @brief Directly obtain vertices from edge, without property and apply from
  /// multi label set
  /// Activation: From Generate vertex set, TruePredicate.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <
      typename VERTEX_SET_T, typename... SELECTOR,
      typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>,
      typename std::enable_if<VERTEX_SET_T::is_general_set>::type* = nullptr>
  static RES_T EdgeExpandV(const GRAPH_INTERFACE& graph,
                           const VERTEX_SET_T& cur_vertex_set,
                           Direction direction, label_id_t edge_label,
                           label_id_t other_label,
                           Filter<TruePredicate, SELECTOR...>&& edge_filter,
                           size_t limit = INT_MAX) {
    VLOG(10) << "[EdgeExpandV] for general vertex set size: "
             << cur_vertex_set.Size();
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);

    std::vector<vertex_id_t> vids;
    std::vector<offset_t> offset;
    static constexpr size_t num_src_labels = VERTEX_SET_T::num_labels;
    using nbr_list_t = typename GRAPH_INTERFACE::nbr_list_t;
    using nbr_list_array_t = typename GRAPH_INTERFACE::nbr_list_array_t;
    nbr_list_array_t nbr_list_array;
    nbr_list_array.resize(state.cur_vertex_set_.Size());

    for (auto i = 0; i < num_src_labels; ++i) {
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> active_inds;
      std::tie(cur_vids, active_inds) = state.cur_vertex_set_.GetVertices(i);
      label_id_t cur_label = state.cur_vertex_set_.GetLabel(i);
      label_id_t src_label, dst_label;
      std::tie(src_label, dst_label) =
          get_graph_label_pair(direction, cur_label, state.other_label_);

      VLOG(10) << "[EdgeExpandV]: from label: "
               << ",edge label: " << state.edge_label_ << "src: " << src_label
               << ",dst: " << dst_label << ",dire: " << state.direction_;
      auto tmp_nbr_list_array = state.graph_.GetOtherVertices(
          src_label, dst_label, state.edge_label_, cur_vids,
          gs::to_string(state.direction_), state.limit_);
      // nbr_lists.emplace_back(std::move(nbr_list_array));

      CHECK(tmp_nbr_list_array.size() == active_inds.size());
      for (auto i = 0; i < active_inds.size(); ++i) {
        auto dst_ind = active_inds[i];
        CHECK(nbr_list_array.get(dst_ind).size() == 0);
        nbr_list_array.get_vector(dst_ind).swap(
            tmp_nbr_list_array.get_vector(i));
      }
    }
    CHECK(nbr_list_array.size() == state.cur_vertex_set_.Size());

    offset.reserve(state.cur_vertex_set_.Size() + 1);
    // first gather size.
    offset.emplace_back(vids.size());
    for (auto i = 0; i < nbr_list_array.size(); ++i) {
      for (auto nbr : nbr_list_array.get(i)) {
        // TODO: use edge_filter to filter.
        vids.emplace_back(nbr.neighbor());
      }
      offset.emplace_back(vids.size());
    }
    VLOG(10) << "vids size: " << vids.size();
    VLOG(10) << "offset: " << gs::to_string(offset);
    vertex_set_t result_set(std::move(vids), state.other_label_);
    auto pair = std::make_pair(std::move(result_set), std::move(offset));
    return pair;
  }

  /// @brief Directly obtain vertices from edge.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <typename... SET_T, typename EDGE_FILTER_T, typename... SELECTOR,
            typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>,
            typename std::enable_if<
                !IsTruePredicate<EDGE_FILTER_T>::value>::type* = nullptr>
  static RES_T EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      Filter<EDGE_FILTER_T, SELECTOR...>&& edge_filter,
      size_t limit = INT_MAX) {
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);
    label_id_t src_label, dst_label;
    std::tie(src_label, dst_label) = get_graph_label_pair(
        state.direction_, state.cur_vertex_set_.GetLabel(), state.other_label_);

    VLOG(10) << "edgeExpandV: from label: " << state.cur_vertex_set_.GetLabel()
             << ", other label: " << state.other_label_
             << ",edge label: " << state.edge_label_
             << ",dire: " << state.direction_ << ", propert name: ";
    auto selectors = state.edge_filter_.selectors_;
    auto adj_list_array =
        get_adj_list_array_with_filter(state, src_label, dst_label, selectors);
    VLOG(10) << "got adj list array: " << adj_list_array.size();

    std::vector<vertex_id_t> vids;
    std::vector<offset_t> offset;
    offset.reserve(state.cur_vertex_set_.Size() + 1);
    CHECK(adj_list_array.size() == state.cur_vertex_set_.Size());
    // first gather size.
    offset.emplace_back(vids.size());
    auto cur_v_set_size = cur_vertex_set.Size();

    // for (auto iter : state.cur_vertex_set_) {
    for (auto i = 0; i < cur_v_set_size; ++i) {
      auto adj_list = adj_list_array.get(i);
      for (auto adj : adj_list) {
        // if (edge_filter(adj.properties())) {
        if (std::apply(edge_filter.expr_, adj.properties())) {
          vids.emplace_back(adj.neighbor());
        }
      }
      offset.emplace_back(vids.size());
    }
    VLOG(10) << "vids size: " << vids.size();
    // VLOG(10) << "offset: " << gs::to_string(offset);
    vertex_set_t result_set(std::move(vids), state.other_label_);
    auto pair = std::make_pair(std::move(result_set), std::move(offset));
    return pair;
  }

  /// @brief Directly obtain multiple label vertices from edge.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <typename VERTEX_SET_T, size_t num_labels, typename EDGE_FILTER_T,
            size_t... Is,
            typename std::enable_if<(num_labels != 2)>::type* = nullptr,
            typename RES_T =
                std::pair<GeneralVertexSet<vertex_id_t, label_id_t, num_labels>,
                          std::vector<offset_t>>>
  static RES_T EdgeExpandV(const GRAPH_INTERFACE& graph,
                           const VERTEX_SET_T& cur_vertex_set,
                           Direction direction, label_id_t edge_label,
                           std::array<label_id_t, num_labels>& other_labels,
                           std::array<EDGE_FILTER_T, num_labels>&& edge_filter,
                           std::index_sequence<Is...>) {
    auto tuple = std::make_tuple(EdgeExpandV(
        graph, cur_vertex_set, direction, edge_label,
        std::get<Is>(other_labels), std::move(std::get<Is>(edge_filter)))...);

    size_t offset_array_size = std::get<0>(tuple).second.size();
    // std::vector<offset_t> res_offset(offset_array_size, 0);
    VLOG(10) << "prev set size: " << cur_vertex_set.Size()
             << ", new offset size: " << offset_array_size;
    CHECK(offset_array_size == cur_vertex_set.Size() + 1);
    size_t prev_set_size = cur_vertex_set.Size();

    auto set_offset_array =
        get_set_from_pair_tuple(tuple, std::make_index_sequence<num_labels>());
    auto& offset_arrays = std::get<1>(set_offset_array);
    auto& vertex_sets = std::get<0>(set_offset_array);

    std::vector<vertex_id_t> res_vids;
    std::array<grape::Bitset, num_labels> res_bitset;
    std::vector<offset_t> res_offset;

    size_t total_size = 0;
    for (auto i = 0; i < vertex_sets.size(); ++i) {
      total_size += vertex_sets[i].Size();
    }
    VLOG(10) << "total size: " << total_size;
    res_vids.reserve(total_size);
    res_offset.reserve(prev_set_size + 1);
    for (auto i = 0; i < num_labels; ++i) {
      res_bitset[i].init(total_size);
    }

    size_t cur_ind = 0;
    res_offset.emplace_back(0);
    for (auto i = 0; i < prev_set_size; ++i) {
      for (auto j = 0; j < num_labels; ++j) {
        auto& vec = vertex_sets[j].GetVertices();
        auto start_off = offset_arrays[j][i];
        auto end_off = offset_arrays[j][i + 1];
        for (auto k = start_off; k < end_off; ++k) {
          res_vids.emplace_back(vec[k]);
          res_bitset[j].set_bit(cur_ind);
          cur_ind += 1;
        }
      }
      res_offset.emplace_back(cur_ind);
    }
    CHECK(cur_ind == total_size);
    auto copied_labels(other_labels);
    GeneralVertexSet<vertex_id_t, label_id_t, num_labels> res_set(
        std::move(res_vids), std::move(copied_labels), std::move(res_bitset));

    return std::make_pair(std::move(res_set), std::move(res_offset));
  }

  /// @brief Directly obtain multiple label vertices from edge. specialization
  /// for two label.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <typename... SET_T, size_t num_labels, size_t... Is,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr,
            typename RES_T = std::pair<
                TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType>,
                std::vector<offset_t>>>
  static RES_T EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label,
      std::array<label_id_t, num_labels>& other_labels,
      std::array<Filter<TruePredicate>, num_labels>&& edge_filter,
      std::index_sequence<Is...>) {
    label_id_t src_label, dst_label;

    std::tie(src_label, dst_label) = get_graph_label_pair(
        direction, cur_vertex_set.GetLabel(), other_labels[0]);
    LOG(INFO) << "EdgeExpandV: with two dst labels"
              << gs::to_string(other_labels);

    auto vid_and_offset1 = graph.GetOtherVerticesV2(
        src_label, dst_label, edge_label, cur_vertex_set.GetVertices(),
        gs::to_string(direction), INT_MAX);

    if (direction == Direction::In) {
      src_label = other_labels[1];
    } else {
      dst_label = other_labels[1];
    }

    auto vid_and_offset2 = graph.GetOtherVerticesV2(
        src_label, dst_label, edge_label, cur_vertex_set.GetVertices(),
        gs::to_string(direction), INT_MAX);

    auto& vids1 = vid_and_offset1.first;
    auto& off1 = vid_and_offset1.second;
    auto& vids2 = vid_and_offset2.first;
    auto& off2 = vid_and_offset2.second;
    size_t prev_set_size = cur_vertex_set.Size();
    CHECK(off1.size() == prev_set_size + 1);
    CHECK(off2.size() == prev_set_size + 1);

    std::vector<vertex_id_t> res_vids;
    grape::Bitset res_bitset;

    size_t total_size = vids1.size() + vids2.size();
    VLOG(10) << "total size: " << total_size;
    res_vids.reserve(total_size);
    res_bitset.init(total_size);

    size_t cur = 0;
    for (auto i = 0; i < prev_set_size; ++i) {
      auto start_off = off1[i];
      auto end_off = off1[i + 1];
      for (auto k = start_off; k < end_off; ++k) {
        res_vids.emplace_back(vids1[k]);
        res_bitset.set_bit(cur++);
      }
      start_off = off2[i];
      end_off = off2[i + 1];
      for (auto k = start_off; k < end_off; ++k) {
        res_vids.emplace_back(vids2[k]);
        cur++;
      }
    }
    {
      for (auto i = 0; i < off1.size(); ++i) {
        off1[i] += off2[i];
      }
    }
    CHECK(cur == total_size);
    auto copied_labels(other_labels);
    TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType> res_set(
        std::move(res_vids), std::move(copied_labels), std::move(res_bitset));
    return std::make_pair(std::move(res_set), std::move(off1));
  }

  // Transform tuple to array.
  template <typename... T, size_t... Is>
  static auto get_set_from_pair_tuple(std::tuple<T...>& tuple,
                                      std::index_sequence<Is...>) {
    using set_and_vec_t = typename std::tuple_element_t<0, std::tuple<T...>>;
    using set_t = typename set_and_vec_t::first_type;
    using vec_t = typename set_and_vec_t::second_type;
    auto set_array = std::array<set_t, sizeof...(T)>{
        std::move(std::get<0>(std::get<Is>(tuple)))...};
    auto offset_array = std::array<vec_t, sizeof...(T)>{
        std::move(std::get<1>(std::get<Is>(tuple)))...};
    return std::make_pair(std::move(set_array), std::move(offset_array));
  }

  /////////////////////////////////////////////////////////////////////////////
  /////////////////////////// Edge Expand E ///////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////

  /// @brief Obtain edge set from source vertices. with rvalue vertex set.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T general vertex set
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  //
  template <
      typename... T, size_t num_labels, typename EDGE_FILTER_T,
      typename std::enable_if<sizeof...(T) == 0>::type* = nullptr,
      typename RES_T = std::pair<
          MulLabelSrcGrootEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t,
                                  label_id_t, grape::EmptyType>,
          std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      GeneralVertexSet<vertex_id_t, label_id_t, num_labels>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    static_assert("not implemented");
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         GeneralVertexSet<vertex_id_t, label_id_t, num_labels>,
                         EDGE_FILTER_T>(graph, cur_vertex_set, direction,
                                        edge_label, other_label, props,
                                        edge_filter, limit);

    return EdgeExpandENoPropImpl(state);
  }

  // for input vertex set with only one label.
  template <typename... T, typename... SET_T, typename EDGE_FILTER_T,
            typename std::enable_if<sizeof...(T) == 0>::type* = nullptr,
            typename RES_T = std::pair<AdjEdgeSet<GRAPH_INTERFACE, vertex_id_t,
                                                  label_id_t, grape::EmptyType>,
                                       std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         RowVertexSet<label_id_t, vertex_id_t, SET_T...>,
                         EDGE_FILTER_T>(graph, cur_vertex_set, direction,
                                        edge_label, other_label, props,
                                        edge_filter, limit);

    return EdgeExpandENoPropImpl(state);
  }
  // EdgeExpandE when input vertex are single label. and get multiple props
  template <
      typename... T, typename... SET_T, typename EDGE_FILTER_T,
      typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr,
      typename RES_T = std::pair<FlatEdgeSet<vertex_id_t, label_id_t, 1, T...>,
                                 std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         RowVertexSet<label_id_t, vertex_id_t, SET_T...>,
                         EDGE_FILTER_T, T...>(graph, cur_vertex_set, direction,
                                              edge_label, other_label, props,
                                              edge_filter, limit);
    return EdgeExpandESingleLabelSrcImpl(state);
  }
  // EdgeExpandE when input vertex are single label. and get multiple props
  // Input set is keyedVertexSet.
  template <
      typename... T, typename... SET_T, typename EDGE_FILTER_T,
      typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr,
      typename RES_T = std::pair<FlatEdgeSet<vertex_id_t, label_id_t, 1, T...>,
                                 std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      KeyedRowVertexSet<label_id_t, vertex_id_t, vertex_id_t, SET_T...>&
          cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    LOG(INFO) << "EdgeExpandE from keyed vertex set";
    auto state = EdgeExpandEState<
        GRAPH_INTERFACE,
        KeyedRowVertexSet<label_id_t, vertex_id_t, vertex_id_t, SET_T...>,
        EDGE_FILTER_T, T...>(graph, cur_vertex_set, direction, edge_label,
                             other_label, props, edge_filter, limit);
    return EdgeExpandESingleLabelSrcImpl(state);
  }

  // EdgeExpandE when input vertex are multi label.
  template <
      typename... T, typename VERTEX_SET_T, typename EDGE_FILTER_T,
      typename std::enable_if<(sizeof...(T) > 0) &&
                              VERTEX_SET_T::is_multi_label &&
                              !VERTEX_SET_T::is_general_set>::type* = nullptr,
      typename RES_T = std::pair<
          MulLabelSrcGrootEdgeSet<VERTEX_SET_T::num_labels, GRAPH_INTERFACE,
                                  vertex_id_t, label_id_t, T...>,
          std::vector<offset_t>>>
  static RES_T EdgeExpandE(const GRAPH_INTERFACE& graph,
                           VERTEX_SET_T& cur_vertex_set, Direction direction,
                           label_id_t edge_label, label_id_t other_label,
                           EDGE_FILTER_T& edge_filter,
                           PropNameArray<T...>& props, size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T, T...>(
            graph, cur_vertex_set, direction, edge_label, other_label, props,
            edge_filter, limit);
    return EdgeExpandEMultiLabelSrcImpl(state);
  }

  // EdgeExpandE when input vertex are general set.
  template <
      typename... T, size_t num_labels, typename EDGE_FILTER_T,
      typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr,
      typename RES_T = std::pair<AdjEdgeSet<GRAPH_INTERFACE, vertex_id_t, T...>,
                                 std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      GeneralVertexSet<vertex_id_t, label_id_t, num_labels>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         GeneralVertexSet<vertex_id_t, label_id_t, num_labels>,
                         EDGE_FILTER_T, T...>(graph, cur_vertex_set, direction,
                                              edge_label, other_label, props,
                                              edge_filter, limit);
    return EdgeExpandEGeneralSetImpl(state);
  }

  // EdgeExpand E for two-label vertex set
  template <typename... T, typename... SET_T, typename EDGE_FILTER_T>
  static auto EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      TwoLabelVertexSet<vertex_id_t, label_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         TwoLabelVertexSet<vertex_id_t, label_id_t, SET_T...>,
                         EDGE_FILTER_T, T...>(graph, cur_vertex_set, direction,
                                              edge_label, other_label, props,
                                              edge_filter, limit);
    return EdgeExpandETwoLabelSetImpl(state);
  }

  // EdgeExpand with single src vertex, one edge label, but multiple dst labels,
  // no edge props
  template <
      typename... T, typename... SET_T, size_t num_labels,
      typename EDGE_FILTER_T,
      typename RES_T = std::pair<
          MultiLabelDstEdgeSet<num_labels, GRAPH_INTERFACE, grape::EmptyType>,
          std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label,
      std::array<label_id_t, num_labels> other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T...>& props,
      size_t limit = INT_MAX) {
    auto state = EdgeExpandEMutltiDstState<
        GRAPH_INTERFACE, RowVertexSet<label_id_t, vertex_id_t, SET_T...>,
        num_labels, EDGE_FILTER_T, T...>(graph, cur_vertex_set, direction,
                                         edge_label, other_label, props,
                                         edge_filter, limit);
    return EdgeExpandESingleLabelSrcMutliDstImpl(state);
  }

  // impl EdgeExpandESingleLabelSrcMutliDstImpl, no prop
  template <
      typename VERTEX_SET_T, size_t num_labels, typename EDGE_FILTER_T,
      typename RES_T = std::pair<
          MultiLabelDstEdgeSet<num_labels, GRAPH_INTERFACE, grape::EmptyType>,
          std::vector<offset_t>>>
  static RES_T EdgeExpandESingleLabelSrcMutliDstImpl(
      EdgeExpandEMutltiDstState<GRAPH_INTERFACE, VERTEX_SET_T, num_labels,
                                EDGE_FILTER_T>& state) {
    auto& cur_set = state.cur_vertex_set_;
    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<>;
    std::array<adj_list_array_t, num_labels>
        res_adj_list_arrays;  // one for each dst labels.

    for (auto i = 0; i < num_labels; ++i) {
      label_id_t src_label, dst_label;
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_[i];
        dst_label = cur_set.GetLabel();
      } else {
        src_label = cur_set.GetLabel();
        dst_label = state.other_label_[i];
      }
      LOG(INFO) << "Obtaining edges from " << gs::to_string(src_label) << " to "
                << gs::to_string(dst_label) << " with edge label "
                << gs::to_string(state.edge_label_);
      auto tmp = state.graph_.template GetEdges<>(
          src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
          gs::to_string(state.direction_), state.limit_, {});
      res_adj_list_arrays[i].swap(tmp);
      VLOG(10) << "fetch " << res_adj_list_arrays[i].size() << "edges from "
               << cur_set.GetVertices().size() << "vertices";
    }

    std::vector<size_t> offset;
    size_t prev_set_size = cur_set.Size();
    offset.reserve(prev_set_size + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto i = 0; i < prev_set_size; ++i) {
      for (auto j = 0; j < num_labels; ++j) {
        auto edges = res_adj_list_arrays[j].get(i);
        size += edges.size();  // number of edges in this AdjList
      }
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_vids(cur_set.GetVertices());

    // construct a edge set which contains dst vertices of multiple labels.
    MultiLabelDstEdgeSet<num_labels, GRAPH_INTERFACE, grape::EmptyType>
        edge_set(std::move(copied_vids), std::move(res_adj_list_arrays),
                 state.edge_label_, cur_set.GetLabel(), state.other_label_,
                 state.direction_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

 private:
  template <typename VERTEX_SET_T, typename... SELECTOR>
  static auto EdgeExpandVFromSingleLabel(
      EdgeExpandVState<GRAPH_INTERFACE, VERTEX_SET_T,
                       Filter<TruePredicate, SELECTOR...>>& state) {
    label_id_t src_label, dst_label;
    std::tie(src_label, dst_label) = get_graph_label_pair(
        state.direction_, state.cur_vertex_set_.GetLabel(), state.other_label_);

    VLOG(10) << "[EdgeExpandV]: from label: "
             << std::to_string(state.cur_vertex_set_.GetLabel())
             << ", vertex num: " << std::to_string(state.cur_vertex_set_.Size())
             << ", other label: " << std::to_string(state.other_label_)
             << ",edge label: " << std::to_string(state.edge_label_)
             << "src: " << std::to_string(src_label)
             << ",dst: " << std::to_string(dst_label)
             << ",direction: " << state.direction_;
    auto nbr_list_array = state.graph_.GetOtherVertices(
        src_label, dst_label, state.edge_label_,
        state.cur_vertex_set_.GetVertices(), gs::to_string(state.direction_),
        state.limit_);
    std::vector<vertex_id_t> vids;
    std::vector<offset_t> offset;
    offset.reserve(state.cur_vertex_set_.Size() + 1);
    CHECK(nbr_list_array.size() == state.cur_vertex_set_.Size());
    // first gather size.
    offset.emplace_back(vids.size());
    for (auto i = 0; i < nbr_list_array.size(); ++i) {
      auto nbr_list = nbr_list_array.get(i);
      for (auto nbr : nbr_list) {
        vids.emplace_back(nbr.neighbor());
      }
      offset.emplace_back(vids.size());
    }

    vertex_set_t result_set(std::move(vids), state.other_label_);
    auto pair = std::make_pair(std::move(result_set), std::move(offset));
    return pair;
  }
  // the input src is multilabel.
  // construct a multi label vertex set whose's src are multi label, but dst are
  // same label.
  // required props >= 1
  template <typename... T, typename VERTEX_SET_T, typename EDGE_FILTER_T>
  static auto EdgeExpandEMultiLabelSrcImpl(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T, T...>&
          state) {
    auto prop_names = state.prop_names_;
    VLOG(10) << "[EdgeExpandEMultiLabelSrcImpl]" << prop_names.size();
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    auto& multi_label_set = state.cur_vertex_set_;
    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<T...>;
    std::array<adj_list_array_t, num_labels> res_adj_list_arrays;
    std::array<std::vector<vertex_id_t>, num_labels> vids_arrays;
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    for (auto i = 0; i < num_labels; ++i) {
      auto& cur_set = multi_label_set.GetSet(i);
      vids_arrays[i] = cur_set.GetVertices();
      offset_arrays[i] = multi_label_set.GetOffset(i);
      VLOG(10) << "offset array for: " << i
               << "is: " << gs::to_string(offset_arrays[i]);

      label_id_t src_label, dst_label;
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_;
        dst_label = cur_set.GetLabel();
      } else {
        src_label = cur_set.GetLabel();
        dst_label = state.other_label_;
      }

      auto tmp = state.graph_.template GetEdges<T...>(
          src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
          gs::to_string(state.direction_), state.limit_, prop_names);
      res_adj_list_arrays[i].swap(tmp);
      VLOG(10) << "fetch " << res_adj_list_arrays[i].size() << "edges from "
               << cur_set.GetVertices().size() << "vertices";
    }

    std::vector<size_t> offset;
    offset.reserve(multi_label_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto iter : multi_label_set) {
      auto cur_set_ind = iter.GetCurInd();
      auto inner_ind = iter.GetCurSetInnerInd();
      auto edges = res_adj_list_arrays[cur_set_ind].get(inner_ind);
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_labels = multi_label_set.GetLabels();
    for (auto l : copied_labels) {
      VLOG(10) << l;
    }
    MulLabelSrcGrootEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t,
                            label_id_t, T...>
        edge_set(std::move(vids_arrays), std::move(offset_arrays),
                 std::move(res_adj_list_arrays), prop_names, state.edge_label_,
                 copied_labels, state.other_label_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // the input src is multilabel and is general set.
  // construct a multi label vertex set whose's src are multi label, but dst are
  // same label.
  // required props >= 1
  template <typename... T, typename VERTEX_SET_T, typename EDGE_FILTER_T>
  static auto EdgeExpandEGeneralSetImpl(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T, T...>&
          state) {
    auto prop_names = state.prop_names_;
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    auto& general_set = state.cur_vertex_set_;
    auto total_vertices_num = general_set.Size();
    VLOG(10) << "[EdgeExpandEGeneralSetImpl]" << prop_names.size()
             << ", total vnum: " << total_vertices_num;

    using adj_list_t = typename GRAPH_INTERFACE::template adj_list_t<T...>;
    std::vector<adj_list_t> res_adj_list_arrays(total_vertices_num);
    // overall vid array.
    std::vector<vertex_id_t> vids_arrays(general_set.GetVertices());
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    label_id_t src_label, dst_label;
    if (state.direction_ == Direction::In) {
      src_label = state.other_label_;
      dst_label = general_set.GetLabel();
    } else {
      src_label = general_set.GetLabel();
      dst_label = state.other_label_;
    }

    auto direction_str = gs::to_string(state.direction_);
    for (auto i = 0; i < num_labels; ++i) {
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> cur_active_inds;
      std::tie(cur_vids, cur_active_inds) = general_set.GetVertices(i);
      auto tmp = state.graph_.template GetEdges<T...>(
          src_label, dst_label, state.edge_label_, cur_vids, direction_str,
          state.limit_, prop_names);
      CHECK(tmp.size() == cur_active_inds.size());
      for (auto j = 0; j < cur_active_inds.size(); ++i) {
        res_adj_list_arrays[cur_active_inds[j]] = tmp.get(j);
      }
    }

    std::vector<size_t> offset;
    offset.reserve(general_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto edges : res_adj_list_arrays) {
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_labels(general_set.GetLabels());
    auto copied_bitsets(general_set.GetBitsets());

    GeneralEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t, label_id_t, T...>
        edge_set(std::move(vids_arrays), std::move(res_adj_list_arrays),
                 std::move(copied_bitsets), prop_names, state.edge_label_,
                 copied_labels, state.other_label_, state.direction_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // the input set is two label set, and the result set is one label set
  template <typename... T, typename VERTEX_SET_T, typename EDGE_FILTER_T>
  static auto EdgeExpandETwoLabelSetImpl(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T, T...>&
          state) {
    auto prop_names = state.prop_names_;
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    auto& general_set = state.cur_vertex_set_;
    auto total_vertices_num = general_set.Size();
    VLOG(10) << "[EdgeExpandETwoLabelSetImpl]" << prop_names.size()
             << ", total vnum: " << total_vertices_num;

    using adj_list_t = typename GRAPH_INTERFACE::template adj_list_t<T...>;
    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<T...>;
    // std::vector<adj_list_t>> res_adj_list_arrays(total_vertices_num);
    adj_list_array_t res_adj_list_arrays;
    res_adj_list_arrays.resize(total_vertices_num);
    // overall vid array.
    std::vector<vertex_id_t> vids_arrays(general_set.GetVertices());
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    label_id_t src_label, dst_label;

    auto direction_str = gs::to_string(state.direction_);
    for (auto i = 0; i < num_labels; ++i) {
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_;
        dst_label = general_set.GetLabel(i);
      } else {
        src_label = general_set.GetLabel(i);
        dst_label = state.other_label_;
      }
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> cur_active_inds;
      std::tie(cur_vids, cur_active_inds) = general_set.GetVertices(i);
      auto tmp = state.graph_.template GetEdges<T...>(
          src_label, dst_label, state.edge_label_, cur_vids, direction_str,
          state.limit_, prop_names);
      CHECK(tmp.size() == cur_active_inds.size());
      if constexpr (GRAPH_INTERFACE::is_grape) {
        // for grape graph, we can use operator =, since all data is already in
        // memory
        for (auto j = 0; j < cur_active_inds.size(); ++j) {
          // res_adj_list_arrays[cur_active_inds[j]] = tmp.get(j);
          res_adj_list_arrays.set(cur_active_inds[j], tmp.get(j));
        }
      } else {
        for (auto j = 0; j < cur_active_inds.size(); ++j) {
          res_adj_list_arrays.get_vector(cur_active_inds[j])
              .swap(tmp.get_vector(j));
        }
      }
    }

    std::vector<size_t> offset;
    offset.reserve(general_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto i = 0; i < res_adj_list_arrays.size(); ++i) {
      auto edges = res_adj_list_arrays.get(i);
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_labels(general_set.GetLabels());
    auto& old_bitset = general_set.GetBitset();
    grape::Bitset new_bitset;
    new_bitset.init(old_bitset.cardinality());
    for (auto i = 0; i < old_bitset.cardinality(); ++i) {
      new_bitset.set_bit(i);
    }

    GeneralEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t, label_id_t, T...>
        edge_set(std::move(vids_arrays), std::move(res_adj_list_arrays),
                 std::move(new_bitset), prop_names, state.edge_label_,
                 copied_labels, state.other_label_, state.direction_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // optimize for filter expr is true predicate
  template <typename... T, typename VERTEX_SET_T, typename EDGE_FILTER_T>
  static auto EdgeExpandESingleLabelSrcImpl(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T, T...>&
          state) {
    auto prop_names = state.prop_names_;
    auto& cur_set = state.cur_vertex_set_;
    VLOG(10) << "[EdgeExpandESingleLabelSrcImpl]" << prop_names.size()
             << ", set size: " << cur_set.Size();
    for (auto v : prop_names) {
      VLOG(10) << "prop:" << v;
    }

    label_id_t src_label, dst_label;
    if (state.direction_ == Direction::In) {
      src_label = state.other_label_;
      dst_label = cur_set.GetLabel();
    } else {
      src_label = cur_set.GetLabel();
      dst_label = state.other_label_;
    }

    auto adj_list_array = state.graph_.template GetEdges<T...>(
        src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
        gs::to_string(state.direction_), state.limit_, prop_names);

    std::vector<size_t> offset;
    offset.reserve(cur_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    CHECK(cur_set.Size() == adj_list_array.size());
    std::vector<std::tuple<vertex_id_t, vertex_id_t, std::tuple<T...>>>
        prop_tuples;
    prop_tuples.reserve(cur_set.Size() + 1);
    // Construct offset from adj_list.
    auto cur_set_iter = cur_set.begin();
    auto end_iter = cur_set.end();
    for (auto i = 0; i < adj_list_array.size(); ++i) {
      auto edges = adj_list_array.get(i);
      CHECK(cur_set_iter != end_iter);
      auto src = cur_set_iter.GetVertex();
      for (auto edge : edges) {
        auto& props = edge.properties();
        // current hack impl for edge property
        // TODO: better performance
        if (run_expr_filter(state.edge_filter_.expr_, props)) {
          prop_tuples.emplace_back(
              std::make_tuple(src, edge.neighbor(), props));
        }
      }
      ++cur_set_iter;
      offset.emplace_back(prop_tuples.size());
    }
    VLOG(10) << "num edges: " << prop_tuples.size();
    // VLOG(10) << "offset: array: " << gs::to_string(offset);
    // copy vids
    auto copied_vids(cur_set.GetVertices());
    std::vector<label_id_t> label_vec(prop_tuples.size(), cur_set.GetLabel());
    FlatEdgeSet<vertex_id_t, label_id_t, 1, T...> edge_set(
        std::move(prop_tuples), state.edge_label_, {cur_set.GetLabel()},
        state.other_label_, prop_names, std::move(label_vec), state.direction_);

    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  template <typename FILTER_T, typename... T>
  static inline bool run_expr_filter(const FILTER_T& filter,
                                     const std::tuple<T...>& props) {
    return run_expr_filter(filter, props,
                           std::make_index_sequence<sizeof...(T)>());
  }

  template <typename FILTER_T, typename... T, size_t... Is>
  static inline bool run_expr_filter(const FILTER_T& filter,
                                     const std::tuple<T...>& props,
                                     std::index_sequence<Is...>) {
    return filter(std::get<Is>(props)...);
  }

  // EdgeExpandE for multilabel input vertex set.
  template <
      typename VERTEX_SET_T, typename EDGE_FILTER_T,
      typename std::enable_if<VERTEX_SET_T::is_multi_label &&
                              !VERTEX_SET_T::is_two_label_set>::type* = nullptr>
  static auto EdgeExpandENoPropImpl(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T>& state) {
    // no prop.
    auto prop_names = state.prop_names_;
    VLOG(10) << "[EdgeExpandEMultiLabelSrcImpl]" << prop_names.size();
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<>;
    auto& multi_label_set = state.cur_vertex_set_;

    std::array<adj_list_array_t, num_labels> res_adj_list_arrays;
    std::array<std::vector<vertex_id_t>, num_labels> vids_arrays;
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    for (auto i = 0; i < num_labels; ++i) {
      auto& cur_set = multi_label_set.GetSet(i);
      vids_arrays[i] = cur_set.GetVertices();
      offset_arrays[i] = multi_label_set.GetOffset(i);
      VLOG(10) << "offset array for: " << i
               << "is: " << gs::to_string(offset_arrays[i]);

      label_id_t src_label, dst_label;
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_;
        dst_label = cur_set.GetLabel();
      } else {
        src_label = cur_set.GetLabel();
        dst_label = state.other_label_;
      }

      auto tmp = state.graph_.template GetEdges<>(
          src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
          gs::to_string(state.direction_), state.limit_, prop_names);
      res_adj_list_arrays[i].swap(tmp);
      VLOG(10) << "fetch " << res_adj_list_arrays[i].size() << "edges from "
               << cur_set.GetVertices().size() << "vertices";
    }

    std::vector<size_t> offset;
    offset.reserve(multi_label_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto iter : multi_label_set) {
      auto cur_set_ind = iter.GetCurInd();
      auto inner_ind = iter.GetCurSetInnerInd();
      auto edges = res_adj_list_arrays[cur_set_ind].get(inner_ind);
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_labels = multi_label_set.GetLabels();
    for (auto l : copied_labels) {
      VLOG(10) << l;
    }
    MulLabelSrcGrootEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t,
                            label_id_t, grape::EmptyType>
        edge_set(std::move(vids_arrays), std::move(offset_arrays),
                 std::move(res_adj_list_arrays), state.edge_label_,
                 copied_labels, state.other_label_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // EdgeExpandE for general input vertex set.
  template <
      typename VERTEX_SET_T, typename EDGE_FILTER_T,
      typename std::enable_if<VERTEX_SET_T::is_general_set>::type* = nullptr>
  static auto EdgeExpandENoPropImplForGeneralSet(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T>& state) {
    // no prop.
    auto prop_names = state.prop_names_;
    VLOG(10) << "[EdgeExpandENoPropImpl] for general vertex set of prop size: "
             << prop_names.size();
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<>;
    auto& multi_label_set = state.cur_vertex_set_;

    std::array<adj_list_array_t, num_labels> res_adj_list_arrays;
    std::array<std::vector<vertex_id_t>, num_labels> vids_arrays;
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    for (auto i = 0; i < num_labels; ++i) {
      auto& cur_set = multi_label_set.GetSet(i);
      vids_arrays[i] = cur_set.GetVertices();
      offset_arrays[i] = multi_label_set.GetOffset(i);
      VLOG(10) << "offset array for: " << i
               << "is: " << gs::to_string(offset_arrays[i]);

      label_id_t src_label, dst_label;
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_;
        dst_label = cur_set.GetLabel();
      } else {
        src_label = cur_set.GetLabel();
        dst_label = state.other_label_;
      }

      auto tmp = state.graph_.template GetEdges<>(
          src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
          gs::to_string(state.direction_), state.limit_, prop_names);
      res_adj_list_arrays[i].swap(tmp);
      VLOG(10) << "fetch " << res_adj_list_arrays[i].size() << "edges from "
               << cur_set.GetVertices().size() << "vertices";
    }

    std::vector<size_t> offset;
    offset.reserve(multi_label_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto iter : multi_label_set) {
      auto cur_set_ind = iter.GetCurInd();
      auto inner_ind = iter.GetCurSetInnerInd();
      auto edges = res_adj_list_arrays[cur_set_ind].get(inner_ind);
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_labels = multi_label_set.GetLabels();
    for (auto l : copied_labels) {
      VLOG(10) << l;
    }
    MulLabelSrcGrootEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t,
                            label_id_t, grape::EmptyType>
        edge_set(std::move(vids_arrays), std::move(offset_arrays),
                 std::move(res_adj_list_arrays), state.edge_label_,
                 copied_labels, state.other_label_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // EdgeExpandE for single label input vertex set.
  template <typename... SET_T, typename EDGE_FILTER_T>
  static auto EdgeExpandENoPropImpl(
      EdgeExpandEState<GRAPH_INTERFACE,
                       RowVertexSet<label_id_t, vertex_id_t, SET_T...>,
                       EDGE_FILTER_T>& state) {
    // no prop.
    auto prop_names = state.prop_names_;
    label_id_t src_label, dst_label;
    if (state.direction_ == Direction::In) {
      src_label = state.other_label_;
      dst_label = state.cur_vertex_set_.GetLabel();
    } else {
      src_label = state.cur_vertex_set_.GetLabel();
      dst_label = state.other_label_;
    }
    LOG(INFO) << "[EdgeExpandENoPropImpl] for single label vertex set. "
              << (int) src_label << " " << (int) dst_label;
    auto adj_list_array = state.graph_.template GetEdges<>(
        src_label, dst_label, state.edge_label_,
        state.cur_vertex_set_.GetVertices(), gs::to_string(state.direction_),
        state.limit_, prop_names);
    LOG(INFO) << "after get edges";
    std::vector<offset_t> offset;
    offset.reserve(state.cur_vertex_set_.Size() + 1);
    size_t size = 0;
    size_t adj_list_ind = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (auto iter : state.cur_vertex_set_) {
      auto edges = adj_list_array.get(adj_list_ind);
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
      adj_list_ind++;
    }
    LOG(INFO) << "total size of edges: " << size;
    auto copied_vids(state.cur_vertex_set_.GetVertices());
    auto edge_set =
        AdjEdgeSet<GRAPH_INTERFACE, vertex_id_t, label_id_t, grape::EmptyType>(
            std::move(copied_vids), std::move(adj_list_array),
            state.edge_label_, state.cur_vertex_set_.GetLabel(),
            state.other_label_, state.direction_);
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // only support one property
  template <typename LabelT, typename VERTEX_SET_T, typename EDGE_FILTER_T,
            typename T>
  static auto get_adj_list_array_with_filter(
      EdgeExpandVState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T>& state,
      LabelT src_label, LabelT dst_label,
      std::tuple<PropertySelector<T>>& selectors) {
    return get_adj_list_array_with_filter(state, src_label, dst_label,
                                          std::get<0>(selectors));
  }

  template <typename T, typename LabelT, typename VERTEX_SET_T,
            typename EDGE_FILTER_T>
  static auto get_adj_list_array_with_filter(
      EdgeExpandVState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T>& state,
      LabelT src_label, LabelT dst_label, PropertySelector<T>& selector) {
    VLOG(10) << "before get edges" << gs::to_string(selector.prop_name_);
    std::array<std::string, 1> prop_names = {selector.prop_name_};
    auto adj_list_array = state.graph_.template GetEdges<T>(
        src_label, dst_label, state.edge_label_,
        state.cur_vertex_set_.GetVertices(), gs::to_string(state.direction_),
        state.limit_, prop_names);
    return adj_list_array;
  }

  static std::tuple<label_id_t, label_id_t> get_graph_label_pair(
      Direction& direction, label_id_t query_src_label,
      label_id_t query_dst_label) {
    label_id_t src_label, dst_label;
    if (direction == Direction::In) {
      src_label = query_dst_label;
      dst_label = query_src_label;
    } else {
      src_label = query_src_label;
      dst_label = query_dst_label;
    }
    return std::tuple{src_label, dst_label};
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_EDGE_EXPAND_H_
