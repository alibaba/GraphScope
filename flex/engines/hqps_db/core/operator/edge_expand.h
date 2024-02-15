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
#include "flex/engines/hqps_db/structures/multi_edge_set/untyped_edge_set.h"
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
  EDGE_FILTER_T edge_filter_;
  size_t limit_;

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
        prop_names_(prop_names),
        edge_filter_(edge_filter),
        limit_(limit) {}
};

template <typename GRAPH_INTERFACE, typename VERTEX_SET_T, size_t num_labels,
          typename EDGE_FILTER_T, typename... T>
struct EdgeExpandEMultiDstState {
  const GRAPH_INTERFACE& graph_;
  VERTEX_SET_T& cur_vertex_set_;
  Direction direction_;
  typename GRAPH_INTERFACE::label_id_t edge_label_;
  std::array<typename GRAPH_INTERFACE::label_id_t, num_labels> other_label_;
  const PropNameArray<T...>& prop_names_;
  const EDGE_FILTER_T& edge_filter_;
  size_t limit_;

  EdgeExpandEMultiDstState(
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
    using nbr_list_array_t = typename GRAPH_INTERFACE::nbr_list_array_t;
    nbr_list_array_t nbr_list_array;
    nbr_list_array.resize(state.cur_vertex_set_.Size());

    for (size_t i = 0; i < num_src_labels; ++i) {
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
      for (size_t i = 0; i < active_inds.size(); ++i) {
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
    for (size_t i = 0; i < nbr_list_array.size(); ++i) {
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
  template <typename... SELECTOR, typename... SET_T,
            typename RES_T = std::pair<vertex_set_t, std::vector<offset_t>>>
  static RES_T EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      const GeneralVertexSet<vertex_id_t, label_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      Filter<TruePredicate, SELECTOR...>&& edge_filter,
      size_t limit = INT_MAX) {
    VLOG(10) << "[EdgeExpandV] for general vertex set size: "
             << cur_vertex_set.Size();
    auto state = EdgeExpandVState(graph, cur_vertex_set, direction, edge_label,
                                  other_label, std::move(edge_filter), limit);

    std::vector<vertex_id_t> vids;
    std::vector<offset_t> offset;
    auto src_labels = cur_vertex_set.GetLabels();
    using nbr_list_array_t = typename GRAPH_INTERFACE::nbr_list_array_t;
    nbr_list_array_t nbr_list_array;
    nbr_list_array.resize(state.cur_vertex_set_.Size());

    for (size_t i = 0; i < src_labels.size(); ++i) {
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> active_inds;
      std::tie(cur_vids, active_inds) =
          state.cur_vertex_set_.GetVerticesWithIndex(i);
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
      for (size_t i = 0; i < active_inds.size(); ++i) {
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
    for (size_t i = 0; i < nbr_list_array.size(); ++i) {
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
             << ",dire: " << state.direction_ << ", property name: ";
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
    for (size_t i = 0; i < cur_v_set_size; ++i) {
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
            typename... SET_T, size_t... Is,
            typename std::enable_if<(num_labels != 2)>::type* = nullptr,
            typename RES_T =
                std::pair<GeneralVertexSet<vertex_id_t, label_id_t, SET_T...>,
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
    for (size_t i = 0; i < vertex_sets.size(); ++i) {
      total_size += vertex_sets[i].Size();
    }
    VLOG(10) << "total size: " << total_size;
    res_vids.reserve(total_size);
    res_offset.reserve(prev_set_size + 1);
    for (size_t i = 0; i < num_labels; ++i) {
      res_bitset[i].init(total_size);
    }

    size_t cur_ind = 0;
    res_offset.emplace_back(0);
    for (size_t i = 0; i < prev_set_size; ++i) {
      for (size_t j = 0; j < num_labels; ++j) {
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
    GeneralVertexSet<vertex_id_t, label_id_t, SET_T...> res_set(
        std::move(res_vids), std::move(copied_labels), std::move(res_bitset));

    return std::make_pair(std::move(res_set), std::move(res_offset));
  }

  /// @brief Directly obtain multiple label triplets.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <typename VERTEX_SET_T, typename EDGE_FILTER_T, typename... SET_T>
  static auto EdgeExpandV(
      const GRAPH_INTERFACE& graph, const VERTEX_SET_T& cur_vertex_set,
      Direction direction,
      const std::vector<std::array<label_id_t, 3>>& edge_triplets,
      const EDGE_FILTER_T& edge_filter) {
    CHECK(edge_triplets.size() > 0);
    using result_pair_t = std::pair<vertex_set_t, std::vector<offset_t>>;
    std::vector<result_pair_t> result_pairs;
    for (auto i = 0; i < edge_triplets.size(); ++i) {
      auto copied_filter = edge_filter;
      result_pairs.emplace_back(
          EdgeExpandV(graph, cur_vertex_set, direction, edge_triplets[i][2],
                      edge_triplets[i][1], std::move(copied_filter)));
    }

    size_t offset_array_size = result_pairs[0].second.size();

    VLOG(10) << "prev set size: " << cur_vertex_set.Size()
             << ", new offset size: " << offset_array_size;
    CHECK(offset_array_size == cur_vertex_set.Size() + 1);
    size_t prev_set_size = cur_vertex_set.Size();

    std::vector<vertex_id_t> res_vids;
    std::vector<grape::Bitset> res_bitset;
    std::unordered_map<label_id_t, int32_t> label_to_ind;
    std::vector<offset_t> res_offset;

    size_t num_labels = 0;
    {
      for (auto i = 0; i < edge_triplets.size(); ++i) {
        auto& triplet = edge_triplets[i];
        if (direction == Direction::In || direction == Direction::Both) {
          if (label_to_ind.find(triplet[0]) == label_to_ind.end()) {
            label_to_ind[triplet[0]] = num_labels++;
          }
        }
        if (direction == Direction::Out || direction == Direction::Both) {
          if (label_to_ind.find(triplet[1]) == label_to_ind.end()) {
            label_to_ind[triplet[1]] = num_labels++;
          }
        }
      }
      VLOG(10) << "num labels: " << num_labels;
    }
    res_bitset.resize(num_labels);

    size_t total_size = 0;
    for (size_t i = 0; i < result_pairs.size(); ++i) {
      total_size += result_pairs[i].first.Size();
    }
    VLOG(10) << "total size: " << total_size;
    res_vids.reserve(total_size);
    res_offset.reserve(prev_set_size + 1);
    for (size_t i = 0; i < num_labels; ++i) {
      res_bitset[i].init(total_size);
    }

    size_t cur_ind = 0;
    res_offset.emplace_back(0);
    for (size_t i = 0; i < prev_set_size; ++i) {
      for (size_t j = 0; j < result_pairs.size(); ++j) {
        auto& vertex_set = result_pairs[j].first;
        auto& vertex_set_label = vertex_set.GetLabel();
        CHECK(label_to_ind.find(vertex_set_label) != label_to_ind.end())
            << "label " << vertex_set_label << " not found";
        auto res_label_ind = label_to_ind[vertex_set_label];
        auto& offset_array = result_pairs[j].second;
        auto& vec = vertex_set.GetVertices();
        auto start_off = offset_array[i];
        auto end_off = offset_array[i + 1];
        for (auto k = start_off; k < end_off; ++k) {
          res_vids.emplace_back(vec[k]);
          res_bitset[res_label_ind].set_bit(cur_ind);
          // res_bitset[j].set_bit(cur_ind);
          cur_ind += 1;
        }
      }
      res_offset.emplace_back(cur_ind);
    }
    CHECK(cur_ind == total_size);
    std::vector<label_id_t> copied_labels;
    for (auto pair : label_to_ind) {
      copied_labels.emplace_back(pair.first);
    }
    CHECK(copied_labels.size() == num_labels &&
          res_bitset.size() == num_labels);
    if constexpr (sizeof...(SET_T) > 0) {
      GeneralVertexSet<vertex_id_t, label_id_t, SET_T...> res_set(
          std::move(res_vids), std::move(copied_labels), std::move(res_bitset));

      return std::make_pair(std::move(res_set), std::move(res_offset));
    } else {
      GeneralVertexSet<vertex_id_t, label_id_t, grape::EmptyType> res_set(
          std::move(res_vids), std::move(copied_labels), std::move(res_bitset));
      return std::make_pair(std::move(res_set), std::move(res_offset));
    }
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
    for (size_t i = 0; i < prev_set_size; ++i) {
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
      for (size_t i = 0; i < off1.size(); ++i) {
        off1[i] += off2[i];
      }
    }
    CHECK(cur == total_size);
    auto copied_labels(other_labels);
    TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType> res_set(
        std::move(res_vids), std::move(copied_labels), std::move(res_bitset));
    return std::make_pair(std::move(res_set), std::move(off1));
  }

  /// @brief Expand from multi label vertices and though multi edge labels,
  /// resulting in multilabel vertices
  /// @tparam ...PropTuple
  /// @tparam ...SET_T
  /// @tparam num_pairs
  /// @param graph
  /// @param cur_vertex_set
  /// @param direction
  /// @param edge_labels
  /// @param prop_names
  /// @param edge_filter
  /// @param limit
  /// @return /
  template <size_t num_pairs, typename... PropTuple, typename... SET_T>
  static auto EdgeExpandVMultiTriplet(
      const GRAPH_INTERFACE& graph,
      const GeneralVertexSet<vertex_id_t, label_id_t, SET_T...>& cur_vertex_set,
      Direction& direction,
      std::array<std::array<label_id_t, 3>, num_pairs>& edge_labels,
      std::tuple<PropTupleArrayT<PropTuple>...>& prop_names,
      Filter<TruePredicate>&& edge_filter, size_t limit) {
    // Expand from multi label vertices and though multi edge labels.
    // result in general edge set.
    std::vector<label_id_t> src_v_label_vec = cur_vertex_set.GetLabels();

    LOG(INFO) << "[EdgeExpandVMultiTriplet] real labels triplet size: "
              << edge_labels.size();

    // for each triplet, returns a vector of edge iters.
    auto& vertices = cur_vertex_set.GetVertices();
    std::vector<std::pair<std::vector<vertex_id_t>, std::vector<size_t>>>
        nbr_vertices;
    std::vector<std::vector<vertex_id_t>> tmp_nbr_vertices(vertices.size());
    std::vector<std::vector<uint8_t>> tmp_nbr_labels(vertices.size());

    for (size_t i = 0; i < edge_labels.size(); ++i) {
      // Check whether the edge triplet match input vertices.
      // return a handler to get edges
      std::vector<vertex_id_t> cur_src_vids;
      std::vector<int32_t> cur_active_inds;
      if (direction == Direction::Out || direction == Direction::Both) {
        std::tie(cur_src_vids, cur_active_inds) =
            cur_vertex_set.GetVerticesWithLabel(edge_labels[i][0]);
        expand_other_vertices_and_put_back(
            graph, tmp_nbr_vertices, tmp_nbr_labels, edge_labels[i][0],
            edge_labels[i][1], edge_labels[i][2], Direction::Out, cur_src_vids,
            cur_active_inds);
      } else if (direction == Direction::In || direction == Direction::Both) {
        std::tie(cur_src_vids, cur_active_inds) =
            cur_vertex_set.GetVerticesWithLabel(edge_labels[i][1]);
        expand_other_vertices_and_put_back(
            graph, tmp_nbr_vertices, tmp_nbr_labels, edge_labels[i][0],
            edge_labels[i][1], edge_labels[i][2], Direction::In, cur_src_vids,
            cur_active_inds);
      } else {
        LOG(FATAL) << "not possible";
      }
    }
    std::unordered_map<label_id_t, size_t> appeared_labels;
    {
      // get all unique labels
      for (size_t i = 0; i < tmp_nbr_labels.size(); ++i) {
        for (size_t j = 0; j < tmp_nbr_labels[i].size(); ++j) {
          if (appeared_labels.find(tmp_nbr_labels[i][j]) ==
              appeared_labels.end()) {
            appeared_labels.emplace(tmp_nbr_labels[i][j],
                                    appeared_labels.size());
          }
        }
      }
      VLOG(10) << "[EdgeExpandVMultiTriplet] appeared labels: "
               << appeared_labels.size();
    }

    std::vector<vertex_id_t> res_vids;
    std::vector<grape::Bitset> res_bitset(appeared_labels.size());
    size_t total_vertices = 0;
    {
      for (size_t i = 0; i < tmp_nbr_vertices.size(); ++i) {
        total_vertices += tmp_nbr_vertices[i].size();
      }
    }
    res_vids.reserve(total_vertices);
    for (size_t i = 0; i < res_bitset.size(); ++i) {
      res_bitset[i].init(total_vertices);
    }
    std::vector<offset_t> res_offset;
    res_offset.reserve(tmp_nbr_vertices.size() + 1);
    for (size_t i = 0; i < tmp_nbr_vertices.size(); ++i) {
      res_offset.emplace_back(res_vids.size());
      for (size_t j = 0; j < tmp_nbr_vertices[i].size(); ++j) {
        res_vids.emplace_back(tmp_nbr_vertices[i][j]);
        auto cur_label = tmp_nbr_labels[i][j];
        auto label_ind = appeared_labels[cur_label];
        CHECK(label_ind < res_bitset.size());
        res_bitset[label_ind].set_bit(res_vids.size() - 1);
      }
    }
    res_offset.emplace_back(res_vids.size());
    std::vector<label_t> res_label_vec(appeared_labels.size());
    for (auto iter : appeared_labels) {
      res_label_vec[iter.second] = iter.first;
    }
    auto set = make_general_set(std::move(res_vids), res_label_vec,
                                std::move(res_bitset));

    return std::make_pair(std::move(set), std::move(res_offset));
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

  // Edge ExpandE with multiple edge label triplets. (src, dst, edge)
  // For specified edge labels triplet, only expand from vertices with
  // same src labels.
  // specialization for TruePredicate.
  template <size_t num_pairs, typename... PropTuple, typename... SET_T>
  static auto EdgeExpandEMultiTriplet(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction& direction,
      std::array<std::array<label_id_t, 3>, num_pairs>& edge_labels,
      std::tuple<PropTupleArrayT<PropTuple>...>& prop_names,
      Filter<TruePredicate>&& edge_filter, size_t limit) {
    // Expand from multi label vertices and though multi edge labels.
    // result in general edge set.
    auto src_label = cur_vertex_set.GetLabel();
    LOG(INFO) << "[EdgeExpandEMultiTriplet] real labels: ";
    for (size_t i = 0; i < edge_labels.size(); ++i) {
      LOG(INFO) << std::to_string(edge_labels[i][0]) << " "
                << std::to_string(edge_labels[i][1]) << " "
                << std::to_string(edge_labels[i][2]);
    }

    // for each triplet, returns a vector of edge iters.
    auto& vertices = cur_vertex_set.GetVertices();
    using sub_graph_t = typename GRAPH_INTERFACE::sub_graph_t;
    using edge_iter_t = typename sub_graph_t::iterator;
    std::vector<sub_graph_t> sub_graphs;
    auto prop_names_vec = prop_names_to_vec<PropTuple...>(prop_names);
    for (size_t i = 0; i < edge_labels.size(); ++i) {
      // Check whether the edge triplet match input vertices.
      // return a handler to get edges
      auto sub_graph_vec = graph.GetSubGraph(
          edge_labels[i][0], edge_labels[i][1], edge_labels[i][2],
          gs::to_string(direction), prop_names_vec[i]);
      for (auto sub_graph : sub_graph_vec) {
        sub_graphs.emplace_back(sub_graph);
      }
    }

    std::vector<std::array<label_t, 3>> label_triplets;
    // each vertex.
    // generating offsets array
    {
      label_triplets.reserve(edge_labels.size());
      for (size_t i = 0; i < edge_labels.size(); ++i) {
        label_triplets.emplace_back(edge_labels[i]);
      }
      VLOG(10) << "[EdgeExpandEMultiTriplet] label triplets: ";
      for (size_t i = 0; i < label_triplets.size(); ++i) {
        std::stringstream ss;
        ss << std::to_string(label_triplets[i][0]) << " "
           << std::to_string(label_triplets[i][1]) << " "
           << std::to_string(label_triplets[i][2]);
        VLOG(10) << ss.str();
      }
    }

    std::vector<uint8_t> label_indices(vertices.size(), 0);
    std::vector<label_t> label_vec{src_label};
    std::unordered_map<label_id_t, std::vector<sub_graph_t>> label_to_subgraphs;
    {
      // generate label_to_subgraphs
      label_to_subgraphs.emplace(label_vec[0], std::vector<sub_graph_t>());

      for (size_t i = 0; i < sub_graphs.size(); ++i) {
        auto cur_src_label = sub_graphs[i].GetSrcLabel();
        if (cur_src_label == src_label) {
          label_to_subgraphs[cur_src_label].emplace_back(sub_graphs[i]);
        }
      }
    }

    std::vector<offset_t> offsets;
    {
      // generate offset_array
      std::vector<std::vector<edge_iter_t>> grouped_edge_iters;
      auto& real_sub_graphs = label_to_subgraphs[src_label];
      for (size_t i = 0; i < vertices.size(); ++i) {
        std::vector<edge_iter_t> cur_iters;
        for (size_t j = 0; j < real_sub_graphs.size(); ++j) {
          cur_iters.emplace_back(real_sub_graphs[j].get_edges(vertices[i]));
        }
        grouped_edge_iters.emplace_back(std::move(cur_iters));
      }
      offsets.reserve(vertices.size() + 1);
      offsets.emplace_back(0);
      size_t cur_cnt = 0;
      for (size_t i = 0; i < vertices.size(); ++i) {
        auto& iters = grouped_edge_iters[i];
        for (size_t j = 0; j < iters.size(); ++j) {
          cur_cnt += iters[j].Size();
        }
        offsets.emplace_back(cur_cnt);
      }
      VLOG(10) << "[EdgeExpandEMultiTriplet] offsets: "
               << gs::to_string(offsets);
      LOG(INFO) << "total edge found: " << cur_cnt;
    }

    auto set = UnTypedEdgeSet<vertex_id_t, label_id_t, sub_graph_t>(
        vertices, label_indices, label_vec, std::move(label_to_subgraphs),
        direction);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  /// @brief Expand from multi label vertices and though multi edge labels.
  /// @tparam ...PropTuple
  /// @tparam ...SET_T
  /// @tparam num_pairs
  /// @param graph
  /// @param cur_vertex_set
  /// @param direction
  /// @param edge_labels
  /// @param prop_names
  /// @param edge_filter
  /// @param limit
  /// @return /
  template <size_t num_pairs, typename... PropTuple, typename... SET_T>
  static auto EdgeExpandEMultiTriplet(
      const GRAPH_INTERFACE& graph,
      const GeneralVertexSet<vertex_id_t, label_id_t, SET_T...>& cur_vertex_set,
      Direction& direction,
      std::array<std::array<label_id_t, 3>, num_pairs>& edge_labels,
      std::tuple<PropTupleArrayT<PropTuple>...>& prop_names,
      Filter<TruePredicate>&& edge_filter, size_t limit) {
    // Expand from multi label vertices and though multi edge labels.
    // result in general edge set.
    std::vector<label_id_t> label_vec;
    {
      auto labels = cur_vertex_set.GetLabels();
      label_vec.reserve(labels.size());
      for (size_t i = 0; i < labels.size(); ++i) {
        label_vec.emplace_back(labels[i]);
      }
    }

    LOG(INFO) << "[EdgeExpandEMultiTriplet] real labels: "
              << gs::to_string(edge_labels);

    // for each triplet, returns a vector of edge iters.
    auto& vertices = cur_vertex_set.GetVertices();
    using sub_graph_t = typename GRAPH_INTERFACE::sub_graph_t;
    using edge_iter_t = typename sub_graph_t::iterator;
    std::vector<sub_graph_t> sub_graphs;
    auto prop_names_vec = prop_names_to_vec<PropTuple...>(prop_names);
    for (size_t i = 0; i < edge_labels.size(); ++i) {
      // Check whether the edge triplet match input vertices.
      // return a handler to get edges
      auto sub_graph_vec = graph.GetSubGraph(
          edge_labels[i][0], edge_labels[i][1], edge_labels[i][2],
          gs::to_string(direction), prop_names_vec[i]);
      for (auto sub_graph : sub_graph_vec) {
        sub_graphs.emplace_back(sub_graph);
      }
    }

    std::vector<std::array<label_t, 3>> label_triplets;
    // each vertex.
    // generating offsets array
    {
      label_triplets.reserve(edge_labels.size());
      for (size_t i = 0; i < edge_labels.size(); ++i) {
        label_triplets.emplace_back(edge_labels[i]);
      }
      VLOG(10) << "[EdgeExpandEMultiTriplet] label triplets: ";
      for (size_t i = 0; i < label_triplets.size(); ++i) {
        std::stringstream ss;
        ss << std::to_string(label_triplets[i][0]) << " "
           << std::to_string(label_triplets[i][1]) << " "
           << std::to_string(label_triplets[i][2]);
        VLOG(10) << ss.str();
      }
    }

    std::vector<uint8_t> label_indices = cur_vertex_set.GenerateLabelIndices();
    std::unordered_map<label_id_t, std::vector<sub_graph_t>> label_to_subgraphs;
    {
      // generate label_to_subgraphs
      for (size_t i = 0; i < label_vec.size(); ++i) {
        label_to_subgraphs.emplace(label_vec[i], std::vector<sub_graph_t>());
      }

      for (size_t i = 0; i < sub_graphs.size(); ++i) {
        auto cur_src_label = sub_graphs[i].GetSrcLabel();
        if (std::find(label_vec.begin(), label_vec.end(), cur_src_label) !=
            label_vec.end()) {
          label_to_subgraphs[cur_src_label].emplace_back(sub_graphs[i]);
        }
      }
    }
    VLOG(10) << "[EdgeExpandEMultiTriplet] label_to_subgraphs size: "
             << label_to_subgraphs.size();

    std::vector<offset_t> offsets;
    {
      // generate offset_array
      std::vector<std::vector<edge_iter_t>> grouped_edge_iters(vertices.size());
      for (size_t i = 0; i < label_vec.size(); ++i) {
        auto cur_src_label = label_vec[i];
        // for all this type of vertices, emplace back subgraph
        auto& real_sub_graphs = label_to_subgraphs[cur_src_label];
        for (size_t k = 0; k < vertices.size(); ++k) {
          if (label_indices[k] != i) {
            continue;
          }

          for (size_t j = 0; j < real_sub_graphs.size(); ++j) {
            auto cur_edges = real_sub_graphs[j].get_edges(vertices[k]);
            VLOG(10) << "vid index: " << k << " label ind: " << i
                     << " cur label: " << gs::to_string(cur_src_label)
                     << " real subgraphs:[" << j << "]: " << cur_edges.Size();
            grouped_edge_iters[k].emplace_back(cur_edges);
          }
        }
      }
      offsets.reserve(vertices.size() + 1);
      offsets.emplace_back(0);
      size_t cur_cnt = 0;
      for (size_t i = 0; i < vertices.size(); ++i) {
        auto& iters = grouped_edge_iters[i];
        for (size_t j = 0; j < iters.size(); ++j) {
          cur_cnt += iters[j].Size();
        }
        offsets.emplace_back(cur_cnt);
      }
      VLOG(10) << "[EdgeExpandEMultiTriplet] offsets: "
               << gs::to_string(offsets);
      LOG(INFO) << "total edge found: " << cur_cnt;
    }

    auto set = UnTypedEdgeSet<vertex_id_t, label_id_t, sub_graph_t>(
        vertices, label_indices, label_vec, std::move(label_to_subgraphs),
        direction);
    return std::make_pair(std::move(set), std::move(offsets));
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
  // Currently only support one edge property
  template <typename T, typename... SET_T, typename EDGE_FILTER_T,
            typename RES_T = std::pair<
                SingleLabelEdgeSet<vertex_id_t, label_id_t, std::tuple<T>>,
                std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      RowVertexSet<label_id_t, vertex_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T>& props,
      size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         RowVertexSet<label_id_t, vertex_id_t, SET_T...>,
                         EDGE_FILTER_T, T>(graph, cur_vertex_set, direction,
                                           edge_label, other_label, props,
                                           edge_filter, limit);
    return EdgeExpandESingleLabelSrcImpl(state);
  }
  // EdgeExpandE when input vertex are single label. and get multiple props
  // Input set is keyedVertexSet.
  // Currently only support one edge property
  template <typename T, typename... SET_T, typename EDGE_FILTER_T,
            typename RES_T = std::pair<
                SingleLabelEdgeSet<vertex_id_t, label_id_t, std::tuple<T>>,
                std::vector<offset_t>>>
  static RES_T EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      KeyedRowVertexSet<label_id_t, vertex_id_t, vertex_id_t, SET_T...>&
          cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      EDGE_FILTER_T& edge_filter, PropNameArray<T>& props,
      size_t limit = INT_MAX) {
    LOG(INFO) << "EdgeExpandE from keyed vertex set";
    auto state = EdgeExpandEState<
        GRAPH_INTERFACE,
        KeyedRowVertexSet<label_id_t, vertex_id_t, vertex_id_t, SET_T...>,
        EDGE_FILTER_T, T>(graph, cur_vertex_set, direction, edge_label,
                          other_label, props, edge_filter, limit);
    return EdgeExpandESingleLabelSrcImpl(state);
  }

  // EdgeExpand E for two-label vertex set, with expression.
  template <typename... T, typename... SET_T, typename EDGE_FILTER_T,
            typename... SELECTOR,
            typename std::enable_if<
                !IsTruePredicate<EDGE_FILTER_T>::value>::type* = nullptr>
  static auto EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      TwoLabelVertexSet<vertex_id_t, label_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      Filter<EDGE_FILTER_T, SELECTOR...>& edge_filter,
      PropNameArray<T...>& props, size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         TwoLabelVertexSet<vertex_id_t, label_id_t, SET_T...>,
                         Filter<EDGE_FILTER_T, SELECTOR...>, T...>(
            graph, cur_vertex_set, direction, edge_label, other_label, props,
            edge_filter, limit);
    return EdgeExpandETwoLabelSetImplWithExpr(state);
  }

  // specialization for two label vertex set, with no filter.
  template <typename... T, typename... SET_T, typename EDGE_FILTER_T,
            typename... SELECTOR,
            typename std::enable_if<
                IsTruePredicate<EDGE_FILTER_T>::value>::type* = nullptr>
  static auto EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      TwoLabelVertexSet<vertex_id_t, label_id_t, SET_T...>& cur_vertex_set,
      Direction direction, label_id_t edge_label, label_id_t other_label,
      Filter<EDGE_FILTER_T, SELECTOR...>& edge_filter,
      PropNameArray<T...>& props, size_t limit = INT_MAX) {
    auto state =
        EdgeExpandEState<GRAPH_INTERFACE,
                         TwoLabelVertexSet<vertex_id_t, label_id_t, SET_T...>,
                         Filter<EDGE_FILTER_T, SELECTOR...>, T...>(
            graph, cur_vertex_set, direction, edge_label, other_label, props,
            edge_filter, limit);
    return EdgeExpandETwoLabelSetImplNoExpr(state);
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
    for (size_t i = 0; i < nbr_list_array.size(); ++i) {
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

  // the input set is two label set, and the result set is one label set,
  //  when true predicate is passed, we just return general edge set.
  template <typename... T, typename VERTEX_SET_T>
  static auto EdgeExpandETwoLabelSetImplNoExpr(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, Filter<TruePredicate>,
                       T...>& state) {
    auto prop_names = state.prop_names_;
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    auto& two_label_set = state.cur_vertex_set_;
    auto total_vertices_num = two_label_set.Size();
    VLOG(10) << "[EdgeExpandETwoLabelSetImpl]" << prop_names.size()
             << ", total vnum: " << total_vertices_num;

    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<T...>;
    adj_list_array_t res_adj_list_arrays;
    res_adj_list_arrays.resize(total_vertices_num);
    // overall vid array.
    std::vector<vertex_id_t> vids_arrays(two_label_set.GetVertices());
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    label_id_t src_label, dst_label;

    auto direction_str = gs::to_string(state.direction_);
    for (size_t i = 0; i < num_labels; ++i) {
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_;
        dst_label = two_label_set.GetLabel(i);
      } else if (state.direction_ == Direction::Out) {
        src_label = two_label_set.GetLabel(i);
        dst_label = state.other_label_;
      } else {
        // If direction is both, we need to make sure what is src and what is
        // dst.
        src_label = two_label_set.GetLabel(i);
        dst_label = state.other_label_;
        auto& schema = state.graph_.schema();
        if (!schema.exist(src_label, dst_label, state.edge_label_)) {
          std::swap(src_label, dst_label);
        }
      }
      VLOG(1) << "src label: " << (int) src_label
              << ", dst label: " << (int) dst_label;
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> cur_active_inds;
      std::tie(cur_vids, cur_active_inds) = two_label_set.GetVertices(i);
      auto tmp = state.graph_.template GetEdges<T...>(
          src_label, dst_label, state.edge_label_, cur_vids, direction_str,
          state.limit_, prop_names);
      CHECK(tmp.size() == cur_active_inds.size());
      if (i == 0) {
        // first time, update flag field.
        res_adj_list_arrays.set_flag(tmp.get_flag());
      }

      for (size_t j = 0; j < cur_active_inds.size(); ++j) {
        res_adj_list_arrays.set(cur_active_inds[j], tmp.get(j));
      }
    }

    std::vector<size_t> offset;
    offset.reserve(two_label_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    // Construct offset from adj_list.
    for (size_t i = 0; i < res_adj_list_arrays.size(); ++i) {
      auto edges = res_adj_list_arrays.get(i);
      size += edges.size();  // number of edges in this AdjList
      offset.emplace_back(size);
    }
    VLOG(10) << "num edges: " << size;
    VLOG(10) << "offset: array: " << gs::to_string(offset);
    auto copied_labels(two_label_set.GetLabels());
    auto& old_bitset = two_label_set.GetBitset();
    grape::Bitset new_bitset;
    new_bitset.init(old_bitset.cardinality());
    for (size_t i = 0; i < old_bitset.cardinality(); ++i) {
      new_bitset.set_bit(i);
    }

    auto prop_names_vec = array_to_vec(prop_names);

    GeneralEdgeSet<num_labels, GRAPH_INTERFACE, vertex_id_t, label_id_t,
                   std::tuple<T...>, std::tuple<T...>>
        edge_set(std::move(vids_arrays), std::move(res_adj_list_arrays),
                 std::move(new_bitset), prop_names_vec, state.edge_label_,
                 copied_labels, state.other_label_, state.direction_);
    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // the input set is two label set, and the result set is one label set,
  //  Evaluate the filter expression, producing a flat edge set.
  template <typename... T, typename VERTEX_SET_T, typename FUNC_T,
            typename... SELECTOR>
  static auto EdgeExpandETwoLabelSetImplWithExpr(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T,
                       Filter<FUNC_T, SELECTOR...>, T...>& state) {
    auto prop_names = state.prop_names_;
    static constexpr size_t num_labels = VERTEX_SET_T::num_labels;
    static_assert(num_labels == 2, "num_labels should be 2");
    auto& two_label_set = state.cur_vertex_set_;
    auto total_vertices_num = two_label_set.Size();
    VLOG(10) << "[EdgeExpandETwoLabelSetImplWithExpr]" << prop_names.size()
             << ", total vnum: " << total_vertices_num;

    using adj_list_array_t =
        typename GRAPH_INTERFACE::template adj_list_array_t<T...>;
    adj_list_array_t res_adj_list_arrays;
    res_adj_list_arrays.resize(total_vertices_num);
    // overall vid array.
    std::vector<vertex_id_t> vids_arrays(two_label_set.GetVertices());
    std::array<std::vector<offset_t>, num_labels> offset_arrays;

    label_id_t src_label, dst_label;

    auto direction_str = gs::to_string(state.direction_);
    for (size_t i = 0; i < num_labels; ++i) {
      if (state.direction_ == Direction::In) {
        src_label = state.other_label_;
        dst_label = two_label_set.GetLabel(i);
      } else if (state.direction_ == Direction::Out) {
        src_label = two_label_set.GetLabel(i);
        dst_label = state.other_label_;
      } else {
        // If direction is both, we need to make sure what is src and what is
        // dst.
        src_label = two_label_set.GetLabel(i);
        dst_label = state.other_label_;
        auto& schema = state.graph_.schema();
        if (!schema.exist(src_label, dst_label, state.edge_label_)) {
          std::swap(src_label, dst_label);
        }
      }
      std::vector<vertex_id_t> cur_vids;
      std::vector<int32_t> cur_active_inds;
      std::tie(cur_vids, cur_active_inds) = two_label_set.GetVertices(i);
      auto tmp = state.graph_.template GetEdges<T...>(
          src_label, dst_label, state.edge_label_, cur_vids, direction_str,
          state.limit_, prop_names);
      CHECK(tmp.size() == cur_active_inds.size());
      if (i == 0) {
        // first time, update flag field.
        res_adj_list_arrays.set_flag(tmp.get_flag());
      }
      for (size_t j = 0; j < cur_active_inds.size(); ++j) {
        // res_adj_list_arrays[cur_active_inds[j]] = tmp.get(j);
        res_adj_list_arrays.set(cur_active_inds[j], tmp.get(j));
      }
    }
    using edge_tuple_t = std::tuple<vertex_id_t, vertex_id_t, std::tuple<T...>>;
    std::vector<edge_tuple_t> edge_tuples;
    std::vector<size_t> offset;
    std::vector<label_t> label_inds;
    std::vector<std::array<label_id_t, 3>> label_triplets;

    offset.reserve(two_label_set.Size() + 1);
    offset.emplace_back(0);
    size_t num_pre_edges = 0;
    {
      // Construct offset from adj_list.
      for (size_t i = 0; i < res_adj_list_arrays.size(); ++i) {
        auto edges = res_adj_list_arrays.get(i);
        num_pre_edges += edges.size();  // number of edges in this AdjList
      }
    }
    VLOG(10) << "num edges, before filtering: " << num_pre_edges;
    edge_tuples.reserve(num_pre_edges);
    label_inds.reserve(num_pre_edges);

    auto expr_filter = state.edge_filter_.expr_;
    auto& old_bitset = two_label_set.GetBitset();
    auto labels_vec = two_label_set.GetLabels();
    label_triplets.emplace_back(std::array<label_id_t, 3>{
        labels_vec[0], state.other_label_, state.edge_label_});
    label_triplets.emplace_back(std::array<label_id_t, 3>{
        labels_vec[1], state.other_label_, state.edge_label_});

    for (size_t i = 0; i < res_adj_list_arrays.size(); ++i) {
      auto edges = res_adj_list_arrays.get(i);
      auto src = vids_arrays[i];
      for (auto edge : edges) {
        auto& props = edge.properties();
        // current hack impl for edge property
        if (expr_filter(std::get<0>(props))) {
          edge_tuples.emplace_back(
              std::make_tuple(src, edge.neighbor(), props));
          if (old_bitset.get_bit(i)) {
            label_inds.emplace_back(0);
          } else {
            label_inds.emplace_back(1);
          }
        }
      }
      offset.emplace_back(edge_tuples.size());
    }
    LOG(INFO) << "Got edge tuples: " << edge_tuples.size() << " from "
              << num_pre_edges << " edges";
    std::vector<std::vector<std::string>> vec_vec_prop_names;
    vec_vec_prop_names.emplace_back(array_to_vec(prop_names));
    vec_vec_prop_names.emplace_back(array_to_vec(prop_names));
    auto edge_set = FlatEdgeSet<vertex_id_t, label_id_t, std::tuple<T...>>(
        std::move(edge_tuples), std::move(label_triplets), vec_vec_prop_names,
        std::move(label_inds), state.direction_);

    CHECK(offset.back() == edge_set.Size())
        << "offset: " << offset.back() << ", " << edge_set.Size();
    return std::make_pair(std::move(edge_set), std::move(offset));
  }

  // optimize for filter expr is true predicate
  template <
      typename T, typename VERTEX_SET_T, typename EDGE_FILTER_T,
      typename std::enable_if<VERTEX_SET_T::is_row_vertex_set>::type* = nullptr>
  static auto EdgeExpandESingleLabelSrcImpl(
      EdgeExpandEState<GRAPH_INTERFACE, VERTEX_SET_T, EDGE_FILTER_T, T>&
          state) {
    auto prop_names = state.prop_names_;
    auto& cur_set = state.cur_vertex_set_;
    VLOG(10) << "[EdgeExpandESingleLabelSrcImpl]" << prop_names.size()
             << ", set size: " << cur_set.Size()
             << ", direction: " << gs::to_string(state.direction_);
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

    VLOG(10) << "src label: " << (int) src_label
             << ", dst label: " << (int) dst_label
             << ", edge label: " << (int) state.edge_label_;

    auto adj_list_array = state.graph_.template GetEdges<T>(
        src_label, dst_label, state.edge_label_, cur_set.GetVertices(),
        gs::to_string(state.direction_), state.limit_, prop_names);

    std::vector<size_t> offset;
    offset.reserve(cur_set.Size() + 1);
    size_t size = 0;
    offset.emplace_back(size);
    CHECK(cur_set.Size() == adj_list_array.size())
        << "cur_set.Size(): " << cur_set.Size()
        << ", adj_list_array.size():" << adj_list_array.size();
    std::vector<std::tuple<vertex_id_t, vertex_id_t, std::tuple<T>>>
        prop_tuples;
    prop_tuples.reserve(cur_set.Size() + 1);
    // Construct offset from adj_list.
    auto cur_set_iter = cur_set.begin();
    auto end_iter = cur_set.end();
    for (size_t i = 0; i < adj_list_array.size(); ++i) {
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

    std::array<label_id_t, 3> label_triplet{src_label, dst_label,
                                            state.edge_label_};
    SingleLabelEdgeSet<vertex_id_t, label_id_t, std::tuple<T>> edge_set(
        std::move(prop_tuples), std::move(label_triplet),
        std::vector{array_to_vec(prop_names)}, state.direction_);

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
            state.other_label_, array_to_vec(prop_names), state.direction_);
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

  static void expand_other_vertices_and_put_back(
      const GRAPH_INTERFACE& graph,
      std::vector<std::vector<vertex_id_t>>& ret_nbr_vertices,
      std::vector<std::vector<uint8_t>>& ret_label_vec, label_id_t src_label_id,
      label_id_t dst_label_id, label_id_t edge_label_id,
      const Direction& direction, const std::vector<vertex_id_t>& src_v,
      const std::vector<int32_t> cur_active_inds) {
    CHECK(direction != Direction::Both);
    std::vector<vertex_id_t> dst_vertices;
    std::vector<size_t> tmp_offset;
    std::tie(dst_vertices, tmp_offset) =
        graph.GetOtherVerticesV2(src_label_id, dst_label_id, edge_label_id,
                                 src_v, gs::to_string(direction), INT_MAX);
    // put these vertices into tmp_nbr_vertices
    label_id_t label_id;
    if (direction == Direction::Out) {
      label_id = dst_label_id;
    } else {
      label_id = src_label_id;
    }  // both is not allowed here
    for (size_t j = 0; j < cur_active_inds.size(); ++j) {
      auto cur_ind = cur_active_inds[j];
      auto& cur_vec = ret_nbr_vertices[cur_ind];
      auto& cur_label_vec = ret_label_vec[cur_ind];
      auto start_off = tmp_offset[j];
      auto end_off = tmp_offset[j + 1];
      for (auto k = start_off; k < end_off; ++k) {
        cur_vec.emplace_back(dst_vertices[k]);
        cur_label_vec.emplace_back(label_id);
      }
    }
    VLOG(10) << "Finish expand other vertices for edge triplet direction "
             << direction << ": " << gs::to_string(src_label_id) << ", "
             << gs::to_string(dst_label_id) << ", "
             << gs::to_string(edge_label_id)
             << ", new vertices count: " << tmp_offset.back();
  }

  template <typename... PropTuple, size_t... Is>
  static void emplace_prop_names_to_vec(
      std::vector<std::vector<std::string>>& vec_vec_prop_names,
      std::tuple<PropTupleArrayT<PropTuple>...>& prop_names,
      std::index_sequence<Is...>) {
    (vec_vec_prop_names.emplace_back(array_to_vec(std::get<Is>(prop_names))),
     ...);
  }
  template <typename... PropTuple>
  static std::vector<std::vector<std::string>> prop_names_to_vec(
      std::tuple<PropTupleArrayT<PropTuple>...>& prop_names) {
    std::vector<std::vector<std::string>> vec_vec_prop_names;
    vec_vec_prop_names.reserve(sizeof...(PropTuple));
    emplace_prop_names_to_vec<PropTuple...>(
        vec_vec_prop_names, prop_names,
        std::make_index_sequence<sizeof...(PropTuple)>());
    return vec_vec_prop_names;
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_EDGE_EXPAND_H_
