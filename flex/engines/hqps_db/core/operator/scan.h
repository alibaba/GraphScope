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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_SCAN_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_SCAN_H_

#include <array>
#include <string>

#include "grape/utils/bitset.h"

#include "flex/engines/hqps_db/structures/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"

namespace gs {

// scan for a single vertex
template <typename GRAPH_INTERFACE>
class Scan {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;
  using two_label_set_t =
      TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType>;

  // scan vertex with expression, support label_key in expression,
  template <typename EXPR, typename... SELECTOR>
  static vertex_set_t ScanVertex(const GRAPH_INTERFACE& graph,
                                 const label_id_t& v_label_id,
                                 Filter<EXPR, SELECTOR...>&& filter) {
    auto expr = filter.expr_;
    auto selectors = filter.selectors_;
    auto gids = scan_vertex_with_selector(graph, v_label_id, expr, selectors);
    return make_default_row_vertex_set<vertex_id_t, label_id_t>(std::move(gids),
                                                                v_label_id);
  }

  template <typename EXPR, typename... SELECTOR, size_t num_labels>
  static GeneralVertexSet<vertex_id_t, label_id_t, grape::EmptyType>
  ScanMultiLabelVertex(const GRAPH_INTERFACE& graph,
                       const std::array<label_id_t, num_labels>& labels,
                       Filter<EXPR, SELECTOR...>&& filter) {
    auto expr = filter.expr_;
    auto selectors = filter.selectors_;
    return scan_multi_label_vertex_with_selector(graph, labels, expr,
                                                 selectors);
  }

  /// @brief Scan Vertex from two labels.
  /// @tparam FUNC
  /// @param graph
  /// @param v_label_id
  /// @param e_label_id
  /// @param func
  /// @return
  template <typename EXPR, typename... SELECTOR>
  static two_label_set_t ScanVertex(const GRAPH_INTERFACE& graph,
                                    std::array<label_id_t, 2>&& labels,
                                    Filter<EXPR, SELECTOR...>&& filter) {
    auto expr = filter.expr_;
    auto selectors = filter.selectors_;
    auto gids0 = scan_vertex_with_selector(graph, labels[0], expr, selectors);
    auto gids1 = scan_vertex_with_selector(graph, labels[1], expr, selectors);

    // merge gids0 and gids1
    std::vector<vertex_id_t> gids;
    gids.reserve(gids0.size() + gids1.size());
    gids.insert(gids.end(), gids0.begin(), gids0.end());
    gids.insert(gids.end(), gids1.begin(), gids1.end());

    grape::Bitset bitset;
    bitset.init(gids.size());
    for (size_t i = 0; i < gids0.size(); ++i) {
      bitset.set_bit(i);
    }
    return make_two_label_set(std::move(gids), std::move(labels),
                              std::move(bitset));
  }

  /// @brief Scan vertex with oid
  /// @param graph
  /// @param v_label_id
  /// @param oid
  /// @return
  template <typename OID_T>
  static vertex_set_t ScanVertexWithOid(const GRAPH_INTERFACE& graph,
                                        const label_id_t& v_label_id,
                                        OID_T oid) {
    std::vector<vertex_id_t> gids;
    vertex_id_t vid;
    if (graph.ScanVerticesWithOid(v_label_id, oid, vid)) {
      gids.emplace_back(vid);
    }
    return make_default_row_vertex_set(std::move(gids), v_label_id);
  }

  /// @brief Scan vertex with oid
  /// @param graph
  /// @param v_label_ids
  /// @param oid
  /// @return
  template <typename OID_T, size_t num_labels>
  static auto ScanVertexWithOid(
      const GRAPH_INTERFACE& graph,
      const std::array<label_id_t, num_labels>& v_label_ids, OID_T oid) {
    std::vector<vertex_id_t> gids;
    std::vector<label_id_t> labels_vec;
    std::vector<grape::Bitset> bitsets;
    vertex_id_t vid;
    for (size_t i = 0; i < num_labels; ++i) {
      if (graph.ScanVerticesWithOid(v_label_ids[i], oid, vid)) {
        labels_vec.emplace_back(v_label_ids[i]);
        gids.emplace_back(vid);
      }
    }
    bitsets.resize(labels_vec.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].init(gids.size());
      bitsets[i].set_bit(i);
    }

    return make_general_set(std::move(gids), std::move(labels_vec),
                            std::move(bitsets));
  }

 private:
  template <typename EXPR, typename... SELECTOR, size_t num_labels>
  static GeneralVertexSet<vertex_id_t, label_id_t, grape::EmptyType>
  scan_multi_label_vertex_with_selector(
      const GRAPH_INTERFACE& graph,
      const std::array<label_id_t, num_labels>& labels, const EXPR& expr,
      const std::tuple<SELECTOR...>& selectors) {
    std::vector<vertex_id_t> gids;
    std::vector<size_t> cur_cnt;
    for (size_t i = 0; i < num_labels; ++i) {
      cur_cnt.emplace_back(gids.size());
      auto gids_i =
          scan_vertex_with_selector(graph, labels[i], expr, selectors);
      gids.insert(gids.end(), gids_i.begin(), gids_i.end());
    }
    cur_cnt.emplace_back(gids.size());
    std::vector<grape::Bitset> bitsets(num_labels);
    CHECK(cur_cnt.size() == num_labels + 1);
    for (size_t i = 0; i < num_labels; ++i) {
      bitsets[i].init(cur_cnt.back());
      VLOG(10) << "Scan label " << std::to_string(labels[i])
               << ", vertices cnt: " << cur_cnt[i + 1] - cur_cnt[i];
      for (auto j = cur_cnt[i]; j < cur_cnt[i + 1]; ++j) {
        bitsets[i].set_bit(j);
      }
    }
    auto labels_vec = array_to_vec(labels);
    return make_general_set(std::move(gids), labels_vec, std::move(bitsets));
  }

  template <typename FUNC, typename... PropT>
  static std::vector<vertex_id_t> scan_vertex1_impl(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label_id,
      const FUNC& func, const std::tuple<PropT...>& props) {
    std::vector<vertex_id_t> gids;
    auto filter = [&](vertex_id_t v,
                      const std::tuple<typename PropT::prop_t...>& real_props) {
      if (apply_on_tuple(func, real_props)) {
        gids.push_back(v);
      }
    };

    graph.template ScanVertices(v_label_id, props, filter);
    return gids;
  }

  template <typename FUNC, typename... SELECTOR>
  static std::vector<vertex_id_t> scan_vertex_with_selector(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label_id,
      const FUNC& func, const std::tuple<SELECTOR...>& selectors) {
    std::vector<vertex_id_t> gids;
    auto filter =
        [&](vertex_id_t v,
            const std::tuple<typename SELECTOR::prop_t...>& real_props) {
          if (apply_on_tuple(func, real_props)) {
            gids.push_back(v);
          }
        };

    graph.template ScanVertices(v_label_id, selectors, filter);
    return gids;
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_SCAN_H_
