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

#ifndef GRAPHSCOPE_OPERATOR_SCAN_H_
#define GRAPHSCOPE_OPERATOR_SCAN_H_

#include <array>
#include <string>

#include "flex/engines/hqps/ds/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/two_label_vertex_set.h"

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
  template <typename FUNC, typename... Selector>
  static vertex_set_t ScanVertexV2(int64_t time_stamp,
                                   const GRAPH_INTERFACE& graph,
                                   const label_id_t& v_label_id,
                                   const FUNC& func,
                                   const std::tuple<Selector...>& selectors) {
    auto gids =
        scan_vertex2_impl(time_stamp, graph, v_label_id, func, selectors);
    return MakeDefaultRowVertexSet<vertex_id_t, label_id_t>(std::move(gids),
                                                            v_label_id);
  }

  // scan vertex with expression, support label_key in expression,
  template <typename FUNC>
  static vertex_set_t ScanVertex(int64_t time_stamp,
                                 const GRAPH_INTERFACE& graph,
                                 const label_id_t& v_label_id, FUNC&& func) {
    auto copied_properties(func.Properties());
    auto gids = scan_vertex1_impl(time_stamp, graph, v_label_id, func,
                                  copied_properties);
    return MakeDefaultRowVertexSet<vertex_id_t, label_id_t>(std::move(gids),
                                                            v_label_id);
  }

  /// @brief Scan Vertex from two labels.
  /// @tparam FUNC
  /// @param time_stamp
  /// @param graph
  /// @param v_label_id
  /// @param e_label_id
  /// @param func
  /// @return
  template <size_t N, typename FUNC>
  static two_label_set_t ScanVertex(int64_t time_stamp,
                                    const GRAPH_INTERFACE& graph,
                                    std::array<label_id_t, N>&& labels,
                                    FUNC&& func) {
    static_assert(N == 2, "ScanVertex only support two labels");
    auto copied_properties(func.Properties());
    auto copied_func(func);
    auto gids0 = scan_vertex1_impl(time_stamp, graph, labels[0], func,
                                   copied_properties);
    auto gids1 = scan_vertex1_impl(time_stamp, graph, labels[1], copied_func,
                                   copied_properties);

    // merge gids0 and gids1
    std::vector<vertex_id_t> gids;
    gids.reserve(gids0.size() + gids1.size());
    gids.insert(gids.end(), gids0.begin(), gids0.end());
    gids.insert(gids.end(), gids1.begin(), gids1.end());

    gs::Bitset bitset;
    bitset.init(gids.size());
    for (auto i = 0; i < gids0.size(); ++i) {
      bitset.set_bit(i);
    }
    return make_two_label_set(std::move(gids), std::move(labels),
                              std::move(bitset));
  }

  /// @brief Scan vertex with oid
  /// @param time_stamp
  /// @param graph
  /// @param v_label_id
  /// @param oid
  /// @return
  static vertex_set_t ScanVertexWithOid(int64_t time_stamp,
                                        const GRAPH_INTERFACE& graph,
                                        const label_id_t& v_label_id,
                                        int64_t oid) {
    std::vector<vertex_id_t> gids;
    gids.emplace_back(graph.ScanVerticesWithOid(time_stamp, v_label_id, oid));
    return MakeDefaultRowVertexSet(std::move(gids), v_label_id);
  }

 private:
  template <typename FUNC, typename... PropT>
  static std::vector<vertex_id_t> scan_vertex1_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      const label_id_t& v_label_id, const FUNC& func,
      const std::tuple<PropT...>& props) {
    std::vector<vertex_id_t> gids;
    auto filter = [&](vertex_id_t v,
                      const std::tuple<typename PropT::prop_t...>& real_props) {
      if (apply_on_tuple(func, real_props)) {
        gids.push_back(v);
      }
    };

    graph.template ScanVertices(time_stamp, v_label_id, props, filter);
    return gids;
  }

  template <typename FUNC, typename... Selector>
  static std::vector<vertex_id_t> scan_vertex2_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      const label_id_t& v_label_id, const FUNC& func,
      const std::tuple<Selector...>& selectors) {
    std::vector<vertex_id_t> gids;
    auto filter =
        [&](vertex_id_t v,
            const std::tuple<typename Selector::prop_t...>& real_props) {
          if (apply_on_tuple(func, real_props)) {
            gids.push_back(v);
          }
        };

    graph.template ScanVerticesV2(time_stamp, v_label_id, selectors, filter);
    return gids;
  }
};

}  // namespace gs

#endif  // GRAPHSCOPE_OPERATOR_SCAN_H_
