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

// define struct FilterNull, which's value is true if FUNC::filter_null is
// defined and value is true
template <typename FUNC, typename Void = void>
struct FilterNull {
  static constexpr bool value = false;
};

template <typename FUNC>
struct FilterNull<FUNC, typename std::enable_if<FUNC::filter_null>::type> {
  static constexpr bool value = FUNC::filter_null;
};

// scan for a single vertex
template <typename GRAPH_INTERFACE>
class Scan {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using gid_t = typename GRAPH_INTERFACE::gid_t;
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
                                        std::vector<OID_T>&& oids) {
    std::vector<vertex_id_t> gids;
    vertex_id_t vid;
    for (auto oid : oids) {
      if (graph.ScanVerticesWithOid(v_label_id, oid, vid)) {
        gids.emplace_back(vid);
      }
    }
    return make_default_row_vertex_set(std::move(gids), v_label_id);
  }

  static vertex_set_t ScanVertexWithGid(const GRAPH_INTERFACE& graph,
                                        const label_id_t& v_label_id,
                                        std::vector<gid_t>&& gids) {
    std::vector<vertex_id_t> lids;
    auto vnum = graph.vertex_num(v_label_id);
    for (auto gid : gids) {
      vertex_id_t vid;
      if (GlobalId::get_label_id(gid) == v_label_id) {
        vid = GlobalId::get_vid(gid);
      }
      if (vid < vnum) {
        lids.emplace_back(vid);
      }
    }
    VLOG(10) << "Scan vertex with gid, label: " << v_label_id
             << ", valid vertices cnt: " << lids.size()
             << ", input cnt: " << gids.size();
    return make_default_row_vertex_set(std::move(lids), v_label_id);
  }

  /// @brief Scan vertex with oid
  /// @param graph
  /// @param v_label_ids
  /// @param oid
  /// @return
  template <typename OID_T, size_t num_labels>
  static auto ScanVertexWithOid(
      const GRAPH_INTERFACE& graph,
      const std::array<label_id_t, num_labels>& v_label_ids,
      std::vector<OID_T>&& oids) {
    std::vector<vertex_id_t> gids;
    std::vector<label_id_t> labels_vec;
    std::vector<grape::Bitset> bitsets;
    vertex_id_t vid;
    for (size_t i = 0; i < num_labels; ++i) {
      for (auto oid : oids) {
        if (graph.ScanVerticesWithOid(v_label_ids[i], oid, vid)) {
          labels_vec.emplace_back(v_label_ids[i]);
          gids.emplace_back(vid);
        }
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

  template <size_t num_labels>
  static auto ScanVertexWithGid(
      const GRAPH_INTERFACE& graph,
      const std::array<label_id_t, num_labels>& v_label_ids,
      std::vector<gid_t>&& gids) {
    std::unordered_map<label_id_t, size_t> label_to_index;

    size_t valid_label_num = 0;
    for (size_t i = 0; i < num_labels; ++i) {
      if (v_label_ids[i] < graph.schema().vertex_label_num()) {
        label_to_index[v_label_ids[i]] = valid_label_num++;
      }
    }

    std::vector<vertex_id_t> lids;
    std::vector<label_id_t> label_ind_vec;
    for (auto gid : gids) {
      auto label_id = GlobalId::get_label_id(gid);
      auto vid = GlobalId::get_vid(gid);
      if (label_to_index.find(label_id) != label_to_index.end()) {
        label_ind_vec.emplace_back(label_to_index[label_id]);
        lids.emplace_back(vid);
      }
    }

    std::vector<grape::Bitset> bitsets;
    bitsets.resize(valid_label_num);
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].init(lids.size());
    }
    for (size_t i = 0; i < label_ind_vec.size(); ++i) {
      bitsets[label_ind_vec[i]].set_bit(i);
    }
    std::vector<label_id_t> labels_vec(valid_label_num);
    for (auto& pair : label_to_index) {
      labels_vec[pair.second] = pair.first;
    }
    return make_general_set(std::move(lids), std::move(labels_vec),
                            std::move(bitsets));
  }

  /// @brief Scan vertex with oid and Expr
  /// @param graph
  /// @param v_label_id
  /// @param oid
  /// @return
  template <typename OID_T, typename EXPR, typename... SELECTOR>
  static vertex_set_t ScanVertexWithOidExpr(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label_id,
      std::vector<OID_T>&& oids, Filter<EXPR, SELECTOR...>&& filter) {
    std::vector<vertex_id_t> gids;
    vertex_id_t vid;
    for (auto oid : oids) {
      if (graph.ScanVerticesWithOid(v_label_id, oid, vid)) {
        gids.emplace_back(vid);
      }
    }

    auto real_gids = filter_vertex_with_selector(
        graph, v_label_id, filter.expr_, filter.selectors_, gids);
    return make_default_row_vertex_set(std::move(real_gids), v_label_id);
  }

  template <typename EXPR, typename... SELECTOR>
  static vertex_set_t ScanVertexWithGidExpr(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label_id,
      std::vector<gid_t>&& gids, Filter<EXPR, SELECTOR...>&& filter) {
    std::vector<vertex_id_t> lids;
    auto vnum = graph.vertex_num(v_label_id);
    for (auto gid : gids) {
      if (GlobalId::get_label_id(gid) == v_label_id) {
        auto vid = GlobalId::get_vid(gid);
        if (vid < vnum) {
          lids.emplace_back(vid);
        }
      }
    }

    auto real_lids = filter_vertex_with_selector(
        graph, v_label_id, filter.expr_, filter.selectors_, lids);
    return make_default_row_vertex_set(std::move(real_lids), v_label_id);
  }

  /// @brief Scan vertex with oid
  /// @param graph
  /// @param v_label_ids
  /// @param oid
  /// @return
  template <typename OID_T, size_t num_labels, typename FilterT>
  static auto ScanVertexWithOidExpr(
      const GRAPH_INTERFACE& graph,
      const std::array<label_id_t, num_labels>& v_label_ids,
      std::vector<OID_T>&& oids, FilterT&& filter) {
    std::vector<vertex_id_t> gids;
    std::vector<label_id_t> labels_vec;
    std::vector<grape::Bitset> bitsets;
    vertex_id_t vid;
    for (size_t i = 0; i < num_labels; ++i) {
      std::vector<vertex_id_t> tmp_gids;
      for (auto oid : oids) {
        if (graph.ScanVerticesWithOid(v_label_ids[i], oid, vid)) {
          tmp_gids.emplace_back(vid);
        }
      }
      auto real_gids = filter_vertex_with_selector(
          graph, v_label_ids[i], filter.expr_, filter.selectors_, tmp_gids);
      for (auto gid : real_gids) {
        labels_vec.emplace_back(v_label_ids[i]);
        gids.emplace_back(gid);
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

  template <size_t num_labels, typename FilterT>
  static auto ScanVertexWithGidExpr(
      const GRAPH_INTERFACE& graph,
      const std::array<label_id_t, num_labels>& v_label_ids,
      std::vector<gid_t>&& gids, FilterT&& filter) {
    std::unordered_map<label_id_t, size_t> label_to_index;
    size_t valid_label_num = 0;
    for (size_t i = 0; i < num_labels; ++i) {
      if (v_label_ids[i] < graph.schema().vertex_label_num()) {
        label_to_index[v_label_ids[i]] = valid_label_num++;
      }
    }

    std::vector<vertex_id_t> lids;
    std::vector<label_id_t> label_ind_vec;
    for (auto gid : gids) {
      auto label_id = GlobalId::get_label_id(gid);
      auto vid = GlobalId::get_vid(gid);
      if (label_to_index.find(label_id) != label_to_index.end() &&
          eval_vertex_with_expr(graph, label_id, filter.expr_,
                                filter.selectors_, vid)) {
        label_ind_vec.emplace_back(label_to_index[label_id]);
        lids.emplace_back(vid);
      }
    }

    std::vector<grape::Bitset> bitsets;
    bitsets.resize(valid_label_num);
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].init(lids.size());
    }
    for (size_t i = 0; i < label_ind_vec.size(); ++i) {
      bitsets[label_ind_vec[i]].set_bit(i);
    }
    std::vector<label_id_t> labels_vec(valid_label_num);
    for (auto& pair : label_to_index) {
      labels_vec[pair.second] = pair.first;
    }
    return make_general_set(std::move(lids), std::move(labels_vec),
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
    // if FUNC has filter_null constexpr member, we can use it to filter
    // vertices
    if constexpr (FilterNull<FUNC>::value) {
      graph.template ScanVertices(v_label_id, selectors, filter, true);
    } else {
      graph.template ScanVertices(v_label_id, selectors, filter, false);
    }
    return gids;
  }

  template <typename FUNC, typename... SELECTOR>
  static inline bool eval_vertex_with_expr(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label_id,
      const FUNC& func, const std::tuple<SELECTOR...>& selectors,
      vertex_id_t vid) {
    std::tuple<typename SELECTOR::prop_t...> real_props;
    if constexpr (sizeof...(SELECTOR) == 0) {
      return apply_on_tuple(func, real_props);
    } else {
      auto columns =
          graph.GetPropertyColumnWithSelectors(v_label_id, selectors);
      if (exists_nullptr_in_tuple(columns)) {
        VLOG(10) << "When scanning for label " << std::to_string(v_label_id)
                 << ", there is null column, using default NULL value";
      }
      get_tuple_from_column_tuple(vid, real_props, columns);
      return apply_on_tuple(func, real_props);
    }
  }

  // Filter the vertex with selector
  template <typename FUNC, typename... SELECTOR>
  static std::vector<vertex_id_t> filter_vertex_with_selector(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label_id,
      const FUNC& func, const std::tuple<SELECTOR...>& selectors,
      std::vector<vertex_id_t>& vids) {
    std::vector<vertex_id_t> gids;
    std::tuple<typename SELECTOR::prop_t...> real_props;
    if constexpr (sizeof...(SELECTOR) == 0) {
      for (auto vid : vids) {
        if (apply_on_tuple(func, real_props)) {
          gids.push_back(vid);
        }
      }
    } else {
      auto columns =
          graph.GetPropertyColumnWithSelectors(v_label_id, selectors);
      if (exists_nullptr_in_tuple(columns)) {
        VLOG(10) << "When scanning for label " << std::to_string(v_label_id)
                 << ", there is null column, using default NULL value";
      }
      for (auto vid : vids) {
        get_tuple_from_column_tuple(vid, real_props, columns);
        if (apply_on_tuple(func, real_props)) {
          gids.push_back(vid);
        }
      }
    }
    return gids;
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_SCAN_H_
