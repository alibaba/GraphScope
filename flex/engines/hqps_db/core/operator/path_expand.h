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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_PATH_EXPAND_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_PATH_EXPAND_H_

#include <string>

#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/column.h"
#include "grape/utils/bitset.h"

namespace gs {

/**
 * Path Expand expand from vertices to vertices via path.
 * Can result to two different kind of input.
 * - DefaultVertexSet.(EndV)
 * - Path Object.(AllV)
 *
 * Currently we only support path expand with only one edge label and only one
 *dst label.
 * The input vertex set must be of one labe.
 **/

template <typename GRAPH_INTERFACE>
class PathExpand {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  template <typename... T>
  using vertex_set_t = RowVertexSet<label_id_t, vertex_id_t, T...>;
  // Path expand to vertices with columns.

  // PathExpand to vertices with vertex properties also retreived
  template <typename... V_SET_T, typename EXPR, typename LabelT,
            typename EDGE_FILTER_T, typename... T,
            typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr,
            typename RES_SET_T = vertex_set_t<int32_t, T...>,  // int32_t is the
                                                               // length.
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& vertex_set,
      PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T, T...>&& path_expand_opt) {
    //
    auto cur_label = vertex_set.GetLabel();
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto tuple = PathExpandRawVMultiV(
        graph, cur_label, vertex_set.GetVertices(), range, edge_expand_opt);

    auto& vids_vec = std::get<0>(tuple);
    auto tuple_vec = graph.template GetVertexPropsFromVid<T...>(
        cur_label, vids_vec, get_v_opt.props_);
    CHECK(tuple_vec.size() == vids_vec.size());
    // prepend dist info.
    auto new_tuple_vec =
        prepend_tuple(std::move(std::get<1>(tuple)), std::move(tuple_vec));
    auto row_vertex_set = make_row_vertex_set(
        std::move(std::get<0>(tuple)), edge_expand_opt.other_label_,
        std::move(new_tuple_vec), {"dist"});
    return std::make_pair(std::move(row_vertex_set),
                          std::move(std::get<2>(tuple)));
  }

  // PathExpandV for row vertex set as input.
  template <typename... V_SET_T, typename EXPR, typename LabelT,
            typename EDGE_FILTER_T, typename RES_SET_T = vertex_set_t<int32_t>,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const TwoLabelVertexSet<vertex_id_t, LabelT, V_SET_T...>& vertex_set,
      PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T>&& path_expand_opt) {
    //
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;

    std::vector<vertex_id_t> input_v_0, input_v_1;
    std::vector<int32_t> active_ind0, active_ind1;
    std::tie(input_v_0, active_ind0) = vertex_set.GetVertices(0);
    std::tie(input_v_1, active_ind1) = vertex_set.GetVertices(1);

    std::vector<vertex_id_t> vids_vec0, vids_vec1;
    std::vector<int32_t> dist_vec0, dist_vec1;
    std::vector<offset_t> offsets0, offsets1;
    std::tie(vids_vec0, dist_vec0, offsets0) = PathExpandRawVMultiV(
        graph, vertex_set.GetLabel(0), input_v_0, range, edge_expand_opt);
    std::tie(vids_vec1, dist_vec1, offsets1) = PathExpandRawVMultiV(
        graph, vertex_set.GetLabel(1), input_v_1, range, edge_expand_opt);
    // merge to label output together.

    // Default vertex set to vertex set.
    std::vector<vertex_id_t> res_vids;
    std::vector<int32_t> res_dist;
    std::vector<offset_t> res_offsets;
    res_vids.reserve(vids_vec0.size() + vids_vec1.size());
    res_dist.reserve(dist_vec0.size() + dist_vec1.size());
    res_offsets.reserve(offsets0.size() + offsets1.size());
    res_offsets.emplace_back(0);
    auto& bitset = vertex_set.GetBitset();
    auto input_size = vertex_set.GetVertices().size();

    size_t cur_0_cnt = 0, cur_1_cnt = 0;
    CHECK(offsets0.size() + offsets1.size() == input_size + 2);
    for (auto i = 0; i < input_size; ++i) {
      if (bitset.get_bit(i)) {
        CHECK(cur_0_cnt < offsets0.size() - 1);
        auto start = offsets0[cur_0_cnt];
        auto end = offsets0[cur_0_cnt + 1];
        for (auto j = start; j < end; ++j) {
          res_vids.emplace_back(vids_vec0[j]);
          res_dist.emplace_back(dist_vec0[j]);
        }
        cur_0_cnt += 1;
      } else {
        CHECK(cur_1_cnt < offsets1.size() - 1);
        auto start = offsets1[cur_1_cnt];
        auto end = offsets1[cur_1_cnt + 1];
        for (auto j = start; j < end; ++j) {
          res_vids.emplace_back(vids_vec1[j]);
          res_dist.emplace_back(dist_vec1[j]);
        }
        cur_1_cnt += 1;
      }
      res_offsets.emplace_back(res_vids.size());
    }

    auto tuple_vec = single_col_vec_to_tuple_vec(std::move(res_dist));
    auto row_vertex_set =
        make_row_vertex_set(std::move(res_vids), edge_expand_opt.other_label_,
                            std::move(tuple_vec), {"dist"});
    return std::make_pair(std::move(row_vertex_set), std::move(res_offsets));
  }

  // PathExpandV for two_label_vertex set as input.
  template <typename... V_SET_T, typename EXPR, typename LabelT,
            typename EDGE_FILTER_T, typename RES_SET_T = vertex_set_t<Dist>,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& vertex_set,
      PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T>&& path_expand_opt) {
    //
    auto cur_label = vertex_set.GetLabel();
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto tuple = PathExpandRawVMultiV(
        graph, cur_label, vertex_set.GetVertices(), range, edge_expand_opt);

    // Default vertex set to vertex set.
    auto& vids_vec = std::get<0>(tuple);
    auto tuple_vec = single_col_vec_to_tuple_vec(std::move(std::get<1>(tuple)));
    auto row_vertex_set = make_row_vertex_set(std::move(std::get<0>(tuple)),
                                              edge_expand_opt.other_label_,
                                              std::move(tuple_vec), {"dist"});
    return std::make_pair(std::move(row_vertex_set),
                          std::move(std::get<2>(tuple)));
  }

  template <typename LabelT, typename EDGE_FILTER_T, typename... SELECTOR>
  static std::tuple<std::vector<vertex_id_t>, std::vector<Dist>,
                    std::vector<offset_t>>
  PathExpandRawV2ForSingleV(
      const GRAPH_INTERFACE& graph, LabelT src_label,
      const std::vector<vertex_id_t>& src_vertices_vec, Range& range,
      EdgeExpandOpt<LabelT, EDGE_FILTER_T, SELECTOR...>& edge_expand_opt) {
    // auto src_label = vertex_set.GetLabel();
    // auto src_vertices_vec = vertex_set.GetVertices();
    auto src_vertices_size = src_vertices_vec.size();
    vertex_id_t src_id = src_vertices_vec[0];

    std::vector<vertex_id_t> gids;
    std::vector<vertex_id_t> tmp_vec;
    std::vector<offset_t> offsets;
    // std::vector<std::vector<vertex_id_t>> gids;
    // std::vector<std::vector<offset_t>> offsets;
    std::unordered_set<vertex_id_t> visited_vertices;
    std::vector<Dist> dists;

    // init for index 0
    tmp_vec.emplace_back(src_id);
    visited_vertices.insert(src_id);
    if (range.start_ == 0) {
      gids.emplace_back(src_id);
      dists.emplace_back(0);
    }

    double visit_array_time = 0.0;
    for (auto cur_hop = 1; cur_hop < range.limit_; ++cur_hop) {
      double t0 = -grape::GetCurrentTime();
      std::vector<size_t> unused;
      std::tie(tmp_vec, unused) = graph.GetOtherVerticesV2(
          src_label, edge_expand_opt.other_label_, edge_expand_opt.edge_label_,
          tmp_vec, gs::to_string(edge_expand_opt.dir_), INT_MAX);
      // remove duplicate
      size_t limit = 0;
      for (auto i = 0; i < tmp_vec.size(); ++i) {
        if (visited_vertices.find(tmp_vec[i]) == visited_vertices.end()) {
          tmp_vec[limit++] = tmp_vec[i];
        }
      }
      tmp_vec.resize(limit);
      if (cur_hop >= range.start_) {
        // emplace tmp_vec to gids;
        for (auto i = 0; i < tmp_vec.size(); ++i) {
          auto nbr_gid = tmp_vec[i];
          auto insert_res = visited_vertices.insert(nbr_gid);
          if (insert_res.second) {
            gids.emplace_back(nbr_gid);
            dists.emplace_back(cur_hop);
          }
        }
      } else {
        // when cur_hop is not included, we also need to insert vertices into
        // set, to avoid duplicated.
        for (auto i = 0; i < tmp_vec.size(); ++i) {
          auto nbr_gid = tmp_vec[i];
          visited_vertices.insert(nbr_gid);
        }
      }
    }
    LOG(INFO) << "visit array time: " << visit_array_time
              << ", gid size: " << gids.size();
    // select vetices that are in range.
    offsets.emplace_back(0);
    offsets.emplace_back(gids.size());

    return std::make_tuple(std::move(gids), std::move(dists),
                           std::move(offsets));
  }

  // TODO: dedup can be used to speed up the query when the input vertices
  // size if 1.
  // const VERTEX_SET_T& vertex_set,
  template <typename LabelT, typename EDGE_FILTER_T>
  static std::tuple<std::vector<vertex_id_t>, std::vector<Dist>,
                    std::vector<offset_t>>
  PathExpandRawVMultiV(const GRAPH_INTERFACE& graph, LabelT src_label,
                       const std::vector<vertex_id_t>& src_vertices_vec,
                       Range& range,
                       EdgeExpandOpt<LabelT, EDGE_FILTER_T>& edge_expand_opt) {
    // auto src_label = vertex_set.GetLabel();
    // auto src_vertices_vec = vertex_set.GetVertices();
    auto src_vertices_size = src_vertices_vec.size();
    if (src_vertices_size == 1) {
      LOG(INFO)
          << "[NOTE:] PathExpandRawVMultiV is used for single vertex expand, "
             "dedup is enabled.";
      return PathExpandRawV2ForSingleV(graph, src_label, src_vertices_vec,
                                       range, edge_expand_opt);
    }
    std::vector<std::vector<vertex_id_t>> gids;
    std::vector<std::vector<offset_t>> offsets;
    std::unordered_set<vertex_id_t> visited_vertices;

    gids.resize(range.limit_);
    offsets.resize(range.limit_);
    for (auto i = 0; i < range.limit_; ++i) {
      offsets.reserve(src_vertices_size + 1);
    }

    // init for index 0
    gids[0].insert(gids[0].begin(), src_vertices_vec.begin(),
                   src_vertices_vec.end());
    // offsets[0] set with all 1s
    for (auto i = 0; i < src_vertices_size; ++i) {
      offsets[0].emplace_back(i);
    }
    offsets[0].emplace_back(src_vertices_size);
    visited_vertices.insert(src_vertices_vec.begin(), src_vertices_vec.end());

    double visit_array_time = 0.0;
    for (auto cur_hop = 1; cur_hop < range.limit_; ++cur_hop) {
      double t0 = -grape::GetCurrentTime();
      auto pair = graph.GetOtherVerticesV2(
          src_label, edge_expand_opt.other_label_, edge_expand_opt.edge_label_,
          gids[cur_hop - 1], gs::to_string(edge_expand_opt.dir_), INT_MAX);

      gids[cur_hop].swap(pair.first);
      CHECK(gids[cur_hop - 1].size() + 1 == pair.second.size());
      // offsets[cur_hop].swap(pair.second);
      for (auto j = 0; j < offsets[cur_hop - 1].size(); ++j) {
        auto& new_off_vec = pair.second;
        offsets[cur_hop].emplace_back(new_off_vec[offsets[cur_hop - 1][j]]);
      }
      t0 += grape::GetCurrentTime();
      visit_array_time += t0;
    }
    LOG(INFO) << "visit array time: " << visit_array_time;
    // select vetices that are in range.
    std::vector<vertex_id_t> flat_gids;
    std::vector<offset_t> flat_offsets;
    std::vector<Dist> dists;

    {
      size_t flat_size = 0;
      for (auto i = range.start_; i < range.limit_; ++i) {
        flat_size += gids[i].size();
      }
      VLOG(10) << "flat size: " << flat_size;
      flat_gids.reserve(flat_size);
      dists.reserve(flat_size);
      flat_offsets.reserve(src_vertices_size + 1);

      flat_offsets.emplace_back(0);
      // for vertices already appears in [0, range.start_)
      // we add vertices to vertex set, but we don't add them to flat_gids
      // and dists.

      for (auto i = 0; i < src_vertices_size; ++i) {
        // size_t prev_size = flat_gids.size();
        for (auto j = range.start_; j < range.limit_; ++j) {
          auto start = offsets[j][i];
          auto end = offsets[j][i + 1];
          for (auto k = start; k < end; ++k) {
            auto gid = gids[j][k];
            flat_gids.emplace_back(gids[j][k]);
            dists.emplace_back(j);
            // }
          }
        }
        flat_offsets.emplace_back(flat_gids.size());
      }
    }

    return std::make_tuple(std::move(flat_gids), std::move(dists),
                           std::move(flat_offsets));
  }

 private:
  template <typename T, typename... Ts>
  static auto prepend_tuple(std::vector<T>&& first_col,
                            std::vector<std::tuple<Ts...>>&& old_cols) {
    CHECK(first_col.size() == old_cols.size());
    std::vector<std::tuple<T, Ts...>> res_vec;
    res_vec.reserve(old_cols.size());
    for (auto i = 0; i < old_cols.size(); ++i) {
      res_vec.emplace_back(std::tuple_cat(std::make_tuple(first_col[i]),
                                          std::move(old_cols[i])));
    }
    return res_vec;
  }

  template <typename T>
  static auto single_col_vec_to_tuple_vec(std::vector<T>&& vec) {
    std::vector<std::tuple<T>> res_vec;
    res_vec.reserve(vec.size());
    for (auto i = 0; i < vec.size(); ++i) {
      res_vec.emplace_back(std::make_tuple(vec[i]));
    }
    return res_vec;
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_PATH_EXPAND_H_
