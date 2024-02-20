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
#include "flex/engines/hqps_db/structures/path.h"

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
 * The input vertex set must be of one label.
 **/

template <typename GRAPH_INTERFACE>
class PathExpand {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  template <typename... T>
  using vertex_set_t = RowVertexSet<label_id_t, vertex_id_t, T...>;

  // PathExpandPath
  template <typename... V_SET_T, typename LabelT, typename EDGE_FILTER_T,
            typename VERTEX_FILTER_T>
  static auto PathExpandP(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& vertex_set,
      PathExpandPOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T>&&
          path_expand_opt) {
    // we can choose different path store type with regard to different
    // result_opt
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto& range = path_expand_opt.range_;

    auto cur_label = vertex_set.GetLabel();

    std::vector<offset_t> offsets;
    CompressedPathSet<vertex_id_t, label_id_t> path_set;
    std::tie(path_set, offsets) = path_expand_from_single_label(
        graph, cur_label, vertex_set.GetVertices(), range, edge_expand_opt,
        get_v_opt);

    return std::make_pair(std::move(path_set), std::move(offsets));
  }

  // PathExpand Path with multiple edge triplet.
  template <typename VERTEX_SET_T, typename LabelT, size_t get_v_num_labels,
            typename EDGE_FILTER_T, typename VERTEX_FILTER_T>
  static auto PathExpandP(
      const GRAPH_INTERFACE& graph, const VERTEX_SET_T& vertex_set,
      PathExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T, get_v_num_labels,
                                 VERTEX_FILTER_T>&& path_expand_opt) {
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto& edge_triplets = edge_expand_opt.edge_label_triplets_;
    auto& vertex_other_labels = get_v_opt.v_labels_;
    auto vertex_other_labels_vec = array_to_vec(vertex_other_labels);

    std::vector<std::vector<vertex_id_t>> other_vertices;
    std::vector<std::vector<label_id_t>> other_labels_vec;
    std::vector<std::vector<offset_t>> other_offsets;
    auto& vertices_vec = vertex_set.GetVertices();
    std::vector<label_id_t> src_label_id_vec;
    std::vector<label_id_t> src_labels_set;
    static_assert(VERTEX_SET_T::is_row_vertex_set ||
                      VERTEX_SET_T::is_two_label_set ||
                      VERTEX_SET_T::is_general_set,
                  "Unsupported vertex set type");
    if constexpr (VERTEX_SET_T::is_row_vertex_set) {
      auto src_label = vertex_set.GetLabel();
      src_label_id_vec =
          std::vector<label_id_t>(vertices_vec.size(), vertex_set.GetLabel());
      src_labels_set = {src_label};
    } else if constexpr (VERTEX_SET_T::is_two_label_set) {
      auto src_label_vec = vertex_set.GetLabelVec();
      src_labels_set = array_to_vec(vertex_set.GetLabels());
      src_label_id_vec = label_key_vec_2_label_id_vec(src_label_vec);
    } else {
      src_labels_set = vertex_set.GetLabels();
      src_label_id_vec = label_key_vec_2_label_id_vec(vertex_set.GetLabelVec());
    }
    std::tie(other_vertices, other_labels_vec, other_offsets) =
        path_expandp_multi_triplet(graph, edge_triplets,
                                   vertex_other_labels_vec,
                                   edge_expand_opt.direction_, vertices_vec,
                                   src_labels_set, src_label_id_vec, range);

    // The path are stored in a compressed way, and we flat it.
    std::vector<offset_t> res_offsets;
    res_offsets.reserve(vertices_vec.size() + 1);
    res_offsets.emplace_back(0);

    std::vector<std::vector<Path<vid_t, LabelT>>> cur_path, next_path;
    std::vector<std::vector<Path<vid_t, LabelT>>> prev_path;
    // indexed by src_vid,
    for (size_t i = 0; i < vertices_vec.size(); ++i) {
      std::vector<Path<vid_t, LabelT>> tmp_path;
      tmp_path.emplace_back(vertices_vec[i], src_label_id_vec[i]);
      cur_path.emplace_back(tmp_path);
    }
    prev_path.resize(vertices_vec.size());

    for (auto j = range.start_; j < range.limit_; ++j) {
      next_path.clear();
      auto& cur_offset_vec = other_offsets[j];
      CHECK(cur_path.size() == vertices_vec.size());
      next_path.resize(vertices_vec.size());

      for (auto i = 0; i < vertices_vec.size(); ++i) {
        auto& tmp_path = cur_path[i];
        auto start = cur_offset_vec[i];
        auto end = cur_offset_vec[i + 1];
        for (auto k = start; k < end; ++k) {
          auto& next_vid = other_vertices[j][k];
          auto& label = other_labels_vec[j][k];
          for (auto& path : tmp_path) {
            path.EmplaceBack(next_vid, label);
            next_path[i].emplace_back(path);
            path.PopBack();
          }
        }
        // push all next_path[i] to tmp_path
        prev_path[i].insert(prev_path[i].end(), next_path[i].begin(),
                            next_path[i].end());
        next_path[i].swap(cur_path[i]);
      }
    }

    std::vector<Path<vid_t, LabelT>> res_path;
    for (auto i = 0; i < vertices_vec.size(); ++i) {
      auto& tmp_path = prev_path[i];
      res_path.insert(res_path.end(), tmp_path.begin(), tmp_path.end());
      res_offsets.emplace_back(res_path.size());
    }
    return std::make_pair(PathSet<vertex_id_t, label_id_t>(std::move(res_path)),
                          std::move(res_offsets));
  }

  // Path expand to vertices with columns.
  // PathExpand to vertices with vertex properties also retrieved
  template <typename... V_SET_T, typename VERTEX_FILTER_T, typename LabelT,
            typename EDGE_FILTER_T, typename... T,
            typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr,
            typename RES_SET_T = vertex_set_t<int32_t, T...>,  // int32_t is the
                                                               // length.
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& vertex_set,
      PathExpandVOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T, T...>&&
          path_expand_opt) {
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

  // PathExpandV for two_label_vertex set as input.
  template <typename... V_SET_T, typename VERTEX_FILTER_T, typename LabelT,
            typename EDGE_FILTER_T, typename RES_SET_T = vertex_set_t<int32_t>,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const TwoLabelVertexSet<vertex_id_t, LabelT, V_SET_T...>& vertex_set,
      PathExpandVOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T>&&
          path_expand_opt) {
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
    for (size_t i = 0; i < input_size; ++i) {
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

  // PathExpandV for row vertex set as input, retrieve no properties.
  template <typename... V_SET_T, typename VERTEX_FILTER_T, typename LabelT,
            typename EDGE_FILTER_T, typename RES_SET_T = vertex_set_t<Dist>,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& vertex_set,
      PathExpandVOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T>&&
          path_expand_opt) {
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

  // PathExpandV with multiple dst labels, for row vertex set as input, output
  // no properties.
  template <
      typename... V_SET_T, typename VERTEX_FILTER_T, typename LabelT,
      size_t num_labels, typename EDGE_FILTER_T, size_t get_v_num_labels,
      typename RES_SET_T = GeneralVertexSet<vertex_id_t, label_id_t, Dist>,
      typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& vertex_set,
      PathExpandVMultiDstOpt<LabelT, num_labels, EDGE_FILTER_T,
                             get_v_num_labels, VERTEX_FILTER_T>&&
          path_expand_opt) {
    //
    auto cur_label = vertex_set.GetLabel();
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    return PathExpandMultiDstV(graph, cur_label, vertex_set.GetVertices(),
                               range, edge_expand_opt, get_v_opt);
  }

  // PathExpandV with multiple dst labels, for twoLabelSet vertex set as input,
  // output no properties.
  template <
      typename... V_SET_T, typename VERTEX_FILTER_T, typename LabelT,
      size_t num_labels, typename EDGE_FILTER_T, size_t get_v_num_labels,
      typename RES_SET_T = GeneralVertexSet<vertex_id_t, label_id_t, Dist>,
      typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const TwoLabelVertexSet<vertex_id_t, label_id_t, V_SET_T...>& vertex_set,
      PathExpandVMultiDstOpt<LabelT, num_labels, EDGE_FILTER_T,
                             get_v_num_labels, VERTEX_FILTER_T>&&
          path_expand_opt) {
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto src_label_vec = vertex_set.GetLabelVec();
    auto src_label_set = array_to_vec(vertex_set.GetLabels());
    auto src_label_id_vec = label_key_vec_2_label_id_vec(src_label_vec);
    return PathExpandMultiDstVFromGeneralSet(graph, vertex_set.GetVertices(),
                                             src_label_set, src_label_id_vec,
                                             range, edge_expand_opt, get_v_opt);
  }

  // PathExpandV with multiple dst labels, for general vertex set as input,
  // output no properties.
  template <
      typename... V_SET_T, typename VERTEX_FILTER_T, typename LabelT,
      size_t num_labels, typename EDGE_FILTER_T, size_t get_v_num_labels,
      typename RES_SET_T = GeneralVertexSet<vertex_id_t, label_id_t, Dist>,
      typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      const GeneralVertexSet<vertex_id_t, label_id_t, V_SET_T...>& vertex_set,
      PathExpandVMultiDstOpt<LabelT, num_labels, EDGE_FILTER_T,
                             get_v_num_labels, VERTEX_FILTER_T>&&
          path_expand_opt) {
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto& bitsets = vertex_set.GetBitsets();
    auto src_label_set = vertex_set.GetLabels();
    auto src_label_vec = vertex_set.GetLabelVec();
    auto src_label_id_vec = label_key_vec_2_label_id_vec(src_label_vec);
    return PathExpandMultiDstVFromGeneralSet(graph, vertex_set.GetVertices(),
                                             src_label_set, src_label_id_vec,
                                             range, edge_expand_opt, get_v_opt);
  }

  // PathExpandV with multiple edge triplet as input, output no properties.
  template <
      typename VERTEX_SET_T, typename LabelT, typename VERTEX_FILTER_T,
      typename EDGE_FILTER_T, size_t get_v_num_labels,
      typename RES_SET_T = GeneralVertexSet<vertex_id_t, label_id_t, Dist>,
      typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  static RES_T PathExpandVMultiTriplet(
      const GRAPH_INTERFACE& graph, const VERTEX_SET_T& vertex_set,
      PathExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T, get_v_num_labels,
                                 VERTEX_FILTER_T>&& path_expand_opt) {
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& get_v_opt = path_expand_opt.get_v_opt_;
    auto& edge_triplets = edge_expand_opt.edge_label_triplets_;
    auto& vertex_other_labels = get_v_opt.v_labels_;
    auto vertex_other_labels_vec = array_to_vec(vertex_other_labels);

    std::vector<vertex_id_t> res_vertices;
    std::vector<offset_t> res_offsets;
    std::vector<Dist> res_dists;
    std::vector<label_id_t> res_labels_vec;
    if constexpr (VERTEX_SET_T::is_row_vertex_set) {
      auto src_label = vertex_set.GetLabel();
      auto& vertices_vec = vertex_set.GetVertices();
      std::vector<label_id_t> src_labels_vec(vertices_vec.size(),
                                             vertex_set.GetLabel());
      std::vector<label_id_t> src_labels_set{src_label};
      // expand from row_vertex_set;
      std::tie(res_vertices, res_dists, res_labels_vec, res_offsets) =
          path_expandv_multi_triplet(graph, edge_triplets,
                                     vertex_other_labels_vec,
                                     edge_expand_opt.direction_, vertices_vec,
                                     src_labels_set, src_labels_vec, range);

    } else if constexpr (VERTEX_SET_T::is_two_label_set) {
      // expand from two_label_vertex_set;
      auto& vertices_vec = vertex_set.GetVertices();
      auto src_label_vec = vertex_set.GetLabelVec();
      auto src_label_set = array_to_vec(vertex_set.GetLabels());
      auto src_label_id_vec = label_key_vec_2_label_id_vec(src_label_vec);
      std::tie(res_vertices, res_dists, res_labels_vec, res_offsets) =
          path_expandv_multi_triplet(graph, edge_triplets,
                                     vertex_other_labels_vec,
                                     edge_expand_opt.direction_, vertices_vec,
                                     src_label_set, src_label_id_vec, range);

    } else if constexpr (VERTEX_SET_T::is_general_set) {
      // expand from general_vertex_set;
      auto src_label_set = vertex_set.GetLabels();
      auto src_label_vec = vertex_set.GetLabelVec();
      auto src_label_id_vec = label_key_vec_2_label_id_vec(src_label_vec);
      std::tie(res_vertices, res_dists, res_labels_vec, res_offsets) =
          path_expandv_multi_triplet(
              graph, edge_triplets, vertex_other_labels_vec,
              edge_expand_opt.direction_, vertex_set.GetVertices(),
              src_label_set, src_label_id_vec, range);
    } else {
      // fail at compile time.
      static_assert(VERTEX_SET_T::is_row_vertex_set ||
                        VERTEX_SET_T::is_two_label_set ||
                        VERTEX_SET_T::is_general_set,
                    "Unsupported vertex set type");
    }

    auto res_dist_tuple = single_col_vec_to_tuple_vec(std::move(res_dists));
    std::vector<grape::Bitset> res_bitsets;
    std::vector<label_id_t> label_id_vec;
    std::tie(res_bitsets, label_id_vec) =
        convert_label_id_vec_to_bitsets(res_labels_vec);
    if (label_id_vec.size() > vertex_other_labels.size()) {
      LOG(ERROR) << "Error state: label_id_vec.size(): " << label_id_vec.size()
                 << ", vertex_other_labels.size(): "
                 << vertex_other_labels.size();
    }
    if (res_bitsets.size() != label_id_vec.size()) {
      LOG(ERROR) << "Error state: res_bitsets.size(): " << res_bitsets.size()
                 << ", label_id_vec.size(): " << label_id_vec.size();
    }
    auto set = make_general_set(
        std::move(res_vertices), std::move(res_dist_tuple), {"dist"},
        std::move(label_id_vec), std::move(res_bitsets));
    return std::make_pair(std::move(set), std::move(res_offsets));
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
      std::vector<size_t> unused;
      std::tie(tmp_vec, unused) = graph.GetOtherVerticesV2(
          src_label, edge_expand_opt.other_label_, edge_expand_opt.edge_label_,
          tmp_vec, gs::to_string(edge_expand_opt.dir_), INT_MAX);
      // remove duplicate
      size_t limit = 0;
      for (size_t i = 0; i < tmp_vec.size(); ++i) {
        if (visited_vertices.find(tmp_vec[i]) == visited_vertices.end()) {
          tmp_vec[limit++] = tmp_vec[i];
        }
      }
      tmp_vec.resize(limit);
      if (cur_hop >= range.start_) {
        // emplace tmp_vec to gids;
        for (size_t i = 0; i < tmp_vec.size(); ++i) {
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
        for (size_t i = 0; i < tmp_vec.size(); ++i) {
          auto nbr_gid = tmp_vec[i];
          visited_vertices.insert(nbr_gid);
        }
      }
    }
    LOG(INFO) << "visit array time: " << visit_array_time
              << ", gid size: " << gids.size();
    // select vertices that are in range.
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

    gids.resize(range.limit_);
    offsets.resize(range.limit_);
    for (size_t i = 0; i < range.limit_; ++i) {
      offsets.reserve(src_vertices_size + 1);
    }

    // init for index 0
    gids[0].insert(gids[0].begin(), src_vertices_vec.begin(),
                   src_vertices_vec.end());
    // offsets[0] set with all 1s
    for (size_t i = 0; i < src_vertices_size; ++i) {
      offsets[0].emplace_back(i);
    }
    offsets[0].emplace_back(src_vertices_size);

    double visit_array_time = 0.0;
    for (auto cur_hop = 1; cur_hop < range.limit_; ++cur_hop) {
      double t0 = -grape::GetCurrentTime();
      auto pair = graph.GetOtherVerticesV2(
          src_label, edge_expand_opt.other_label_, edge_expand_opt.edge_label_,
          gids[cur_hop - 1], gs::to_string(edge_expand_opt.dir_), INT_MAX);

      gids[cur_hop].swap(pair.first);
      CHECK(gids[cur_hop - 1].size() + 1 == pair.second.size());
      // offsets[cur_hop].swap(pair.second);
      for (size_t j = 0; j < offsets[cur_hop - 1].size(); ++j) {
        auto& new_off_vec = pair.second;
        offsets[cur_hop].emplace_back(new_off_vec[offsets[cur_hop - 1][j]]);
      }
      t0 += grape::GetCurrentTime();
      visit_array_time += t0;
    }
    LOG(INFO) << "visit array time: " << visit_array_time;
    // select vertices that are in range.
    std::vector<vertex_id_t> flat_gids;
    std::vector<offset_t> flat_offsets;
    std::vector<Dist> dists;

    {
      size_t flat_size = 0;
      for (size_t i = range.start_; i < range.limit_; ++i) {
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

      for (size_t i = 0; i < src_vertices_size; ++i) {
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
  // Expand V from single label vertices, only take vertices.
  // Collect multiple dst label vertices.
  template <typename EDGE_FILTER_FUNC, size_t num_labels,
            typename VERTEX_FILTER_T, size_t get_v_num_labels>
  static auto PathExpandMultiDstV(
      const GRAPH_INTERFACE& graph, label_id_t src_label,
      const std::vector<vertex_id_t>& vertices_vec, const Range& range,
      const EdgeExpandOptMultiLabel<label_id_t, num_labels, EDGE_FILTER_FUNC>&
          edge_opt,
      const GetVOpt<label_id_t, get_v_num_labels, VERTEX_FILTER_T>& get_vopt) {
    // We suppose VERTEX_FILTER is true
    auto& edge_other_labels = edge_opt.other_labels_;
    auto& vertex_other_labels = get_vopt.v_labels_;
    auto edge_other_labels_vec = array_to_vec(edge_other_labels);
    auto vertex_other_labels_vec = array_to_vec(vertex_other_labels);
    auto edge_label = edge_opt.edge_label_;
    std::vector<vertex_id_t> res_vertices;
    std::vector<offset_t> res_offsets;
    std::vector<Dist> res_dists;
    std::vector<label_id_t> res_labels_vec;
    std::vector<label_id_t> src_labels_vec(vertices_vec.size(), src_label);
    std::vector<label_id_t> src_labels_set{src_label};
    std::vector<std::array<label_id_t, 3>> edge_triplets;
    // Since other_labels is extracted when code generating, we reverse back.
    if (edge_opt.direction_ == Direction::Out) {
      for (auto& other_label : edge_other_labels) {
        edge_triplets.emplace_back(
            std::array<label_id_t, 3>{src_label, other_label, edge_label});
      }
    } else {  // In and both.
      for (auto& other_label : edge_other_labels) {
        edge_triplets.emplace_back(
            std::array<label_id_t, 3>{other_label, src_label, edge_label});
      }
    }
    std::tie(res_vertices, res_dists, res_labels_vec, res_offsets) =
        path_expandv_multi_triplet(
            graph, edge_triplets, vertex_other_labels_vec, edge_opt.direction_,
            vertices_vec, src_labels_set, src_labels_vec, range);
    auto res_dist_tuple = single_col_vec_to_tuple_vec(std::move(res_dists));
    std::vector<grape::Bitset> res_bitsets;
    std::vector<label_id_t> label_id_vec;
    std::tie(res_bitsets, label_id_vec) =
        convert_label_id_vec_to_bitsets(res_labels_vec);
    if (label_id_vec.size() > vertex_other_labels.size()) {
      LOG(ERROR) << "Error state: label_id_vec.size(): " << label_id_vec.size()
                 << ", vertex_other_labels.size(): "
                 << vertex_other_labels.size();
    }
    if (res_bitsets.size() != label_id_vec.size()) {
      LOG(ERROR) << "Error state: res_bitsets.size(): " << res_bitsets.size()
                 << ", label_id_vec.size(): " << label_id_vec.size();
    }
    auto set = make_general_set(
        std::move(res_vertices), std::move(res_dist_tuple), {"dist"},
        std::move(label_id_vec), std::move(res_bitsets));
    return std::make_pair(std::move(set), std::move(res_offsets));
  }

  // Expand V from two label vertices.
  // Collect multiple dst label vertices.
  template <typename EDGE_FILTER_FUNC, size_t num_labels,
            typename VERTEX_FILTER_T, size_t get_v_num_labels>
  static auto PathExpandMultiDstVFromGeneralSet(
      const GRAPH_INTERFACE& graph,
      const std::vector<vertex_id_t>& vertices_vec,
      const std::vector<label_id_t>& src_labels_set,
      const std::vector<label_id_t>& src_labels_vec, const Range& range,
      const EdgeExpandOptMultiLabel<label_id_t, num_labels, EDGE_FILTER_FUNC>&
          edge_opt,
      const GetVOpt<label_id_t, get_v_num_labels, VERTEX_FILTER_T>& get_vopt) {
    auto& edge_other_labels = edge_opt.other_labels_;
    auto& vertex_other_labels = get_vopt.v_labels_;
    auto edge_other_labels_vec = array_to_vec(edge_other_labels);
    auto vertex_other_labels_vec = array_to_vec(vertex_other_labels);
    auto edge_label = edge_opt.edge_label_;
    std::vector<vertex_id_t> res_vertices;
    std::vector<offset_t> res_offsets;
    std::vector<Dist> res_dists;
    std::vector<label_id_t> res_labels_vec;
    std::vector<std::array<label_id_t, 3>> edge_triplets;
    if (edge_opt.direction_ == Direction::Out) {
      for (auto& src_label : src_labels_set) {
        for (auto& other_label : edge_other_labels_vec) {
          edge_triplets.emplace_back(
              std::array<label_id_t, 3>{src_label, other_label, edge_label});
        }
      }
    } else {
      for (auto& src_label : src_labels_set) {
        for (auto& other_label : edge_other_labels_vec) {
          edge_triplets.emplace_back(
              std::array<label_id_t, 3>{other_label, src_label, edge_label});
        }
      }
    }
    std::tie(res_vertices, res_dists, res_labels_vec, res_offsets) =
        path_expandv_multi_triplet(
            graph, edge_triplets, vertex_other_labels_vec, edge_opt.direction_,
            vertices_vec, src_labels_set, src_labels_vec, range);
    auto res_dist_tuple = single_col_vec_to_tuple_vec(std::move(res_dists));
    std::vector<grape::Bitset> res_bitsets;
    std::vector<label_id_t> label_id_vec;
    std::tie(res_bitsets, label_id_vec) =
        convert_label_id_vec_to_bitsets(res_labels_vec);
    if (label_id_vec.size() > vertex_other_labels.size()) {
      LOG(ERROR) << "Error state: label_id_vec.size(): " << label_id_vec.size()
                 << ", vertex_other_labels.size(): "
                 << vertex_other_labels.size();
    }
    if (res_bitsets.size() != label_id_vec.size()) {
      LOG(ERROR) << "Error state: res_bitsets.size(): " << res_bitsets.size()
                 << ", label_id_vec.size(): " << label_id_vec.size();
    }
    auto set = make_general_set(
        std::move(res_vertices), std::move(res_dist_tuple), {"dist"},
        std::move(label_id_vec), std::move(res_bitsets));
    return std::make_pair(std::move(set), std::move(res_offsets));
  }

  // Expand Path from single label vertices, only take vertices.
  template <typename EDGE_FILTER_FUNC, typename VERTEX_FILTER_T,
            typename... EDATA_T>
  static auto path_expand_from_single_label(
      const GRAPH_INTERFACE& graph, label_id_t src_label,
      const std::vector<vertex_id_t>& vertices_vec, const Range& range,
      const EdgeExpandOpt<label_id_t, EDGE_FILTER_FUNC>& edge_opt,
      const SimpleGetVNoPropOpt<label_id_t, VERTEX_FILTER_T>& get_vopt) {
    std::vector<std::vector<vertex_id_t>> other_vertices;
    std::vector<std::vector<offset_t>> other_offsets;
    if (edge_opt.other_label_ != src_label) {
      LOG(FATAL) << "PathExpand only support one kind labels along path"
                 << std::to_string(edge_opt.other_label_) << ", "
                 << std::to_string(src_label);
    }

    VLOG(10) << "PathExpand with vertices num: " << vertices_vec.size()
             << " of label: " << std::to_string(src_label)
             << ", range: " << range.start_ << ", " << range.limit_;
    CHECK(range.limit_ > range.start_);
    other_vertices.resize(range.limit_);
    other_offsets.resize(range.limit_);
    // distance 0 is the src vertices itself.
    // init with dist 0
    {
      auto& cur_other_vertices = other_vertices[0];
      auto& cur_other_offsets = other_offsets[0];
      cur_other_vertices.insert(cur_other_vertices.end(), vertices_vec.begin(),
                                vertices_vec.end());

      for (size_t i = 0; i < vertices_vec.size(); ++i) {
        cur_other_offsets.emplace_back(i);
      }
      cur_other_offsets.emplace_back(vertices_vec.size());
    }
    VLOG(10) << " Finish set distance 0 vertices.";

    for (size_t i = 1; i < range.limit_; ++i) {
      auto& cur_other_vertices = other_vertices[i];
      auto& cur_other_offsets = other_offsets[i];
      auto& prev_other_vertices = other_vertices[i - 1];

      std::tie(cur_other_vertices, cur_other_offsets) =
          graph.GetOtherVerticesV2(src_label, edge_opt.other_label_,
                                   edge_opt.edge_label_, prev_other_vertices,
                                   gs::to_string(edge_opt.dir_), INT_MAX);
      VLOG(10) << "PathExpand at distance: " << i << ", got vertices: "
               << "size : " << cur_other_vertices.size();
    }

    // create a copy of other_offsets.
    auto copied_other_offsets(other_offsets);
    std::vector<label_id_t> labels_vec(range.limit_, src_label);
    auto path_set = CompressedPathSet<vertex_id_t, label_id_t>(
        std::move(other_vertices), std::move(other_offsets),
        std::move(labels_vec), range.start_);

    std::vector<std::vector<offset_t>> offset_amplify(
        range.limit_, std::vector<offset_t>(copied_other_offsets[0].size(), 0));
    offset_amplify[0] = copied_other_offsets[0];
    for (size_t i = 1; i < offset_amplify.size(); ++i) {
      for (size_t j = 0; j < offset_amplify[i].size(); ++j) {
        offset_amplify[i][j] =
            copied_other_offsets[i][offset_amplify[i - 1][j]];
      }
    }

    std::vector<size_t> path_num_cnt;
    path_num_cnt.resize(vertices_vec.size() + 1, 0);
    for (size_t i = 0; i < vertices_vec.size(); ++i) {
      for (auto j = range.start_; j < range.limit_; ++j) {
        auto start = offset_amplify[j][i];
        auto end = offset_amplify[j][i + 1];
        path_num_cnt[i] += (end - start);
      }
    }
    std::vector<offset_t> ctx_offsets;
    ctx_offsets.resize(vertices_vec.size() + 1);
    ctx_offsets[0] = 0;
    for (size_t i = 0; i < vertices_vec.size(); ++i) {
      ctx_offsets[i + 1] = ctx_offsets[i] + path_num_cnt[i];
    }
    VLOG(10) << "Ctx offsets: " << gs::to_string(ctx_offsets);

    return std::make_pair(std::move(path_set), std::move(ctx_offsets));
  }

  // expand from vertices, with multiple edge triplets.
  // The intermediate vertices can also have multiple labels, and expand with
  // multiple edge triplet.
  static auto path_expandp_multi_triplet(
      const GRAPH_INTERFACE& graph,
      const std::vector<std::array<label_id_t, 3>>&
          edge_label_triplets,  // src, dst, edge
      const std::vector<label_id_t>& get_v_labels, const Direction& direction,
      const std::vector<vertex_id_t>& vertices_vec,
      const std::vector<label_id_t>& src_labels_set,
      const std::vector<label_id_t>& src_v_labels_vec, const Range& range) {
    std::vector<std::vector<vertex_id_t>> other_vertices;
    std::vector<std::vector<label_id_t>> other_labels_vec;
    std::vector<std::vector<offset_t>> other_offsets;
    other_vertices.resize(range.limit_);
    other_offsets.resize(range.limit_);
    other_labels_vec.resize(range.limit_);
    for (size_t i = 0; i < range.limit_; ++i) {
      other_offsets[i].reserve(vertices_vec.size() + 1);
    }
    other_vertices[0].insert(other_vertices[0].end(), vertices_vec.begin(),
                             vertices_vec.end());
    other_labels_vec[0] = src_v_labels_vec;
    for (size_t i = 0; i < vertices_vec.size(); ++i) {
      other_offsets[0].emplace_back(i);
    }
    other_offsets[0].emplace_back(vertices_vec.size());
    // the input vertices can have many labels. Should be the union of
    // src_vertex_labels and other_labels.
    std::vector<label_id_t> src_label_candidates;
    {
      // insert src and dst labels in edge_label_triplets to
      // src_label_candidates
      for (auto& edge_label_triplet : edge_label_triplets) {
        auto src_label = edge_label_triplet[0];
        auto dst_label = edge_label_triplet[1];
        src_label_candidates.emplace_back(src_label);
        src_label_candidates.emplace_back(dst_label);
      }
      std::sort(src_label_candidates.begin(), src_label_candidates.end());
      // dedup
      auto last =
          std::unique(src_label_candidates.begin(), src_label_candidates.end());
      src_label_candidates.erase(last, src_label_candidates.end());
    }
    VLOG(10) << "src_label_candidates: " << gs::to_string(src_label_candidates);
    // iterate for all hops
    for (size_t cur_hop = 1; cur_hop < range.limit_; ++cur_hop) {
      using nbr_list_type =
          std::pair<std::vector<gs::mutable_csr_graph_impl::Nbr>, label_id_t>;
      std::vector<std::vector<nbr_list_type>> nbr_lists;
      nbr_lists.resize(other_vertices[cur_hop - 1].size());
      std::vector<bool> indicator(other_vertices[cur_hop - 1].size(), false);

      for (auto& src_other_label : src_label_candidates) {
        // for each kind of src vertices, try each edge triplet.
        label_id_t dst_other_label;
        for (auto& edge_triplet : edge_label_triplets) {
          if (direction == Direction::In) {
            if (src_other_label != edge_triplet[1]) {
              continue;
            } else {
              dst_other_label = edge_triplet[0];
            }
          } else if (direction == Direction::Out) {
            if (src_other_label != edge_triplet[0]) {
              continue;
            } else {
              dst_other_label = edge_triplet[1];
            }
          } else {
            // both
            if (src_other_label != edge_triplet[0] &&
                src_other_label != edge_triplet[1]) {
              continue;
            } else {
              if (src_other_label == edge_triplet[0]) {
                dst_other_label = edge_triplet[1];
              } else {
                dst_other_label = edge_triplet[0];
              }
            }
          }
          auto cur_edge_label = edge_triplet[2];

          std::vector<size_t> indices;

          std::vector<vertex_id_t> other_vertices_for_cur_label;
          std::tie(other_vertices_for_cur_label, indices) =
              get_vertices_with_label(other_vertices[cur_hop - 1],
                                      other_labels_vec[cur_hop - 1],
                                      src_other_label);
          if (indices.size() > 0) {
            VLOG(10) << "Get vertices with label: "
                     << std::to_string(src_other_label) << ", "
                     << other_vertices_for_cur_label.size();

            label_id_t real_src_label, real_dst_label;
            if (direction == Direction::Out) {
              real_src_label = src_other_label;
              real_dst_label = dst_other_label;
            } else {
              // in or both.
              real_src_label = dst_other_label;
              real_dst_label = src_other_label;
            }
            auto cur_nbr_list = graph.GetOtherVertices(
                real_src_label, real_dst_label, cur_edge_label,
                other_vertices_for_cur_label, gs::to_string(direction),
                INT_MAX);
            {
              size_t tmp_sum = 0;
              for (size_t i = 0; i < cur_nbr_list.size(); ++i) {
                tmp_sum += cur_nbr_list.get_vector(i).size();
              }
              VLOG(10) << "Get other vertices: " << cur_nbr_list.size()
                       << ", nbr size: " << tmp_sum
                       << ", from: " << std::to_string(real_src_label)
                       << ", to: " << std::to_string(real_dst_label)
                       << ", dst other_label: "
                       << std::to_string(dst_other_label)
                       << ", edge_label: " << std::to_string(cur_edge_label)
                       << ", direction: " << gs::to_string(direction);
            }

            for (size_t i = 0; i < indices.size(); ++i) {
              auto index = indices[i];
              nbr_lists[index].emplace_back(cur_nbr_list.get_vector(i),
                                            dst_other_label);
              indicator[index] = true;
            }

          } else {
            VLOG(10) << "No vertices with label: "
                     << std::to_string(src_other_label);
          }
        }
      }
      // extract vertices from nbrs, and add them to other_vertices[cur_hop]
      // and update other_offset
      auto& cur_other_vertices = other_vertices[cur_hop];
      auto& cur_other_offsets =
          other_offsets[cur_hop];  // other_offset is always aligned with
                                   // src_vertices.
      auto& cur_other_labels_vec = other_labels_vec[cur_hop];
      size_t cur_hop_new_vnum = 0;
      for (size_t i = 0; i < nbr_lists.size(); ++i) {
        for (auto& nbr_list_pair : nbr_lists[i]) {
          cur_hop_new_vnum += nbr_list_pair.first.size();
        }
      }
      VLOG(10) << "cur_hop_new_vnum: " << cur_hop_new_vnum;
      cur_other_vertices.reserve(cur_hop_new_vnum);
      // cur_other_offsets.reserve(cur_hop_new_vnum);
      cur_other_labels_vec.reserve(cur_hop_new_vnum);
      cur_other_offsets.reserve(vertices_vec.size() + 1);
      // cur_other_offsets.emplace_back(0);
      std::vector<offset_t> tmp_cur_offset;
      tmp_cur_offset.reserve(cur_hop_new_vnum);
      tmp_cur_offset.emplace_back(0);
      size_t cur_cnt = 0;
      for (size_t i = 0; i < nbr_lists.size(); ++i) {
        for (auto& nbr_list_pair : nbr_lists[i]) {
          auto cur_other_vertex_label = nbr_list_pair.second;
          auto& nbr_list = nbr_list_pair.first;
          for (size_t j = 0; j < nbr_list.size(); ++j) {
            auto& nbr = nbr_list[j];
            cur_other_vertices.emplace_back(nbr.neighbor());
            cur_other_labels_vec.emplace_back(cur_other_vertex_label);
          }
          cur_cnt += nbr_list.size();
        }
        tmp_cur_offset.emplace_back(cur_cnt);
      }
      for (size_t i = 0; i < other_offsets[cur_hop - 1].size(); ++i) {
        other_offsets[cur_hop].emplace_back(
            tmp_cur_offset[other_offsets[cur_hop - 1][i]]);
      }
    }
    return std::make_tuple(std::move(other_vertices),
                           std::move(other_labels_vec),
                           std::move(other_offsets));
  }

  static auto path_expandv_multi_triplet(
      const GRAPH_INTERFACE& graph,
      const std::vector<std::array<label_id_t, 3>>&
          edge_label_triplets,  // src, dst, edge
      const std::vector<label_id_t>& get_v_labels, const Direction& direction,
      const std::vector<vertex_id_t>& vertices_vec,
      const std::vector<label_id_t>& src_labels_set,
      const std::vector<label_id_t>& src_v_labels_vec, const Range& range) {
    // (range, other_label_ind, vertices)
    LOG(INFO) << "PathExpandV with multiple edge triplets: "
              << gs::to_string(edge_label_triplets)
              << ", direction: " << gs::to_string(direction)
              << ", vertices size: " << vertices_vec.size()
              << ", src_labels_set: " << gs::to_string(src_labels_set)
              << ", range: " << range.start_ << ", " << range.limit_;

    std::vector<std::vector<vertex_id_t>> other_vertices;
    std::vector<std::vector<label_id_t>> other_labels_vec;
    std::vector<std::vector<offset_t>> other_offsets;

    std::tie(other_vertices, other_labels_vec, other_offsets) =
        path_expandp_multi_triplet(graph, edge_label_triplets, get_v_labels,
                                   direction, vertices_vec, src_labels_set,
                                   src_v_labels_vec, range);

    // select vertices that are in range and are in vertex_other_labels.
    std::vector<vertex_id_t> res_vertices;
    std::vector<offset_t> res_offsets;
    std::vector<Dist> res_dists;
    std::vector<label_id_t> res_labels_vec;
    std::vector<bool> valid_labels(sizeof(label_id_t) * 8, false);
    for (auto& v_label : get_v_labels) {
      valid_labels[v_label] = true;
    }
    auto num_valid_labels =
        std::accumulate(valid_labels.begin(), valid_labels.end(), 0);
    VLOG(10) << "Select vertices within " << num_valid_labels
             << " valid labels, from " << get_v_labels.size();

    size_t flat_size = 0;
    for (size_t i = range.start_; i < range.limit_; ++i) {
      flat_size += other_vertices[i].size();
    }
    VLOG(10) << "PathExpandV with multiple triplet flat size: " << flat_size;
    res_vertices.reserve(flat_size);
    res_dists.reserve(flat_size);
    res_labels_vec.reserve(flat_size);
    res_offsets.reserve(vertices_vec.size() + 1);
    res_offsets.emplace_back(0);
    for (size_t i = 0; i < vertices_vec.size(); ++i) {
      for (auto j = range.start_; j < range.limit_; ++j) {
        auto start = other_offsets[j][i];
        auto end = other_offsets[j][i + 1];
        for (auto k = start; k < end; ++k) {
          auto gid = other_vertices[j][k];
          auto label = other_labels_vec[j][k];
          if (valid_labels[label]) {
            res_vertices.emplace_back(gid);
            res_dists.emplace_back(j);
            res_labels_vec.emplace_back(label);
          }
        }
      }
      res_offsets.emplace_back(res_vertices.size());
    }
    return std::make_tuple(std::move(res_vertices), std::move(res_dists),
                           std::move(res_labels_vec), std::move(res_offsets));
  }

  // returns the vector of valid labels and the bitsets.
  static std::pair<std::vector<grape::Bitset>, std::vector<label_id_t>>
  convert_label_id_vec_to_bitsets(const std::vector<label_id_t>& label_vec) {
    // convert label_id_vec to bitsets.
    std::vector<grape::Bitset> res_bitsets;
    std::vector<label_id_t> res_label_id_vec;
    std::vector<int32_t>
        label_to_index;  // label to index in res_bitsets vector.
    label_to_index.resize(sizeof(label_id_t) * 8, -1);
    for (size_t i = 0; i < label_vec.size(); ++i) {
      if (label_to_index[label_vec[i]] == -1) {
        label_to_index[label_vec[i]] = res_bitsets.size();
        res_bitsets.emplace_back();
        res_bitsets.back().init(label_vec.size());
        res_label_id_vec.emplace_back(label_vec[i]);
      }
    }
    auto num_valid_labels = res_bitsets.size();
    VLOG(10) << "num valid labels: " << num_valid_labels;

    for (size_t i = 0; i < label_vec.size(); ++i) {
      auto index = label_to_index[label_vec[i]];
      res_bitsets[index].set_bit(i);
    }
    return std::make_pair(std::move(res_bitsets), std::move(res_label_id_vec));
  }

  template <typename T, typename... Ts>
  static auto prepend_tuple(std::vector<T>&& first_col,
                            std::vector<std::tuple<Ts...>>&& old_cols) {
    CHECK(first_col.size() == old_cols.size());
    std::vector<std::tuple<T, Ts...>> res_vec;
    res_vec.reserve(old_cols.size());
    for (size_t i = 0; i < old_cols.size(); ++i) {
      res_vec.emplace_back(std::tuple_cat(std::make_tuple(first_col[i]),
                                          std::move(old_cols[i])));
    }
    return res_vec;
  }

  template <typename T>
  static auto single_col_vec_to_tuple_vec(std::vector<T>&& vec) {
    std::vector<std::tuple<T>> res_vec;
    res_vec.reserve(vec.size());
    for (size_t i = 0; i < vec.size(); ++i) {
      res_vec.emplace_back(std::make_tuple(vec[i]));
    }
    return res_vec;
  }

  static std::pair<std::vector<vertex_id_t>, std::vector<size_t>>
  get_vertices_with_label(const std::vector<vertex_id_t>& vertices,
                          const std::vector<label_id_t>& label_vec,
                          const label_id_t query_label) {
    std::vector<vertex_id_t> res_vertices;
    std::vector<size_t> indices;
    for (size_t i = 0; i < label_vec.size(); ++i) {
      if (label_vec[i] == query_label) {
        res_vertices.emplace_back(vertices[i]);
        indices.emplace_back(i);
      }
    }
    return std::make_pair(std::move(res_vertices), std::move(indices));
  }

  static std::vector<label_id_t> label_key_vec_2_label_id_vec(
      const std::vector<LabelKey>& label_key_vec) {
    std::vector<label_id_t> res_vec;
    res_vec.reserve(label_key_vec.size());
    for (auto& label_key : label_key_vec) {
      res_vec.emplace_back(label_key.label_id);
    }
    return res_vec;
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_PATH_EXPAND_H_
