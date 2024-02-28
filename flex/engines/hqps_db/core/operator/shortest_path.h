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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_SHORTEST_PATH_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_SHORTEST_PATH_H_

#include <array>
#include <string>

#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/path.h"

namespace gs {

// scan for a single vertex
template <typename GRAPH_INTERFACE>
class ShortestPathOp {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;

  // Specialize for only one label.
  template <typename SET_T, typename LabelT, typename EXPR,
            typename EDGE_FILTER_T, typename UNTIL_CONDITION, typename... T,
            typename std::enable_if<
                (SET_T::is_vertex_set && sizeof...(T) == 0 &&
                 IsTruePredicate<EDGE_FILTER_T>::value)>::type* = nullptr,
            typename path_set_t = PathSet<vertex_id_t, LabelT>>
  static std::pair<path_set_t, std::vector<size_t>> ShortestPath(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      const ShortestPathOpt<LabelT, EXPR, EDGE_FILTER_T, UNTIL_CONDITION, T...>&
          opt) {
    CHECK(set.Size() == 1);
    auto src_label = set.GetLabel();
    // find the vertices that satisfy the condition.
    auto dst_vertices = find_vertices_satisfy_condition(
        graph, opt.until_condition_.expr_, set.GetLabel(),
        opt.until_condition_.selectors_);
    CHECK(dst_vertices.size() == 1);
    CHECK(opt.edge_expand_opt_.other_label_ == src_label);
    CHECK(opt.get_v_opt_.v_labels_[0] == src_label);
    vertex_id_t src_vid = set.GetVertices()[0];
    vertex_id_t dst_vid = dst_vertices[0];
    VLOG(10) << "[ShortestPath]: src: " << src_vid << ", dst:" << dst_vid;
    // only support one-to-one shortest path.

    auto path_set =
        shortest_path_impl(graph, src_vid, dst_vid, opt.edge_expand_opt_.dir_,
                           opt.edge_expand_opt_.edge_label_, src_label);

    std::vector<offset_t> offsets{0, path_set.Size()};
    return std::make_pair(std::move(path_set), std::move(offsets));
  }

 private:
  template <typename LabelT>
  static PathSet<vertex_id_t, LabelT> shortest_path_impl(
      const GRAPH_INTERFACE& graph, vertex_id_t src_vid, vertex_id_t dst_vid,
      Direction direction, LabelT edge_label, LabelT vertex_label) {
    std::unordered_map<vertex_id_t, int8_t> src_vid_dist;
    std::unordered_map<vertex_id_t, int8_t> dst_vid_dist;
    std::string direction_str = gs::to_string(direction);
    int8_t src_dep = 0, dst_dep = 0;
    std::queue<vertex_id_t> src_q, dst_q;
    std::queue<vertex_id_t> tmp_q;
    std::vector<vertex_id_t> met_vertices;  // store the vertices met.
    src_vid_dist[src_vid] = 0;
    dst_vid_dist[dst_vid] = 0;
    src_q.push(src_vid);
    dst_q.push(dst_vid);
    while (true) {
      if (!src_q.empty() && (src_q.size() <= dst_q.size())) {
        // expand from src.
        ++src_dep;
        VLOG(10) << "Expand From src, current depth: " << src_dep
                 << " queue size: " << src_q.size();

        expand_from_queue(graph, vertex_label, edge_label, direction_str,
                          src_dep, src_q, tmp_q, src_vid_dist, dst_vid_dist,
                          met_vertices);
        if (!met_vertices.empty()) {
          break;
        }
        std::swap(src_q, tmp_q);
      } else {
        // expand from dst.
        ++dst_dep;
        expand_from_queue(graph, vertex_label, edge_label, direction_str,
                          dst_dep, dst_q, tmp_q, dst_vid_dist, src_vid_dist,
                          met_vertices);
        if (!met_vertices.empty()) {
          break;
        }
        std::swap(dst_q, tmp_q);
      }
      if (src_q.empty() || dst_q.empty()) {
        break;
      }
    }

    if (met_vertices.empty()) {
      VLOG(10) << "no meet vertices found";
      return make_empty_path_set<vertex_id_t, LabelT>({vertex_label});
    }

    // to find the path.
    return find_paths(graph, vertex_label, edge_label, direction_str,
                      met_vertices, src_vid, dst_vid, src_vid_dist,
                      dst_vid_dist);
  }

  template <typename LabelT>
  static void expand_from_queue(
      const GRAPH_INTERFACE& graph, LabelT v_label, LabelT edge_label,
      const std::string& direction, int8_t depth,
      std::queue<vertex_id_t>& src_q, std::queue<vertex_id_t>& tmp_q,
      std::unordered_map<vertex_id_t, int8_t>& cur_vid_dist,
      std::unordered_map<vertex_id_t, int8_t>& other_vid_dist,
      std::vector<vertex_id_t>& met_vertices) {
    std::vector<vertex_id_t> ids_to_query;
    ids_to_query.reserve(src_q.size());
    while (!src_q.empty()) {
      auto src_v = src_q.front();
      src_q.pop();
      ids_to_query.emplace_back(src_v);
    }
    auto nbr_list_array = graph.GetOtherVertices(
        v_label, v_label, edge_label, ids_to_query, direction, INT_MAX);
    for (size_t i = 0; i < nbr_list_array.size(); ++i) {
      for (auto nbr : nbr_list_array.get(i)) {
        auto v = nbr.neighbor();
        if (cur_vid_dist.find(v) == cur_vid_dist.end()) {
          cur_vid_dist[v] = depth;
          tmp_q.push(v);
          if (other_vid_dist.find(v) != other_vid_dist.end()) {
            met_vertices.push_back(v);
          }
        }
      }
    }
    VLOG(10) << "push " << tmp_q.size() << " ele to new queue"
             << ", met vertices: " << met_vertices.size();
  }

  static void dfs(
      vertex_id_t src_vid, vertex_id_t dst_vid,
      std::vector<vertex_id_t>& cur_path,
      std::unordered_map<vertex_id_t, int8_t> dist_from_src,
      std::unordered_set<vertex_id_t> valid_vertex_set,
      std::vector<std::vector<vertex_id_t>>& paths,
      std::unordered_map<vertex_id_t,
                         std::vector<typename GRAPH_INTERFACE::nbr_t>>&
          vid_to_nbr_list) {
    VLOG(10) << "cur: " << src_vid << ", cur_path: " << gs::to_string(cur_path);
    cur_path.push_back(src_vid);

    if (src_vid == dst_vid) {
      VLOG(10) << "Reach dst : " << gs::to_string(cur_path);
      paths.push_back(cur_path);
      cur_path.pop_back();
      return;
    }
    CHECK(vid_to_nbr_list.find(src_vid) != vid_to_nbr_list.end());
    for (auto nbr : vid_to_nbr_list[src_vid]) {
      CHECK(dist_from_src.count(src_vid) > 0);
      auto v = nbr.neighbor();
      if (valid_vertex_set.find(v) != valid_vertex_set.end()) {
        CHECK(dist_from_src.count(v) > 0) << "check failed for : " << v;
        if (dist_from_src[src_vid] + 1 == dist_from_src[v]) {
          dfs(v, dst_vid, cur_path, dist_from_src, valid_vertex_set, paths,
              vid_to_nbr_list);
        }
      }
    }
    cur_path.pop_back();
  }

  template <typename LabelT>
  static PathSet<vertex_id_t, LabelT> find_paths(
      const GRAPH_INTERFACE& graph, LabelT v_label, LabelT edge_label,
      const std::string& direction, std::vector<vertex_id_t>& met_vertices,
      vertex_id_t src_vid, vertex_id_t dst_vid,
      std::unordered_map<vertex_id_t, int8_t>& src_vid_dist,
      std::unordered_map<vertex_id_t, int8_t>& dst_vid_dist) {
    std::unordered_set<vertex_id_t> vertex_set;
    std::unordered_map<vertex_id_t,
                       std::vector<typename GRAPH_INTERFACE::nbr_t>>
        vid_to_nbr_list;
    std::queue<vertex_id_t> q;
    for (auto v : met_vertices) {
      vertex_set.insert(v);
      q.push(v);
    }

    std::vector<vertex_id_t> tmp_vec;
    while (!q.empty()) {
      tmp_vec.clear();
      for (size_t i = 0; i < q.size(); ++i) {
        auto v = q.front();
        q.pop();
        tmp_vec.emplace_back(v);
      }

      auto nbr_list_array = graph.GetOtherVertices(v_label, v_label, edge_label,
                                                   tmp_vec, direction, INT_MAX);
      for (size_t i = 0; i < nbr_list_array.size(); ++i) {
        auto cur_v = tmp_vec[i];
        for (auto nbr : nbr_list_array.get(i)) {
          auto v = nbr.neighbor();
          if (vertex_set.find(v) != vertex_set.end()) {
            continue;
          }
          if (src_vid_dist.find(v) != src_vid_dist.end() &&
              src_vid_dist[v] + 1 == src_vid_dist[cur_v]) {
            q.push(v);
            vertex_set.insert(v);
          }
          if (dst_vid_dist.find(v) != dst_vid_dist.end() &&
              dst_vid_dist[v] + 1 == dst_vid_dist[cur_v]) {
            q.push(v);
            vertex_set.insert(v);
            src_vid_dist[v] = src_vid_dist[cur_v] + 1;
          }
        }
        if (vid_to_nbr_list.find(cur_v) == vid_to_nbr_list.end()) {
          vid_to_nbr_list.insert({cur_v, nbr_list_array.get_vector(i)});
          VLOG(10) << "cache nbr list for v: " << cur_v;
        }
      }
    }

    // dfs to find path.
    std::vector<std::vector<vertex_id_t>> paths;
    std::vector<vertex_id_t> cur_path;
    dfs(src_vid, dst_vid, cur_path, src_vid_dist, vertex_set, paths,
        vid_to_nbr_list);
    VLOG(10) << "Got path size: " << paths.size();
    for (auto path : paths) {
      VLOG(10) << "path: " << gs::to_string(path);
    }
    auto path_set = PathSet<vertex_id_t, LabelT>({v_label});
    for (auto path : paths) {
      size_t s = path.size();
      std::vector<int32_t> offset(s, 0);
      Path<vertex_id_t, label_id_t> new_path(std::move(path),
                                             std::move(offset));
      path_set.EmplacePath(std::move(new_path));
    }
    return path_set;
  }

  template <typename UNTIL_CONDITION, typename LabelT, typename T>
  static std::vector<vertex_id_t> find_vertices_satisfy_condition(
      const GRAPH_INTERFACE& graph, UNTIL_CONDITION& condition, LabelT v_label,
      const std::tuple<PropertySelector<T>>& selectors) {
    std::vector<vertex_id_t> gids;
    auto filter = [&](vertex_id_t v, const std::tuple<T>& props) {
      if (condition(std::get<0>(props))) {
        gids.push_back(v);
      }
    };
    // TODO: make label param?
    // auto names = std::array<std::string,
    // 1>{std::get<0>(selectors).prop_name_};
    graph.template ScanVertices(v_label, selectors, filter);
    return gids;
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_SHORTEST_PATH_H_
