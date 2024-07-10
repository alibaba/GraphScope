/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FLEX_ENGINES_HQPS_DB_CORE_UTILS_GRAPH_UTILS_H_
#define FLEX_ENGINES_HQPS_DB_CORE_UTILS_GRAPH_UTILS_H_

#include <vector>

#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface_v2.h"

namespace gs {

// Helper functions for hqps engine

template <typename GRAPH_INTERFACE>
std::pair<std::vector<typename GRAPH_INTERFACE::vertex_id_t>,
          std::vector<size_t>>
get_other_vertices_in_batch(
    const GRAPH_INTERFACE& graph, label_t src_label_id, label_t dst_label_id,
    label_t edge_label,
    const std::vector<typename GRAPH_INTERFACE::vertex_id_t>& vertices,
    Direction direction) {
  auto nbr_list = graph.GetOtherVertices(src_label_id, dst_label_id, edge_label,
                                         vertices, direction);
  CHECK(nbr_list.size() == vertices.size());
  std::vector<typename GRAPH_INTERFACE::vertex_id_t> other_vertices;
  std::vector<size_t> offsets;
  offsets.push_back(0);
  for (size_t i = 0; i < vertices.size(); ++i) {
    auto list = nbr_list.get(i);
    for (auto nbr : list) {
      other_vertices.push_back(nbr.neighbor());
    }
    offsets.push_back(other_vertices.size());
  }
  return std::make_pair(other_vertices, offsets);
}

template <typename PropGetterTuple, size_t... Is,
          typename std::enable_if<gs::is_tuple<PropGetterTuple>::value>::type* =
              nullptr>
inline auto get_view_from_prop_getters_impl(const PropGetterTuple& tuple,
                                            vid_t vid,
                                            std::index_sequence<Is...>) {
  static_assert(std::tuple_size_v<PropGetterTuple> == sizeof...(Is),
                "The number of PropGetter should be equal to the number of Is");
  return std::make_tuple(std::get<Is>(tuple).get_view(vid)...);
}

template <typename... PropGetter>
inline auto get_view_from_prop_getters(const std::tuple<PropGetter...>& tuple,
                                       vid_t vid) {
  return get_view_from_prop_getters_impl(
      tuple, vid, std::make_index_sequence<sizeof...(PropGetter)>());
}

template <typename GRAPH_INTERFACE, typename... T, size_t... Is>
std::vector<std::tuple<T...>> get_vertex_props_from_vids_impl(
    const GRAPH_INTERFACE& graph, label_t vertex_label,
    const std::vector<typename GRAPH_INTERFACE::vertex_id_t>& vids,
    const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
        prop_names,
    std::index_sequence<Is...>) {
  std::vector<std::tuple<T...>> res;
  auto prop_getters = std::make_tuple(graph.template GetVertexPropertyGetter<T>(
      vertex_label, std::get<Is>(prop_names))...);
  for (auto vid : vids) {
    res.push_back(get_view_from_prop_getters(prop_getters, vid));
  }
  return res;
}

template <typename GRAPH_INTERFACE, typename... T>
std::vector<std::tuple<T...>> get_vertex_props_from_vids(
    const GRAPH_INTERFACE& graph, label_t vertex_label,
    const std::vector<typename GRAPH_INTERFACE::vertex_id_t>& vids,
    const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
        prop_names) {
  return get_vertex_props_from_vids_impl<GRAPH_INTERFACE, T...>(
      graph, vertex_label, vids, prop_names,
      std::make_index_sequence<sizeof...(T)>());
}

}  // namespace gs

#endif  // FLEX_ENGINES_HQPS_DB_CORE_UTILS_GRAPH_UTILS_H_