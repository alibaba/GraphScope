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
#ifndef ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_
#define ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_

#include <tuple>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/core/null_record.h"
#include "flex/engines/hqps_db/core/params.h"

#include "flex/engines/hqps_db/database/adj_list.h"
#include "grape/utils/bitset.h"

#include "grape/util.h"

namespace gs {

template <typename T>
bool exists_nullptr_in_tuple(const T& t) {
  return std::apply([](auto&&... args) { return ((args == nullptr) || ...); },
                    t);
}

template <size_t I = 0, typename... T>
void get_tuple_from_column_tuple(
    size_t index, std::tuple<T...>& t,
    const std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) {
  using cur_ele_t = std::tuple_element_t<I, std::tuple<T...>>;
  auto ptr = std::get<I>(columns);
  if (ptr) {
    std::get<I>(t) = ptr->get_view(index);
  } else {
    std::get<I>(t) = NullRecordCreator<cur_ele_t>::GetNull();
  }

  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_from_column_tuple<I + 1>(index, t, columns);
  }
}

template <size_t I = 0, typename... T, typename... COL_T>
void get_tuple_from_column_tuple(size_t index, std::tuple<T...>& t,
                                 const std::tuple<COL_T...>& columns) {
  auto ptr = std::get<I>(columns);
  using cur_ele_t = std::tuple_element_t<I, std::tuple<T...>>;
  if (ptr) {
    std::get<I>(t) = ptr->get_view(index);
  } else {
    std::get<I>(t) = NullRecordCreator<cur_ele_t>::GetNull();
  }

  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_from_column_tuple<I + 1>(index, t, columns);
  }
}

/**
 * @brief MutableCSRInterface is the interface for the mutable CSR graph
 * implementation.
 *
 */
class MutableCSRInterface {
 public:
  using vertex_id_t = vid_t;
  using gid_t = uint64_t;
  using label_id_t = uint8_t;

  using nbr_list_array_t = mutable_csr_graph_impl::NbrListArray;

  template <typename... T>
  using adj_list_array_t = mutable_csr_graph_impl::AdjListArray<T...>;

  template <typename... T>
  using adj_list_t = mutable_csr_graph_impl::AdjList<T...>;

  template <typename... T>
  using adj_t = mutable_csr_graph_impl::Adj<T...>;

  using nbr_t = mutable_csr_graph_impl::Nbr;

  using nbr_list_t = mutable_csr_graph_impl::NbrList;

  template <typename T>
  using single_prop_getter_t = mutable_csr_graph_impl::SinglePropGetter<T>;

  template <typename... T>
  using multi_prop_getter_t = mutable_csr_graph_impl::MultiPropGetter<T...>;

  using sub_graph_t = mutable_csr_graph_impl::SubGraph<label_id_t, vertex_id_t>;

  /**
   * @brief Get the Vertex Label id
   *
   * @param label
   * @return label_id_t
   */
  virtual label_id_t GetVertexLabelId(const std::string& label) const = 0;

  /**
   * @brief Get the Edge Label id
   *
   * @param label
   * @return label_id_t
   */
  virtual label_id_t GetEdgeLabelId(const std::string& label) const = 0;

  /**
   * @brief Scans all vertices with the given label and calls the
   * given function on each vertex for filtering.
   * @tparam FUNC_T
   * @tparam SELECTOR
   * @param label_id
   * @param selectors The Property selectors. The selected properties will be
   * fed to the filtering function
   * @param func The lambda function for filtering.
   */
  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const label_id_t& label_id,
                    const std::tuple<SELECTOR...>& selectors,
                    const FUNC_T& func) const;

  /**
   * @brief ScanVertices scans all vertices with the given label with give
   * original id.
   * @param label_id The label id.
   * @param oid The original id.
   * @param vid The result internal id.
   */
  template <typename OID_T>
  bool ScanVerticesWithOid(const label_id_t& label_id, OID_T oid,
                           vertex_id_t& vid) const;

  /**
   * @brief GetVertexProps gets the properties of single label vertices.
   * @tparam T The property types.
   * @param label
   * @param vids
   * @param prop_names
   */
  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const label_id_t& label_id, const std::vector<vertex_id_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    // auto label_id = db_session_.schema().get_vertex_label_id(label);
    CHECK(label_id < db_session_.schema().vertex_label_num());
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    get_tuple_column_from_graph(label_id, prop_names, columns);
    std::vector<std::tuple<T...>> props(vids.size());
    fetch_properties_in_column(vids, props, columns);
    return props;
  }

  /**
   * @brief GetVertexProps gets the properties of vertices from multiple labels.
   * @tparam T
   * @param vids The vertex ids.
   * @param label_ids The label ids.
   * @param vid_inds The indices of the vertex ids in the input vids.
   * @param prop_names The property names.
   */
  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const std::vector<vertex_id_t>& vids,
      const std::vector<label_id_t>& label_ids,
      const std::vector<std::vector<int32_t>>& vid_inds,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const;

  /**
   * @brief GetSubGraph gets the subgraph with the given label and edge label.
   * @param src_label_id The source label id.
   * @param dst_label_id The destination label id.
   * @param edge_label_id The edge label id.
   * @param direction_str The direction string.
   * @param prop_names The property names.
   */
  std::vector<mutable_csr_graph_impl::SubGraph<label_id_t, vertex_id_t>>
  GetSubGraph(const label_id_t src_label_id, const label_id_t dst_label_id,
              const label_id_t edge_label_id, const std::string& direction_str,
              const std::vector<std::string>& prop_names) const;

  /**
   * @brief GetEdges gets the edges with the given label and edge label, and
   * with the starting vertex internal ids.
   * When the direction is "out", the edges are from the source label to the
   * destination label, and vice versa when the direction is "in". When the
   * direction is "both", the src and dst labels SHOULD be the same.
   * @tparam T The property types.
   * @param src_label_id The source label id.
   * @param dst_label_id The destination label id.
   * @param edge_label_id The edge label id.
   * @param vids The starting vertex internal ids.
   * @param direction_str The direction string.
   * @param limit The limit of the edges.
   * @param prop_names The property names.
   */
  template <typename... T>
  mutable_csr_graph_impl::AdjListArray<T...> GetEdges(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const;

  /**
   * @brief Get vertices on the other side of edges, via the given edge label
   * and the starting vertex internal ids.
   * When the direction is "out", the vertices are on the destination label side
   * of the edges, and vice versa when the direction is "in". When the direction
   * is "both", the src and dst labels SHOULD be the same.
   * @param src_label_id The source label id.
   * @param dst_label_id The destination label id.
   * @param edge_label_id The edge label id.
   * @param vids The starting vertex internal ids.
   * @param direction_str The direction string.
   * @param limit The limit of the vertices.
   */
  mutable_csr_graph_impl::NbrListArray GetOtherVertices(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const;

  template <typename... T>
  mutable_csr_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      const label_id_t& label_id,
      const std::array<std::string, sizeof...(T)>& prop_names) const;

  template <typename T>
  mutable_csr_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      const label_id_t& label_id, const std::string& prop_name) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column = GetTypedRefColumn<T>(label_id, prop_name);
    return mutable_csr_graph_impl::SinglePropGetter<T>(std::move(column));
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_
