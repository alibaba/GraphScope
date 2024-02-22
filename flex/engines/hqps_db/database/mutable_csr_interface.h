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

template <size_t I = 0, typename... T>
void get_tuple_from_column_tuple(
    size_t index, std::tuple<T...>& t,
    const std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) {
  auto ptr = std::get<I>(columns);
  if (ptr) {
    std::get<I>(t) = ptr->get_view(index);
  }

  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_from_column_tuple<I + 1>(index, t, columns);
  }
}

template <size_t I = 0, typename... T, typename... COL_T>
void get_tuple_from_column_tuple(size_t index, std::tuple<T...>& t,
                                 const std::tuple<COL_T...>& columns) {
  auto ptr = std::get<I>(columns);
  if (ptr) {
    std::get<I>(t) = ptr->get_view(index);
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
  const GraphDBSession& GetDBSession() const { return db_session_; }

  using vertex_id_t = vid_t;
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

  static constexpr bool is_grape = true;

  MutableCSRInterface(const MutableCSRInterface&) = delete;

  MutableCSRInterface(MutableCSRInterface&& other)
      : db_session_(other.db_session_) {
    LOG(INFO) << "Move MutableCSRInterface";
  }

  explicit MutableCSRInterface(const GraphDBSession& session)
      : db_session_(session) {
    LOG(INFO) << "Creating MutableCSRInterface";
    LOG(INFO) << "person label num: " << db_session_.graph().vertex_num(1);
  }

  const Schema& schema() const { return db_session_.schema(); }

  /**
   * @brief Get the Vertex Label id
   *
   * @param label
   * @return label_id_t
   */
  label_id_t GetVertexLabelId(const std::string& label) const {
    LOG(INFO) << "GetVertexLabelId: " << label;
    LOG(INFO) << "label num: " << db_session_.schema().vertex_label_num();
    LOG(INFO) << "edge labelnum: " << db_session_.schema().edge_label_num();
    return db_session_.schema().get_vertex_label_id(label);
  }

  /**
   * @brief Get the Edge Label id
   *
   * @param label
   * @return label_id_t
   */
  label_id_t GetEdgeLabelId(const std::string& label) const {
    return db_session_.schema().get_edge_label_id(label);
  }

  /**
   * @brief ScanVertices scans all vertices with the given label and calls the
   * given function on each vertex for filtering.
   * @tparam FUNC_T
   * @tparam SELECTOR
   * @param label
   * @param props
   * @param func
   */
  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const std::string& label,
                    const std::tuple<SELECTOR...>& props,
                    const FUNC_T& func) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    return ScanVertices(label_id, props, func);
  }

  /**
   * @brief ScanVertices scans all vertices with the given label and calls the
   * given function on each vertex for filtering.
   * @tparam FUNC_T
   * @tparam SELECTOR
   * @param label_id
   * @param props
   * @param func
   */
  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const label_id_t& label_id,
                    const std::tuple<SELECTOR...>& selectors,
                    const FUNC_T& func) const {
    auto vnum = db_session_.graph().vertex_num(label_id);
    std::tuple<typename SELECTOR::prop_t...> t;
    if constexpr (sizeof...(SELECTOR) == 0) {
      for (size_t v = 0; v != vnum; ++v) {
        func(v, t);
      }
    } else {
      auto columns =
          get_tuple_column_from_graph_with_property(label_id, selectors);
      for (size_t v = 0; v != vnum; ++v) {
        get_tuple_from_column_tuple(v, t, columns);
        func(v, t);
      }
    }
  }

  /**
   * @brief ScanVertices scans all vertices with the given label with give
   * original id.
   * @param label
   * @param oid
   */
  template <typename OID_T>
  vertex_id_t ScanVerticesWithOid(const std::string& label, OID_T oid,
                                  vertex_id_t& vid) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    return db_session_.graph().get_lid(label_id, Any::From(oid), vid);
  }

  /**
   * @brief ScanVertices scans all vertices with the given label with give
   * original id.
   * @param label_id
   * @param oid
   */
  template <typename OID_T>
  vertex_id_t ScanVerticesWithOid(const label_id_t& label_id, OID_T oid,
                                  vertex_id_t& vid) const {
    return db_session_.graph().get_lid(label_id, Any::From(oid), vid);
  }

  /**
   * @brief ScanVerticesWithoutProperty scans all vertices with the given label
   * and calls the given function on each vertex for filtering. With no
   * property.
   * @tparam FUNC_T
   * @param label
   * @param func
   */
  template <typename FUNC_T>
  void ScanVerticesWithoutProperty(const std::string& label,
                                   const FUNC_T& func) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    auto vnum = db_session_.graph().vertex_num(label_id);
    for (size_t v = 0; v != vnum; ++v) {
      func(v);
    }
  }

  /**
   * @brief GetVertexProps gets the properties of the given vertex.
   * @tparam T
   * @param label
   * @param vid
   * @param prop_names
   */
  template <typename... T>
  std::pair<std::vector<vertex_id_t>, std::vector<std::tuple<T...>>>
  GetVertexPropsFromOid(
      const std::string& label, const std::vector<int64_t> oids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    std::tuple<const TypedColumn<T>*...> columns;
    get_tuple_column_from_graph(label_id, prop_names, columns);
    std::vector<vertex_id_t> vids(oids.size());
    std::vector<std::tuple<T...>> props(oids.size());

    for (size_t i = 0; i < oids.size(); ++i) {
      db_session_.graph().get_lid(label_id, Any::From(oids[i]), vids[i]);
      get_tuple_from_column_tuple(vids[i], props[i], columns);
    }

    return std::make_pair(std::move(vids), std::move(props));
  }

  /**
   * @brief GetVertexProps gets the properties of the given vertices.
   * @tparam T
   * @param label
   * @param vids
   * @param prop_names
   */
  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const std::string& label, const std::vector<vertex_id_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    get_tuple_column_from_graph(label_id, prop_names, columns);
    std::vector<std::tuple<T...>> props(vids.size());
    fetch_properties_in_column(vids, props, columns);
    return std::move(props);
  }

  /**
   * @brief GetVertexPropsFromVid gets the properties of the given vertices.
   * @tparam T
   * @param label_id
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
   * @brief GetVertexPropsFromVid gets the properties of the given vertices.
   * Works for multiple labels.
   * @tparam T
   * @param vids
   * @param label_ids
   * @param vid_inds
   * @param prop_names
   */
  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const std::vector<vertex_id_t>& vids,
      const std::vector<label_id_t>& label_ids,
      const std::vector<std::vector<int32_t>>& vid_inds,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    std::vector<std::tuple<T...>> props(vids.size());
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (size_t i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(label_ids[i], prop_names, columns[i]);
    }

    VLOG(10) << "start getting vertices's property for property : "
             << gs::to_string(prop_names);
    double t0 = -grape::GetCurrentTime();

    fetch_properties<0>(props, columns, vids, vid_inds);
    t0 += grape::GetCurrentTime();
    VLOG(10) << "Finish getting vertices's property, cost: " << t0;

    return props;
  }

  /**
   * @brief GetVertexPropsFromVidV2 gets the properties of the given vertices.
   * Works for 2 labels.
   * @tparam T
   * @param vids
   * @param labels
   * @param bitset
   * @param prop_names
   */
  template <typename... T, size_t num_labels,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr>
  std::vector<std::tuple<T...>> GetVertexPropsFromVidV2(
      const std::vector<vertex_id_t>& vids,
      const std::array<std::string, num_labels>& labels,
      const grape::Bitset& bitset,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    size_t total_size = vids.size();
    std::vector<std::tuple<T...>> props(total_size);
    std::vector<label_t> label_ids;
    for (auto label : labels) {
      label_ids.emplace_back(db_session_.schema().get_vertex_label_id(label));
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (size_t i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(label_ids[i], prop_names, columns[i]);
    }

    fetch_propertiesV2<0>(props, columns, vids, bitset);

    return std::move(props);
  }

  /**
   * @brief GetVertexPropsFromVidV2 gets the properties of the given vertices.
   * Works for 2 labels.
   * @tparam T
   * @param vids
   * @param labels
   * @param bitset
   * @param prop_names
   */
  template <typename... T, size_t num_labels,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr>
  std::vector<std::tuple<T...>> GetVertexPropsFromVidV2(
      const std::vector<vertex_id_t>& vids,
      const std::array<label_id_t, num_labels>& labels,
      const grape::Bitset& bitset,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    size_t total_size = vids.size();
    std::vector<std::tuple<T...>> props(total_size);
    std::vector<label_t> label_ids;
    for (label_id_t label : labels) {
      CHECK(label < db_session_.schema().vertex_label_num());
      label_ids.emplace_back(label);
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (size_t i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(label_ids[i], prop_names, columns[i]);
    }

    fetch_propertiesV2<0>(props, columns, vids, bitset);

    return props;
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void fetch_propertiesV2(std::vector<std::tuple<T...>>& props,
                          std::vector<column_tuple_t>& columns,
                          const std::vector<vertex_id_t>& vids,
                          const grape::Bitset& bitset) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    {
      auto& column_tuple0 = columns[0];
      auto& column_tuple1 = columns[1];
      auto ptr0 = std::get<Is>(column_tuple0);
      auto ptr1 = std::get<Is>(column_tuple1);
      if (ptr0 && ptr1) {
        for (size_t i = 0; i < vids.size(); ++i) {
          if (bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr0->get_view(vids[i]);
          } else {
            std::get<Is>(props[i]) = ptr1->get_view(vids[i]);
          }
        }
      } else if (ptr0) {
        for (size_t i = 0; i < vids.size(); ++i) {
          if (bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr0->get_view(vids[i]);
          }
        }
      } else if (ptr1) {
        for (size_t i = 0; i < vids.size(); ++i) {
          if (!bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr1->get_view(vids[i]);
          }
        }
      } else {
        VLOG(10) << "skip for column " << Is;
      }
    }
    fetch_propertiesV2<Is + 1>(props, columns, vids, bitset);
  }

  template <size_t Is = 0, typename... T, typename column_tuple_t>
  void fetch_properties_in_column(const std::vector<vertex_id_t>& vids,
                                  std::vector<std::tuple<T...>>& props,
                                  column_tuple_t& column) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    auto& cur_column = std::get<Is>(column);
    if (cur_column) {
      for (size_t i = 0; i < vids.size(); ++i) {
        std::get<Is>(props[i]) = cur_column->get_view(vids[i]);
      }
    }

    if constexpr (Is + 1 < sizeof...(T)) {
      fetch_properties_in_column<Is + 1>(vids, props, column);
    }
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void fetch_propertiesV2(std::vector<std::tuple<T...>>& props,
                          std::vector<column_tuple_t>& columns,
                          const std::vector<vertex_id_t>& vids,
                          const grape::Bitset& bitset) const {}

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void fetch_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::vector<std::vector<int32_t>>& vid_inds) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};
    auto num_labels = vid_inds.size();
    for (size_t i = 0; i < num_labels; ++i) {
      auto column_tuple = columns[i];
      auto ptr = std::get<Is>(column_tuple);
      if (ptr) {
        for (size_t j = 0; j < vid_inds[i].size(); ++j) {
          auto vid_ind = vid_inds[i][j];
          auto vid = vids[vid_ind];
          std::get<Is>(props[vid_ind]) = ptr->get_view(vid);
        }
      } else {
        VLOG(10) << "skip for column " << Is;
      }
    }

    fetch_properties<Is + 1>(props, columns, vids, vid_inds);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void fetch_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::vector<std::vector<int32_t>>& vid_inds) const {}

  // get edges with input vids. return a edge list.
  std::vector<mutable_csr_graph_impl::SubGraph<label_id_t, vertex_id_t>>
  GetSubGraph(const label_id_t src_label_id, const label_id_t dst_label_id,
              const label_id_t edge_label_id, const std::string& direction_str,
              const std::vector<std::string>& prop_names) const {
    const CsrBase *csr = nullptr, *other_csr = nullptr;
    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                           edge_label_id);
      return std::vector<sub_graph_t>{sub_graph_t{
          csr, {src_label_id, dst_label_id, edge_label_id}, prop_names}};
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                           edge_label_id);
      return std::vector<sub_graph_t>{sub_graph_t{
          csr, {dst_label_id, src_label_id, edge_label_id}, prop_names}};
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                           edge_label_id);
      other_csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      return std::vector<sub_graph_t>{
          sub_graph_t{
              csr, {src_label_id, dst_label_id, edge_label_id}, prop_names},
          sub_graph_t{other_csr,
                      {dst_label_id, src_label_id, edge_label_id},
                      prop_names}};
    } else {
      throw std::runtime_error("Not implemented - " + direction_str);
    }
    if (csr && !other_csr) {}
  }

  template <typename... T>
  mutable_csr_graph_impl::AdjListArray<T...> GetEdges(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      return mutable_csr_graph_impl::AdjListArray<T...>(csr, vids);
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      return mutable_csr_graph_impl::AdjListArray<T...>(csr, vids);
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      auto csr0 = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                 edge_label_id);
      auto csr1 = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      // CHECK(csr0);
      // CHECK(csr1);
      return mutable_csr_graph_impl::AdjListArray<T...>(csr0, csr1, vids);
    } else {
      // LOG(FATAL) << "Not implemented - " << direction_str;
      throw std::runtime_error("Not implemented - " + direction_str);
    }
  }

  template <typename... T>
  mutable_csr_graph_impl::AdjListArray<T...> GetEdges(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto src_label_id = db_session_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = db_session_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = db_session_.schema().get_edge_label_id(edge_label);

    return GetEdges<T...>(src_label_id, dst_label_id, edge_label_id, vids,
                          direction_str, limit, prop_names);
  }

  std::pair<std::vector<vertex_id_t>, std::vector<size_t>> GetOtherVerticesV2(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const {
    auto src_label_id = db_session_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = db_session_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = db_session_.schema().get_edge_label_id(edge_label);

    return GetOtherVerticesV2(src_label_id, dst_label_id, edge_label_id, vids,
                              direction_str, limit);
  }

  // return the vids, and offset array.
  std::pair<std::vector<vertex_id_t>, std::vector<size_t>> GetOtherVerticesV2(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const {
    std::vector<vertex_id_t> ret_v;
    std::vector<size_t> ret_offset;

    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      auto size = 0;
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size());
      ret_offset.emplace_back(0);

      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        while (iter->is_valid()) {
          ret_v.emplace_back(iter->get_neighbor());
          iter->next();
        }
        ret_offset.emplace_back(ret_v.size());
      }
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      auto size = 0;
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size());
      ret_offset.emplace_back(0);

      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        while (iter->is_valid()) {
          ret_v.emplace_back(iter->get_neighbor());
          iter->next();
        }
        ret_offset.emplace_back(ret_v.size());
      }
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      auto ie_csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                   edge_label_id);
      auto oe_csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                   edge_label_id);
      auto size = 0;
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += ie_csr->edge_iter(v)->size();
        size += oe_csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size() + 1);
      ret_offset.emplace_back(0);
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        {
          auto iter = ie_csr->edge_iter(v);
          while (iter->is_valid()) {
            ret_v.emplace_back(iter->get_neighbor());
            iter->next();
          }
        }
        {
          auto iter = oe_csr->edge_iter(v);
          while (iter->is_valid()) {
            ret_v.emplace_back(iter->get_neighbor());
            iter->next();
          }
        }
        ret_offset.emplace_back(ret_v.size());
      }
    } else {
      LOG(FATAL) << "Not implemented - " << direction_str;
    }
    return std::make_pair(std::move(ret_v), std::move(ret_offset));
  }

  mutable_csr_graph_impl::NbrListArray GetOtherVertices(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const {
    auto src_label_id = db_session_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = db_session_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = db_session_.schema().get_edge_label_id(edge_label);
    return GetOtherVertices(src_label_id, dst_label_id, edge_label_id, vids,
                            direction_str, limit);
  }

  mutable_csr_graph_impl::NbrListArray GetOtherVertices(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const {
    mutable_csr_graph_impl::NbrListArray ret;
    ret.resize(vids.size());
    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      if (csr) {
        for (size_t i = 0; i < vids.size(); ++i) {
          auto v = vids[i];
          auto iter = csr->edge_iter(v);
          auto& vec = ret.get_vector(i);
          while (iter->is_valid()) {
            vec.push_back(mutable_csr_graph_impl::Nbr(iter->get_neighbor()));
            iter->next();
          }
        }
      }
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      if (csr) {
        for (size_t i = 0; i < vids.size(); ++i) {
          auto v = vids[i];
          auto iter = csr->edge_iter(v);
          auto& vec = ret.get_vector(i);
          while (iter->is_valid()) {
            vec.push_back(mutable_csr_graph_impl::Nbr(iter->get_neighbor()));
            iter->next();
          }
        }
      }
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      auto ocsr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                 edge_label_id);
      auto icsr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto& vec = ret.get_vector(i);
        if (ocsr) {
          auto iter = ocsr->edge_iter(v);
          while (iter->is_valid()) {
            vec.push_back(mutable_csr_graph_impl::Nbr(iter->get_neighbor()));
            iter->next();
          }
        }
        if (icsr) {
          auto iter = icsr->edge_iter(v);
          while (iter->is_valid()) {
            vec.push_back(mutable_csr_graph_impl::Nbr(iter->get_neighbor()));
            iter->next();
          }
        }
      }
    } else {
      LOG(FATAL) << "Not implemented - " << direction_str;
    }
    return ret;
  }

  template <typename... T>
  mutable_csr_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      const std::string& label,
      const std::array<std::string, sizeof...(T)>& prop_names) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    return GetMultiPropGetter<T...>(label_id, prop_names);
  }

  template <typename... T>
  mutable_csr_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      const label_id_t& label_id,
      const std::array<std::string, sizeof...(T)>& prop_names) const {
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    column_tuple_t columns;
    get_tuple_column_from_graph(label_id, prop_names, columns);
    return mutable_csr_graph_impl::MultiPropGetter<T...>(columns);
  }

  template <typename T>
  mutable_csr_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      const std::string& label, const std::string& prop_name) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    return GetSinglePropGetter<T>(label_id, prop_name);
  }

  template <typename T>
  mutable_csr_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      const label_id_t& label_id, const std::string& prop_name) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column = GetTypedRefColumn<T>(label_id, prop_name);
    return mutable_csr_graph_impl::SinglePropGetter<T>(std::move(column));
  }

  // get the vertex property
  template <typename T>
  std::shared_ptr<TypedRefColumn<T>> GetTypedRefColumn(
      const label_t& label_id, const std::string& prop_name) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    if constexpr (std::is_same_v<T, LabelKey>) {
      return std::make_shared<TypedRefColumn<LabelKey>>(label_id);
    }
    if (prop_name == "id" || prop_name == "ID" || prop_name == "Id") {
      column = std::dynamic_pointer_cast<TypedRefColumn<T>>(
          db_session_.get_vertex_id_column(label_id));
    } else {
      auto ptr = db_session_.get_vertex_property_column(label_id, prop_name);
      if (ptr) {
        column = std::dynamic_pointer_cast<TypedRefColumn<T>>(
            create_ref_column(ptr));
      } else {
        return nullptr;
      }
    }
    return column;
  }

  std::shared_ptr<RefColumnBase> GetRefColumnBase(
      const label_t& label_id, const std::string& prop_name) const {
    if (prop_name == "id" || prop_name == "ID" || prop_name == "Id") {
      return db_session_.get_vertex_id_column(label_id);
    } else if (prop_name == "Label" || prop_name == "LabelKey") {
      return std::make_shared<TypedRefColumn<LabelKey>>(label_id);
    } else {
      return create_ref_column(
          db_session_.get_vertex_property_column(label_id, prop_name));
    }
  }

 private:
  std::shared_ptr<RefColumnBase> create_ref_column(
      std::shared_ptr<ColumnBase> column) const {
    auto type = column->type();
    if (type == PropertyType::kBool) {
      return std::make_shared<TypedRefColumn<bool>>(
          *std::dynamic_pointer_cast<TypedColumn<bool>>(column));
    } else if (type == PropertyType::kInt32) {
      return std::make_shared<TypedRefColumn<int>>(
          *std::dynamic_pointer_cast<TypedColumn<int>>(column));
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<TypedRefColumn<int64_t>>(
          *std::dynamic_pointer_cast<TypedColumn<int64_t>>(column));
    } else if (type == PropertyType::kUInt32) {
      return std::make_shared<TypedRefColumn<uint32_t>>(
          *std::dynamic_pointer_cast<TypedColumn<uint32_t>>(column));
    } else if (type == PropertyType::kUInt64) {
      return std::make_shared<TypedRefColumn<uint64_t>>(
          *std::dynamic_pointer_cast<TypedColumn<uint64_t>>(column));
    } else if (type == PropertyType::kDate) {
      return std::make_shared<TypedRefColumn<Date>>(
          *std::dynamic_pointer_cast<TypedColumn<Date>>(column));
    } else if (type == PropertyType::kString) {
      return std::make_shared<TypedRefColumn<std::string_view>>(
          *std::dynamic_pointer_cast<TypedColumn<std::string_view>>(column));
    } else if (type == PropertyType::kFloat) {
      return std::make_shared<TypedRefColumn<float>>(
          *std::dynamic_pointer_cast<TypedColumn<float>>(column));
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type.type_enum);
      return nullptr;
    }
  }

  template <typename PropT>
  auto get_single_column_from_graph_with_property(
      label_t label, const PropertySelector<PropT>& selector) const {
    auto res = GetTypedRefColumn<PropT>(label, selector.prop_name_);
    CHECK(res) << "Property " << selector.prop_name_ << " not found";
    return res;
  }

  template <typename... SELECTOR, size_t... Is>
  auto get_tuple_column_from_graph_with_property_impl(
      label_t label, const std::tuple<SELECTOR...>& selectors,
      std::index_sequence<Is...>) const {
    return std::make_tuple(get_single_column_from_graph_with_property(
        label, std::get<Is>(selectors))...);
  }

  template <typename... SELECTOR>
  inline auto get_tuple_column_from_graph_with_property(
      label_t label, const std::tuple<SELECTOR...>& selectors) const {
    return get_tuple_column_from_graph_with_property_impl(
        label, selectors, std::make_index_sequence<sizeof...(SELECTOR)>());
  }

  template <size_t I = 0, typename... T,
            typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr>
  void get_tuple_column_from_graph(
      label_t label,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names,
      std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) const {
    // TODO: support label_property
    using PT = std::tuple_element_t<I, std::tuple<T...>>;
    std::get<I>(columns) = std::dynamic_pointer_cast<TypedRefColumn<PT>>(
        GetTypedRefColumn<PT>(label, prop_names[I]));
    if constexpr (I + 1 < sizeof...(T)) {
      get_tuple_column_from_graph<I + 1>(label, prop_names, columns);
    }
  }

  template <size_t I = 0, typename... T,
            typename std::enable_if<(sizeof...(T) == 0)>::type* = nullptr>
  void get_tuple_column_from_graph(
      label_t label,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names,
      std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) const {}

  const GraphDBSession& db_session_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_
