#ifndef GRAPHSCOPE_MUTABLE_CSR_GRAPE_GRAPH_INTERFACE_H_
#define GRAPHSCOPE_MUTABLE_CSR_GRAPE_GRAPH_INTERFACE_H_

#include <tuple>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps/database/adj_list.h"
#include "flex/engines/hqps/engine/null_record.h"
#include "flex/engines/hqps/engine/params.h"
#include "flex/engines/hqps/engine/utils/bitset.h"
#include "flex/engines/hqps/engine/utils/operator_utils.h"

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

template <size_t I = 0, typename... T>
void get_tuple_column_from_graph(
    const GraphDBSession& sess, label_t label,
    const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
        prop_names,
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) {
  // TODO: support label_property
  using PT = std::tuple_element_t<I, std::tuple<T...>>;
  std::get<I>(columns) = std::dynamic_pointer_cast<TypedRefColumn<PT>>(
      sess.get_vertex_property_ref_column(label, prop_names[I]));
  if (std::get<I>(columns) == nullptr) {}
  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_column_from_graph<I + 1>(sess, label, prop_names, columns);
  }
}

template <typename PropT>
auto get_single_column_from_graph_with_property(
    const GraphDBSession& sess, label_t label,
    const PropertySelector<PropT>& selector) {
  return std::dynamic_pointer_cast<TypedRefColumn<PropT>>(
      sess.get_vertex_property_ref_column(label, selector.prop_name_));
}

template <typename... SELECTOR, size_t... Is>
auto get_tuple_column_from_graph_with_property_impl(
    const GraphDBSession& sess, label_t label,
    const std::tuple<SELECTOR...>& selectors, std::index_sequence<Is...>) {
  return std::make_tuple(get_single_column_from_graph_with_property(
      sess, label, std::get<Is>(selectors))...);
}

template <typename... SELECTOR>
inline auto get_tuple_column_from_graph_with_property(
    const GraphDBSession& sess, label_t label,
    const std::tuple<SELECTOR...>& selectors) {
  return get_tuple_column_from_graph_with_property_impl(
      sess, label, selectors, std::make_index_sequence<sizeof...(SELECTOR)>());
}

/// @brief GrapeGraphInterface is a wrapper of GraphDBSession, which provides
/// the interface for grape.
class GrapeGraphInterface {
 public:
  const GraphDBSession& GetDBSession() { return db_session_; }

  using vertex_id_t = vid_t;
  using outer_vertex_id_t = oid_t;
  using label_id_t = uint8_t;

  using nbr_list_array_t = grape_graph_impl::NbrListArray;

  template <typename... T>
  using adj_list_array_t = grape_graph_impl::AdjListArray<T...>;

  template <typename... T>
  using adj_list_t = grape_graph_impl::AdjList<T...>;

  template <typename... T>
  using adj_t = grape_graph_impl::Adj<T...>;

  using nbr_t = grape_graph_impl::Nbr;

  using nbr_list_t = grape_graph_impl::NbrList;

  template <typename T>
  using single_prop_getter_t = grape_graph_impl::SinglePropGetter<T>;

  template <typename... T>
  using multi_prop_getter_t = grape_graph_impl::MultiPropGetter<T...>;

  static constexpr bool is_grape = true;

  static GrapeGraphInterface& get();

  GrapeGraphInterface(const GraphDBSession& session) : db_session_(session) {}

  label_id_t GetVertexLabelId(const std::string& label) const {
    return db_session_.schema().get_vertex_label_id(label);
  }

  label_id_t GetEdgeLabelId(const std::string& label) const {
    return db_session_.schema().get_edge_label_id(label);
  }

  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const std::string& label,
                    const std::tuple<SELECTOR...>& props,
                    const FUNC_T& func) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    return ScanVertices(label_id, props, func);
  }

  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const label_id_t& label_id,
                    const std::tuple<SELECTOR...>& selectors,
                    const FUNC_T& func) const {
    auto columns = get_tuple_column_from_graph_with_property(
        db_session_, label_id, selectors);
    auto vnum = db_session_.graph().vertex_num(label_id);
    std::tuple<typename SELECTOR::prop_t...> t;
    for (auto v = 0; v != vnum; ++v) {
      get_tuple_from_column_tuple(v, t, columns);
      func(v, t);
    }
  }

  vertex_id_t ScanVerticesWithOid(const std::string& label,
                                  outer_vertex_id_t oid) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    vertex_id_t vid;
    CHECK(db_session_.graph().get_lid(label_id, oid, vid));
    return vid;
  }

  vertex_id_t ScanVerticesWithOid(const label_id_t& label_id,
                                  outer_vertex_id_t oid) const {
    vertex_id_t vid;
    CHECK(db_session_.graph().get_lid(label_id, oid, vid));
    return vid;
  }

  template <typename FUNC_T>
  void ScanVerticesWithoutProperty(const std::string& label,
                                   const FUNC_T& func) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    auto vnum = db_session_.graph().vertex_num(label_id);
    for (auto v = 0; v != vnum; ++v) {
      func(v);
    }
  }

  template <typename... T>
  std::pair<std::vector<vertex_id_t>, std::vector<std::tuple<T...>>>
  GetVertexPropsFromOid(
      const std::string& label, const std::vector<int64_t> oids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    std::tuple<const TypedColumn<T>*...> columns;
    get_tuple_column_from_graph(db_session_, label_id, prop_names, columns);
    std::vector<vertex_id_t> vids(oids.size());
    std::vector<std::tuple<T...>> props(oids.size());

    for (size_t i = 0; i < oids.size(); ++i) {
      db_session_.graph().get_lid(label_id, oids[i], vids[i]);
      get_tuple_from_column_tuple(vids[i], props[i], columns);
    }

    return std::make_pair(std::move(vids), std::move(props));
  }

  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const std::string& label, const std::vector<vertex_id_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    get_tuple_column_from_graph(db_session_, label_id, prop_names, columns);
    std::vector<std::tuple<T...>> props(vids.size());
    fetch_properties_in_column(vids, props, columns);
    return std::move(props);
  }

  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const label_id_t& label_id, const std::vector<vertex_id_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    // auto label_id = db_session_.schema().get_vertex_label_id(label);
    CHECK(label_id < db_session_.schema().vertex_label_num());
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    get_tuple_column_from_graph(db_session_, label_id, prop_names, columns);
    std::vector<std::tuple<T...>> props(vids.size());
    fetch_properties_in_column(vids, props, columns);
    return std::move(props);
  }

  // Get props from multiple label of vertices.
  // NOTE: performance not good, use v2.
  template <typename... T, size_t num_labels>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      const std::vector<vertex_id_t>& vids,
      const std::array<std::string, num_labels>& labels,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    std::vector<std::tuple<T...>> props(vids.size());
    std::vector<label_t> label_ids;
    for (auto label : labels) {
      label_ids.emplace_back(db_session_.schema().get_vertex_label_id(label));
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (auto i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(db_session_, label_ids[i], prop_names,
                                  columns[i]);
    }

    VLOG(10) << "start getting vertices's property";
    double t0 = -grape::GetCurrentTime();
    fetch_properties<0>(props, columns, vids, vid_inds);
    t0 += grape::GetCurrentTime();
    VLOG(10) << "Finish getting vertices's property, cost: " << t0;

    return std::move(props);
  }

  // Get props from multiple label of vertices.
  template <typename... T, size_t num_labels,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr>
  std::vector<std::tuple<T...>> GetVertexPropsFromVidV2(
      const std::vector<vertex_id_t>& vids,
      const std::array<std::string, num_labels>& labels, const Bitset& bitset,
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
    for (auto i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(db_session_, label_ids[i], prop_names,
                                  columns[i]);
    }

    fetch_propertiesV2<0>(props, columns, vids, bitset);

    return std::move(props);
  }

  template <typename... T, size_t num_labels,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr>
  std::vector<std::tuple<T...>> GetVertexPropsFromVidV2(
      const std::vector<vertex_id_t>& vids,
      const std::array<label_id_t, num_labels>& labels, const Bitset& bitset,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    size_t total_size = vids.size();
    std::vector<std::tuple<T...>> props(total_size);
    std::vector<label_t> label_ids;
    for (auto label : labels) {
      CHECK(label < db_session_.schema().vertex_label_num());
      label_ids.emplace_back(label);
      // label_ids.emplace_back(db_session_.schema().get_vertex_label_id(label));
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (auto i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(db_session_, label_ids[i], prop_names,
                                  columns[i]);
    }

    fetch_propertiesV2<0>(props, columns, vids, bitset);

    return std::move(props);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void fetch_propertiesV2(std::vector<std::tuple<T...>>& props,
                          std::vector<column_tuple_t>& columns,
                          const std::vector<vertex_id_t>& vids,
                          const Bitset& bitset) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    {
      auto& column_tuple0 = columns[0];
      auto& column_tuple1 = columns[1];
      auto ptr0 = std::get<Is>(column_tuple0);
      auto ptr1 = std::get<Is>(column_tuple1);
      if (ptr0 && ptr1) {
        for (auto i = 0; i < vids.size(); ++i) {
          if (bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr0->get_view(vids[i]);
          } else {
            std::get<Is>(props[i]) = ptr1->get_view(vids[i]);
          }
        }
      } else if (ptr0) {
        for (auto i = 0; i < vids.size(); ++i) {
          if (bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr0->get_view(vids[i]);
          }
        }
      } else if (ptr1) {
        for (auto i = 0; i < vids.size(); ++i) {
          if (!bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr1->get_view(vids[i]);
          }
        }
      } else {
        LOG(INFO) << "skip for column " << Is;
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
      for (auto i = 0; i < vids.size(); ++i) {
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
                          const Bitset& bitset) const {}

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void fetch_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    for (size_t i = 0; i < num_labels; ++i) {
      auto column_tuple = columns[i];
      auto ptr = std::get<Is>(column_tuple);
      if (ptr) {
        for (auto j = 0; j < vid_inds[i].size(); ++j) {
          auto vid_ind = vid_inds[i][j];
          auto vid = vids[vid_ind];
          std::get<Is>(props[vid_ind]) = ptr->get_view(vid);
        }
      } else {
        LOG(INFO) << "skip for column " << Is;
      }
    }

    fetch_properties<Is + 1>(props, columns, vids, vid_inds);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void fetch_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {}

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void visit_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    for (size_t i = 0; i < num_labels; ++i) {
      auto column_tuple = columns[i];
      auto ptr = std::get<Is>(column_tuple);
      if (ptr) {
        std::tuple_element_t<Is, std::tuple<T...>> tmp;
        for (auto j = 0; j < vid_inds[i].size(); ++j) {
          auto vid_ind = vid_inds[i][j];
          auto vid = vids[vid_ind];
          tmp = ptr->get_view(vid);
        }
        VLOG(10) << tmp;
      } else {
        LOG(INFO) << "skip for column " << Is;
      }
    }

    visit_properties<Is + 1>(props, columns, vids, vid_inds);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void visit_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {}

  template <typename... T>
  grape_graph_impl::AdjListArray<T...> GetEdges(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      return grape_graph_impl::AdjListArray<T...>(csr, vids);
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      return grape_graph_impl::AdjListArray<T...>(csr, vids);
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      auto csr0 = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                 edge_label_id);
      auto csr1 = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      CHECK(csr0);
      CHECK(csr1);
      return grape_graph_impl::AdjListArray<T...>(csr0, csr1, vids);
    } else {
      // LOG(FATAL) << "Not implemented - " << direction_str;
      throw std::runtime_error("Not implemented - " + direction_str);
    }
  }

  template <typename... T>
  grape_graph_impl::AdjListArray<T...> GetEdges(
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
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size());
      ret_offset.emplace_back(0);

      for (auto i = 0; i < vids.size(); ++i) {
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
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size());
      ret_offset.emplace_back(0);

      for (auto i = 0; i < vids.size(); ++i) {
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
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += ie_csr->edge_iter(v)->size();
        size += oe_csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size() + 1);
      ret_offset.emplace_back(0);
      for (auto i = 0; i < vids.size(); ++i) {
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

  grape_graph_impl::NbrListArray GetOtherVertices(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const {
    auto src_label_id = db_session_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = db_session_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = db_session_.schema().get_edge_label_id(edge_label);
    return GetOtherVertices(src_label_id, dst_label_id, edge_label_id, vids,
                            direction_str, limit);
  }

  grape_graph_impl::NbrListArray GetOtherVertices(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const std::string& direction_str, size_t limit) const {
    grape_graph_impl::NbrListArray ret;

    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      ret.resize(vids.size());
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        auto& vec = ret.get_vector(i);
        while (iter->is_valid()) {
          vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          iter->next();
        }
      }
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      ret.resize(vids.size());
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        auto& vec = ret.get_vector(i);
        while (iter->is_valid()) {
          vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          iter->next();
        }
      }
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      ret.resize(vids.size());
      auto ocsr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                 edge_label_id);
      auto icsr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto& vec = ret.get_vector(i);
        auto iter = ocsr->edge_iter(v);
        while (iter->is_valid()) {
          vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          iter->next();
        }
        iter = icsr->edge_iter(v);
        while (iter->is_valid()) {
          vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          iter->next();
        }
      }
    } else {
      LOG(FATAL) << "Not implemented - " << direction_str;
    }
    return ret;
  }

  template <typename... T>
  grape_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      const std::string& label,
      const std::array<std::string, sizeof...(T)>& prop_names) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    static constexpr auto ind_seq = std::make_index_sequence<sizeof...(T)>();
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    column_tuple_t columns;
    get_tuple_column_from_graph(db_session_, label_id, prop_names, columns);
    return grape_graph_impl::MultiPropGetter<T...>(columns);
  }

  template <typename... T>
  grape_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      const label_id_t& label_id,
      const std::array<std::string, sizeof...(T)>& prop_names) const {
    static constexpr auto ind_seq = std::make_index_sequence<sizeof...(T)>();
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    column_tuple_t columns;
    get_tuple_column_from_graph(db_session_, label_id, prop_names, columns);
    return grape_graph_impl::MultiPropGetter<T...>(columns);
  }

  template <typename T>
  grape_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      const std::string& label, const std::string& prop_name) const {
    auto label_id = db_session_.schema().get_vertex_label_id(label);
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    column = std::dynamic_pointer_cast<TypedRefColumn<T>>(
        db_session_.get_vertex_property_ref_column(label_id, prop_name));
    return grape_graph_impl::SinglePropGetter<T>(std::move(column));
  }

  template <typename T>
  grape_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      const label_id_t& label_id, const std::string& prop_name) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    column = std::dynamic_pointer_cast<TypedRefColumn<T>>(
        db_session_.get_vertex_property_ref_column(label_id, prop_name));
    return grape_graph_impl::SinglePropGetter<T>(std::move(column));
  }

  template <typename T>
  std::shared_ptr<TypedRefColumn<T>> GetTypedRefColumn(
      label_t& label_id, const NamedProperty<T>& named_prop) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    return std::dynamic_pointer_cast<TypedRefColumn<T>>(
        db_session_.get_vertex_property_ref_column(label_id,
                                                   named_prop.names[0]));
  }

 private:
  const GraphDBSession& db_session_;
  bool initialized_ = false;
};

}  // namespace gs

#endif  // GRAPHSCOPE_MUTABLE_CSR_GRAPE_GRAPH_INTERFACE_H_
