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
#ifndef ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_V2
#define ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_V2

#include <tuple>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/core/null_record.h"
#include "flex/engines/hqps_db/core/params.h"

#include "flex/engines/hqps_db/database/adj_list_v2.h"
#include "flex/engines/hqps_db/database/nbr_list.h"
#include "flex/engines/hqps_db/database/sub_graph.h"
#include "grape/utils/bitset.h"

#include "grape/util.h"

namespace gs {

namespace mutable_csr_graph_impl {
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

// Here we use alias to make the code more readable, and make it easier to
// change the implementation in the future.
template <typename T>
struct PropertyGetter {
  using value_type = T;
  std::shared_ptr<TypedRefColumn<T>> column;
  bool is_valid;
  PropertyGetter(std::shared_ptr<TypedRefColumn<T>> column)
      : column(column), is_valid((column != nullptr)) {}

  bool IsValid() const { return is_valid; }

  inline T Get(size_t index) const { return column->get_view(index); }

  inline T get_view(size_t index) const { return column->get_view(index); }
};

struct UntypedPropertyGetter {
  using value_type = Any;
  std::shared_ptr<RefColumnBase> column;
  bool is_valid;
  UntypedPropertyGetter(std::shared_ptr<RefColumnBase> column)
      : column(column), is_valid((column != nullptr)) {}

  inline bool IsValid() const { return is_valid; }

  inline Any Get(size_t index) const { return column->get(index); }

  inline Any get_view(size_t index) const { return column->get(index); }
};

}  // namespace mutable_csr_graph_impl

/**
 * @brief The MutableCSRInterface class is the implementation of IGraphInterface
 * on rt_mutable_graph store, providing a read-only view.
 */
class MutableCSRInterface {
 public:
  using vertex_id_t = vid_t;
  using label_id_t = uint8_t;

  template <typename T>
  using adj_list_array_t = mutable_csr_graph_impl::AdjListArray<T>;

  using nbr_list_array_t = mutable_csr_graph_impl::NbrListArray;

  using sub_graph_t = mutable_csr_graph_impl::SubGraph;

  template <typename T>
  using prop_getter_t = mutable_csr_graph_impl::PropertyGetter<T>;

  using untyped_prop_getter_t = mutable_csr_graph_impl::UntypedPropertyGetter;

  /////////////////////////Constructors////////////////////////////
  const GraphDBSession& GetDBSession() const { return db_session_; }

  MutableCSRInterface(const MutableCSRInterface&) = delete;

  MutableCSRInterface(MutableCSRInterface&& other)
      : db_session_(other.db_session_) {}

  explicit MutableCSRInterface(const GraphDBSession& session)
      : db_session_(session) {}

  //////////////////////////////Schema Related/////////////////////

  const Schema& schema() const { return db_session_.schema(); }

  //////////////////////////////Graph Metadata Related////////////
  inline size_t VertexLabelNum() const {
    return db_session_.schema().vertex_label_num();
  }

  inline size_t EdgeLabelNum() const {
    return db_session_.schema().edge_label_num();
  }

  inline size_t VertexNum() const { return get_vertex_num_impl(); }

  inline size_t VertexNum(const label_t& label) const {
    return db_session_.graph().vertex_num(label);
  }

  inline size_t EdgeNum() const {
    size_t ret = 0;
    for (label_t src_label = 0; src_label < VertexLabelNum(); ++src_label) {
      for (label_t dst_label = 0; dst_label < VertexLabelNum(); ++dst_label) {
        for (label_t edge_label = 0; edge_label < EdgeLabelNum();
             ++edge_label) {
          if (ExitEdgeTriplet(src_label, dst_label, edge_label)) {
            ret += get_edge_num_impl(src_label, dst_label, edge_label);
          }
        }
      }
    }
    return ret;
  }

  inline size_t EdgeNum(label_t src_label, label_t dst_label,
                        label_t edge_label) const {
    return get_edge_num_impl(src_label, dst_label, edge_label);
  }

  label_id_t GetVertexLabelId(const std::string& label) const {
    return db_session_.schema().get_vertex_label_id(label);
  }

  label_id_t GetEdgeLabelId(const std::string& label) const {
    return db_session_.schema().get_edge_label_id(label);
  }

  std::string GetVertexLabelName(label_t index) const {
    return db_session_.schema().get_vertex_label_name(index);
  }

  std::string GetEdgeLabelName(label_t index) const {
    return db_session_.schema().get_edge_label_name(index);
  }

  bool ExitVertexLabel(const std::string& label) const {
    return db_session_.schema().contains_vertex_label(label);
  }

  bool ExitEdgeLabel(const std::string& edge_label) const {
    return db_session_.schema().contains_edge_label(edge_label);
  }

  bool ExitEdgeTriplet(const label_t& src_label, const label_t& dst_label,
                       const label_t& edge_label) const {
    return db_session_.schema().has_edge_label(src_label, dst_label,
                                               edge_label);
  }

  std::vector<std::pair<std::string, PropertyType>> GetEdgeTripletProperties(
      const label_t& src_label, const label_t& dst_label,
      const label_t& label) const {
    const std::vector<PropertyType>& props =
        db_session_.schema().get_edge_properties(src_label, dst_label, label);
    const std::vector<std::string>& prop_names =
        db_session_.schema().get_edge_property_names(src_label, dst_label,
                                                     label);
    std::vector<std::pair<std::string, PropertyType>> res;
    for (size_t i = 0; i < props.size(); ++i) {
      res.emplace_back(prop_names[i], props[i]);
    }
    return res;
  }

  std::vector<std::pair<std::string, PropertyType>> GetVertexProperties(
      label_t label) const {
    const std::vector<PropertyType>& props =
        db_session_.schema().get_vertex_properties(label);
    const std::vector<std::string>& prop_names =
        db_session_.schema().get_vertex_property_names(label);
    std::vector<std::pair<std::string, PropertyType>> res;
    for (size_t i = 0; i < props.size(); ++i) {
      res.emplace_back(prop_names[i], props[i]);
    }
    return res;
  }

  //////////////////////////////Vertex-related Interface////////////

  /**
   * @brief
    Scan all points with label label_id, for each point, get the properties
   specified by selectors, and input them into func. The function signature of
   func_t should be: void func(vertex_id_t v, const std::tuple<xxx>& props)
    Users implement their own logic in the function. This function has no return
   value. In the example below, we scan all person points, find all points with
   age , and save them to a vector. std::vector<vertex_id_t> vids;
       graph.ScanVertices(person_label_id,
   gs::PropertySelector<int32_t>("age"),
       [&vids](vertex_id_t vid, const std::tuple<int32_t>& props){
          if (std::get<0>(props) == 18){
              vids.emplace_back(vid);
          }
       });
    It is important to note that the properties specified by selectors will be
   input into the lambda function in a tuple manner.
   * @tparam FUNC_T
   * @tparam SELECTOR
   * @param label_id
   * @param selectors The Property selectors. The selected properties will be
   * fed to the function
   * @param func The lambda function for filtering.
   */
  // TODO(zhanglei): fix filter_null in scan.h
  template <typename FUNC_T, typename... T>
  void ScanVertices(const label_id_t& label_id,
                    const std::tuple<PropertySelector<T>...>& selectors,
                    const FUNC_T& func) const {
    auto vnum = db_session_.graph().vertex_num(label_id);
    std::tuple<T...> t;
    if constexpr (sizeof...(T) == 0) {
      for (size_t v = 0; v != vnum; ++v) {
        func(v, t);
      }
    } else {
      auto columns = get_vertex_property_columns(label_id, selectors);
      for (size_t v = 0; v != vnum; ++v) {
        mutable_csr_graph_impl::get_tuple_from_column_tuple(v, t, columns);
        func(v, t);
      }
    }
  }

  /**
   * @brief ScanVertices scans all vertices with the given label with give
   * original id.
   * @param label_id The label id.
   * @param oid The original id.
   * @param vid The result internal id.
   */
  bool ScanVerticesWithOid(const label_id_t& label_id, Any oid,
                           vertex_id_t& vid) const {
    return db_session_.graph().get_lid(label_id, oid, vid);
  }

  /**
   * @brief GetVertexPropertyGetter gets the property getter for the given
   * vertex label and property name.
   * @tparam T The property type.
   * @param label_id The vertex label id.
   * @param prop_name The property name.
   * @return The property getter.
   */
  template <typename T>
  mutable_csr_graph_impl::PropertyGetter<T> GetVertexPropertyGetter(
      const label_id_t& label_id, const std::string& prop_name) const {
    auto column = get_vertex_property_column<T>(label_id, prop_name);
    return mutable_csr_graph_impl::PropertyGetter<T>(column);
  }

  mutable_csr_graph_impl::UntypedPropertyGetter GetUntypedVertexPropertyGetter(
      const label_t& label_id, const std::string& prop_name) const {
    auto column = get_vertex_property_column(label_id, prop_name);
    return mutable_csr_graph_impl::UntypedPropertyGetter(column);
  }

  //////////////////////////////Edge-related Interface////////////

  /**
   * @brief GetEdges gets the edges with the given label and edge label, and
   * with the starting vertex internal ids.
   * When the direction is "out", the edges are from the source label to the
   * destination label, and vice versa when the direction is "in". When the
   * direction is "both", the src and dst labels SHOULD be the same.
   */
  template <typename T>
  mutable_csr_graph_impl::AdjListArray<T> GetEdges(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const Direction& direction, size_t limit = INT_MAX) const {
    if (direction == Direction::Out) {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      // return mutable_csr_graph_impl::AdjListArray<T...>(csr, vids);
      return mutable_csr_graph_impl::create_adj_list_array<T>(csr, vids);
    } else if (direction == Direction::In) {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      // return mutable_csr_graph_impl::AdjListArray<T...>(csr, vids);
      return mutable_csr_graph_impl::create_adj_list_array<T>(csr, vids);
    } else if (direction == Direction::Both) {
      auto csr0 = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                 edge_label_id);
      auto csr1 = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      return mutable_csr_graph_impl::create_adj_list_array<T>(csr0, csr1, vids);
    } else {
      throw std::runtime_error("Not implemented - " + direction);
    }
  }

  /**
   * @brief Get vertices on the other side of edges, via the given edge label
   * and the starting vertex internal ids.
   * When the direction is "out", the vertices are on the destination label side
   * of the edges, and vice versa when the direction is "in". When the direction
   * is "both", the src and dst labels SHOULD be the same.
   */
  mutable_csr_graph_impl::NbrListArray GetOtherVertices(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id, const std::vector<vertex_id_t>& vids,
      const Direction& direction, size_t limit = INT_MAX) const {
    if (direction == Direction::Out) {
      auto csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                edge_label_id);
      return mutable_csr_graph_impl::create_nbr_list_array(csr, vids);
    } else if (direction == Direction::In) {
      auto csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                edge_label_id);
      return mutable_csr_graph_impl::create_nbr_list_array(csr, vids);
    } else if (direction == Direction::Both) {
      auto csr0 = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                                 edge_label_id);
      auto csr1 = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      return mutable_csr_graph_impl::create_nbr_list_array(csr0, csr1, vids);
    } else {
      throw std::runtime_error("Not implemented - " + gs::to_string(direction));
    }
  }

  //////////////////////////////Subgraph-related Interface////////////
  mutable_csr_graph_impl::SubGraph GetSubGraph(
      const label_id_t src_label_id, const label_id_t dst_label_id,
      const label_id_t edge_label_id, const Direction& direction) const {
    const CsrBase *csr = nullptr, *other_csr = nullptr;
    if (direction == Direction::Out) {
      csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                           edge_label_id);
      return mutable_csr_graph_impl::SubGraph{
          csr, {src_label_id, dst_label_id, edge_label_id}, Direction::Out};
    } else if (direction == Direction::In) {
      csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                           edge_label_id);
      return mutable_csr_graph_impl::SubGraph{
          csr, {dst_label_id, src_label_id, edge_label_id}, Direction::In};
    } else if (direction == Direction::Both) {
      csr = db_session_.graph().get_oe_csr(src_label_id, dst_label_id,
                                           edge_label_id);
      other_csr = db_session_.graph().get_ie_csr(dst_label_id, src_label_id,
                                                 edge_label_id);
      return mutable_csr_graph_impl::SubGraph{
          csr,
          other_csr,
          {src_label_id, dst_label_id, edge_label_id},
          Direction::Both};
    } else {
      throw std::runtime_error("Not implemented - " + gs::to_string(direction));
    }
  }

  //////////////////////////////Private Functions////////////////////
 private:
  template <typename... T>
  inline auto get_vertex_property_columns(
      label_t label,
      const std::tuple<PropertySelector<T>...>& selectors) const {
    return get_vertex_property_columns_impl(
        label, selectors, std::make_index_sequence<sizeof...(T)>());
  }

  template <typename... T, size_t... Is>
  auto get_vertex_property_columns_impl(
      label_t label, const std::tuple<PropertySelector<T>...>& selectors,
      std::index_sequence<Is...>) const {
    return std::make_tuple(get_vertex_property_column<T>(
        label, std::get<Is>(selectors).prop_name_)...);
  }

  // get the vertex property
  template <typename T>
  std::shared_ptr<TypedRefColumn<T>> get_vertex_property_column(
      const label_t& label_id, const std::string& prop_name) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    if constexpr (std::is_same_v<T, LabelKey>) {
      return std::make_shared<TypedRefColumn<LabelKey>>(label_id);
    }
    if constexpr (std::is_same_v<T, GlobalId>) {
      return std::make_shared<TypedRefColumn<GlobalId>>(label_id);
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

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      const label_t& label_id, const std::string& prop_name) const {
    if (prop_name == "id" || prop_name == "ID" || prop_name == "Id") {
      return db_session_.get_vertex_id_column(label_id);
    } else {
      auto col = db_session_.get_vertex_property_column(label_id, prop_name);
      if (col) {
        return create_ref_column(col);
      } else {
        return nullptr;
      }
    }
  }

  size_t get_vertex_num_impl() const {
    size_t ret = 0;
    for (label_t label_id = 0;
         label_id < db_session_.schema().vertex_label_num(); ++label_id) {
      ret += db_session_.graph().vertex_num(label_id);
    }
    return ret;
  }

  size_t get_edge_num_impl(const label_t& src_label_id,
                           const label_t& dst_label_id,
                           const label_t& edge_label_id) const {
    return db_session_.graph().edge_num(src_label_id, dst_label_id,
                                        edge_label_id);
  }

  const GraphDBSession& db_session_;
};
}  // namespace gs

#endif  // ENGINES_HQPS_DATABASE_MUTABLE_CSR_INTERFACE_H_V2