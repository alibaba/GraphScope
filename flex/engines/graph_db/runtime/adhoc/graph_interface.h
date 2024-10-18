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

#ifndef RUNTIME_ADHOC_GRAPH_INTERFACE_H_
#define RUNTIME_ADHOC_GRAPH_INTERFACE_H_

#include <vector>
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
#include "flex/utils/property/types.h"

namespace gs {

namespace runtime {

namespace impl {

/*
  The following classes are used to provide a unified interface for adhoc
  queries to access graph data. If you want to use your own Graph implementation
  to run adhoc queries, you should specialize the following classes with your
  own graph implementation.
*/

template <typename T, typename GRAPH_IMPL>
class PropertyGetter {
 public:
  using vertex_index_t = typename GRAPH_IMPL::vertex_index_t;

  PropertyGetter();

  T operator[](vertex_index_t idx) const;

  bool EmptyProperty() const;

  T get_view(vertex_index_t idx) const;

  Any get_any(vertex_index_t idx) const;
};

/**
 * Note: The generic class is not implemented, you should specialize it with
 * your own graph implementation.
 */
template <typename GRAPH_IMPL>
class EdgeIterator {
 public:
  using vertex_index_t = typename GRAPH_IMPL::vertex_index_t;

  EdgeIterator();
  ~EdgeIterator();

  Any GetData() const;

  bool IsValid() const;

  void Next();

  vertex_index_t GetNeighbor() const;

  label_t GetNeighborLabel() const;

  label_t GetEdgeLabel() const;
};

/**
 * Note: The generic class is not implemented, you should specialize it with
 * your own graph implementation.
 */
template <typename T, typename GRAPH_IMPL>
class AdjList {
 public:
  using self_t = AdjList<T, GRAPH_IMPL>;
  using vertex_index_t = typename GRAPH_IMPL::vertex_index_t;
  class NbrIterator {
   public:
    using self_t = NbrIterator;

    const T& GetData() const;
    vertex_index_t GetNeighbor() const;
    const self_t& operator*() const;
    const self_t* operator->() const;
    NbrIterator& operator++();
    bool operator==(const NbrIterator& rhs) const;
    bool operator!=(const NbrIterator& rhs) const;
  };
  using edata_t = T;
  using iterator = NbrIterator;

  AdjList();

  inline iterator begin() const;
  inline iterator end() const;
};

/**
 * Note: The generic class is not implemented, you should specialize it with
 * your own graph implementation.
 */
template <typename T, typename GRAPH_IMPL>
class SubGraph {
 public:
  using label_id_t = uint8_t;
  using vertex_index_t = typename GRAPH_IMPL::vertex_index_t;
  using edata_t = T;

  SubGraph();

  AdjList<T, GRAPH_IMPL> GetEdges(vertex_index_t vid) const;
};

//////// Specialization for ReadTransaction ////////
template <typename T>
class PropertyGetter<T, ReadTransaction> {
 public:
  using vertex_index_t = typename ReadTransaction::vertex_index_t;
  PropertyGetter() : column_(nullptr) {}
  PropertyGetter(const TypedColumn<T>* col) { column_ = col; }

  PropertyGetter(const ColumnBase* col) {
    column_ = dynamic_cast<const TypedColumn<T>*>(col);
  }

  T operator[](vertex_index_t idx) const {
    if (column_ == nullptr) {
      return T();
    }
    return column_->get_view(idx);
  }

  bool EmptyProperty() const { return column_ == nullptr; }

  T get_view(vertex_index_t idx) const {
    if (column_ == nullptr) {
      return T();
    }
    return column_->get_view(idx);
  }

  RTAny get_any(vertex_index_t idx) const {
    if (column_ == nullptr) {
      return RTAny(RTAnyType::kNull);
    }
    return TypedConverter<T>::from_typed(column_->get_view(idx));
  }

 private:
  const TypedColumn<T>* column_;
};

template <>
class EdgeIterator<ReadTransaction> {
 public:
  using vertex_index_t = typename ReadTransaction::vertex_index_t;
  using inner_iter_t = typename ReadTransaction::edge_iterator;

  EdgeIterator(inner_iter_t&& iter) : iter_(std::move(iter)) {}
  ~EdgeIterator() {}

  inline Any GetData() const { return iter_.GetData(); }

  inline bool IsValid() const { return iter_.IsValid(); }

  inline void Next() { iter_.Next(); }

  inline vertex_index_t GetNeighbor() const { return iter_.GetNeighbor(); }

  inline label_t GetNeighborLabel() const { return iter_.GetNeighborLabel(); }

  inline label_t GetEdgeLabel() const { return iter_.GetEdgeLabel(); }

 private:
  inner_iter_t iter_;
};

template <typename T>
class AdjList<T, ReadTransaction> {
 public:
  using self_t = AdjList<T, ReadTransaction>;
  using vertex_index_t = typename ReadTransaction::vertex_index_t;
  class NbrIterator {
   public:
    using inner_iterator = typename AdjListView<T>::nbr_iterator;
    using self_t = NbrIterator;
    NbrIterator(inner_iterator&& iter) : iter_(std::move(iter)) {}

    const T& GetData() const { return iter_->get_data(); }

    vertex_index_t GetNeighbor() const { return iter_->get_neighbor(); }

    const self_t& operator*() const { return *this; }

    const self_t* operator->() const { return this; }

    NbrIterator& operator++() {
      ++iter_;
      return *this;
    }

    bool operator==(const NbrIterator& rhs) const { return iter_ == rhs.iter_; }

    bool operator!=(const NbrIterator& rhs) const { return iter_ != rhs.iter_; }

   private:
    inner_iterator iter_;
  };
  using edata_t = T;
  using iterator = NbrIterator;
  using inner_adj_list_t = AdjListView<edata_t>;

  AdjList(inner_adj_list_t&& adj_list) : adj_list_(std::move(adj_list)) {}

  inline iterator begin() const { return iterator(adj_list_.begin()); }
  inline iterator end() const { return iterator(adj_list_.end()); }

 private:
  inner_adj_list_t adj_list_;
};

template <typename T>
class SubGraph<T, ReadTransaction> {
 public:
  using label_id_t = uint8_t;
  using vertex_index_t = typename ReadTransaction::vertex_index_t;
  using edata_t = T;
  using inner_graph_view = GraphView<edata_t>;

  SubGraph(inner_graph_view&& view) : view_(std::move(view)) {}

  AdjList<T, ReadTransaction> GetEdges(vertex_index_t vid) const {
    return AdjList<T, ReadTransaction>(view_.get_edges(vid));
  }

 private:
  inner_graph_view view_;
};

}  // namespace impl

/*
 * The GraphInterface class provides a unified interface for adhoc queries to
 * access graph data. The interface is designed to be generic and can be
 * specialized with different graph implementations. The interface provides
 * functions to access vertex and edge properties, get neighbors of a vertex,
 * and get subgraphs.
 *
 * Note that the functions in these classes are not implemented, you should
 * implement them by yourself. For example, if you want to use a graph with
 * name `MyGraph`, you specialize the GraphInterface class like this:
 *
 * ```c++
 * class MyGraph {
 *  // Implement the graph interface functions
 * };
 *
 * template <>
 * class GraphInterface<MyGraph> {
 *
 *  label_id_t VertexLabelNum() { ... }
 *  label_id_t EdgeLabelNum() const { ...}
 *  std::vector<label_id_t> GetVertexLabels() const {...}
 *
 *  // Other functions ...
 * };
 *
 * ```
 */
template <typename GRAPH_IMPL>
class GraphInterface {
 public:
  using label_id_t = label_t;
  using vertex_index_t = typename GRAPH_IMPL::vertex_index_t;
  using edge_iterator_t = impl::EdgeIterator<GRAPH_IMPL>;

  template <typename EDATA_T>
  using sub_graph_t = impl::SubGraph<EDATA_T, GRAPH_IMPL>;

  label_id_t VertexLabelNum();
  label_id_t EdgeLabelNum() const;
  std::vector<label_id_t> GetVertexLabels() const;
  std::vector<label_id_t> GetEdgeLabels() const;
  label_id_t GetVertexLabelId(const std::string& label) const;
  label_id_t GetEdgeLabelId(const std::string& label) const;
  std::string GetVertexLabelName(label_id_t label_id) const;
  std::string GetEdgeLabelName(label_id_t label_id) const;
  std::vector<std::tuple<gs::PropertyType, std::string, size_t>>
  GetVertexPrimaryKeys(label_id_t label) const;
  bool ExistVertexLabel(const std::string& label) const;
  bool ExistVertexLabel(label_id_t label) const;
  bool ExistEdgeLabel(const std::string& label) const;
  bool ExistEdgeLabel(label_id_t label) const;
  size_t VertexNum() const;
  size_t VertexNum(label_id_t label) const;
  size_t EdgeNum() const;
  size_t EdgeNum(const label_id_t& src_label_id, const label_id_t& dst_label_id,
                 const label_id_t& edge_label_id) const;

  bool ExistEdgeTriplet(const label_id_t& src_label_id,
                        const label_id_t& dst_label_id,
                        const label_id_t& edge_label_id) const;

  const std::vector<std::pair<std::string, PropertyType>>& GetEdgeProperties(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id) const;

  const std::vector<PropertyType>& GetEdgePropertyTypes(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id) const;

  const std::vector<std::pair<std::string, PropertyType>>& GetVertexProperties(
      label_id_t label) const;

  /////////////////////////GRAPH DATA/////////////////////////
  bool GetVertexIndex(label_id_t label, const Any& id,
                      vertex_index_t& index) const;

  RTAny GetVertexId(label_id_t label, vertex_index_t index) const;

  template <typename T>
  impl::PropertyGetter<T, GRAPH_IMPL> GetVertexPropertyGetter(
      const label_id_t& label_id, const std::string& prop_name) const;

  Any GetVertexProperty(const label_id_t& label_id,
                        const std::string& prop_name, vertex_index_t vid) const;

  edge_iterator_t GetOutEdgeIterator(label_id_t src_label_id,
                                     label_id_t nbr_label_id,
                                     label_id_t edge_label_id,
                                     vertex_index_t vid) const;

  // TODO(zhanglei):Change the order of the src_label, dst_label
  edge_iterator_t GetInEdgeIterator(label_id_t dst_label_id,
                                    label_id_t nbr_label_id,
                                    label_id_t edge_label_id,
                                    vertex_index_t vid) const;

  template <typename EDATA_T>
  sub_graph_t<EDATA_T> GetOutgoingGraphView(label_id_t src_label_id,
                                            label_id_t dst_label_id,
                                            label_id_t edge_label_id) const;

  template <typename EDATA_T>
  sub_graph_t<EDATA_T> GetIncomingGraphView(label_id_t src_label_id,
                                            label_id_t dst_label_id,
                                            label_id_t edge_label_id) const;
};

template <>
class GraphInterface<ReadTransaction> {
 public:
  using label_id_t = label_t;
  using vertex_index_t = typename ReadTransaction::vertex_index_t;
  using edge_iterator_t = impl::EdgeIterator<ReadTransaction>;

  template <typename EDATA_T>
  using sub_graph_t = impl::SubGraph<EDATA_T, ReadTransaction>;

  GraphInterface(ReadTransaction& txn) : txn_(txn) {}

  label_id_t VertexLabelNum() const { return txn_.schema().vertex_label_num(); }

  label_id_t EdgeLabelNum() const { return txn_.schema().edge_label_num(); }

  std::vector<label_id_t> GetVertexLabels() const {
    std::vector<label_id_t> labels;
    for (label_id_t i = 0; i < VertexLabelNum(); ++i) {
      labels.push_back(i);
    }
    return labels;
  }

  std::vector<label_id_t> GetEdgeLabels() const {
    std::vector<label_id_t> labels;
    for (label_id_t i = 0; i < EdgeLabelNum(); ++i) {
      labels.push_back(i);
    }
    return labels;
  }

  label_id_t GetVertexLabelId(const std::string& label) const {
    return txn_.schema().get_vertex_label_id(label);
  }

  label_id_t GetEdgeLabelId(const std::string& label) const {
    return txn_.schema().get_edge_label_id(label);
  }

  std::string GetVertexLabelName(label_id_t label_id) const {
    return txn_.schema().get_vertex_label_name(label_id);
  }

  std::string GetEdgeLabelName(label_id_t label_id) const {
    return txn_.schema().get_edge_label_name(label_id);
  }

  const std::vector<std::tuple<gs::PropertyType, std::string, size_t>>&
  GetVertexPrimaryKeys(label_id_t label) const {
    return txn_.schema().get_vertex_primary_key(label);
  }

  bool ExistVertexLabel(const std::string& label) const {
    return txn_.schema().contains_vertex_label(label);
  }

  bool ExistEdgeLabel(const std::string& label) const {
    return txn_.schema().contains_edge_label(label);
  }

  bool ExistVertexLabel(label_id_t label) const {
    return label < VertexLabelNum();
  }

  bool ExistEdgeLabel(label_id_t label) const { return label < EdgeLabelNum(); }

  size_t VertexNum() const {
    size_t cnt = 0;
    for (label_id_t i = 0; i < VertexLabelNum(); ++i) {
      cnt += VertexNum(i);
    }
    return cnt;
  }

  size_t VertexNum(label_id_t label) const { return txn_.GetVertexNum(label); }

  size_t EdgeNum() const {
    size_t cnt = 0;
    for (label_id_t src_label = 0; src_label < VertexLabelNum(); ++src_label) {
      for (label_id_t dst_label = 0; dst_label < VertexLabelNum();
           ++dst_label) {
        for (label_id_t edge_label = 0; edge_label < EdgeLabelNum();
             ++edge_label) {
          cnt += EdgeNum(src_label, dst_label, edge_label);
        }
      }
    }
    return cnt;
  }

  size_t EdgeNum(const label_id_t& src_label_id, const label_id_t& dst_label_id,
                 const label_id_t& edge_label_id) const {
    if (!ExistEdgeTriplet(src_label_id, dst_label_id, edge_label_id)) {
      return 0;
    }
    size_t res = 0;
    auto oe_csr =
        txn_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
    auto ie_csr =
        txn_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
    if (oe_csr) {
      res = oe_csr->edge_num();
    } else if (ie_csr) {
      res = ie_csr->edge_num();
    }
    return res;
  }

  bool ExistEdgeTriplet(const label_id_t& src_label_id,
                        const label_id_t& dst_label_id,
                        const label_id_t& edge_label_id) const {
    return txn_.schema().exist(src_label_id, dst_label_id, edge_label_id);
  }
  std::vector<std::string> GetEdgePropertyNames(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id) const {
    if (!ExistEdgeTriplet(src_label_id, dst_label_id, edge_label_id)) {
      return {};
    }
    return txn_.schema().get_edge_property_names(src_label_id, dst_label_id,
                                                 edge_label_id);
  }

  std::vector<PropertyType> GetEdgePropertyTypes(
      const label_id_t& src_label_id, const label_id_t& dst_label_id,
      const label_id_t& edge_label_id) const {
    if (!ExistEdgeTriplet(src_label_id, dst_label_id, edge_label_id)) {
      return {};
    }
    return txn_.schema().get_edge_properties(src_label_id, dst_label_id,
                                             edge_label_id);
  }

  std::vector<std::string> GetVertexPropertyNames(label_id_t label) const {
    return txn_.schema().get_vertex_property_names(label);
  }

  std::vector<PropertyType> GetVertexPropertyTypes(label_id_t label) const {
    return txn_.schema().get_vertex_properties(label);
  }

  /////////////////////////GRAPH DATA/////////////////////////
  bool GetVertexIndex(label_id_t label, const Any& id,
                      vertex_index_t& index) const {
    return txn_.GetVertexIndex(label, id, index);
  }

  Any GetVertexId(label_id_t label, vertex_index_t index) const {
    return txn_.GetVertexId(label, index);
  }

  template <typename T>
  impl::PropertyGetter<T, ReadTransaction> GetVertexPropertyGetter(
      const label_id_t& label_id, const std::string& prop_name) const {
    return impl::PropertyGetter<T, ReadTransaction>(
        txn_.get_vertex_property_column(label_id, prop_name).get());
  }

  Any GetVertexProperty(const label_id_t& label_id, const size_t prop_index,
                        vertex_index_t vid) const {
    auto vertex_iter = txn_.GetVertexIterator(label_id);
    vertex_iter.Goto(vid);
    if (!vertex_iter.IsValid() ||
        vertex_iter.FieldNum() <= (int32_t) prop_index) {
      LOG(ERROR) << "Invalid vertex or property index: " << vid << " "
                 << prop_index;
      return Any();
    }
    return vertex_iter.GetField(prop_index);
  }

  edge_iterator_t GetOutEdgeIterator(label_id_t src_label_id,
                                     label_id_t nbr_label_id,
                                     label_id_t edge_label_id,
                                     vertex_index_t vid) const {
    return edge_iterator_t(txn_.GetOutEdgeIterator(
        src_label_id, vid, nbr_label_id, edge_label_id));
  }

  edge_iterator_t GetInEdgeIterator(label_id_t dst_label_id,
                                    label_id_t nbr_label_id,
                                    label_id_t edge_label_id,
                                    vertex_index_t vid) const {
    return edge_iterator_t(
        txn_.GetInEdgeIterator(dst_label_id, vid, nbr_label_id, edge_label_id));
  }

  template <typename EDATA_T>
  sub_graph_t<EDATA_T> GetOutgoingGraphView(label_id_t src_label_id,
                                            label_id_t dst_label_id,
                                            label_id_t edge_label_id) const {
    return sub_graph_t<EDATA_T>(txn_.GetOutgoingGraphView<EDATA_T>(
        src_label_id, dst_label_id, edge_label_id));
  }

  template <typename EDATA_T>
  sub_graph_t<EDATA_T> GetIncomingGraphView(label_id_t src_label_id,
                                            label_id_t dst_label_id,
                                            label_id_t edge_label_id) const {
    return sub_graph_t<EDATA_T>(txn_.GetIncomingGraphView<EDATA_T>(
        src_label_id, dst_label_id, edge_label_id));
  }

 private:
  ReadTransaction& txn_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_GRAPH_INTERFACE_H_