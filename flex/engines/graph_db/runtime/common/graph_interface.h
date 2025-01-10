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

#ifndef RUNTIME_COMMON_GRAPH_INTERFACE_H_
#define RUNTIME_COMMON_GRAPH_INTERFACE_H_

#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

namespace runtime {

namespace graph_interface_impl {

using gs::label_t;
using gs::timestamp_t;
using gs::vid_t;

template <typename PROP_T>
class VertexColumn {
 public:
  VertexColumn(const std::shared_ptr<ColumnBase>& column) {
    if (column == nullptr) {
      column_ = nullptr;
    } else {
      column_ = dynamic_cast<const TypedColumn<PROP_T>*>(column.get());
    }
  }
  VertexColumn() : column_(nullptr) {}

  inline PROP_T get_view(vid_t v) const { return column_->get_view(v); }

  inline bool is_null() const { return column_ == nullptr; }

 private:
  const TypedColumn<PROP_T>* column_;
};

class VertexSet {
 public:
  VertexSet(vid_t size) : size_(size) {}
  ~VertexSet() {}

  class iterator {
   public:
    iterator(vid_t v) : v_(v) {}
    ~iterator() {}

    inline vid_t operator*() const { return v_; }

    inline iterator& operator++() {
      ++v_;
      return *this;
    }

    inline bool operator==(const iterator& rhs) const { return v_ == rhs.v_; }

    inline bool operator!=(const iterator& rhs) const { return v_ != rhs.v_; }

   private:
    vid_t v_;
  };

  inline iterator begin() const { return iterator(0); }
  inline iterator end() const { return iterator(size_); }
  inline size_t size() const { return size_; }

 private:
  vid_t size_;
};

class EdgeIterator {
 public:
  EdgeIterator(gs::ReadTransaction::edge_iterator&& iter)
      : iter_(std::move(iter)) {}
  ~EdgeIterator() {}

  inline Any GetData() const { return iter_.GetData(); }
  inline bool IsValid() const { return iter_.IsValid(); }
  inline void Next() { iter_.Next(); }
  inline vid_t GetNeighbor() const { return iter_.GetNeighbor(); }
  inline label_t GetNeighborLabel() const { return iter_.GetNeighborLabel(); }
  inline label_t GetEdgeLabel() const { return iter_.GetEdgeLabel(); }

 private:
  gs::ReadTransaction::edge_iterator iter_;
};

template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    // const_nbr_t provide two methods:
    // 1. vid_t get_neighbor() const;
    // 2. const EDATA_T& get_data() const;
    using const_nbr_t = typename gs::MutableNbrSlice<EDATA_T>::const_nbr_t;
    using const_nbr_ptr_t =
        typename gs::MutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

   public:
    nbr_iterator(const_nbr_ptr_t ptr, const_nbr_ptr_t end,
                 timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_ != end_ && ptr_->get_timestamp() > timestamp_) {
        ++ptr_;
      }
    }

    inline const_nbr_t& operator*() const { return *ptr_; }

    inline const_nbr_ptr_t operator->() const { return ptr_; }

    inline nbr_iterator& operator++() {
      ++ptr_;
      while (ptr_ != end_ && ptr_->get_timestamp() > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    inline bool operator==(const nbr_iterator& rhs) const {
      return (ptr_ == rhs.ptr_);
    }

    inline bool operator!=(const nbr_iterator& rhs) const {
      return (ptr_ != rhs.ptr_);
    }

   private:
    const_nbr_ptr_t ptr_;
    const_nbr_ptr_t end_;
    timestamp_t timestamp_;
  };

 public:
  using slice_t = gs::MutableNbrSlice<EDATA_T>;
  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  inline nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  inline nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView() : csr_(nullptr), timestamp_(0), unsorted_since_(0) {}
  GraphView(const gs::MutableCsr<EDATA_T>* csr, timestamp_t timestamp)
      : csr_(csr),
        timestamp_(timestamp),
        unsorted_since_(csr->unsorted_since()) {}

  inline bool is_null() const { return csr_ == nullptr; }

  inline AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_->get_edges(v), timestamp_);
  }

  template <typename FUNC_T>
  inline void foreach_edges_gt(vid_t v, const EDATA_T& min_value,
                        const FUNC_T& func) const {
    const auto& edges = csr_->get_edges(v);
    auto ptr = edges.end() - 1;
    auto end = edges.begin() - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp_) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since_) {
        break;
      }
      if (min_value < ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    while (ptr != end) {
      if (ptr->data < min_value) {
        break;
      }
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

  template <typename FUNC_T>
  inline void foreach_edges_lt(vid_t v, const EDATA_T& max_value,
                        const FUNC_T& func) const {
    const auto& edges = csr_->get_edges(v);
    auto ptr = edges.end() - 1;
    auto end = edges.begin() - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp_) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since_) {
        break;
      }
      if (ptr->data < max_value) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    if (ptr == end) {
      return;
    }
    ptr = std::upper_bound(end + 1, ptr + 1, max_value,
                           [](const EDATA_T& a, const MutableNbr<EDATA_T>& b) {
                             return a < b.data;
                           }) -
          1;
    while (ptr != end) {
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

 private:
  const gs::MutableCsr<EDATA_T>* csr_;
  timestamp_t timestamp_;
  timestamp_t unsorted_since_;
};

template <typename T>
class VertexArray {
 public:
  VertexArray() : data_() {}
  VertexArray(const VertexSet& keys, const T& val) : data_(keys.size(), val) {}
  ~VertexArray() {}

  inline void Init(const VertexSet& keys, const T& val) {
    data_.resize(keys.size(), val);
  }

  inline typename std::vector<T>::reference operator[](vid_t v) { return data_[v]; }
  inline typename std::vector<T>::const_reference operator[](vid_t v) const {
    return data_[v];
  }

 private:
  std::vector<T> data_;
};

}  // namespace graph_interface_impl

class GraphReadInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = graph_interface_impl::VertexColumn<PROP_T>;

  using vertex_set_t = graph_interface_impl::VertexSet;

  using edge_iterator_t = graph_interface_impl::EdgeIterator;

  template <typename EDATA_T>
  using graph_view_t = graph_interface_impl::GraphView<EDATA_T>;

  template <typename T>
  using vertex_array_t = graph_interface_impl::VertexArray<T>;

  static constexpr vid_t kInvalidVid = std::numeric_limits<vid_t>::max();

  GraphReadInterface(const gs::ReadTransaction& txn) : txn_(txn) {}
  ~GraphReadInterface() {}

  template <typename PROP_T>
  inline vertex_column_t<PROP_T> GetVertexColumn(label_t label,
                                          const std::string& prop_name) const {
    return vertex_column_t<PROP_T>(
        txn_.get_vertex_property_column(label, prop_name));
  }

  inline vertex_set_t GetVertexSet(label_t label) const {
    return vertex_set_t(txn_.GetVertexNum(label));
  }

  inline bool GetVertexIndex(label_t label, const Any& id, vid_t& index) const {
    return txn_.GetVertexIndex(label, id, index);
  }

  inline Any GetVertexId(label_t label, vid_t index) const {
    return txn_.GetVertexId(label, index);
  }

  inline Any GetVertexProperty(label_t label, vid_t index, int prop_id) const {
    return txn_.graph().get_vertex_table(label).at(index, prop_id);
  }

  inline edge_iterator_t GetOutEdgeIterator(label_t label, vid_t v,
                                     label_t neighbor_label,
                                     label_t edge_label) const {
    return edge_iterator_t(
        txn_.GetOutEdgeIterator(label, v, neighbor_label, edge_label));
  }

  inline edge_iterator_t GetInEdgeIterator(label_t label, vid_t v,
                                    label_t neighbor_label,
                                    label_t edge_label) const {
    return edge_iterator_t(
        txn_.GetInEdgeIterator(label, v, neighbor_label, edge_label));
  }

  template <typename EDATA_T>
  inline graph_view_t<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                             label_t neighbor_label,
                                             label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        txn_.graph().get_oe_csr(v_label, neighbor_label, edge_label));
    return graph_view_t<EDATA_T>(csr, txn_.timestamp());
  }

  template <typename EDATA_T>
  inline graph_view_t<EDATA_T> GetIncomingGraphView(label_t v_label,
                                             label_t neighbor_label,
                                             label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        txn_.graph().get_ie_csr(v_label, neighbor_label, edge_label));
    return graph_view_t<EDATA_T>(csr, txn_.timestamp());
  }

  inline const Schema& schema() const { return txn_.schema(); }

 private:
  const gs::ReadTransaction& txn_;
};

class GraphInsertInterface {
 public:
  GraphInsertInterface(gs::InsertTransaction& txn) : txn_(txn) {}
  ~GraphInsertInterface() {}

  inline bool AddVertex(label_t label, const Any& id, const std::vector<Any>& props) {
    return txn_.AddVertex(label, id, props);
  }

  inline bool AddEdge(label_t src_label, const Any& src, label_t dst_label,
               const Any& dst, label_t edge_label, const Any& prop) {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, prop);
  }

  inline void Commit() { txn_.Commit(); }

  inline void Abort() { txn_.Abort(); }

  inline const Schema& schema() const { return txn_.schema(); }

 private:
  gs::InsertTransaction& txn_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_GRAPH_INTERFACE_H_
