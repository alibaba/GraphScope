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

#ifndef GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

#include <limits>
#include <utility>

#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

class MutablePropertyFragment;
class GraphDBSession;
class VersionManager;
template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    using const_nbr_t = typename MutableNbrSlice<EDATA_T>::const_nbr_t;
    using const_nbr_ptr_t = typename MutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

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
  using slice_t = MutableNbrSlice<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  inline nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  inline nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

  inline int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class ImmutableAdjListView {
  class nbr_iterator {
    using const_nbr_t = typename ImmutableNbrSlice<EDATA_T>::const_nbr_t;
    using const_nbr_ptr_t =
        typename ImmutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

   public:
    nbr_iterator(const_nbr_ptr_t ptr, const_nbr_ptr_t end)
        : ptr_(ptr), end_(end) {}

    inline const_nbr_t& operator*() const { return *ptr_; }

    inline const_nbr_ptr_t operator->() const { return ptr_; }

    inline nbr_iterator& operator++() {
      ++ptr_;
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
  };

 public:
  using slice_t = ImmutableNbrSlice<EDATA_T>;

  ImmutableAdjListView(const slice_t& slice) : edges_(slice) {}

  inline nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end());
  }
  inline nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end());
  }

  inline int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView(const MutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr),
        timestamp_(timestamp),
        unsorted_since_(csr.unsorted_since()) {}

  inline AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_.get_edges(v), timestamp_);
  }

  // iterate edges with data in [min_value, max_value)
  template <typename FUNC_T>
  void foreach_edges_between(vid_t v, EDATA_T& min_value, EDATA_T& max_value,
                             const FUNC_T& func) const {
    const auto& edges = csr_.get_edges(v);
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
      if (!(ptr->data < min_value) && (ptr->data < max_value)) {
        func(*ptr, min_value, max_value);
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
      if (ptr->data < min_value) {
        break;
      }
      func(*ptr, min_value, max_value);
      --ptr;
    }
  }

  // iterate edges with data in (min_value, +inf)
  template <typename FUNC_T>
  void foreach_edges_gt(vid_t v, EDATA_T& min_value, const FUNC_T& func) const {
    const auto& edges = csr_.get_edges(v);
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
        func(*ptr, min_value);
      }
      --ptr;
    }
    while (ptr != end) {
      if (!(min_value < ptr->data)) {
        break;
      }
      func(*ptr, min_value);
      --ptr;
    }
  }

  template <typename FUNC_T>
  void foreach_edges_lt(vid_t v, const EDATA_T& max_value,
                        const FUNC_T& func) const {
    const auto& edges = csr_.get_edges(v);
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
        func(*ptr);
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
      func(*ptr);
      --ptr;
    }
  }

  // iterate edges with data in [min_value, +inf)
  template <typename FUNC_T>
  void foreach_edges_ge(vid_t v, EDATA_T& min_value, const FUNC_T& func) const {
    const auto& edges = csr_.get_edges(v);
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
      if (!(ptr->data < min_value)) {
        func(*ptr, min_value);
      }
      --ptr;
    }
    while (ptr != end) {
      if (ptr->data < min_value) {
        break;
      }
      func(*ptr, min_value);
      --ptr;
    }
  }

 private:
  const MutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
  timestamp_t unsorted_since_;
};

template <typename EDATA_T>
class ImmutableGraphView {
 public:
  ImmutableGraphView(const ImmutableCsr<EDATA_T>& csr) : csr_(csr) {}

  inline ImmutableAdjListView<EDATA_T> get_edges(vid_t v) const {
    return ImmutableAdjListView<EDATA_T>(csr_.get_edges(v));
  }

 private:
  const ImmutableCsr<EDATA_T>& csr_;
};

template <typename EDATA_T>
class SingleGraphView {
 public:
  SingleGraphView(const SingleMutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  inline bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }

  inline const MutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

 private:
  const SingleMutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

template <>
class SingleGraphView<std::string_view> {
 public:
  SingleGraphView(const SingleMutableCsr<std::string_view>& csr,
                  timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }

  MutableNbr<std::string_view> get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

 private:
  const SingleMutableCsr<std::string_view>& csr_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class SingleImmutableGraphView {
 public:
  SingleImmutableGraphView(const SingleImmutableCsr<EDATA_T>& csr)
      : csr_(csr) {}

  inline bool exist(vid_t v) const {
    return (csr_.get_edge(v).neighbor != std::numeric_limits<vid_t>::max());
  }

  inline const ImmutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

 private:
  const SingleImmutableCsr<EDATA_T>& csr_;
};

template <>
class SingleImmutableGraphView<std::string_view> {
 public:
  SingleImmutableGraphView(const SingleImmutableCsr<std::string_view>& csr)
      : csr_(csr) {}

  bool exist(vid_t v) const {
    return (csr_.get_edge(v).neighbor != std::numeric_limits<vid_t>::max());
  }

  ImmutableNbr<std::string_view> get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

 private:
  const SingleImmutableCsr<std::string_view>& csr_;
};

class ReadTransaction {
 public:
  ReadTransaction(const GraphDBSession& session,
                  const MutablePropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  std::string run(const std::string& cypher,
                  const std::map<std::string, std::string>& params) const;

  timestamp_t timestamp() const;

  bool Commit();

  void Abort();

  const MutablePropertyFragment& graph() const;

  /*
   * @brief Get the handle of the vertex property column, only for non-primary
   * key columns.
   */
  const std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    return graph_.get_vertex_table(label).get_column(col_name);
  }

  /**
   * @brief Get the handle of the vertex property column, including the primary
   * key.
   * @tparam T The type of the column.
   * @param label The label of the vertex.
   * @param col_name The name of the column.
   */
  template <typename T>
  const std::shared_ptr<TypedRefColumn<T>> get_vertex_ref_property_column(
      uint8_t label, const std::string& col_name) const {
    auto pk = graph_.schema().get_vertex_primary_key(label);
    CHECK(pk.size() == 1) << "Only support single primary key";
    if (col_name == std::get<1>(pk[0])) {
      return std::dynamic_pointer_cast<TypedRefColumn<T>>(
          graph_.get_vertex_id_column(label));
    } else {
      auto ptr = graph_.get_vertex_table(label).get_column(col_name);
      if (ptr) {
        return std::dynamic_pointer_cast<TypedRefColumn<T>>(
            CreateRefColumn(ptr));
      } else {
        return nullptr;
      }
    }
  }

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t num,
                    const MutablePropertyFragment& graph);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    Any GetId() const;
    vid_t GetIndex() const;

    Any GetField(int col_id) const;
    int FieldNum() const;

   private:
    label_t label_;
    vid_t cur_;
    vid_t num_;
    const MutablePropertyFragment& graph_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<CsrConstEdgeIterBase> iter);
    ~edge_iterator();

    Any GetData() const;

    bool IsValid() const;

    void Next();

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<CsrConstEdgeIterBase> iter_;
  };

  vertex_iterator GetVertexIterator(label_t label) const;

  vertex_iterator FindVertex(label_t label, const Any& id) const;

  bool GetVertexIndex(label_t label, const Any& id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  Any GetVertexId(label_t label, vid_t index) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighbor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighbor_label,
                                  label_t edge_label) const;

  size_t GetOutDegree(label_t label, vid_t u, label_t neighbor_label,
                      label_t edge_label) const;

  size_t GetInDegree(label_t label, vid_t u, label_t neighbor_label,
                     label_t edge_label) const;

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  inline const Schema& schema() const { return graph_.schema(); }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleImmutableGraphView<EDATA_T> GetOutgoingSingleImmutableGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleImmutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleImmutableGraphView<EDATA_T>(*csr);
  }

  template <typename EDATA_T>
  SingleImmutableGraphView<EDATA_T> GetIncomingSingleImmutableGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleImmutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleImmutableGraphView<EDATA_T>(*csr);
  }

  template <typename EDATA_T>
  ImmutableGraphView<EDATA_T> GetOutgoingImmutableGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const ImmutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return ImmutableGraphView<EDATA_T>(*csr);
  }

  template <typename EDATA_T>
  ImmutableGraphView<EDATA_T> GetIncomingImmutableGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const ImmutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return ImmutableGraphView<EDATA_T>(*csr);
  }

  const GraphDBSession& GetSession() const;

 private:
  void release();
  const GraphDBSession& session_;
  const MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
