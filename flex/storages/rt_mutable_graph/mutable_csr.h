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

#ifndef GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
#define GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_

#include <atomic>
#include <type_traits>
#include <vector>

#include "flex/storages/rt_mutable_graph/mutable_csr_impl.h"
#include "flex/storages/rt_mutable_graph/mutable_csr_view.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/allocators.h"
#include "flex/utils/mmap_array.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"

namespace gs {

template <typename VID_T>
class GenericNbrIteratorMut {
 public:
  GenericNbrIteratorMut() = default;
  virtual ~GenericNbrIteratorMut() = default;

  virtual bool IsValid() const = 0;

  virtual void Next() = 0;

  virtual Property GetData() const = 0;

  virtual VID_T GetNeighbor() const = 0;

  virtual void SetData(const Property& value) = 0;
};

template <typename VID_T, typename TS_T>
class MutableCsrBase {
 public:
  MutableCsrBase() {}
  virtual ~MutableCsrBase() {}

  virtual void batch_init(VID_T vnum, const std::vector<int>& degree) = 0;

  virtual void batch_put_generic_edge(VID_T src, VID_T dst,
                                      const Property& data, TS_T ts) = 0;

  virtual void put_generic_edge(VID_T src, VID_T dst, const Property& data,
                                TS_T ts, ArenaAllocator& alloc) = 0;

  virtual void Serialize(const std::string& path) = 0;

  virtual void Deserialize(const std::string& path) = 0;

  virtual std::shared_ptr<GenericMutableCsrViewBase<VID_T>>
  get_generic_basic_graph_view(TS_T ts) const = 0;

  virtual std::shared_ptr<GenericNbrIteratorMut<VID_T>> generic_edge_iter_mut(
      VID_T v, TS_T ts) = 0;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class TypedMutableCsrBase : public MutableCsrBase<VID_T, TS_T> {
 public:
  virtual void batch_put_edge(VID_T src, VID_T dst, const EDATA_T& data,
                              TS_T ts = 0) = 0;
  virtual void put_edge(VID_T src, VID_T, const EDATA_T& data, TS_T ts,
                        ArenaAllocator& alloc) = 0;

  void batch_put_generic_edge(VID_T src, VID_T dst, const Property& data,
                              TS_T ts) override {
    batch_put_edge(src, dst, data.get_value<EDATA_T>(), ts);
  }

  void put_generic_edge(VID_T src, VID_T dst, const Property& data, TS_T ts,
                        ArenaAllocator& alloc) override {
    put_edge(src, dst, data.get_value<EDATA_T>(), ts, alloc);
  }

  virtual std::shared_ptr<MutableCsrViewBase<VID_T, EDATA_T>>
  get_basic_graph_view(TS_T ts) const = 0;

  std::shared_ptr<GenericMutableCsrViewBase<VID_T>>
  get_generic_basic_graph_view(TS_T ts) const override {
    return std::dynamic_pointer_cast<GenericMutableCsrViewBase<VID_T>>(
        get_basic_graph_view(ts));
  }
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class MutableCsrView : public MutableCsrViewBase<VID_T, EDATA_T> {
  using adjlist_t = mutable_csr_impl::AdjList<VID_T, EDATA_T, TS_T>;

 public:
  MutableCsrView(const adjlist_t* adjlists, TS_T timestamp)
      : adjlists_(adjlists), timestamp_(timestamp) {}
  ~MutableCsrView() {}

  mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T> get_edges(
      VID_T v) const {
    const auto& adjlist = adjlists_[v];
    return mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T>(
        adjlist.data(), adjlist.data() + adjlist.size(), timestamp_);
  }

  std::shared_ptr<NbrIterator<VID_T, EDATA_T>> get_basic_edges(
      VID_T src) const override {
    const auto& adjlist = adjlists_[src];
    return std::make_shared<
        mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T>>(
        adjlist.data(), adjlist.data() + adjlist.size(), timestamp_);
  }

 private:
  const adjlist_t* adjlists_;
  TS_T timestamp_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class MutableCsrNbrIterMut : public GenericNbrIteratorMut<VID_T> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, EDATA_T, TS_T>;

 public:
  MutableCsrNbrIterMut(nbr_t* begin, nbr_t* end, TS_T timestamp)
      : ptr_(begin), end_(end), timestamp_(timestamp) {
    while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
      ++ptr_;
    }
  }
  ~MutableCsrNbrIterMut() = default;

  bool IsValid() const override { return (ptr_ != end_); }

  void Next() override {
    ++ptr_;
    while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
      ++ptr_;
    }
  }

  const EDATA_T& GetTypedData() const { return ptr_->data; }

  Property GetData() const override {
    Property ret;
    ret.set_value<EDATA_T>(GetTypedData());
    return ret;
  }

  VID_T GetNeighbor() const override { return ptr_->neighbor; }

  void SetTypedData(const EDATA_T& value) {
    ptr_->data = value;
    ptr_->timestamp = timestamp_;
  }

  void SetData(const Property& value) override {
    SetTypedData(value.get_value<EDATA_T>());
  }

  void UpdateTimestamp() { ptr_->timestamp = timestamp_; }

 private:
  nbr_t* ptr_;
  nbr_t* end_;
  TS_T timestamp_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class MutableCsr : public TypedMutableCsrBase<VID_T, EDATA_T, TS_T> {
 public:
  using nbr_t = mutable_csr_impl::Nbr<VID_T, EDATA_T, TS_T>;
  using adjlist_t = mutable_csr_impl::AdjList<VID_T, EDATA_T, TS_T>;

  MutableCsr() : adj_lists_(nullptr), locks_(nullptr), capacity_(0) {}
  ~MutableCsr() {
    if (adj_lists_ != nullptr) {
      free(adj_lists_);
    }
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  void batch_init(VID_T vnum, const std::vector<int>& degree) override {
    capacity_ = vnum + (vnum + 3) / 4;
    if (capacity_ == 0) {
      capacity_ = 1024;
    }

    adj_lists_ = static_cast<adjlist_t*>(malloc(sizeof(adjlist_t) * capacity_));
    locks_ = new grape::SpinLock[capacity_];
    size_t edge_capacity = 0;
    for (auto d : degree) {
      edge_capacity += (d + (d + 4) / 5);
    }
    init_nbr_list_.resize(edge_capacity);

    nbr_t* ptr = init_nbr_list_.data();
    for (VID_T i = 0; i < vnum; ++i) {
      size_t cur_cap = degree[i] + (degree[i] + 4) / 5;
      adj_lists_[i].init(ptr, cur_cap, 0);
      ptr += cur_cap;
    }
    for (VID_T i = vnum; i < capacity_; ++i) {
      adj_lists_[i].init(ptr, 0, 0);
    }
  }

  void batch_put_edge(VID_T src, VID_T dst, const EDATA_T& data,
                      TS_T ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_edge(VID_T src, VID_T dst, const EDATA_T& data, TS_T ts,
                ArenaAllocator& allocator) override {
    CHECK_LT(src, capacity_);
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, allocator);
    locks_[src].unlock();
  }

  int degree(VID_T i) const { return adj_lists_[i].size(); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  MutableCsrView<VID_T, EDATA_T, TS_T> get_graph_view(TS_T ts) const {
    return MutableCsrView<VID_T, EDATA_T, TS_T>(adj_lists_, ts);
  }

  std::shared_ptr<MutableCsrViewBase<VID_T, EDATA_T>> get_basic_graph_view(
      TS_T ts) const override {
    return std::make_shared<MutableCsrView<VID_T, EDATA_T, TS_T>>(adj_lists_,
                                                                  ts);
  }

  MutableCsrNbrIterMut<VID_T, EDATA_T, TS_T> edge_iter_mut(VID_T src, TS_T ts) {
    return MutableCsrNbrIterMut<VID_T, EDATA_T, TS_T>(
        adj_lists_[src].begin(), adj_lists_[src].end(), ts);
  }

  std::shared_ptr<GenericNbrIteratorMut<VID_T>> generic_edge_iter_mut(
      VID_T src, TS_T ts) override {
    return std::make_shared<MutableCsrNbrIterMut<VID_T, EDATA_T, TS_T>>(
        adj_lists_[src].begin(), adj_lists_[src].end(), ts);
  }

 private:
  adjlist_t* adj_lists_;
  grape::SpinLock* locks_;
  VID_T capacity_;
  mmap_array<nbr_t> init_nbr_list_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class SingleMutableCsrView : public MutableCsrViewBase<VID_T, EDATA_T> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, EDATA_T, TS_T>;

 public:
  SingleMutableCsrView(const nbr_t* adjlists, TS_T timestamp)
      : adjlists_(adjlists), timestamp_(timestamp) {}

  ~SingleMutableCsrView() {}

  mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T> get_edges(
      VID_T src) const {
    const auto& adjlist = adjlists_[src];
    if (adjlist.timestamp > timestamp_) {
      return mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T>(
          nullptr, nullptr, timestamp_);
    } else {
      return mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T>(
          &adjlist, &adjlist + 1, timestamp_);
    }
  }

  std::shared_ptr<NbrIterator<VID_T, EDATA_T>> get_basic_edges(
      VID_T src) const override {
    const auto& adjlist = adjlists_[src];
    if (adjlist.timestamp > timestamp_) {
      return std::make_shared<
          mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T>>(
          nullptr, nullptr, timestamp_);
    } else {
      return std::make_shared<
          mutable_csr_view::PackedNbrIterator<VID_T, EDATA_T, TS_T>>(
          &adjlist, &adjlist + 1, timestamp_);
    }
  }

  const nbr_t& get_edge(VID_T src) const { return adjlists_[src]; }

  bool exist(VID_T src) const { return adjlists_[src].timestamp <= timestamp_; }

 private:
  const nbr_t* adjlists_;
  TS_T timestamp_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class SingleMutableCsr : public TypedMutableCsrBase<VID_T, EDATA_T, TS_T> {
 public:
  using nbr_t = mutable_csr_impl::Nbr<VID_T, EDATA_T, TS_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  void batch_init(VID_T vnum, const std::vector<int>& degree) override {
    VID_T capacity = vnum + (vnum + 3) / 4;
    nbr_list_.resize(capacity);
    for (VID_T i = 0; i < capacity; ++i) {
      nbr_list_[i].timestamp.store(std::numeric_limits<TS_T>::max());
    }
  }

  void batch_put_edge(VID_T src, VID_T dst, const EDATA_T& data,
                      TS_T ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(), std::numeric_limits<TS_T>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_generic_edge(VID_T src, VID_T dst, const Property& data, TS_T ts,
                        ArenaAllocator& alloc) override {
    put_edge(src, dst, data.get_value<EDATA_T>(), ts, alloc);
  }

  void put_edge(VID_T src, VID_T dst, const EDATA_T& data, TS_T ts,
                ArenaAllocator& alloc) override {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<TS_T>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  const nbr_t& get_edge(VID_T i) const { return nbr_list_[i]; }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  SingleMutableCsrView<VID_T, EDATA_T, TS_T> get_graph_view(TS_T ts) const {
    return SingleMutableCsrView<VID_T, EDATA_T, TS_T>(nbr_list_.data(), ts);
  }

  std::shared_ptr<MutableCsrViewBase<VID_T, EDATA_T>> get_basic_graph_view(
      TS_T ts) const override {
    return std::make_shared<SingleMutableCsrView<VID_T, EDATA_T, TS_T>>(
        nbr_list_.data(), ts);
  }

  MutableCsrNbrIterMut<VID_T, EDATA_T, TS_T> edge_iter_mut(VID_T src, TS_T ts) {
    return MutableCsrNbrIterMut<VID_T, EDATA_T, TS_T>(&nbr_list_[src],
                                                      &nbr_list_[src + 1], ts);
  }

  std::shared_ptr<GenericNbrIteratorMut<VID_T>> generic_edge_iter_mut(
      VID_T src, TS_T ts) override {
    return std::make_shared<MutableCsrNbrIterMut<VID_T, EDATA_T, TS_T>>(
        &nbr_list_[src], &nbr_list_[src + 1], ts);
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

template <typename VID_T, typename TS_T>
class StringMutableCsrView
    : public MutableCsrViewBase<VID_T, std::string_view> {
 public:
  StringMutableCsrView(const MutableCsr<VID_T, size_t, TS_T>& topo,
                       const StringColumn* col, TS_T ts)
      : rs_view_(topo.get_graph_view(ts)), column_(col) {}
  ~StringMutableCsrView() {}

  mutable_csr_view::StringNbrIterator<VID_T, TS_T> get_edges(VID_T src) const {
    return mutable_csr_view::StringNbrIterator<VID_T, TS_T>(
        rs_view_.get_edges(src), column_);
  }

  std::shared_ptr<NbrIterator<VID_T, std::string_view>> get_basic_edges(
      VID_T src) const override {
    return std::make_shared<mutable_csr_view::StringNbrIterator<VID_T, TS_T>>(
        rs_view_.get_edges(src), column_);
  }

  bool exist(VID_T src) const { return get_edges(src).IsValid(); }

 private:
  MutableCsrView<VID_T, size_t, TS_T> rs_view_;
  const StringColumn* column_;
};

template <typename VID_T, typename TS_T>
class StringMutableCsrNbrIterMut : public GenericNbrIteratorMut<VID_T> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, size_t, TS_T>;

 public:
  StringMutableCsrNbrIterMut(
      const MutableCsrNbrIterMut<VID_T, size_t, TS_T>& rs_iter,
      StringColumn* col)
      : rs_iter_(rs_iter), column_(col) {}
  ~StringMutableCsrNbrIterMut() = default;

  bool IsValid() const override { return rs_iter_.IsValid(); }

  void Next() override { rs_iter_.Next(); }

  Property GetData() const override {
    Property ret;
    ret.set_value<std::string_view>(column_->get_view(rs_iter_.GetTypedData()));
    return ret;
  }

  VID_T GetNeighbor() const override { return rs_iter_.GetNeighbor(); }

  void SetData(const Property& prop) override {
    if (prop.type() == PropertyType::kString) {
      SetData(prop.get_value<std::string>());
    } else if (prop.type() == PropertyType::kStringView) {
      SetData(prop.get_value<std::string_view>());
    } else {
      LOG(FATAL) << "Unexpected property type: " << prop.type() << ", while string or string_view is expected...";
    }
    rs_iter_.UpdateTimestamp();
  }

  void SetData(const std::string_view& value) {
    column_->set_value(rs_iter_.GetTypedData(), value);
    rs_iter_.UpdateTimestamp();
  }

  void SetData(const std::string& value) {
    column_->set_value(rs_iter_.GetTypedData(), value);
    rs_iter_.UpdateTimestamp();
  }

 private:
  MutableCsrNbrIterMut<VID_T, size_t, TS_T> rs_iter_;
  StringColumn* column_;
};

template <typename VID_T, typename TS_T>
class StringMutableCsr
    : public TypedMutableCsrBase<VID_T, std::string_view, TS_T> {
 public:
  StringMutableCsr() : column_ptr_(nullptr), topology_() {}
  ~StringMutableCsr() {}

  void set_column(StringColumn* col) { column_ptr_ = col; }

  StringColumn& get_column() { return *column_ptr_; }

  const StringColumn& get_column() const { return *column_ptr_; }

  void batch_init(VID_T vnum, const std::vector<int>& degrees) override {
    topology_.batch_init(vnum, degrees);
  }

  void batch_put_edge_with_index(VID_T src, VID_T dst, size_t index, TS_T ts) {
    topology_.batch_put_edge(src, dst, index, ts);
  }

  void put_edge_with_index(VID_T src, VID_T dst, size_t index, TS_T ts,
                           ArenaAllocator& alloc) {
    topology_.put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge(VID_T src, VID_T dst, const std::string_view& prop,
                      TS_T ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void put_edge(VID_T src, VID_T dst, const std::string_view& prop, TS_T ts,
                ArenaAllocator& alloc) override {
    LOG(FATAL) << "Not implemented";
  }

  int degree(VID_T i) const { return topology_.degree(i); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  StringMutableCsrView<VID_T, TS_T> get_graph_view(TS_T ts) const {
    return StringMutableCsrView<VID_T, TS_T>(topology_, column_ptr_, ts);
  }

  std::shared_ptr<MutableCsrViewBase<VID_T, std::string_view>> get_basic_graph_view(
      TS_T ts) const override {
    return std::make_shared<StringMutableCsrView<VID_T, TS_T>>(topology_,
                                                               column_ptr_, ts);
  }

  StringMutableCsrNbrIterMut<VID_T, TS_T> edge_iter_mut(VID_T src, TS_T ts) {
    return StringMutableCsrNbrIterMut<VID_T, TS_T>(
        topology_.edge_iter_mut(src, ts), column_ptr_);
  }

  std::shared_ptr<GenericNbrIteratorMut<VID_T>> generic_edge_iter_mut(
      VID_T src, TS_T ts) override {
    return std::make_shared<StringMutableCsrNbrIterMut<VID_T, TS_T>>(
        topology_.edge_iter_mut(src, ts), column_ptr_);
  }

 private:
  StringColumn* column_ptr_;
  MutableCsr<VID_T, size_t, TS_T> topology_;
};

template <typename VID_T, typename TS_T>
class TableMutableCsrView : public MutableCsrViewBase<VID_T, Property> {
 public:
  TableMutableCsrView(const MutableCsr<VID_T, size_t, TS_T>& topo,
                      const Table* table, TS_T ts)
      : rs_view_(topo.get_graph_view(ts)), table_(table) {}
  ~TableMutableCsrView() {}

  mutable_csr_view::TableNbrIterator<VID_T, TS_T> get_edges(VID_T src) const {
    return mutable_csr_view::TableNbrIterator<VID_T, TS_T>(
        rs_view_.get_edges(src), table_);
  }

  std::shared_ptr<NbrIterator<VID_T, Property>> get_basic_edges(
      VID_T src) const override {
    return std::make_shared<mutable_csr_view::TableNbrIterator<VID_T, TS_T>>(
        rs_view_.get_edges(src), table_);
  }

  bool exist(VID_T src) const { return get_edges(src).IsValid(); }

 private:
  MutableCsrView<VID_T, size_t, TS_T> rs_view_;
  const Table* table_;
};

template <typename VID_T, typename TS_T>
class TableMutableCsrNbrIterMut : public GenericNbrIteratorMut<VID_T> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, size_t, TS_T>;

 public:
  TableMutableCsrNbrIterMut(
      const MutableCsrNbrIterMut<VID_T, size_t, TS_T>& rs_iter, Table* table)
      : rs_iter_(rs_iter), table_(table) {}
  ~TableMutableCsrNbrIterMut() = default;

  bool IsValid() const override { return rs_iter_.IsValid(); }

  void Next() override { rs_iter_.Next(); }

  Property GetData() const override {
    return table_->get_row(rs_iter_.GetTypedData());
  }

  VID_T GetNeighbor() const override { return rs_iter_.GetNeighbor(); }

  void SetData(const Property& value) override {
    table_->insert(rs_iter_.GetTypedData(), value);
    rs_iter_.UpdateTimestamp();
  }

 private:
  MutableCsrNbrIterMut<VID_T, size_t, TS_T> rs_iter_;
  Table* table_;
};

template <typename VID_T, typename TS_T>
class TableMutableCsr : public TypedMutableCsrBase<VID_T, Property, TS_T> {
 public:
  TableMutableCsr() : table_ptr_(nullptr), topology_() {}
  ~TableMutableCsr() {}

  void set_table(Table* table) { table_ptr_ = table; }

  Table& get_table() { return *table_ptr_; }

  const Table& get_table() const { return *table_ptr_; }

  void batch_init(VID_T vnum, const std::vector<int>& degrees) override {
    topology_.batch_init(vnum, degrees);
  }

  void batch_put_edge_with_index(VID_T src, VID_T dst, size_t index, TS_T ts) {
    topology_.batch_put_edge(src, dst, index, ts);
  }

  void put_edge_with_index(VID_T src, VID_T dst, size_t index, TS_T ts,
                           ArenaAllocator& alloc) {
    topology_.put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge(VID_T src, VID_T dst, const Property& prop,
                      TS_T ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void put_edge(VID_T src, VID_T dst, const Property& prop, TS_T ts,
                ArenaAllocator& alloc) override {
    LOG(FATAL) << "Not implemented";
  }

  int degree(VID_T i) const { return topology_.degree(i); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  TableMutableCsrView<VID_T, TS_T> get_graph_view(TS_T ts) const {
    return TableMutableCsrView<VID_T, TS_T>(topology_, table_ptr_, ts);
  }

  std::shared_ptr<MutableCsrViewBase<VID_T, Property>> get_basic_graph_view(
      TS_T ts) const override {
    return std::make_shared<TableMutableCsrView<VID_T, TS_T>>(topology_,
                                                              table_ptr_, ts);
  }

  TableMutableCsrNbrIterMut<VID_T, TS_T> edge_iter_mut(VID_T src, TS_T ts) {
    return TableMutableCsrNbrIterMut<VID_T, TS_T>(
        topology_.edge_iter_mut(src, ts), table_ptr_);
  }

  std::shared_ptr<GenericNbrIteratorMut<VID_T>> generic_edge_iter_mut(
      VID_T src, TS_T ts) override {
    return std::make_shared<TableMutableCsrNbrIterMut<VID_T, TS_T>>(
        topology_.edge_iter_mut(src, ts), table_ptr_);
  }

 private:
  Table* table_ptr_;
  MutableCsr<VID_T, size_t, TS_T> topology_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
