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
#include <cmath>
#include <filesystem>
#include <thread>
#include <type_traits>
#include <vector>

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/allocators.h"
#include "flex/utils/mmap_array.h"
#include "flex/utils/property/column.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"

namespace gs {

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

  MutableNbr& operator=(const MutableNbr& rhs) {
    neighbor = rhs.neighbor;
    timestamp.store(rhs.timestamp.load());
    data = rhs.data;
    return *this;
  }

  const EDATA_T& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }
  timestamp_t get_timestamp() const { return timestamp.load(); }

  void set_data(const EDATA_T& val, timestamp_t ts) {
    data = val;
    timestamp.store(ts);
  }
  void set_neighbor(vid_t neighbor) { neighbor = neighbor; }
  void set_timestamp(timestamp_t ts) { timestamp.store(ts); }

  vid_t neighbor;
  std::atomic<timestamp_t> timestamp;
  EDATA_T data;
};

template <>
struct MutableNbr<grape::EmptyType> {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor), timestamp(rhs.timestamp.load()) {}
  ~MutableNbr() = default;

  MutableNbr& operator=(const MutableNbr& rhs) {
    neighbor = rhs.neighbor;
    timestamp.store(rhs.timestamp.load());
    return *this;
  }

  void set_data(const grape::EmptyType&, timestamp_t ts) {
    timestamp.store(ts);
  }
  void set_neighbor(vid_t neighbor) { neighbor = neighbor; }
  void set_timestamp(timestamp_t ts) { timestamp.store(ts); }
  const grape::EmptyType& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }
  timestamp_t get_timestamp() const { return timestamp.load(); }
  vid_t neighbor;
  union {
    std::atomic<timestamp_t> timestamp;
    grape::EmptyType data;
  };
};

template <typename EDATA_T>
class MutableNbrSlice {
 public:
  using const_nbr_t = const MutableNbr<EDATA_T>;
  using const_nbr_ptr_t = const MutableNbr<EDATA_T>*;
  MutableNbrSlice() = default;
  MutableNbrSlice(const MutableNbrSlice& rhs)
      : ptr_(rhs.ptr_), size_(rhs.size_) {}
  ~MutableNbrSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(const_nbr_ptr_t ptr) { ptr_ = ptr; }

  const_nbr_ptr_t begin() const { return ptr_; }
  const_nbr_ptr_t end() const { return ptr_ + size_; }

  static MutableNbrSlice empty() {
    MutableNbrSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  const_nbr_ptr_t ptr_;
  int size_;
};

template <>
class MutableNbrSlice<std::string_view> {
 public:
  struct MutableColumnNbr {
    using const_nbr_t = const MutableNbr<size_t>;
    using const_nbr_ptr_t = const MutableNbr<size_t>*;

    MutableColumnNbr(const_nbr_ptr_t ptr, const StringColumn& column)
        : ptr_(ptr), column_(column) {}
    vid_t get_neighbor() const { return ptr_->neighbor; }
    std::string_view get_data() const { return column_.get_view(ptr_->data); }
    timestamp_t get_timestamp() const { return ptr_->timestamp.load(); }

    const MutableColumnNbr& operator*() const { return *this; }
    const MutableColumnNbr* operator->() const { return this; }
    const MutableColumnNbr& operator=(const MutableColumnNbr& nbr) const {
      ptr_ = nbr.ptr_;
      return *this;
    }
    bool operator==(const MutableColumnNbr& nbr) const {
      return ptr_ == nbr.ptr_;
    }
    bool operator!=(const MutableColumnNbr& nbr) const {
      return ptr_ != nbr.ptr_;
    }
    const MutableColumnNbr& operator++() const {
      ++ptr_;
      return *this;
    }

    const MutableColumnNbr& operator+=(size_t n) const {
      ptr_ += n;
      return *this;
    }

    size_t operator-(const MutableColumnNbr& nbr) const {
      return ptr_ - nbr.ptr_;
    }

    bool operator<(const MutableColumnNbr& nbr) const {
      return ptr_ < nbr.ptr_;
    }

    mutable const_nbr_ptr_t ptr_;
    const StringColumn& column_;
  };
  using const_nbr_t = const MutableColumnNbr;
  using const_nbr_ptr_t = const MutableColumnNbr;
  MutableNbrSlice(const StringColumn& column) : slice_(), column_(column) {}
  MutableNbrSlice(const MutableNbrSlice& rhs)
      : slice_(rhs.slice_), column_(rhs.column_) {}
  ~MutableNbrSlice() = default;
  void set_size(int size) { slice_.set_size(size); }
  int size() const { return slice_.size(); }

  void set_begin(const MutableNbr<size_t>* ptr) { slice_.set_begin(ptr); }

  const MutableColumnNbr begin() const {
    return MutableColumnNbr(slice_.begin(), column_);
  }
  const MutableColumnNbr end() const {
    return MutableColumnNbr(slice_.end(), column_);
  }

  static MutableNbrSlice empty(const StringColumn& column) {
    MutableNbrSlice ret(column);
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  MutableNbrSlice<size_t> slice_;
  const StringColumn& column_;
};

template <typename EDATA_T>
class MutableNbrSliceMut {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using nbr_ptr_t = MutableNbr<EDATA_T>*;
  MutableNbrSliceMut() = default;
  ~MutableNbrSliceMut() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(nbr_t* ptr) { ptr_ = ptr; }

  nbr_t* begin() { return ptr_; }
  nbr_t* end() { return ptr_ + size_; }

  static MutableNbrSliceMut empty() {
    MutableNbrSliceMut ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  nbr_t* ptr_;
  int size_;
};

template <>
class MutableNbrSliceMut<std::string_view> {
 public:
  struct MutableColumnNbr {
    using nbr_t = MutableNbr<size_t>;

    MutableColumnNbr(nbr_t* ptr, StringColumn& column)
        : ptr_(ptr), column_(column) {}
    vid_t neighbor() const { return ptr_->neighbor; }
    std::string_view data() { return column_.get_view(ptr_->data); }
    vid_t get_neighbor() const { return ptr_->neighbor; }
    const std::string_view get_data() const {
      return column_.get_view(ptr_->data);
    }
    timestamp_t get_timestamp() const { return ptr_->timestamp.load(); }
    size_t get_index() const { return ptr_->data; }
    void set_data(const std::string_view& sw, timestamp_t ts) {
      column_.set_value(ptr_->data, sw);
      ptr_->timestamp.store(ts);
    }
    void set_neighbor(vid_t neighbor) { ptr_->neighbor = neighbor; }

    void set_timestamp(timestamp_t ts) { ptr_->timestamp.store(ts); }

    const MutableColumnNbr& operator*() const { return *this; }
    MutableColumnNbr& operator*() { return *this; }
    MutableColumnNbr& operator=(const MutableColumnNbr& nbr) {
      ptr_ = nbr.ptr_;
      return *this;
    }
    bool operator==(const MutableColumnNbr& nbr) const {
      return ptr_ == nbr.ptr_;
    }
    bool operator!=(const MutableColumnNbr& nbr) const {
      return ptr_ != nbr.ptr_;
    }

    MutableColumnNbr& operator++() {
      ptr_++;
      return *this;
    }
    MutableColumnNbr& operator+=(size_t n) {
      ptr_ += n;
      return *this;
    }

    bool operator<(const MutableColumnNbr& nbr) { return ptr_ < nbr.ptr_; }
    nbr_t* ptr_;
    StringColumn & column_;
  };
  using nbr_ptr_t = MutableColumnNbr;

  MutableNbrSliceMut(StringColumn& column) : column_(column) {}
  ~MutableNbrSliceMut() = default;
  void set_size(int size) { slice_.set_size(size); }
  int size() const { return slice_.size(); }

  void set_begin(MutableNbr<size_t>* ptr) { slice_.set_begin(ptr); }

  MutableColumnNbr begin() { return MutableColumnNbr(slice_.begin(), column_); }
  MutableColumnNbr end() { return MutableColumnNbr(slice_.end(), column_); }

  static MutableNbrSliceMut empty(StringColumn& column) {
    MutableNbrSliceMut ret(column);
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  MutableNbrSliceMut<size_t> slice_;
  StringColumn& column_;
};

template <typename T>
struct UninitializedUtils {
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    memcpy((void*) new_buffer, (void*) old_buffer, len * sizeof(T));
  }
};

template <typename EDATA_T>
class MutableAdjlist {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;
  MutableAdjlist() : buffer_(NULL), size_(0), capacity_(0) {}
  MutableAdjlist(const MutableAdjlist& rhs)
      : buffer_(rhs.buffer_),
        size_(rhs.size_.load(std::memory_order_acquire)),
        capacity_(rhs.capacity_) {}
  ~MutableAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  void put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts,
                Allocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += ((capacity_) >> 1);
      capacity_ = std::max(capacity_, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      if (size_ > 0) {
        UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      }
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load(std::memory_order_acquire));
    ret.set_begin(buffer_);
    return ret;
  }

  mut_slice_t get_edges_mut() {
    mut_slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data() const { return buffer_; }
  nbr_t* data() { return buffer_; }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};

template <>
class MutableAdjlist<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;
  MutableAdjlist(StringColumn& column)
      : buffer_(NULL), size_(0), capacity_(0) {}
  MutableAdjlist(const MutableAdjlist& rhs)
      : buffer_(rhs.buffer_),
        size_(rhs.size_.load(std::memory_order_acquire)),
        capacity_(rhs.capacity_) {}

  ~MutableAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const size_t& data, timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  void put_edge(vid_t neighbor, const size_t& data, timestamp_t ts,
                Allocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += ((capacity_) >> 1);
      capacity_ = std::max(capacity_, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      if (size_ > 0) {
        UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      }
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges(const StringColumn& column) const {
    slice_t ret(column);
    ret.set_size(size_.load(std::memory_order_acquire));
    ret.set_begin(buffer_);
    return ret;
  }

  mut_slice_t get_edges_mut(StringColumn& column) {
    mut_slice_t ret(column);
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data() const { return buffer_; }
  nbr_t* data() { return buffer_; }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};

class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual size_t size() const = 0;

  virtual MutableCsrConstEdgeIterBase& operator+=(size_t offset) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrEdgeIterBase {
 public:
  MutableCsrEdgeIterBase() = default;
  virtual ~MutableCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual void set_data(const Any& value, timestamp_t ts) = 0;
  virtual MutableCsrEdgeIterBase& operator+=(size_t offset) = 0;
  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrBase {
 public:
  MutableCsrBase() {}
  virtual ~MutableCsrBase() {}

  virtual size_t batch_init(const std::string& name,
                            const std::string& work_dir,
                            const std::vector<int>& degree,
                            double reserve_ratio = 1.2) = 0;

  virtual void batch_sort_by_edge_data(timestamp_t ts) {
    LOG(FATAL) << "not supported...";
  }

  virtual timestamp_t unsorted_since() const { return 0; }

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void open_in_memory(const std::string& prefix, size_t v_cap = 0) = 0;

  virtual void dump(const std::string& name,
                    const std::string& new_spanshot_dir) = 0;

  virtual void warmup(int thread_num) const = 0;

  virtual void resize(vid_t vnum) = 0;
  virtual size_t size() const = 0;
  virtual void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                                timestamp_t ts, Allocator& alloc) = 0;

  virtual void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                           timestamp_t ts, Allocator& alloc) = 0;

  virtual void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                                timestamp_t ts, Allocator& alloc) = 0;

  virtual std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const = 0;

  virtual MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const = 0;

  virtual std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) = 0;
};

template <typename EDATA_T>
class TypedMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
  using const_nbr_ptr_t = typename MutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

 public:
  explicit TypedMutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const override { return (*cur_).get_neighbor(); }
  Any get_data() const override {
    return AnyConverter<EDATA_T>::to_any((*cur_).get_data());
  }
  timestamp_t get_timestamp() const override { return (*cur_).get_timestamp(); }

  void next() override { ++cur_; }
  TypedMutableCsrConstEdgeIter& operator+=(size_t offset) override {
    cur_ += offset;
    if (!(cur_ < end_)) {
      cur_ = end_;
    }
    return *this;
  }
  bool is_valid() const override { return cur_ != end_; }
  size_t size() const override { return end_ - cur_; }

 private:
  const_nbr_ptr_t cur_;
  const_nbr_ptr_t end_;
};

template <typename EDATA_T>
class TypedMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const override { return cur_->neighbor; }
  Any get_data() const override {
    return AnyConverter<EDATA_T>::to_any(cur_->data);
  }
  timestamp_t get_timestamp() const override { return cur_->timestamp.load(); }

  void set_data(const Any& value, timestamp_t ts) override {
    ConvertAny<EDATA_T>::to(value, cur_->data);
    cur_->timestamp.store(ts);
  }

  MutableCsrEdgeIterBase& operator+=(size_t offset) override {
    if (cur_ + offset >= end_) {
      cur_ = end_;
    } else {
      cur_ += offset;
    }
    return *this;
  }

  void next() override { ++cur_; }
  bool is_valid() const override { return cur_ != end_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};

template <>
class TypedMutableCsrEdgeIter<std::string_view>
    : public MutableCsrEdgeIterBase {
  using nbr_ptr_t = typename MutableNbrSliceMut<std::string_view>::nbr_ptr_t;

 public:
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<std::string_view> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const override { return cur_.get_neighbor(); }
  Any get_data() const override {
    return AnyConverter<std::string_view>::to_any(cur_.get_data());
  }
  timestamp_t get_timestamp() const override { return cur_.get_timestamp(); }

  void set_data(const Any& value, timestamp_t ts) override {
    cur_.set_data(value.AsStringView(), ts);
  }
  size_t get_index() const { return cur_.get_index(); }
  void set_timestamp(timestamp_t ts) { cur_.set_timestamp(ts); }

  MutableCsrEdgeIterBase& operator+=(size_t offset) override {
    cur_ += offset;
    if (!(cur_ < end_)) {
      cur_ = end_;
    }
    return *this;
  }

  void next() override { ++cur_; }
  bool is_valid() const override { return cur_ != end_; }

 private:
  nbr_ptr_t cur_;
  nbr_ptr_t end_;
};

template <typename EDATA_T>
class TypedMutableCsrBase : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;
  virtual void put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                        timestamp_t ts, Allocator& alloc) = 0;
  virtual slice_t get_edges(vid_t i) const = 0;
};

template <>
class TypedMutableCsrBase<std::string_view> : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<std::string_view>;
  virtual slice_t get_edges(vid_t i) const = 0;
  virtual void batch_put_edge_with_index(vid_t src, vid_t dst,
                                         const size_t& data,
                                         timestamp_t ts = 0) = 0;
  virtual void put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                   timestamp_t ts, Allocator& alloc) = 0;
};
template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    reserve_ratio = std::max(reserve_ratio, 1.0);
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += (std::ceil(d * reserve_ratio));
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(edge_num);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      int cap = std::ceil(deg * reserve_ratio);
      adj_lists_[i].init(ptr, cap, 0);
      ptr += cap;
    }

    unsorted_since_ = 0;
    return edge_num;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    mmap_array<int>* cap_list = &degree_list;
    if (snapshot_dir != "") {
      degree_list.open(snapshot_dir + "/" + name + ".deg", true);
      if (std::filesystem::exists(snapshot_dir + "/" + name + ".cap")) {
        cap_list = new mmap_array<int>();
        cap_list->open(snapshot_dir + "/" + name + ".cap", true);
      }
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
      load_meta(snapshot_dir + "/" + name);
    }
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
    adj_lists_.open(work_dir + "/" + name + ".adj", false);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      int cap = (*cap_list)[i];
      adj_lists_[i].init(ptr, cap, degree);
      ptr += cap;
    }
    if (cap_list != &degree_list) {
      delete cap_list;
    }
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    mmap_array<int> degree_list;
    degree_list.open_in_memory(prefix + ".deg");
    load_meta(prefix);
    mmap_array<int>* cap_list = &degree_list;
    if (std::filesystem::exists(prefix + ".cap")) {
      cap_list = new mmap_array<int>();
      cap_list->open_in_memory(prefix + ".cap");
    }

    nbr_list_.open_in_memory(prefix + ".nbr");

    adj_lists_.reset();
    v_cap = std::max(v_cap, degree_list.size());
    adj_lists_.resize(v_cap);
    locks_ = new grape::SpinLock[v_cap];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      int cap = (*cap_list)[i];
      adj_lists_[i].init(ptr, cap, degree);
      ptr += cap;
    }
    for (size_t i = degree_list.size(); i < v_cap; ++i) {
      adj_lists_[i].init(ptr, 0, 0);
    }

    if (cap_list != &degree_list) {
      delete cap_list;
    }
  }

  void warmup(int thread_num) const override {
    size_t vnum = adj_lists_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    const size_t chunk = 4096;
    std::atomic<size_t> output(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);

          if (begin == end) {
            break;
          }

          while (begin < end) {
            auto adj_list = get_edges(begin);
            for (auto& nbr : adj_list) {
              ret += nbr.neighbor;
            }
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    dump_meta(new_spanshot_dir + "/" + name);
    mmap_array<int> degree_list;
    std::vector<int> cap_list;
    degree_list.open(new_spanshot_dir + "/" + name + ".deg", false);
    degree_list.resize(vnum);
    cap_list.resize(vnum);
    bool need_cap_list = false;
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      offset += adj_lists_[i].capacity();

      degree_list[i] = adj_lists_[i].size();
      cap_list[i] = adj_lists_[i].capacity();
      if (degree_list[i] != cap_list[i]) {
        need_cap_list = true;
      }
    }

    if (need_cap_list) {
      FILE* fcap_out =
          fopen((new_spanshot_dir + "/" + name + ".cap").c_str(), "wb");
      CHECK_EQ(fwrite(cap_list.data(), sizeof(int), cap_list.size(), fcap_out),
               cap_list.size());
      fflush(fcap_out);
      fclose(fcap_out);
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_spanshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_spanshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        CHECK_EQ(fwrite(adj_lists_[i].data(), sizeof(nbr_t),
                        adj_lists_[i].capacity(), fout),
                 adj_lists_[i].capacity());
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(NULL, 0, 0);
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {
    size_t vnum = adj_lists_.size();
    for (size_t i = 0; i != vnum; ++i) {
      std::sort(adj_lists_[i].data(),
                adj_lists_[i].data() + adj_lists_[i].size(),
                [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.data < rhs.data;
                });
    }
    unsorted_since_ = ts;
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

 private:
  void load_meta(const std::string& prefix) {
    std::string meta_file_path = prefix + ".meta";
    if (std::filesystem::exists(meta_file_path)) {
      FILE* meta_file_fd = fopen(meta_file_path.c_str(), "r");
      CHECK_EQ(
          fread(&unsorted_since_, sizeof(timestamp_t), 1, meta_file_fd),
          1);
      fclose(meta_file_fd);
    } else {
      unsorted_since_ = 0;
    }
  }

  void dump_meta(const std::string& prefix) const {
    std::string meta_file_path = prefix + ".meta";
    FILE* meta_file_fd =
        fopen((prefix + ".meta").c_str(), "wb");
    CHECK_EQ(
        fwrite(&unsorted_since_, sizeof(timestamp_t), 1, meta_file_fd),
        1);
    fflush(meta_file_fd);
    fclose(meta_file_fd);
  }

  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
  timestamp_t unsorted_since_;
};

template <>
class MutableCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using adjlist_t = MutableAdjlist<std::string_view>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;

  MutableCsr(StringColumn& column, std::atomic<size_t>& column_idx)
      : column_(column), column_idx_(column_idx), locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(edge_num);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      adj_lists_[i].init(ptr, deg, 0);
      ptr += deg;
    }
    return edge_num;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    if (snapshot_dir != "") {
      degree_list.open(snapshot_dir + "/" + name + ".deg", true);
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    }
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
    adj_lists_.open(work_dir + "/" + name + ".adj", false);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      adj_lists_[i].init(ptr, degree, degree);
      ptr += degree;
    }
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    mmap_array<int> degree_list;
    degree_list.open_in_memory(prefix + ".deg");
    nbr_list_.open_in_memory(prefix + ".nbr");
    adj_lists_.reset();
    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      adj_lists_[i].init(ptr, degree, degree);
      ptr += degree;
    }
  }

  void warmup(int thread_num) const override {
    size_t vnum = adj_lists_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    const size_t chunk = 4096;
    std::atomic<size_t> output(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);

          if (begin == end) {
            break;
          }

          while (begin < end) {
            auto adj_list = get_edges(begin);
            for (auto& nbr : adj_list) {
              ret += nbr.get_neighbor();
            }
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    mmap_array<int> degree_list;
    degree_list.open(new_spanshot_dir + "/" + name + ".deg", false);
    degree_list.resize(vnum);
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      degree_list[i] = adj_lists_[i].size();
      offset += degree_list[i];
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_spanshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_spanshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        CHECK_EQ(fwrite(adj_lists_[i].data(), sizeof(nbr_t),
                        adj_lists_[i].size(), fout),
                 adj_lists_[i].size());
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(NULL, 0, 0);
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    LOG(FATAL) << "not implemented\n";
  }

  void put_edge(vid_t src, vid_t dst, size_t data, timestamp_t ts,
                Allocator& alloc) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }
  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {
    put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, const size_t& data,
                                 timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges(column_);
  }
  mut_slice_t get_edges_mut(vid_t i) {
    return adj_lists_[i].get_edges_mut(column_);
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    auto row_id = column_idx_.load();
    put_edge(src, dst, row_id - 1, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    std::string_view sw;
    arc >> sw;
    auto row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, sw);
    put_edge(src, dst, row_id, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string_view>>(
        get_edges_mut(v));
  }

 private:
  StringColumn& column_;
  std::atomic<size_t>& column_idx_;
  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
    return vnum;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open_in_memory(prefix + ".snbr");
    if (nbr_list_.size() < v_cap) {
      size_t old_size = nbr_list_.size();
      nbr_list_.reset();
      nbr_list_.resize(v_cap);
      FILE* fin = fopen((prefix + ".snbr").c_str(), "r");
      CHECK_EQ(fread(nbr_list_.data(), sizeof(nbr_t), old_size, fin), old_size);
      fclose(fin);
      for (size_t k = old_size; k != v_cap; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    }
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    assert(!nbr_list_.read_only());
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator&) override {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

  void warmup(int thread_num) const override {
    size_t vnum = nbr_list_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    std::atomic<size_t> output(0);
    const size_t chunk = 4096;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);
          if (begin == end) {
            break;
          }
          while (begin < end) {
            auto& nbr = nbr_list_[begin];
            ret += nbr.neighbor;
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {}

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

template <>
class SingleMutableCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;

  SingleMutableCsr(StringColumn& column, std::atomic<size_t>& column_idx)
      : column_(column), column_idx_(column_idx) {}
  ~SingleMutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
    return vnum;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open_in_memory(prefix + ".snbr");
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    assert(!nbr_list_.read_only());
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    LOG(FATAL) << "not implemented\n";
  }

  void put_edge(vid_t src, vid_t dst, size_t data, timestamp_t ts, Allocator&) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret(column_);
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret(column_);
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const MutableNbr<std::string_view>& get_edge(vid_t i) const {
    nbr_.neighbor = nbr_list_[i].neighbor;
    nbr_.timestamp.store(nbr_list_[i].timestamp.load());
    nbr_.data = column_.get_view(nbr_list_[i].data);
    return nbr_;
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    auto row_id = column_idx_.load();
    put_edge(src, dst, row_id - 1, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    std::string_view sw;
    arc >> sw;

    auto row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, sw);
    put_edge(src, dst, row_id, ts, alloc);
  }
  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {
    put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, const size_t& data,
                                 timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string_view>>(
        get_edges_mut(v));
  }

  void warmup(int thread_num) const override {
    size_t vnum = nbr_list_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    std::atomic<size_t> output(0);
    const size_t chunk = 4096;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);
          if (begin == end) {
            break;
          }
          while (begin < end) {
            auto& nbr = nbr_list_[begin];
            ret += nbr.neighbor;
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

 private:
  StringColumn& column_;
  std::atomic<size_t>& column_idx_;

  mmap_array<nbr_t> nbr_list_;
  mutable MutableNbr<std::string_view> nbr_;
};

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    return 0;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void open_in_memory(const std::string& prefix, size_t v_cap) override {}

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {}

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator&) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator&) override {}
  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator&) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, Allocator&) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {}

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }
};

template <>
class EmptyCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
  using slice_t = MutableNbrSlice<std::string_view>;

 public:
  EmptyCsr(StringColumn& column, std::atomic<size_t>& column_idx)
      : column_(column), column_idx_(column_idx) {}
  ~EmptyCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    return 0;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void open_in_memory(const std::string& prefix, size_t v_cap) override {}

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {}

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  slice_t get_edges(vid_t i) const override { return slice_t::empty(column_); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator&) override {
    LOG(FATAL) << "not implemented\n";
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator&) override {}

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, Allocator&) override {
    std::string_view sw;
    arc >> sw;
    auto row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, sw);
  }
  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {}
  void batch_put_edge_with_index(vid_t src, vid_t dst, const size_t& data,
                                 timestamp_t ts = 0) override {}
  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string_view>>(
        MutableNbrSlice<std::string_view>::empty(column_));
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string_view>(
        MutableNbrSlice<std::string_view>::empty(column_));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string_view>>(
        MutableNbrSliceMut<std::string_view>::empty(column_));
  }
  StringColumn& column_;
  std::atomic<size_t>& column_idx_;
};
}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
