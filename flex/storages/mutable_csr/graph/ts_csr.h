#ifndef GRAPHSCOPE_GRAPH_TS_CSR_H_
#define GRAPHSCOPE_GRAPH_TS_CSR_H_

#include <cassert>
#include <type_traits>
#include <vector>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"
#include "flex/storages/mutable_csr/property/types.h"
#include "flex/storages/mutable_csr/types.h"
#include "flex/utils/allocators.h"
#include "flex/utils/mmap_array.h"

namespace gs {

template <typename EDATA_T>
struct TSNbr {
  TSNbr() = default;
  ~TSNbr() = default;

  vid_t neighbor;
  timestamp_t timestamp;
  EDATA_T data;
};

template <>
struct TSNbr<grape::EmptyType> {
  TSNbr() = default;
  ~TSNbr() = default;

  vid_t neighbor;
  union {
    timestamp_t timestamp;
    grape::EmptyType data;
  };
};

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const TSNbr<std::string>& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              TSNbr<std::string>& value);

template <typename EDATA_T>
class TSNbrSlice {
 public:
  using nbr_t = TSNbr<EDATA_T>;
  TSNbrSlice() = default;
  ~TSNbrSlice() = default;

  TSNbrSlice(TSNbrSlice<EDATA_T>&& other)
      : ptr_(other.ptr_), size_(other.size_) {}

  TSNbrSlice(const TSNbrSlice<EDATA_T>& other)
      : ptr_(other.ptr_), size_(other.size_) {}

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(const nbr_t* ptr) { ptr_ = ptr; }

  const nbr_t* begin() const { return ptr_; }
  const nbr_t* end() const { return ptr_ + size_; }

  static TSNbrSlice empty() {
    TSNbrSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

  TSNbrSlice<EDATA_T>& operator=(const TSNbrSlice<EDATA_T>& other) {
    ptr_ = other.ptr_;
    size_ = other.size_;
    return *this;
  }

 private:
  const nbr_t* ptr_;
  int size_;
};

template <typename EDATA_T>
class TSNbrMutSlice {
 public:
  using nbr_t = TSNbr<EDATA_T>;
  TSNbrMutSlice() = default;
  ~TSNbrMutSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(nbr_t* ptr) { ptr_ = ptr; }

  nbr_t* begin() { return ptr_; }
  nbr_t* end() { return ptr_ + size_; }

  static TSNbrMutSlice empty() {
    TSNbrMutSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  nbr_t* ptr_;
  int size_;
};

template <typename T>
struct UninitializedUtils {
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    memcpy(new_buffer, old_buffer, len * sizeof(T));
  }
};

template <>
struct UninitializedUtils<TSNbr<std::string>> {
  using T = TSNbr<std::string>;
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    while (len--) {
      new_buffer->neighbor = old_buffer->neighbor;
      new_buffer->data = old_buffer->data;
      new_buffer->timestamp = old_buffer->timestamp;
      ++new_buffer;
      ++old_buffer;
    }
  }
};

template <typename EDATA_T>
class TSAdjlist {
 public:
  using nbr_t = TSNbr<EDATA_T>;
  using slice_t = TSNbrSlice<EDATA_T>;
  using mut_slice_t = TSNbrMutSlice<EDATA_T>;
  TSAdjlist() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~TSAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts = 0) {
    assert(size_ < capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp = ts;
  }

  void put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += (((capacity_) >> 1) + 1);
      auto* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp = ts;
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  const nbr_t* get_edge(eid_t inner_eid) const {
    CHECK(inner_eid <= size_.load());
    return (buffer_ + inner_eid);
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
class TSAdjlist<std::string> {
 public:
  using nbr_t = TSNbr<std::string>;
  using slice_t = TSNbrSlice<std::string>;
  using mut_slice_t = TSNbrMutSlice<std::string>;
  TSAdjlist() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~TSAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const std::string& data,
                      timestamp_t ts = 0) {
    assert(size_ < capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp = ts;
  }

  void put_edge(vid_t neighbor, const std::string& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += (((capacity_) >> 1) + 1);
      auto* new_buffer = static_cast<nbr_t*>(allocator.allocate_typed(
          sizeof(nbr_t), capacity_,
          [](void* ptr) { static_cast<nbr_t*>(ptr)->~nbr_t(); }));
      for (int i = 0; i < capacity_; ++i) {
        new (&new_buffer[i]) nbr_t();
      }
      UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp = ts;
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load());
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

class TSCsrConstEdgeIterBase {
 public:
  TSCsrConstEdgeIterBase() = default;
  virtual ~TSCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
  virtual size_t size() const = 0;
};

class TSCsrEdgeIterBase {
 public:
  TSCsrEdgeIterBase() = default;
  virtual ~TSCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual void set_data(const Any& value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class TSCsrBase {
 public:
  TSCsrBase() {}
  virtual ~TSCsrBase() {}

  virtual void batch_init(vid_t vnum, const std::vector<int>& degree) = 0;

  virtual void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                                timestamp_t ts, ArenaAllocator& alloc) = 0;

  virtual void Serialize(const std::string& path) = 0;

  virtual void Deserialize(const std::string& path) = 0;

  virtual void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                           timestamp_t ts, ArenaAllocator& alloc) = 0;
  virtual void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                                timestamp_t ts, ArenaAllocator& alloc) = 0;

  virtual std::shared_ptr<TSCsrConstEdgeIterBase> edge_iter(vid_t v) const = 0;

  virtual std::shared_ptr<TSCsrEdgeIterBase> edge_iter_mut(vid_t v) = 0;
};

template <typename EDATA_T>
class TypedTSCsrConstEdgeIter : public TSCsrConstEdgeIterBase {
  using nbr_t = TSNbr<EDATA_T>;

 public:
  explicit TypedTSCsrConstEdgeIter(const TSNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedTSCsrConstEdgeIter() = default;

  vid_t get_neighbor() const override { return cur_->neighbor; }
  Any get_data() const override {
    return AnyConverter<EDATA_T>::to_any(cur_->data);
  }
  timestamp_t get_timestamp() const override { return cur_->timestamp; }

  void next() override { ++cur_; }
  bool is_valid() const override { return cur_ != end_; }
  size_t size() const override { return end_ - cur_; }

 private:
  const nbr_t* cur_;
  const nbr_t* end_;
};

template <typename EDATA_T>
class TypedTSCsrEdgeIter : public TSCsrEdgeIterBase {
  using nbr_t = TSNbr<EDATA_T>;

 public:
  explicit TypedTSCsrEdgeIter(TSNbrMutSlice<EDATA_T> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedTSCsrEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
  timestamp_t get_timestamp() const { return cur_->timestamp; }

  void set_data(const Any& value, timestamp_t ts) {
    ConvertAny<EDATA_T>::to(value, cur_->data);
    cur_->timestamp = ts;
  }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};

template <typename EDATA_T>
class TypedTSCsrBase : public TSCsrBase {
  using slice_t = TSNbrSlice<EDATA_T>;
 public:
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;

  virtual slice_t get_edges(vid_t v) const = 0;
};

template <typename EDATA_T>
class TSCsr : public TypedTSCsrBase<EDATA_T> {
 public:
  using nbr_t = TSNbr<EDATA_T>;
  using adjlist_t = TSAdjlist<EDATA_T>;
  using slice_t = TSNbrSlice<EDATA_T>;
  using mut_slice_t = TSNbrMutSlice<EDATA_T>;

  TSCsr() : adj_lists_(nullptr), locks_(nullptr), capacity_(0) {}
  ~TSCsr() {
    if (adj_lists_ != nullptr) {
      free(adj_lists_);
    }
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  TSCsr& operator=(const TSCsr& other) {
    if (*this == other)
      return *this;
    this->adj_lists_ = other.adj_lists_;
    return *this;
  }

  bool operator==(const TSCsr& other) { return adj_lists_ == other.adj_lists_; }

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    capacity_ = vnum + (vnum + 4) / 5;
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
    for (vid_t i = 0; i < vnum; ++i) {
      size_t cur_cap = degree[i] + (degree[i] + 4) / 5;
      adj_lists_[i].init(ptr, cur_cap, 0);
      ptr += cur_cap;
    }
    for (vid_t i = vnum; i < capacity_; ++i) {
      adj_lists_[i].init(NULL, 0, 0);
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        ArenaAllocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    assert(src < capacity_);
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, allocator);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override { return adj_lists_[i].get_edges(); }

  const nbr_t* get_edge(vid_t v, eid_t inner_eid) const {
    return adj_lists_[v].get_edge(inner_eid);
  }

  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<TSCsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<TypedTSCsrConstEdgeIter<EDATA_T>>(get_edges(v));
  }

  std::shared_ptr<TSCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedTSCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  adjlist_t* adj_lists_;
  grape::SpinLock* locks_;
  vid_t capacity_;
  mmap_array<nbr_t> init_nbr_list_;
};

template <>
class TSCsr<std::string> : public TypedTSCsrBase<std::string> {
 public:
  using nbr_t = TSNbr<std::string>;
  using adjlist_t = TSAdjlist<std::string>;
  using slice_t = TSNbrSlice<std::string>;
  using mut_slice_t = TSNbrMutSlice<std::string>;

  TSCsr() : adj_lists_(nullptr), locks_(nullptr), capacity_(0) {}
  ~TSCsr() {
    if (adj_lists_ != nullptr) {
      free(adj_lists_);
    }
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    capacity_ = vnum + (vnum + 4) / 5;
    if (capacity_ == 0) {
      capacity_ = 1024;
    }

    adj_lists_ = static_cast<adjlist_t*>(malloc(sizeof(adjlist_t) * capacity_));
    locks_ = new grape::SpinLock[capacity_];
    size_t edge_capacity = 0;
    for (auto d : degree) {
      edge_capacity += (d + (d + 4) / 5);
    }
    nbr_list_.resize(edge_capacity);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      size_t cur_cap = degree[i] + (degree[i] + 4) / 5;
      adj_lists_[i].init(ptr, cur_cap, 0);
      ptr += cur_cap;
    }
    for (vid_t i = vnum; i < capacity_; ++i) {
      adj_lists_[i].init(NULL, 0, 0);
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const std::string& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        ArenaAllocator& alloc) override {
    std::string value(data.value.s);
    put_edge(src, dst, value, ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const std::string& data, timestamp_t ts,
                ArenaAllocator& allocator) {
    assert(src < capacity_);
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, allocator);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override { return adj_lists_[i].get_edges(); }
  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    std::string value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    std::string value;
    arc.Peek<std::string>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<TSCsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<TypedTSCsrConstEdgeIter<std::string>>(get_edges(v));
  }

  std::shared_ptr<TSCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedTSCsrEdgeIter<std::string>>(get_edges_mut(v));
  }

 private:
  adjlist_t* adj_lists_;
  std::vector<nbr_t> nbr_list_;
  grape::SpinLock* locks_;
  vid_t capacity_;
};

template <typename EDATA_T>
class SingleTSCsr : public TypedTSCsrBase<EDATA_T> {
 public:
  using nbr_t = TSNbr<EDATA_T>;
  using slice_t = TSNbrSlice<EDATA_T>;
  using mut_slice_t = TSNbrMutSlice<EDATA_T>;

  SingleTSCsr() {}
  ~SingleTSCsr() {}

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {
    vid_t capacity = vnum + (vnum + 4) / 5;
    nbr_list_.resize(capacity);
    for (vid_t i = 0; i < capacity; ++i) {
      nbr_list_[i].timestamp = std::numeric_limits<timestamp_t>::max();
    }
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].timestamp = ts;
    nbr_list_[src].data = data;
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        ArenaAllocator&) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    assert(src < nbr_list_.size());
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].timestamp = ts;
    nbr_list_[src].data = data;
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(
        nbr_list_[i].timestamp == std::numeric_limits<timestamp_t>::max() ? 0
                                                                          : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(
        nbr_list_[i].timestamp == std::numeric_limits<timestamp_t>::max() ? 0
                                                                          : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  bool valid(vid_t v) const {
    return nbr_list_[v].timestamp != std::numeric_limits<timestamp_t>::max();
  }

  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }

  void Serialize(const std::string& path) override;

  void Deserialize(const std::string& path) override;

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, ArenaAllocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts);
  }

  std::shared_ptr<TSCsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<TypedTSCsrConstEdgeIter<EDATA_T>>(get_edges(v));
  }

  std::shared_ptr<TSCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedTSCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_TS_CSR_H_
