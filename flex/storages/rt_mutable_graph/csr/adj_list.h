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

#ifndef STORAGES_RT_MUTABLE_GRAPH_CSR_ADJ_LIST_H_
#define STORAGES_RT_MUTABLE_GRAPH_CSR_ADJ_LIST_H_

#include "flex/storages/rt_mutable_graph/csr/nbr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/allocators.h"

namespace gs {

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

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_CSR_ADJ_LIST_H_
