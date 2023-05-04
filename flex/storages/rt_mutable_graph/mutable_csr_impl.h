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

#ifndef GRAPHSCOPE_GRAPH_MUTABLE_CSR_IMPL_H_
#define GRAPHSCOPE_GRAPH_MUTABLE_CSR_IMPL_H_

#include <atomic>

#include <grape/serialization/in_archive.h>
#include <grape/serialization/out_archive.h>
#include <grape/types.h>

#include "flex/utils/allocators.h"

namespace gs {

namespace mutable_csr_impl {

template <typename VID_T, typename EDATA_T, typename TS_T>
struct Nbr {
  Nbr() = default;
  Nbr(const Nbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~Nbr() = default;

  VID_T neighbor;
  std::atomic<TS_T> timestamp;
  EDATA_T data;
};

template <typename VID_T, typename TS_T>
struct Nbr<VID_T, grape::EmptyType, TS_T> {
  Nbr() = default;
  Nbr(const Nbr& rhs)
      : neighbor(rhs.neighbor), timestamp(rhs.timestamp.load()) {}
  ~Nbr() = default;

  VID_T neighbor;
  union {
    std::atomic<TS_T> timestamp;
    grape::EmptyType data;
  };
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class NbrSlice {
 public:
  using nbr_t = Nbr<VID_T, EDATA_T, TS_T>;
  NbrSlice() = default;
  ~NbrSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(const nbr_t* ptr) { ptr_ = ptr; }

  const nbr_t* begin() const { return ptr_; }
  const nbr_t* end() const { return ptr_ + size_; }

  static NbrSlice empty() {
    NbrSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  const nbr_t* ptr_;
  int size_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class NbrSliceMut {
 public:
  using nbr_t = Nbr<VID_T, EDATA_T, TS_T>;
  NbrSliceMut() = default;
  ~NbrSliceMut() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(nbr_t* ptr) { ptr_ = ptr; }

  nbr_t* begin() { return ptr_; }
  nbr_t* end() { return ptr_ + size_; }

  static NbrSliceMut empty() {
    NbrSliceMut ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  nbr_t* ptr_;
  int size_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class AdjList {
 public:
  using nbr_t = Nbr<VID_T, EDATA_T, TS_T>;
  using slice_t = NbrSlice<VID_T, EDATA_T, TS_T>;
  using mut_slice_t = NbrSliceMut<VID_T, EDATA_T, TS_T>;

 public:
  AdjList() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~AdjList() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(VID_T dst, const EDATA_T& data, TS_T ts) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = dst;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  void put_edge(VID_T dst, const EDATA_T& data, TS_T ts,
                ArenaAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += (((capacity_) >> 1) + 1);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      std::uninitialized_copy(buffer_, buffer_ + size_, new_buffer);
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_++];
    nbr.neighbor = dst;
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

  const nbr_t* begin() const { return buffer_; }
  nbr_t* begin() { return buffer_; }
  const nbr_t* end() const { return buffer_ + size_.load(); }
  nbr_t* end() { return buffer_ + size_.load(); }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};

}  // namespace mutable_csr_impl

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_IMPL_H_
