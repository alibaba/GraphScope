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

#ifndef GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_NBR_H_
#define GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_NBR_H_

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/column.h"
#include "grape/types.h"

namespace gs {

template <typename EDATA_T>
struct ImmutableNbr {
  ImmutableNbr() = default;
  ImmutableNbr(const ImmutableNbr& rhs)
      : neighbor(rhs.neighbor), data(rhs.data) {}
  ~ImmutableNbr() = default;

  const EDATA_T& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }

  void set_data(const EDATA_T& val) { data = val; }
  void set_neighbor(vid_t neighbor) { neighbor = neighbor; }

  vid_t neighbor;
  EDATA_T data;
};

template <>
struct ImmutableNbr<grape::EmptyType> {
  ImmutableNbr() = default;
  ImmutableNbr(const ImmutableNbr& rhs) : neighbor(rhs.neighbor) {}
  ~ImmutableNbr() = default;
  void set_data(const grape::EmptyType&) {}
  void set_neighbor(vid_t neighbor) { neighbor = neighbor; }
  const grape::EmptyType& get_data() const { return data; }
  vid_t get_neighbor() const { return neighbor; }
  union {
    vid_t neighbor;
    grape::EmptyType data;
  };
};

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

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

template <typename EDATA_T, typename Enable = void>
class MutableNbrSlice {
 public:
  using const_nbr_t = const MutableNbr<EDATA_T>;
  using const_nbr_ptr_t = const MutableNbr<EDATA_T>*;
  MutableNbrSlice() = default;
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

template <typename EDATA_T>
class MutableNbrSlice<
    EDATA_T, typename std::enable_if_t<is_col_property_type<EDATA_T>::value>> {
 public:
  struct MutableColumnNbr {
    using const_nbr_t = const MutableNbr<size_t>;
    using const_nbr_ptr_t = const MutableNbr<size_t>*;

    MutableColumnNbr(const_nbr_ptr_t ptr, const TypedColumn<EDATA_T>& column)
        : ptr_(ptr), column_(column) {}
    vid_t get_neighbor() const { return ptr_->neighbor; }
    EDATA_T get_data() const { return column_.get_view(ptr_->data); }
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
    const TypedColumn<EDATA_T>& column_;
  };

  using const_nbr_t = const MutableColumnNbr;
  using const_nbr_ptr_t = const MutableColumnNbr;
  MutableNbrSlice(const TypedColumn<EDATA_T>& column) : column_(column) {}
  ~MutableNbrSlice() = default;
  void set_size(int size) { slice_.set_size(size); }
  int size() const { return slice_.size(); }

  void set_begin(const MutableNbr<size_t>* ptr) { slice_.set_begin(ptr); }

  const_nbr_ptr_t begin() const {
    return MutableColumnNbr(slice_.begin(), column_);
  }
  const_nbr_ptr_t end() const {
    return MutableColumnNbr(slice_.end(), column_);
  }

  static MutableNbrSlice empty(const TypedColumn<EDATA_T>& column) {
    MutableNbrSlice ret(column);
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  MutableNbrSlice<size_t> slice_;
  const TypedColumn<EDATA_T>& column_;
};

template <typename EDATA_T, typename Enable = void>
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

template <typename EDATA_T>
class MutableNbrSliceMut<
    EDATA_T, typename std::enable_if_t<is_col_property_type<EDATA_T>::value>> {
 public:
  struct MutableColumnNbr {
    using nbr_t = MutableNbr<size_t>;

    MutableColumnNbr(nbr_t* ptr, TypedColumn<EDATA_T>& column)
        : ptr_(ptr), column_(column) {}
    vid_t neighbor() const { return ptr_->neighbor; }
    EDATA_T data() { return column_.get_view(ptr_->data); }
    vid_t get_neighbor() const { return ptr_->neighbor; }
    const EDATA_T get_data() const { return column_.get_view(ptr_->data); }
    timestamp_t get_timestamp() const { return ptr_->timestamp.load(); }
    size_t get_index() const { return ptr_->data; }
    void set_data(const EDATA_T& sw, timestamp_t ts) {
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
    TypedColumn<EDATA_T>& column_;
  };
  using nbr_ptr_t = MutableColumnNbr;

  MutableNbrSliceMut(TypedColumn<EDATA_T>& column) : column_(column) {}
  ~MutableNbrSliceMut() = default;
  void set_size(int size) { slice_.set_size(size); }
  int size() const { return slice_.size(); }

  void set_begin(MutableNbr<size_t>* ptr) { slice_.set_begin(ptr); }

  MutableColumnNbr begin() { return MutableColumnNbr(slice_.begin(), column_); }
  MutableColumnNbr end() { return MutableColumnNbr(slice_.end(), column_); }

  static MutableNbrSliceMut empty(TypedColumn<EDATA_T>& column) {
    MutableNbrSliceMut ret(column);
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  MutableNbrSliceMut<size_t> slice_;
  TypedColumn<EDATA_T>& column_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_NBR_H_