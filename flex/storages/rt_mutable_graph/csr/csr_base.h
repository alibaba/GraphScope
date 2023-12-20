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

#ifndef GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_
#define GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_

#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

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
                            const std::vector<int>& degree) = 0;

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

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
  using immutable_nbr_t = ImmutableNbr<EDATA_T>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;
  virtual void put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                        timestamp_t ts, Allocator& alloc) = 0;
  virtual slice_t get_edges(vid_t i) const = 0;

  virtual const immutable_nbr_t* get_edges_begin(vid_t v) const { return NULL; }
  virtual const immutable_nbr_t* get_edges_end(vid_t v) const { return NULL; }
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

}  // namespace gs

#endif  // GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_