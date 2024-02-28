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

#ifndef STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_
#define STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_

#include <string>
#include <vector>

#include "flex/storages/rt_mutable_graph/csr/nbr.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

class CsrConstEdgeIterBase {
 public:
  CsrConstEdgeIterBase() = default;
  virtual ~CsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual size_t size() const = 0;

  virtual CsrConstEdgeIterBase& operator+=(size_t offset) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class CsrEdgeIterBase {
 public:
  CsrEdgeIterBase() = default;
  virtual ~CsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual size_t size() const = 0;

  virtual CsrEdgeIterBase& operator+=(size_t offset) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;

  virtual void set_data(const Any& value, timestamp_t ts) = 0;
};

class CsrBase {
 public:
  CsrBase() = default;
  virtual ~CsrBase() = default;
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

  virtual void open_in_memory(const std::string& prefix, size_t v_cap) = 0;

  virtual void open_with_hugepages(const std::string& prefix,
                                   size_t v_cap = 0) {
    LOG(FATAL) << "not supported...";
  }

  virtual void dump(const std::string& name,
                    const std::string& new_snapshot_dir) = 0;

  virtual void warmup(int thread_num) const = 0;

  virtual void resize(vid_t vnum) = 0;
  virtual size_t size() const = 0;

  virtual std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const = 0;
  virtual CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const = 0;
  virtual std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) = 0;
};

template <typename EDATA_T>
class TypedCsrBase : public CsrBase {
 public:
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;
  virtual void put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                        timestamp_t ts, Allocator& alloc) = 0;
};

template <>
class TypedCsrBase<std::string_view> : public CsrBase {
 public:
  virtual void batch_put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                         timestamp_t ts = 0) = 0;
  virtual void put_edge_with_index(vid_t src, vid_t dst, size_t index,
                                   timestamp_t ts, Allocator& alloc) = 0;
};

template <typename EDATA_T>
class TypedImmutableCsrBase : public TypedCsrBase<EDATA_T> {
 public:
  using slice_t = ImmutableNbrSlice<EDATA_T>;

  virtual slice_t get_edges(vid_t v) const = 0;
};

template <typename EDATA_T>
class TypedMutableCsrBase : public TypedCsrBase<EDATA_T> {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;

  virtual slice_t get_edges(vid_t v) const = 0;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_CSR_CSR_BASE_H_