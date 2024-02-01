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

#ifndef GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_
#define GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_

#include <stdio.h>

#include <grape/serialization/in_archive.h>
#include "flex/storages/rt_mutable_graph/csr/immutable_csr.h"
#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "flex/utils/allocators.h"

namespace gs {

class DualCsrBase {
 public:
  DualCsrBase() = default;
  virtual ~DualCsrBase() = default;
  virtual void BatchInit(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& work_dir,
                         const std::vector<int>& oe_degree,
                         const std::vector<int>& ie_degree) = 0;
  virtual void Open(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;
  virtual void OpenInMemory(const std::string& oe_name,
                            const std::string& ie_name,
                            const std::string& edata_name,
                            const std::string& snapshot_dir,
                            size_t src_vertex_cap, size_t dst_vertex_cap) = 0;
  virtual void OpenWithHugepages(const std::string& oe_name,
                                 const std::string& ie_name,
                                 const std::string& edata_name,
                                 const std::string& snapshot_dir,
                                 size_t src_vertex_cap,
                                 size_t dst_vertex_cap) = 0;
  virtual void Dump(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& new_snapshot_dir) = 0;

  virtual void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;

  virtual void SortByEdgeData(timestamp_t ts) = 0;

  virtual void UpdateEdge(vid_t src, vid_t dst, const Any& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;

  void Resize(vid_t src_vertex_num, vid_t dst_vertex_num) {
    GetInCsr()->resize(dst_vertex_num);
    GetOutCsr()->resize(src_vertex_num);
  }

  void Warmup(int thread_num) {
    GetInCsr()->warmup(thread_num);
    GetOutCsr()->warmup(thread_num);
  }

  virtual CsrBase* GetInCsr() = 0;
  virtual CsrBase* GetOutCsr() = 0;
  virtual const CsrBase* GetInCsr() const = 0;
  virtual const CsrBase* GetOutCsr() const = 0;
};

template <typename EDATA_T>
class DualCsr : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy, bool oe_mutable,
          bool ie_mutable)
      : in_csr_(nullptr), out_csr_(nullptr) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      in_csr_ = new MutableCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      if (ie_mutable) {
        in_csr_ = new SingleMutableCsr<EDATA_T>();
      } else {
        in_csr_ = new SingleImmutableCsr<EDATA_T>();
      }
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      out_csr_ = new MutableCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      if (oe_mutable) {
        out_csr_ = new SingleMutableCsr<EDATA_T>();
      } else {
        out_csr_ = new SingleImmutableCsr<EDATA_T>();
      }
    }
  }
  ~DualCsr() {
    if (in_csr_ != nullptr) {
      delete in_csr_;
    }
    if (out_csr_ != nullptr) {
      delete out_csr_;
    }
  }
  void BatchInit(const std::string& oe_name, const std::string& ie_name,
                 const std::string& edata_name, const std::string& work_dir,
                 const std::vector<int>& oe_degree,
                 const std::vector<int>& ie_degree) override {
    in_csr_->batch_init(ie_name, work_dir, ie_degree);
    out_csr_->batch_init(oe_name, work_dir, oe_degree);
  }

  void Open(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir, size_t src_vertex_cap,
                    size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir, size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    in_csr_->open_with_hugepages(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_with_hugepages(snapshot_dir + "/" + oe_name, src_vertex_cap);
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
  }

  CsrBase* GetInCsr() override { return in_csr_; }
  CsrBase* GetOutCsr() override { return out_csr_; }
  const CsrBase* GetInCsr() const override { return in_csr_; }
  const CsrBase* GetOutCsr() const override { return out_csr_; }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    EDATA_T data;
    oarc >> data;
    in_csr_->put_edge(dst, src, data, ts, alloc);
    out_csr_->put_edge(src, dst, data, ts, alloc);
  }

  void SortByEdgeData(timestamp_t ts) override {
    in_csr_->batch_sort_by_edge_data(ts);
    out_csr_->batch_sort_by_edge_data(ts);
  }

  void UpdateEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                  Allocator& alloc) override {
    auto oe = out_csr_->edge_iter_mut(src);
    EDATA_T prop;
    ConvertAny<EDATA_T>::to(data, prop);
    bool src_flag = false, dst_flag = false;
    while (oe != nullptr && oe->is_valid()) {
      if (oe->get_neighbor() == dst) {
        oe->set_data(prop, ts);
        src_flag = true;
        break;
      }
      oe->next();
    }
    auto ie = in_csr_->edge_iter_mut(dst);
    while (ie != nullptr && ie->is_valid()) {
      if (ie->get_neighbor() == src) {
        dst_flag = true;
        ie->set_data(prop, ts);
        break;
      }
      ie->next();
    }
    if (!(src_flag || dst_flag)) {
      in_csr_->put_edge(dst, src, prop, ts, alloc);
      out_csr_->put_edge(src, dst, prop, ts, alloc);
    }
  }

  void BatchPutEdge(vid_t src, vid_t dst, const EDATA_T& data) {
    in_csr_->batch_put_edge(dst, src, data);
    out_csr_->batch_put_edge(src, dst, data);
  }

 private:
  TypedCsrBase<EDATA_T>* in_csr_;
  TypedCsrBase<EDATA_T>* out_csr_;
};

template <>
class DualCsr<std::string_view> : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy, uint16_t width)
      : in_csr_(nullptr),
        out_csr_(nullptr),
        column_(StorageStrategy::kMem, width) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<std::string_view>(column_);
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      in_csr_ = new MutableCsr<std::string_view>(column_);
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      in_csr_ = new SingleMutableCsr<std::string_view>(column_);
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<std::string_view>(column_);
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      out_csr_ = new MutableCsr<std::string_view>(column_);
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      out_csr_ = new SingleMutableCsr<std::string_view>(column_);
    }
  }
  ~DualCsr() {
    if (in_csr_ != nullptr) {
      delete in_csr_;
    }
    if (out_csr_ != nullptr) {
      delete out_csr_;
    }
  }
  void BatchInit(const std::string& oe_name, const std::string& ie_name,
                 const std::string& edata_name, const std::string& work_dir,
                 const std::vector<int>& oe_degree,
                 const std::vector<int>& ie_degree) override {
    size_t ie_num = in_csr_->batch_init(ie_name, work_dir, ie_degree);
    size_t oe_num = out_csr_->batch_init(oe_name, work_dir, oe_degree);
    column_.open(edata_name, "", work_dir);
    column_.resize(std::max(ie_num, oe_num));
    column_idx_.store(0);
  }

  void Open(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    in_csr_->open(ie_name, snapshot_dir, work_dir);
    out_csr_->open(oe_name, snapshot_dir, work_dir);
    column_.open(edata_name, snapshot_dir, work_dir);
    column_idx_.store(column_.size());
    column_.resize(std::max(column_.size() + (column_.size() + 4) / 5, 4096ul));
  }

  void OpenInMemory(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& snapshot_dir, size_t src_vertex_cap,
                    size_t dst_vertex_cap) override {
    in_csr_->open_in_memory(snapshot_dir + "/" + ie_name, dst_vertex_cap);
    out_csr_->open_in_memory(snapshot_dir + "/" + oe_name, src_vertex_cap);
    column_.open_in_memory(snapshot_dir + "/" + edata_name);
    column_idx_.store(column_.size());
    column_.resize(std::max(column_.size() + (column_.size() + 4) / 5, 4096ul));
  }

  void OpenWithHugepages(const std::string& oe_name, const std::string& ie_name,
                         const std::string& edata_name,
                         const std::string& snapshot_dir, size_t src_vertex_cap,
                         size_t dst_vertex_cap) override {
    LOG(FATAL) << "not supported...";
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
    column_.resize(column_idx_.load());
    column_.dump(new_snapshot_dir + "/" + edata_name);
  }

  CsrBase* GetInCsr() override { return in_csr_; }
  CsrBase* GetOutCsr() override { return out_csr_; }
  const CsrBase* GetInCsr() const override { return in_csr_; }
  const CsrBase* GetOutCsr() const override { return out_csr_; }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    std::string_view prop;
    oarc >> prop;
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, prop);
    in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
    out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
  }

  void SortByEdgeData(timestamp_t ts) override {
    LOG(FATAL) << "Not implemented";
  }

  void UpdateEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                  Allocator& alloc) override {
    auto oe_ptr = out_csr_->edge_iter_mut(src);
    std::string_view prop = data.AsStringView();
    auto oe = dynamic_cast<MutableCsrEdgeIter<std::string_view>*>(oe_ptr.get());
    size_t index = std::numeric_limits<size_t>::max();
    while (oe != nullptr && oe->is_valid()) {
      if (oe->get_neighbor() == dst) {
        oe->set_timestamp(ts);
        index = oe->get_index();
        break;
      }
      oe->next();
    }
    auto ie_ptr = in_csr_->edge_iter_mut(dst);
    auto ie = dynamic_cast<MutableCsrEdgeIter<std::string_view>*>(ie_ptr.get());
    while (ie != nullptr && ie->is_valid()) {
      if (ie->get_neighbor() == src) {
        ie->set_timestamp(ts);
        index = ie->get_index();
        break;
      }
      ie->next();
    }
    if (index != std::numeric_limits<size_t>::max()) {
      column_.set_value(index, prop);
    } else {
      size_t row_id = column_idx_.fetch_add(1);
      column_.set_value(row_id, prop);
      in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
      out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
    }
  }

  void BatchPutEdge(vid_t src, vid_t dst, const std::string_view& data) {
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, data);
    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

  void BatchPutEdge(vid_t src, vid_t dst, const std::string& data) {
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, data);

    in_csr_->batch_put_edge_with_index(dst, src, row_id);
    out_csr_->batch_put_edge_with_index(src, dst, row_id);
  }

 private:
  TypedCsrBase<std::string_view>* in_csr_;
  TypedCsrBase<std::string_view>* out_csr_;
  std::atomic<size_t> column_idx_;
  StringColumn column_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_
