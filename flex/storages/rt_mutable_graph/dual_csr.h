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
#include "flex/storages/rt_mutable_graph/mutable_csr.h"
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
  virtual void Dump(const std::string& oe_name, const std::string& ie_name,
                    const std::string& edata_name,
                    const std::string& new_snapshot_dir) = 0;

  virtual void PutEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                       Allocator& alloc) = 0;
  virtual void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                          timestamp_t timestamp, Allocator& alloc) = 0;
  virtual MutableCsrBase* GetInCsr() = 0;
  virtual MutableCsrBase* GetOutCsr() = 0;
  /**
  virtual std::shared_ptr<MutableCsrConstEdgeIterBase> get_incoming_edge_iter(
      vid_t v, uint8_t col_id) const = 0;
  virtual std::shared_ptr<MutableCsrConstEdgeIterBase> get_outgoing_edge_iter(
      vid_t v, uint8_t col_id) const = 0;
  virtual MutableCsrConstEdgeIterBase* get_incoming_edge_iter_raw(
      vid_t v, uint8_t col_id) const = 0;
  virtual MutableCsrConstEdgeIterBase* get_outgoing_edge_iter_raw(
      vid_t v, uint8_t col_id) const = 0;

  virtual std::shared_ptr<MutableCsrEdgeIterBase> get_incoming_edge_iter_mut(
      vid_t v, uint8_t col_id) = 0;

  virtual std::shared_ptr<MutableCsrEdgeIterBase> get_outgoing_edge_iter_mut(
      vid_t v, uint8_t col_id) = 0;*/
};

template <typename EDATA_T>
class DualCsr : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy)
      : in_csr_(nullptr), out_csr_(nullptr) {
    if (ie_strategy == EdgeStrategy::kNone) {
      in_csr_ = new EmptyCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kMultiple) {
      in_csr_ = new MutableCsr<EDATA_T>();
    } else if (ie_strategy == EdgeStrategy::kSingle) {
      in_csr_ = new SingleMutableCsr<EDATA_T>();
    }
    if (oe_strategy == EdgeStrategy::kNone) {
      out_csr_ = new EmptyCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kMultiple) {
      out_csr_ = new MutableCsr<EDATA_T>();
    } else if (oe_strategy == EdgeStrategy::kSingle) {
      out_csr_ = new SingleMutableCsr<EDATA_T>();
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

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
  }

  MutableCsrBase* GetInCsr() override { return in_csr_; }
  MutableCsrBase* GetOutCsr() override { return out_csr_; }
  void PutEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
               Allocator& alloc) override {
    in_csr_->put_generic_edge(dst, src, data, ts, alloc);
    out_csr_->put_generic_edge(src, dst, data, ts, alloc);
  }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    in_csr_->peek_ingest_edge(dst, src, oarc, ts, alloc);
    out_csr_->ingest_edge(src, dst, oarc, ts, alloc);
  }

  void BatchPutEdge(vid_t src, vid_t dst, const EDATA_T& data) {
    in_csr_->batch_put_edge(dst, src, data);
    out_csr_->batch_put_edge(src, dst, data);
  }

 private:
  TypedMutableCsrBase<EDATA_T>* in_csr_;
  TypedMutableCsrBase<EDATA_T>* out_csr_;
};

template <>
class DualCsr<std::string_view> : public DualCsrBase {
 public:
  DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strategy)
      : in_csr_(nullptr), out_csr_(nullptr), column_(StorageStrategy::kMem) {
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
    column_.resize(column_.size() + (column_.size() + 4) / 5);
  }

  void Dump(const std::string& oe_name, const std::string& ie_name,
            const std::string& edata_name,
            const std::string& new_snapshot_dir) override {
    in_csr_->dump(ie_name, new_snapshot_dir);
    out_csr_->dump(oe_name, new_snapshot_dir);
    column_.resize(column_idx_.load());
    column_.dump(new_snapshot_dir + "/" + edata_name);
  }

  MutableCsrBase* GetInCsr() override { return in_csr_; }
  MutableCsrBase* GetOutCsr() override { return out_csr_; }
  void PutEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
               Allocator& alloc) override {
    std::string_view val = data.AsStringView();
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, val);
    in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
    out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
  }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts,
                  Allocator& alloc) override {
    std::string_view prop;
    oarc >> prop;
    size_t row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, prop);
    in_csr_->put_edge_with_index(dst, src, row_id, ts, alloc);
    out_csr_->put_edge_with_index(src, dst, row_id, ts, alloc);
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
  TypedMutableCsrBase<std::string_view>* in_csr_;
  TypedMutableCsrBase<std::string_view>* out_csr_;
  std::atomic<size_t> column_idx_;
  StringColumn column_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_DUAL_CSR_H_
