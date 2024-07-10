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
#ifndef FLEX_ENGINES_HQPS_DATABASE_SUB_GRAPH_H_
#define FLEX_ENGINES_HQPS_DATABASE_SUB_GRAPH_H_

#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "flex/engines/hqps_db/core/null_record.h"
#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "flex/utils/property/types.h"

namespace gs {

namespace mutable_csr_graph_impl {

// Base interface for edge iterator
class EdgeIter {
 public:
  using label_id_t = label_t;
  EdgeIter()
      : ptr1_(nullptr),
        ptr2_(nullptr),
        dir_(Direction::Out),
        label_triplet_({0, 0, 0}) {}

  EdgeIter(std::shared_ptr<CsrConstEdgeIterBase> ptr1,
           std::shared_ptr<CsrConstEdgeIterBase> ptr2, const Direction& dir,
           const std::array<label_id_t, 3>& label_triplet)
      : ptr1_(ptr1), ptr2_(ptr2), dir_(dir), label_triplet_(label_triplet) {}

  EdgeIter(std::shared_ptr<CsrConstEdgeIterBase> ptr1, const Direction& dir,
           const std::array<label_id_t, 3>& label_triplet)
      : ptr1_(ptr1), ptr2_(nullptr), dir_(dir), label_triplet_(label_triplet) {}

  EdgeIter(const EdgeIter& other)
      : ptr1_(other.ptr1_),
        ptr2_(other.ptr2_),
        dir_(other.dir_),
        label_triplet_(other.label_triplet_) {}

  inline void Next() const {
    if (ptr1_ && ptr1_->is_valid()) {
      ptr1_->next();
    }
    if (ptr2_ && ptr2_->is_valid()) {
      ptr2_->next();
    }
  }

  inline vid_t GetDstId() const {
    if (ptr1_ && ptr1_->is_valid()) {
      return ptr1_->get_neighbor();
    } else if (ptr2_ && ptr2_->is_valid()) {
      return ptr2_->get_neighbor();
    } else {
      throw std::runtime_error("Invalid edge iterator");
    }
  }

  inline vid_t GetSrcId() const {
    // TODO(fixme):
    LOG(FATAL) << "Not implemented yet";
  }

  inline label_id_t GetDstLabel() const { return label_triplet_[1]; }

  inline label_id_t GetSrcLabel() const { return label_triplet_[0]; }

  inline Direction GetDirection() const { return dir_; }

  inline Any GetData() const {
    if (ptr1_ && ptr1_->is_valid()) {
      return ptr1_->get_data();
    } else if (ptr2_ && ptr2_->is_valid()) {
      return ptr2_->get_data();
    } else {
      throw std::runtime_error("Invalid edge iterator");
    }
  }
  inline bool IsValid() const {
    if (ptr1_ && ptr1_->is_valid()) {
      return true;
    } else if (ptr2_ && ptr2_->is_valid()) {
      return true;
    } else {
      return false;
    }
  }

  EdgeIter& operator=(const EdgeIter& rhs) {
    this->ptr1_ = rhs.ptr1_;
    this->ptr2_ = rhs.ptr2_;
    this->dir_ = rhs.dir_;
    this->label_triplet_ = rhs.label_triplet_;
    return *this;
  }

  size_t Size() const {
    size_t ret = 0;
    if (ptr1_) {
      ret += ptr1_->size();
    }
    if (ptr2_) {
      ret += ptr2_->size();
    }
    return ret;
  }

 private:
  std::shared_ptr<CsrConstEdgeIterBase> ptr1_, ptr2_;
  Direction dir_;
  std::array<label_id_t, 3> label_triplet_;
};

class SubGraph {
 public:
  using label_id_t = label_t;
  using iterator = EdgeIter;
  SubGraph(const CsrBase* first, const CsrBase* second,
           const std::array<label_id_t, 3>& label_triplet, Direction dir)
      : first_csr_(first),
        second_csr_(second),
        label_triplet_(label_triplet),
        dir_(dir) {
    if (dir == Direction::Both) {
      CHECK_NOTNULL(first_csr_);
      CHECK_NOTNULL(second_csr_);
    } else {
      CHECK_NOTNULL(first_csr_);
    }
  }

  SubGraph(const CsrBase* first, const std::array<label_id_t, 3>& label_triplet,
           Direction dir)
      : first_csr_(first),
        second_csr_(nullptr),
        label_triplet_(label_triplet),
        dir_(dir) {
    CHECK_NOTNULL(first_csr_);
    CHECK(dir != Direction::Both);
  }

  inline iterator get_edges(vid_t vid) const {
    // TODO(zhanglei): Fixme
    if (dir_ == Direction::Out) {
      return iterator(first_csr_->edge_iter(vid), dir_, label_triplet_);
    } else if (dir_ == Direction::In) {
      return iterator(first_csr_->edge_iter(vid), dir_, label_triplet_);
    } else {
      return iterator(first_csr_->edge_iter(vid), second_csr_->edge_iter(vid),
                      dir_, label_triplet_);
    }
  }

  // here the src, dst, refer the sub graph, not the csr.
  label_id_t GetSrcLabel() const { return label_triplet_[0]; }
  label_id_t GetEdgeLabel() const { return label_triplet_[2]; }
  label_id_t GetDstLabel() const { return label_triplet_[1]; }
  Direction GetDirection() const { return dir_; }

 private:
  // dir_ = Direction::Out: first_csr_ = out_csr, second_csr_ = nullptr
  // dir_ = Direction::In: first_csr_ = in_csr, second_csr_ = nullptr
  // dir_ = Direction::Both: first_csr_ = out_csr, second_csr_ = in_csr
  const CsrBase *first_csr_, *second_csr_;
  std::array<label_id_t, 3> label_triplet_;
  Direction dir_;
};

}  // namespace mutable_csr_graph_impl

}  // namespace gs

#endif  // FLEX_ENGINES_HQPS_DATABASE_SUB_GRAPH_H_