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
      : ptr1_(nullptr), dir_(Direction::Out), label_triplet_({0, 0, 0}) {}

  EdgeIter(vid_t vid, std::shared_ptr<CsrConstEdgeIterBase> ptr1,
           const Direction& dir, const std::array<label_id_t, 3>& label_triplet)
      : src_vid_(vid), ptr1_(ptr1), dir_(dir), label_triplet_(label_triplet) {}

  EdgeIter(const EdgeIter& other)
      : src_vid_(other.src_vid_),
        ptr1_(other.ptr1_),
        dir_(other.dir_),
        label_triplet_(other.label_triplet_) {}

  inline void Next() const {
    if (ptr1_ && ptr1_->is_valid()) {
      ptr1_->next();
    }
  }

  inline vid_t GetDstId() const {
    if (dir_ == Direction::Out) {
      return ptr1_->get_neighbor();
    } else {
      return src_vid_;
    }
  }

  inline vid_t GetSrcId() const {
    if (dir_ == Direction::Out) {
      return src_vid_;
    } else {
      return ptr1_->get_neighbor();
    }
  }
  inline vid_t GetOtherId() const { return ptr1_->get_neighbor(); }

  inline label_id_t GetDstLabel() const {
    if (dir_ == Direction::Out) {
      return label_triplet_[1];
    } else {
      return label_triplet_[0];
    }
  }

  inline label_id_t GetSrcLabel() const {
    if (dir_ == Direction::Out) {
      return label_triplet_[0];
    } else {
      return label_triplet_[1];
    }
  }

  inline label_id_t GetOtherLabel() const { return label_triplet_[1]; }

  inline Direction GetDirection() const { return dir_; }

  inline Any GetData() const { return ptr1_->get_data(); }
  inline bool IsValid() const { return ptr1_ && ptr1_->is_valid(); }

  EdgeIter& operator=(const EdgeIter& rhs) {
    this->src_vid_ = rhs.src_vid_;
    this->ptr1_ = rhs.ptr1_;
    this->dir_ = rhs.dir_;
    this->label_triplet_ = rhs.label_triplet_;
    return *this;
  }

  size_t Size() const { return ptr1_ ? ptr1_->size() : 0; }

 private:
  vid_t src_vid_;  // the src vid of adj list, not the src of the directed edge.
  std::shared_ptr<CsrConstEdgeIterBase> ptr1_;
  Direction dir_;
  std::array<label_id_t, 3> label_triplet_;
};

/**
 * @brief SubGraph is a wrapper of CsrBase, which provides a way to iterate
 * over the sub graph defined via an edge triplet.
 * When the direction is set to both, We actually need to iterate over both the
 * in edges and out edges.
 */
class SubGraph {
 public:
  using label_id_t = label_t;
  using iterator = EdgeIter;

  SubGraph(const CsrBase* first, const std::array<label_id_t, 3>& label_triplet,
           Direction dir)
      : first_(first), label_triplet_(label_triplet), dir_(dir) {
    if (dir_ == Direction::Both) {
      throw std::runtime_error("SubGraph does not support both direction");
    }
  }

  inline iterator get_edges(vid_t vid) const {
    if (first_) {
      return iterator(vid, first_->edge_iter(vid), dir_, label_triplet_);
    }
    return iterator(vid, nullptr, dir_, label_triplet_);
  }

  // here the src, dst, refer the sub graph, not the csr.
  label_id_t GetSrcLabel() const { return label_triplet_[0]; }
  label_id_t GetDstLabel() const { return label_triplet_[1]; }
  label_id_t GetEdgeLabel() const { return label_triplet_[2]; }
  Direction GetDirection() const { return dir_; }

 private:
  const CsrBase* first_;
  std::array<label_id_t, 3> label_triplet_;
  Direction dir_;
};

}  // namespace mutable_csr_graph_impl

}  // namespace gs

#endif  // FLEX_ENGINES_HQPS_DATABASE_SUB_GRAPH_H_