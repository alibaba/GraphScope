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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_H_

#ifdef EXPERIMENTAL_ON
#include <cassert>
#include <cstddef>

#include <algorithm>
#include <iosfwd>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "folly/dynamic.h"
#include "folly/json.h"

#include "grape/config.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/graph/edge.h"
#include "grape/graph/vertex.h"
#include "grape/io/io_adaptor_base.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/types.h"
#include "grape/util.h"
#include "grape/utils/vertex_array.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/graph/utils/partitioner.h"

#include "core/error.h"
#include "core/io/dynamic_line_parser.h"
#include "core/utils/mpi_utils.h"
#include "core/vertex_map/global_vertex_map.h"
#include "proto/types.pb.h"

namespace gs {

using grape::Array;
using grape::CommSpec;
using grape::OutArchive;

namespace dynamic_fragment_impl {
/**
 * @brief This is the counterpart of VertexArray. VertexArray requires a range
 * of vertices. But this class can store data attached with discontinuous
 * vertices.
 *
 * @tparam T The type of data attached with the vertex
 * @tparam VID_T VID type
 */
template <typename T, typename VID_T>
class SparseVertexArray : public grape::Array<T, grape::Allocator<T>> {
  using Base = grape::Array<T, grape::Allocator<T>>;

 public:
  SparseVertexArray() : vertices_(dummy) {}

  explicit SparseVertexArray(const grape::VertexVector<VID_T>& vertices)
      : SparseVertexArray() {
    Init(vertices);
  }

  SparseVertexArray(const grape::VertexVector<VID_T>& vertices, const T& value)
      : SparseVertexArray() {
    Init(vertices, value);
  }

  ~SparseVertexArray() = default;

  void Init(const grape::VertexVector<VID_T>& vertices) {
    if (vertices.size() == 0) {
      return;
    }
    const auto& min_v = vertices[0];
    const auto& max_v = vertices[vertices.size() - 1];

    Base::resize(max_v.GetValue() - min_v.GetValue() + 1);
    vertices_ = vertices;
    fake_start_ = Base::data() - min_v.GetValue();
  }

  void Init(const grape::VertexVector<VID_T>& vertices, const T& value) {
    if (vertices.size() == 0)
      return;
    const auto& min_v = vertices[0];
    const auto& max_v = vertices[vertices.size() - 1];

    Base::resize(max_v.GetValue() - min_v.GetValue() + 1, value);
    vertices_ = vertices;
    fake_start_ = Base::data() - min_v.GetValue();
  }

  void SetValue(grape::VertexVector<VID_T>& vertices, const T& value) {
    for (auto v : vertices) {
      fake_start_[v] = value;
    }
  }

  void SetValue(const T& value) {
    std::fill_n(Base::data(), Base::size(), value);
  }

  inline T& operator[](const grape::Vertex<VID_T>& loc) {
    return fake_start_[loc.GetValue()];
  }

  inline const T& operator[](const grape::Vertex<VID_T>& loc) const {
    return fake_start_[loc.GetValue()];
  }

  void Swap(SparseVertexArray& rhs) {
    Base::swap((Base&) rhs);
    std::swap(vertices_, rhs.vertices_);
    std::swap(fake_start_, rhs.fake_start_);
  }

  void Clear() {
    SparseVertexArray ga;
    this->Swap(ga);
  }

  const grape::VertexVector<VID_T>& GetVertexRange() const {
    return vertices_.get();
  }

 private:
  const grape::VertexVector<VID_T> dummy;
  std::reference_wrapper<const grape::VertexVector<VID_T>> vertices_;
  T* fake_start_;
};

/**
 * @brief A neighbor of a vertex in the graph.
 *
 * Assume an edge, vertex_a --(edge_data)--> vertex_b.
 * a <Nbr> of vertex_a stores <Vertex> b and the edge_data.
 *
 * @tparam VID_T
 * @tparam EDATA_T
 */
template <typename EDATA_T>
class Nbr {
  using VID_T = vineyard::property_graph_types::VID_TYPE;

 public:
  Nbr() : neighbor_(), data_() {}
  explicit Nbr(const VID_T& nbr) : neighbor_(nbr), data_() {}
  explicit Nbr(const grape::Vertex<VID_T>& nbr) : neighbor_(nbr), data_() {}
  Nbr(const Nbr& rhs) = default;
  Nbr(const VID_T& nbr, const EDATA_T& data) : neighbor_(nbr), data_(data) {}
  Nbr(const grape::Vertex<VID_T>& nbr, const EDATA_T& data)
      : neighbor_(nbr), data_(data) {}
  ~Nbr() = default;

  Nbr& operator=(const Nbr& rhs) {
    neighbor_ = rhs.neighbor_;
    data_ = rhs.data_;
    return *this;
  }

  const grape::Vertex<VID_T>& neighbor() const { return neighbor_; }

  grape::Vertex<VID_T>& neighbor() { return neighbor_; }

  const grape::Vertex<VID_T>& get_neighbor() const { return neighbor_; }

  grape::Vertex<VID_T>& get_neighbor() { return neighbor_; }

  void set_neighbor(grape::Vertex<VID_T>& neighbor) {
    this->neighbor_ = neighbor;
  }

  const EDATA_T& data() const { return data_; }

  EDATA_T& data() { return data_; }

  const EDATA_T& get_data() const { return data_; }

  EDATA_T& get_data() { return data_; }

  void set_data(EDATA_T data) { this->data_ = data; }

  void update_data(const EDATA_T& data) {
    if (data_.isNull()) {
      data_ = data;
    } else if (data_.isObject()) {
      data_.update(data);
    }
  }

 private:
  grape::Vertex<VID_T> neighbor_;
  EDATA_T data_;
};

/**
 * This is an internal representation of neighbor vertices using std::map.
 *
 * @tparam EDATA_T Data type of edge
 */
template <typename EDATA_T>
class AdjList {
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using NbrT = Nbr<EDATA_T>;

 public:
  AdjList() = default;
  AdjList(VID_T id_mask, VID_T ivnum,
          typename std::map<VID_T, NbrT>::iterator map_iter_begin,
          typename std::map<VID_T, NbrT>::iterator map_iter_end)
      : id_mask_(id_mask),
        ivnum_(ivnum),
        map_iter_begin_(map_iter_begin),
        map_iter_end_(map_iter_end) {}
  ~AdjList() = default;

  inline bool Empty() const { return map_iter_begin_ == map_iter_end_; }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const {
    return std::distance(map_iter_begin_, map_iter_end_);
  }

  class iterator {
    using pointer_type = NbrT*;
    using reference_type = NbrT&;

   public:
    iterator() = default;
    iterator(VID_T id_mask, VID_T ivnum,
             typename std::map<VID_T, NbrT>::iterator map_current) noexcept
        : id_mask_(id_mask), ivnum_(ivnum), map_current_(map_current) {}

    reference_type operator*() noexcept {
      set_nbr();
      return internal_nbr;
    }

    pointer_type operator->() noexcept {
      set_nbr();
      return &internal_nbr;
    }

    iterator& operator++() noexcept {
      ++map_current_;
      return *this;
    }

    iterator operator++(int) noexcept {
      return iterator(id_mask_, ivnum_, map_current_++);
    }

    iterator& operator--() noexcept {
      --map_current_;
      return *this;
    }

    iterator operator--(int) noexcept {
      return iterator(id_mask_, ivnum_, map_current_--);
    }

    iterator operator+(size_t offset) noexcept {
      auto curr = map_current_;
      for (int i = 0; i < offset; i++)
        curr++;
      return iterator(id_mask_, ivnum_, curr);
    }

    bool operator==(const iterator& rhs) noexcept {
      return map_current_ == rhs.map_current_;
    }

    bool operator!=(const iterator& rhs) noexcept {
      return map_current_ != rhs.map_current_;
    }

   private:
    void set_nbr() {
      internal_nbr = map_current_->second;
      auto v = internal_nbr.neighbor();
      // convert internal lid to external lid
      if (v.GetValue() >= ivnum_) {
        v.SetValue(ivnum_ + id_mask_ - v.GetValue());
      }
      internal_nbr.set_neighbor(v);
    }

    VID_T id_mask_{};
    VID_T ivnum_{};
    NbrT internal_nbr;
    typename std::map<VID_T, NbrT>::iterator map_current_;
  };

  class const_iterator {
    using pointer_type = const NbrT*;
    using reference_type = const NbrT&;

   public:
    const_iterator() = default;
    const_iterator(
        VID_T id_mask, VID_T ivnum,
        typename std::map<VID_T, NbrT>::iterator map_current) noexcept
        : id_mask_(id_mask), ivnum_(ivnum), map_current_(map_current) {}

    reference_type operator*() const noexcept {
      const_cast<const_iterator*>(this)->set_nbr();
      return internal_nbr;
    }

    pointer_type operator->() const noexcept {
      const_cast<const_iterator*>(this)->set_nbr();
      return &internal_nbr;
    }

    const_iterator& operator++() noexcept {
      ++map_current_;
      return *this;
    }

    const_iterator operator++(int) noexcept {
      return const_iterator(id_mask_, ivnum_, map_current_++);
    }

    const_iterator& operator--() noexcept {
      --map_current_;
      return *this;
    }

    const_iterator operator--(int) noexcept {
      return const_iterator(id_mask_, ivnum_, map_current_--);
    }

    const_iterator operator+(size_t offset) noexcept {
      auto curr = map_current_;
      for (int i = 0; i < offset; i++)
        curr++;
      return const_iterator(id_mask_, ivnum_, curr);
    }

    bool operator==(const const_iterator& rhs) noexcept {
      return map_current_ == rhs.map_current_;
    }
    bool operator!=(const const_iterator& rhs) noexcept {
      return map_current_ != rhs.map_current_;
    }

   private:
    VID_T id_mask_;
    VID_T ivnum_;
    NbrT internal_nbr;
    typename std::map<VID_T, const NbrT>::iterator map_current_;
  };

  iterator begin() { return iterator(id_mask_, ivnum_, map_iter_begin_); }

  iterator end() { return iterator(id_mask_, ivnum_, map_iter_end_); }

  const_iterator begin() const {
    return const_iterator(id_mask_, ivnum_, map_iter_begin_);
  }
  const_iterator end() const {
    return const_iterator(id_mask_, ivnum_, map_iter_end_);
  }

  bool empty() const { return map_iter_begin_ == map_iter_end_; }

 private:
  VID_T id_mask_{};
  VID_T ivnum_{};
  typename std::map<VID_T, NbrT>::iterator map_iter_begin_;
  typename std::map<VID_T, NbrT>::iterator map_iter_end_;
};

/**
 * @brief This is an internal representation of neighbor vertices using
 * std::map.
 *
 * @tparam EDATA_T Data type of edge
 */
template <typename EDATA_T>
class ConstAdjList {
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using NbrT = Nbr<EDATA_T>;

 public:
  ConstAdjList() = default;
  ConstAdjList(VID_T id_mask, VID_T ivnum,
               typename std::map<VID_T, NbrT>::const_iterator map_iter_begin,
               typename std::map<VID_T, NbrT>::const_iterator map_iter_end)
      : id_mask_(id_mask),
        ivnum_(ivnum),
        map_iter_begin_(map_iter_begin),
        map_iter_end_(map_iter_end) {}
  ~ConstAdjList() = default;

  inline bool Empty() const { return map_iter_begin_ == map_iter_end_; }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const {
    return std::distance(map_iter_begin_, map_iter_end_);
  }

  class const_iterator {
    using pointer_type = const NbrT*;
    using reference_type = const NbrT&;

   public:
    const_iterator() = default;
    const_iterator(
        VID_T id_mask, VID_T ivnum,
        typename std::map<VID_T, NbrT>::const_iterator map_current) noexcept
        : id_mask_(id_mask), ivnum_(ivnum), map_current_(map_current) {}

    reference_type operator*() const noexcept {
      const_cast<const_iterator*>(this)->set_nbr();
      return internal_nbr;
    }

    pointer_type operator->() const noexcept {
      const_cast<const_iterator*>(this)->set_nbr();
      return &internal_nbr;
    }

    const_iterator& operator++() noexcept {
      ++map_current_;
      return *this;
    }

    const_iterator operator++(int) noexcept {
      return const_iterator(id_mask_, ivnum_, map_current_++);
    }

    const_iterator& operator--() noexcept {
      --map_current_;
      return *this;
    }

    const_iterator operator--(int) noexcept {
      return const_iterator(id_mask_, ivnum_, map_current_--);
    }

    const_iterator operator+(size_t offset) noexcept {
      auto curr = map_current_;
      for (int i = 0; i < offset; i++)
        curr++;
      return const_iterator(id_mask_, ivnum_, curr);
    }

    bool operator==(const const_iterator& rhs) noexcept {
      return map_current_ == rhs.map_current_;
    }

    bool operator!=(const const_iterator& rhs) noexcept {
      return map_current_ != rhs.map_current_;
    }

   private:
    void set_nbr() {
      internal_nbr = map_current_->second;
      auto v = internal_nbr.neighbor();
      // convert internal lid to external lid
      if (v.GetValue() >= ivnum_) {
        v.SetValue(ivnum_ + id_mask_ - v.GetValue());
      }
      internal_nbr.set_neighbor(v);
    }

    VID_T id_mask_{};
    VID_T ivnum_{};
    NbrT internal_nbr;
    typename std::map<VID_T, NbrT>::const_iterator map_current_;
  };

  const_iterator begin() const {
    return const_iterator(id_mask_, ivnum_, map_iter_begin_);
  }
  const_iterator end() const {
    return const_iterator(id_mask_, ivnum_, map_iter_end_);
  }

  bool empty() const { return map_iter_begin_ == map_iter_end_; }

 private:
  VID_T id_mask_{};
  VID_T ivnum_{};
  typename std::map<VID_T, NbrT>::const_iterator map_iter_begin_;
  typename std::map<VID_T, NbrT>::const_iterator map_iter_end_;
};

/**
 * @brief A container to store edges.
 *
 * @tparam EDATA_T The type of data attached with the edge
 */
template <typename EDATA_T>
class NbrMapSpace {
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using NbrT = Nbr<EDATA_T>;

 public:
  NbrMapSpace() : index_(0) {}

  // Create a new linked list
  inline size_t emplace(VID_T vid, const EDATA_T& edata) {
    buffer_.resize(index_ + 1);
    buffer_[index_] = new std::map<VID_T, NbrT>();
    buffer_[index_]->operator[](vid) = NbrT(vid, edata);
    return index_++;
  }

  // Insert the value to an existing linked list, or update the existing value
  inline size_t emplace(size_t loc, VID_T vid, const EDATA_T& edata,
                        bool& created) {
    if (buffer_[loc]->find(vid) != buffer_[loc]->end()) {
      buffer_[loc]->operator[](vid).update_data(edata);
      created = false;
      return loc;
    } else {
      buffer_[loc]->operator[](vid) = NbrT(vid, edata);
      created = true;
      return loc;
    }
  }

  inline void update(size_t loc, VID_T vid, const EDATA_T& edata) {
    if (buffer_[loc]->find(vid) != buffer_[loc]->end()) {
      buffer_[loc]->operator[](vid).update_data(edata);
    }
  }

  inline void set_data(size_t loc, VID_T vid, const EDATA_T& edata) {
    if (buffer_[loc]->find(vid) != buffer_[loc]->end()) {
      buffer_[loc]->operator[](vid) = NbrT(vid, edata);
    }
  }

  inline void remove_edges(size_t loc) {
    delete buffer_[loc];
    buffer_[loc] = nullptr;
  }

  inline size_t remove_edge(size_t loc, VID_T vid) {
    return buffer_[loc]->erase(vid);
  }

  inline std::map<VID_T, NbrT>& operator[](size_t loc) { return *buffer_[loc]; }

  inline const std::map<VID_T, NbrT>& operator[](size_t loc) const {
    return *buffer_[loc];
  }

  inline std::map<VID_T, NbrT>& InnerNbr(size_t loc) {
    return split_buffer_[loc][0];
  }

  inline const std::map<VID_T, NbrT>& InnerNbr(size_t loc) const {
    return split_buffer_[loc][0];
  }

  inline std::map<VID_T, NbrT>& OuterNbr(size_t loc) {
    return split_buffer_[loc][1];
  }

  inline const std::map<VID_T, NbrT>& OuterNbr(size_t loc) const {
    return split_buffer_[loc][1];
  }

  void copy(const NbrMapSpace<EDATA_T>& other) {
    index_ = other.index_;
    buffer_.resize(other.buffer_.size());
    for (size_t i = 0; i < other.buffer_.size(); ++i) {
      if (other.buffer_[i] != nullptr) {
        buffer_[i] = new std::map<VID_T, NbrT>();
        for (auto& item : *(other.buffer_[i])) {
          buffer_[i]->operator[](item.first) = item.second;
        }
      } else {
        buffer_[i] = nullptr;
      }
    }
  }

  void Clear() {
    for (size_t i = 0; i < buffer_.size(); ++i) {
      delete buffer_[i];
      buffer_[i] = nullptr;
    }
    buffer_.clear();
    index_ = 0;
  }

  void BuildSplitEdges(VID_T ivnum) {
    // free existed buffer
    for (size_t loc = 0; loc < split_buffer_.size(); loc++) {
      delete[] split_buffer_[loc];
    }

    // copy all existed edges and distinguish inner/outer neighbors
    split_buffer_.resize(buffer_.size());
    for (size_t loc = 0; loc < buffer_.size(); loc++) {
      auto& maps = *buffer_[loc];

      split_buffer_[loc] = new std::map<VID_T, NbrT>[2];
      for (const auto& iter : maps) {
        auto lid = iter.first;
        auto& nbr = iter.second;
        auto idx = lid < ivnum ? 0 : 1;

        split_buffer_[loc][idx][lid] = nbr;
      }
    }
  }

 private:
  std::vector<std::map<VID_T, NbrT>*> buffer_;
  // split_buffer_[i][0] represents inner neighbors, split_buffer_[i][1]
  // represents outer neighbors
  std::vector<std::map<VID_T, NbrT>*> split_buffer_;
  size_t index_;
};
}  // namespace dynamic_fragment_impl

/**
 * @brief A mutable non-labeled fragment. The data attached with vertex or edge
 * are represented by folly::dynamic.
 */
class DynamicFragment {
 public:
  using oid_t = folly::dynamic;
  using vid_t = vineyard::property_graph_types::VID_TYPE;
  using edata_t = folly::dynamic;
  using vdata_t = folly::dynamic;
  using edge_t = grape::Edge<vid_t, edata_t>;
  using nbr_t = dynamic_fragment_impl::Nbr<edata_t>;
  using vertex_t = grape::Vertex<vid_t>;
  using internal_vertex_t = grape::internal::Vertex<vid_t, vdata_t>;
  using const_adj_list_t = dynamic_fragment_impl::ConstAdjList<edata_t>;
  using adj_list_t = dynamic_fragment_impl::AdjList<edata_t>;
  using vertex_map_t = grape::GlobalVertexMap<oid_t, vid_t>;
  using partitioner_t = vineyard::HashPartitioner<oid_t>;
  using vertex_range_t = grape::VertexVector<vid_t>;
  template <typename DATA_T>
  using vertex_array_t =
      dynamic_fragment_impl::SparseVertexArray<DATA_T, vid_t>;

  using IsEdgeCut = std::true_type;
  using IsVertexCut = std::false_type;
  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  DynamicFragment() = default;

  explicit DynamicFragment(std::shared_ptr<vertex_map_t> vm_ptr)
      : vm_ptr_(std::move(vm_ptr)) {}

  virtual ~DynamicFragment() = default;

  void Init(fid_t fid, std::vector<internal_vertex_t>& vertices,
            std::vector<edge_t>& edges, bool directed) {
    directed_ = directed;
    directed ? load_strategy_ = grape::LoadStrategy::kBothOutIn
             : load_strategy_ = grape::LoadStrategy::kOnlyOut;

    fid_ = fid;
    fnum_ = vm_ptr_->GetFragmentNum();
    calcFidBitWidth(fnum_, id_mask_, fid_offset_);

    ivnum_ = vm_ptr_->GetInnerVertexSize(fid);
    ovnum_ = 0;
    oenum_ = 0;
    ienum_ = 0;

    inner_ie_pos_.clear();
    inner_oe_pos_.clear();

    // first pass: find out outer vertices and calculate inner/outer
    // vertices num
    {
      std::vector<vid_t> outer_vertices =
          getOuterVerticesAndInvalidEdges(edges, load_strategy_);

      grape::DistinctSort(outer_vertices);

      ovgid_.resize(outer_vertices.size());
      memcpy(&ovgid_[0], &outer_vertices[0],
             outer_vertices.size() * sizeof(vid_t));
    }

    // we encode vertices like this:
    // inner vertices from 0 to ivnum_
    // outer vertices from id_mask_ to ivnum_
    for (auto gid : ovgid_) {
      ovg2i_.emplace(gid, ovnum_);
      ++ovnum_;
    }

    tvnum_ = ivnum_ + ovnum_;
    alive_ivnum_ = ivnum_;
    alive_ovnum_ = ovnum_;

    inner_ie_pos_.resize(ivnum_, -1);
    inner_oe_pos_.resize(ivnum_, -1);
    inner_vertex_alive_.resize(ivnum_, true);
    outer_vertex_alive_.resize(ovnum_, true);

    AddEdges(edges, load_strategy_);

    initOuterVerticesOfFragment();

    vdata_.clear();
    vdata_.resize(ivnum_);
    if (sizeof(internal_vertex_t) > sizeof(vid_t)) {
      for (auto& v : vertices) {
        vid_t gid = v.vid();
        if (gid >> fid_offset_ == fid_) {
          vdata_[(gid & id_mask_)] = v.vdata();
        }
      }
    }

    mirrors_of_frag_.resize(fnum_);
    InvalidCache();
  }

  void Init(fid_t fid, bool directed) {
    std::vector<internal_vertex_t> empty_vertices;
    std::vector<edge_t> empty_edges;

    Init(fid, empty_vertices, empty_edges, directed);
  }

  void Copy(std::shared_ptr<DynamicFragment> other,
            const std::string& copy_type = "identical") {
    ivnum_ = other->ivnum_;
    ovnum_ = other->ovnum_;
    tvnum_ = other->tvnum_;
    alive_ivnum_ = other->alive_ivnum_;
    alive_ovnum_ = other->alive_ovnum_;
    id_mask_ = other->id_mask_;
    fid_offset_ = other->fid_offset_;
    fid_ = other->fid_;
    fnum_ = other->fnum_;
    message_strategy_ = other->message_strategy_;
    directed_ = other->directed_;
    load_strategy_ = other->load_strategy_;

    ovg2i_ = other->ovg2i_;
    ovgid_.resize(other->ovgid_.size());
    memcpy(&ovgid_[0], &(other->ovgid_[0]),
           other->ovgid_.size() * sizeof(vid_t));

    vdata_.clear();
    vdata_.resize(other->vdata_.size());
    for (size_t i = 0; i < ivnum_; ++i) {
      vdata_[i] = other->vdata_[i];
    }

    inner_vertex_alive_.resize(other->inner_vertex_alive_.size());
    memcpy(&inner_vertex_alive_[0], &(other->inner_vertex_alive_[0]),
           other->inner_vertex_alive_.size() * sizeof(bool));

    outer_vertex_alive_.resize(other->outer_vertex_alive_.size());
    memcpy(&outer_vertex_alive_[0], &(other->outer_vertex_alive_[0]),
           other->outer_vertex_alive_.size() * sizeof(bool));

    if (copy_type == "reverse") {
      ienum_ = other->oenum_;
      oenum_ = other->ienum_;
      inner_ie_pos_.resize(other->inner_oe_pos_.size());
      memcpy(&inner_ie_pos_[0], &(other->inner_oe_pos_[0]),
             other->inner_oe_pos_.size() * sizeof(int32_t));
      inner_oe_pos_.resize(other->inner_ie_pos_.size());
      memcpy(&inner_oe_pos_[0], &(other->inner_ie_pos_[0]),
             other->inner_ie_pos_.size() * sizeof(int32_t));
    } else {
      ienum_ = other->ienum_;
      oenum_ = other->oenum_;
      inner_ie_pos_.resize(other->inner_ie_pos_.size());
      memcpy(&inner_ie_pos_[0], &(other->inner_ie_pos_[0]),
             other->inner_ie_pos_.size() * sizeof(int32_t));

      inner_oe_pos_.resize(other->inner_oe_pos_.size());
      memcpy(&inner_oe_pos_[0], &(other->inner_oe_pos_[0]),
             other->inner_oe_pos_.size() * sizeof(int32_t));
    }

    // inner_edge_space_
    inner_edge_space_.copy(other->inner_edge_space_);

    outer_vertices_of_frag_.resize(fnum_);
    for (size_t i = 0; i < fnum_; ++i) {
      outer_vertices_of_frag_[i] = other->outer_vertices_of_frag_[i];
    }

    mirrors_of_frag_.resize(fnum_);
    InvalidCache();
  }

  template <typename IOADAPTOR_T>
  void Serialize(const std::string& prefix) {}

  template <typename IOADAPTOR_T>
  void Deserialize(const std::string& prefix, const fid_t fid) {}

  void PrepareToRunApp(grape::MessageStrategy strategy, bool need_split_edges) {
    message_strategy_ = strategy;
    if (strategy == grape::MessageStrategy::kAlongEdgeToOuterVertex ||
        strategy == grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex ||
        strategy == grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
      initMessageDestination(strategy);
    }

    // a naive implementation by copy edges
    if (need_split_edges) {
      inner_edge_space_.BuildSplitEdges(ivnum_);
    }
  }

  inline fid_t fid() const { return fid_; }

  inline fid_t fnum() const { return fnum_; }

  inline vid_t id_mask() const { return id_mask_; }

  inline int fid_offset() const { return fid_offset_; }

  inline bool directed() const { return directed_; }

  inline const vid_t* GetOuterVerticesGid() const { return &ovgid_[0]; }

  inline size_t GetEdgeNum() const { return ienum_ + oenum_; }

  inline vid_t GetVerticesNum() const { return alive_ivnum_ + alive_ovnum_; }

  size_t GetTotalVerticesNum() const { return vm_ptr_->GetTotalVertexSize(); }

  inline vertex_range_t InnerVertices() const {
    auto inner_vertices = grape::VertexRange<vid_t>(0, ivnum_);
    auto& mutable_vertices =
        const_cast<DynamicFragment*>(this)->alive_inner_vertices_;

    if (!mutable_vertices.first) {
      mutable_vertices.second.clear();
      mutable_vertices.first = true;

      for (auto v : inner_vertices) {
        if (IsAliveInnerVertex(v)) {
          mutable_vertices.second.push_back(v);
        }
      }
    }

    return vertex_range_t(mutable_vertices.second);
  }

  inline vertex_range_t OuterVertices() const {
    auto outer_vertices = grape::VertexRange<vid_t>(ivnum_, tvnum_);
    auto& mutable_vertices =
        const_cast<DynamicFragment*>(this)->alive_outer_vertices_;

    if (!mutable_vertices.first) {
      mutable_vertices.second.clear();
      mutable_vertices.first = true;

      for (auto v : outer_vertices) {
        if (IsAliveOuterVertex(v)) {
          mutable_vertices.second.push_back(v);
        }
      }
    }

    return vertex_range_t(mutable_vertices.second);
  }

  inline vertex_range_t Vertices() const {
    auto vertices = grape::VertexRange<vid_t>(0, tvnum_);
    auto& mutable_vertices =
        const_cast<DynamicFragment*>(this)->alive_vertices_;

    if (!mutable_vertices.first) {
      mutable_vertices.second.clear();
      mutable_vertices.first = true;

      for (auto v : vertices) {
        if (IsAliveVertex(v)) {
          mutable_vertices.second.push_back(v);
        }
      }
    }

    return vertex_range_t(mutable_vertices.second);
  }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (vm_ptr_->GetGid(oid, gid)) {
      return ((gid >> fid_offset_) == fid_) ? InnerVertexGid2Vertex(gid, v)
                                            : OuterVertexGid2Vertex(gid, v);
    } else {
      return false;
    }
  }

  inline oid_t GetId(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexId(v) : GetOuterVertexId(v);
  }

  inline fid_t GetFragId(const vertex_t& u) const {
    return IsInnerVertex(u)
               ? fid_
               : (fid_t)(ovgid_[u.GetValue() - ivnum_] >> fid_offset_);
  }

  inline const vdata_t& GetData(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return vdata_[v.GetValue()];
  }

  inline void SetData(const vertex_t& v, const vdata_t& val) {
    assert(IsInnerVertex(v));
    vdata_[v.GetValue()] = val;
  }

  inline bool HasChild(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    auto pos = inner_oe_pos_[v.GetValue()];
    return pos != -1 && !inner_edge_space_[pos].empty();
  }

  inline bool HasParent(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    auto pos = inner_ie_pos_[v.GetValue()];
    return pos != -1 && !inner_edge_space_[pos].empty();
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    auto pos = inner_oe_pos_[v.GetValue()];
    return pos == -1 ? 0 : inner_edge_space_[pos].size();
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    auto pos = inner_ie_pos_[v.GetValue()];
    return pos == -1 ? 0 : inner_edge_space_[pos].size();
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return ((gid >> fid_offset_) == fid_) ? InnerVertexGid2Vertex(gid, v)
                                          : OuterVertexGid2Vertex(gid, v);
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexGid(v) : GetOuterVertexGid(v);
  }

  inline vid_t GetInnerVerticesNum() const { return alive_ivnum_; }

  inline vid_t GetOuterVerticesNum() const { return alive_ovnum_; }

  inline bool IsInnerVertex(const vertex_t& v) const {
    return (v.GetValue() < ivnum_);
  }

  inline bool IsOuterVertex(const vertex_t& v) const {
    return v.GetValue() < tvnum_ && v.GetValue() >= ivnum_;
  }

  inline bool GetInnerVertex(const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (vm_ptr_->GetGid(oid, gid)) {
      if ((gid >> fid_offset_) == fid_ && isAlive(gid & id_mask_)) {
        v.SetValue(gid & id_mask_);
        return true;
      }
    }
    return false;
  }

  inline bool GetOuterVertex(const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (vm_ptr_->GetGid(oid, gid)) {
      return OuterVertexGid2Vertex(gid, v);
    } else {
      return false;
    }
  }

  inline oid_t GetInnerVertexId(const vertex_t& v) const {
    assert(isAlive(v.GetValue()));
    oid_t internal_oid;
    vm_ptr_->GetOid(fid_, v.GetValue(), internal_oid);
    return internal_oid;
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    assert(isAlive(v.GetValue()));
    vid_t gid = ovgid_[v.GetValue() - ivnum_];
    oid_t internal_oid;
    vm_ptr_->GetOid(gid, internal_oid);
    return internal_oid;
  }

  inline oid_t Gid2Oid(const vid_t& gid) const {
    oid_t internal_oid;
    vm_ptr_->GetOid(gid, internal_oid);
    return internal_oid;
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    return vm_ptr_->GetGid(oid, gid);
  }

  inline bool Gid2Lid(const vid_t& gid, vid_t& lid) const {
    if ((gid >> fid_offset_) == fid_) {
      lid = gid & id_mask_;
      if (lid < ivnum_) {
        return true;
      } else {
        return false;
      }
    } else {
      auto iter = ovg2i_.find(gid);
      if (iter != ovg2i_.end()) {
        lid = id_mask_ - iter->second;
        return true;
      } else {
        return false;
      }
    }
  }

  inline bool InnerVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    vid_t lid = gid & id_mask_;
    if (lid < ivnum_ && isAlive(lid)) {
      assert(isAlive(lid));
      v.SetValue(gid & id_mask_);
      return true;
    }
    return false;
  }

  inline bool OuterVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    auto iter = ovg2i_.find(gid);
    if (iter != ovg2i_.end()) {
      assert(isAlive(ivnum_ + iter->second));
      v.SetValue(ivnum_ + iter->second);
      return true;
    } else {
      return false;
    }
  }

  inline vid_t GetOuterVertexGid(const vertex_t& v) const {
    return ovgid_[v.GetValue() - ivnum_];
  }

  inline vid_t GetInnerVertexGid(const vertex_t& v) const {
    return (v.GetValue() | ((vid_t) fid_ << fid_offset_));
  }

  /**
   * @brief Return the incoming edge destination fragment ID list of a inner
   * vertex.
   *
   * @param v Input vertex.
   *
   * @return The incoming edge destination fragment ID list.
   *
   * @attention This method is only available when application set message
   * strategy as kAlongIncomingEdgeToOuterVertex.
   */
  inline grape::DestList IEDests(const vertex_t& v) const {
    assert(!idoffset_.empty());
    assert(IsInnerVertex(v));
    return {idoffset_[v.GetValue()], idoffset_[v.GetValue() + 1]};
  }

  /**
   * @brief Return the outgoing edge destination fragment ID list of a Vertex.
   *
   * @param v Input vertex.
   *
   * @return The outgoing edge destination fragment ID list.
   *
   * @attention This method is only available when application set message
   * strategy as kAlongOutgoingedge_toOuterVertex.
   */
  inline grape::DestList OEDests(const vertex_t& v) const {
    assert(!odoffset_.empty());
    assert(IsInnerVertex(v));
    return {odoffset_[v.GetValue()], odoffset_[v.GetValue() + 1]};
  }

  /**
   * @brief Return the edge destination fragment ID list of a inner vertex.
   *
   * @param v Input vertex.
   *
   * @return The edge destination fragment ID list.
   *
   * @attention This method is only available when application set message
   * strategy as kAlongedge_toOuterVertex.
   */
  inline grape::DestList IOEDests(const vertex_t& v) const {
    assert(!iodoffset_.empty());
    assert(IsInnerVertex(v));
    return {iodoffset_[v.GetValue()], iodoffset_[v.GetValue() + 1]};
  }

 public:
  /**
   * @brief Returns the incoming adjacent vertices of v.
   *
   * @param v Input vertex.
   *
   * @return The incoming adjacent vertices of v.
   *
   * @attention Only inner vertex is available.
   */
  inline adj_list_t GetIncomingAdjList(const vertex_t& v) {
    auto ie_pos = inner_ie_pos_[v.GetValue()];
    if (ie_pos == -1) {
      return adj_list_t();
    }
    return adj_list_t(id_mask_, ivnum_, inner_edge_space_[ie_pos].begin(),
                      inner_edge_space_[ie_pos].end());
  }
  /**
   * @brief Returns the incoming adjacent vertices of v.
   *
   * @param v Input vertex.
   *
   * @return The incoming adjacent vertices of v.
   *
   * @attention Only inner vertex is available.
   */
  inline const_adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    auto ie_pos = inner_ie_pos_[v.GetValue()];
    if (ie_pos == -1) {
      return const_adj_list_t();
    }
    return const_adj_list_t(id_mask_, ivnum_,
                            inner_edge_space_[ie_pos].cbegin(),
                            inner_edge_space_[ie_pos].cend());
  }

  inline adj_list_t GetIncomingInnerVertexAdjList(const vertex_t& v) {
    auto ie_pos = inner_ie_pos_[v.GetValue()];
    if (ie_pos == -1) {
      return adj_list_t();
    }
    return adj_list_t(id_mask_, ivnum_,
                      inner_edge_space_.InnerNbr(ie_pos).begin(),
                      inner_edge_space_.InnerNbr(ie_pos).end());
  }

  inline const_adj_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) const {
    auto ie_pos = inner_ie_pos_[v.GetValue()];
    if (ie_pos == -1) {
      return const_adj_list_t();
    }
    return const_adj_list_t(id_mask_, ivnum_,
                            inner_edge_space_.InnerNbr(ie_pos).begin(),
                            inner_edge_space_.InnerNbr(ie_pos).end());
  }

  inline adj_list_t GetIncomingOuterVertexAdjList(const vertex_t& v) {
    auto ie_pos = inner_ie_pos_[v.GetValue()];
    if (ie_pos == -1) {
      return adj_list_t();
    }
    return adj_list_t(id_mask_, ivnum_,
                      inner_edge_space_.OuterNbr(ie_pos).begin(),
                      inner_edge_space_.OuterNbr(ie_pos).end());
  }

  inline const_adj_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) const {
    auto ie_pos = inner_ie_pos_[v.GetValue()];
    if (ie_pos == -1) {
      return const_adj_list_t();
    }
    return const_adj_list_t(id_mask_, ivnum_,
                            inner_edge_space_.OuterNbr(ie_pos).begin(),
                            inner_edge_space_.OuterNbr(ie_pos).end());
  }

  /**
   * @brief Returns the outgoing adjacent vertices of v.
   *
   * @param v Input vertex.
   *
   * @return The outgoing adjacent vertices of v.
   *
   * @attention Only inner vertex is available.
   */
  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) {
    auto oe_pos = inner_oe_pos_[v.GetValue()];
    if (oe_pos == -1) {
      return adj_list_t();
    }
    return adj_list_t(id_mask_, ivnum_, inner_edge_space_[oe_pos].begin(),
                      inner_edge_space_[oe_pos].end());
  }
  /**
   * @brief Returns the outgoing adjacent vertices of v.
   *
   * @param v Input vertex.
   *
   * @return The outgoing adjacent vertices of v.
   *
   * @attention Only inner vertex is available.
   */
  inline const_adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    auto oe_pos = inner_oe_pos_[v.GetValue()];
    if (oe_pos == -1) {
      return const_adj_list_t();
    }
    return const_adj_list_t(id_mask_, ivnum_,
                            inner_edge_space_[oe_pos].cbegin(),
                            inner_edge_space_[oe_pos].cend());
  }

  inline adj_list_t GetOutgoingInnerVertexAdjList(const vertex_t& v) {
    auto oe_pos = inner_oe_pos_[v.GetValue()];
    if (oe_pos == -1) {
      return adj_list_t();
    }
    return adj_list_t(id_mask_, ivnum_,
                      inner_edge_space_.InnerNbr(oe_pos).begin(),
                      inner_edge_space_.InnerNbr(oe_pos).end());
  }

  inline const_adj_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) const {
    auto oe_pos = inner_oe_pos_[v.GetValue()];
    if (oe_pos == -1) {
      return const_adj_list_t();
    }
    return const_adj_list_t(id_mask_, ivnum_,
                            inner_edge_space_.InnerNbr(oe_pos).begin(),
                            inner_edge_space_.InnerNbr(oe_pos).end());
  }

  inline adj_list_t GetOutgoingOuterVertexAdjList(const vertex_t& v) {
    auto oe_pos = inner_oe_pos_[v.GetValue()];
    if (oe_pos == -1) {
      return adj_list_t();
    }
    return adj_list_t(id_mask_, ivnum_,
                      inner_edge_space_.OuterNbr(oe_pos).begin(),
                      inner_edge_space_.OuterNbr(oe_pos).end());
  }

  inline const_adj_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) const {
    auto oe_pos = inner_oe_pos_[v.GetValue()];
    if (oe_pos == -1) {
      return const_adj_list_t();
    }
    return const_adj_list_t(id_mask_, ivnum_,
                            inner_edge_space_.OuterNbr(oe_pos).begin(),
                            inner_edge_space_.OuterNbr(oe_pos).end());
  }

  inline const std::vector<vertex_t>& MirrorVertices(fid_t fid) const {
    return mirrors_of_frag_[fid];
  }

  void SetupMirrorInfo(fid_t fid, const std::vector<vid_t>& gid_list) {
    auto& vertex_vec = mirrors_of_frag_[fid];
    vertex_vec.resize(gid_list.size());
    for (size_t i = 0; i < gid_list.size(); ++i) {
      CHECK_EQ(gid_list[i] >> fid_offset_, fid_);
      vertex_vec[i].SetValue(gid_list[i] & id_mask_);
    }
  }

  inline bool HasNode(const oid_t& node) const {
    vid_t gid;
    return vm_ptr_->GetGid(fid_, node, gid) && isAlive(gid & id_mask_);
  }

  inline bool HasEdge(const oid_t& u, const oid_t& v) {
    vid_t uid, vid;
    if (Oid2Gid(u, uid) && Oid2Gid(v, vid)) {
      vid_t ulid, vlid;
      if ((uid >> fid_offset_) == fid_ && Gid2Lid(uid, ulid) &&
          Gid2Lid(vid, vlid) && isAlive(ulid)) {
        auto pos = inner_oe_pos_[ulid];
        if (pos != -1) {
          auto& oe = inner_edge_space_[pos];
          if (oe.find(vlid) != oe.end()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  inline bool GetVertexData(const oid_t& oid, std::string& ret) const {
    vertex_t v;
    if (GetInnerVertex(oid, v) && IsAliveInnerVertex(v)) {
      ret = folly::toJson(GetData(v));
      return true;
    }
    return false;
  }

  inline bool GetEdgeData(const oid_t& u, const oid_t& v, std::string& ret) {
    vid_t uid, vid;
    if (Oid2Gid(u, uid) && Oid2Gid(v, vid)) {
      vid_t ulid, vlid;
      if ((uid >> fid_offset_) == fid_ && Gid2Lid(uid, ulid) &&
          Gid2Lid(vid, vlid) && isAlive(ulid)) {
        auto pos = inner_oe_pos_[ulid];
        if (pos != -1) {
          auto& oe = inner_edge_space_[pos];
          if (oe.find(vlid) != oe.end()) {
            ret = folly::toJson(oe[vlid].data());
            return true;
          }
        }
      }
    }
    return false;
  }

  void ModifyEdges(const std::vector<std::string>& edges_to_modify,
                   const rpc::ModifyType modify_type) {
    std::vector<internal_vertex_t> vertices;
    std::vector<edge_t> edges;

    edges.reserve(edges_to_modify.size());
    InvalidCache();
    {
      edata_t e_data = folly::dynamic::object;
      vdata_t fake_data = folly::dynamic::object;
      oid_t src, dst;
      vid_t src_gid, dst_gid;
      fid_t src_fid, dst_fid;
      partitioner_t partitioner;
      partitioner.Init(fnum_);
      auto line_parser_ptr = std::make_unique<DynamicLineParser>();
      for (auto& line : edges_to_modify) {
        if (line.empty() || line[0] == '#') {
          continue;
        }
        try {
          line_parser_ptr->LineParserForEFile(line, src, dst, e_data);
        } catch (std::exception& e) {
          LOG(ERROR) << e.what() << " line: " << line;
          continue;
        }
        src_fid = partitioner.GetPartitionId(src);
        dst_fid = partitioner.GetPartitionId(dst);
        if (modify_type == rpc::ADD_EDGES) {
          vm_ptr_->AddVertex(src_fid, src, src_gid);
          vm_ptr_->AddVertex(dst_fid, dst, dst_gid);
          if (src_fid == fid_) {
            vertices.emplace_back(src_gid, fake_data);
          }
          if (dst_fid == fid_) {
            vertices.emplace_back(dst_gid, fake_data);
          }
        } else {
          if (!vm_ptr_->GetGid(src_fid, src, src_gid) ||
              !vm_ptr_->GetGid(dst_fid, dst, dst_gid)) {
            continue;
          }
        }
        if (src_fid == fid_ || dst_fid == fid_) {
          edges.emplace_back(src_gid, dst_gid, e_data);
          if (!directed_ && src_gid != dst_gid) {
            edges.emplace_back(dst_gid, src_gid, e_data);
          }
        }
      }
    }

    switch (modify_type) {
    case rpc::ADD_EDGES:
      Insert(vertices, edges);
      break;
    case rpc::UPDATE_EDGES:
      Update(vertices, edges);
      break;
    case rpc::DEL_EDGES:
      Delete(vertices, edges);
      break;
    default:
      CHECK(false);
    }
  }

  void ModifyVertices(const std::vector<std::string>& vertices_to_modify,
                      const rpc::ModifyType& modify_type) {
    std::vector<internal_vertex_t> vertices;
    std::vector<edge_t> empty_edges;

    vertices.reserve(vertices_to_modify.size());
    InvalidCache();
    {
      partitioner_t partitioner;
      partitioner.Init(fnum_);
      oid_t oid;
      vid_t gid;
      vdata_t v_data = folly::dynamic::object;
      fid_t v_fid;
      auto line_parser_ptr = std::make_unique<DynamicLineParser>();
      for (auto& line : vertices_to_modify) {
        if (line.empty() || line[0] == '#') {
          continue;
        }
        try {
          line_parser_ptr->LineParserForVFile(line, oid, v_data);
        } catch (std::exception& e) {
          LOG(ERROR) << e.what();
          continue;
        }
        v_fid = partitioner.GetPartitionId(oid);
        if (modify_type == rpc::ADD_NODES) {
          vm_ptr_->AddVertex(v_fid, oid, gid);
        } else {
          // UPDATE or DEL, if not exist the node, continue.
          if (!vm_ptr_->GetGid(v_fid, oid, gid)) {
            continue;
          }
        }
        if (v_fid == fid_ || (modify_type == rpc::DEL_NODES &&
                              ovg2i_.find(gid) != ovg2i_.end())) {
          vertices.emplace_back(gid, v_data);
        }
      }
    }
    if (vertices.empty())
      return;

    switch (modify_type) {
    case rpc::ADD_NODES:
      Insert(vertices, empty_edges);
      break;
    case rpc::UPDATE_NODES:
      Update(vertices, empty_edges);
      break;
    case rpc::DEL_NODES:
      Delete(vertices, empty_edges);
      break;
    default:
      CHECK(false);
    }
  }

  /**
   * Collect property keys and types for existed vertices
   *
   * @return a std::map, key is the property key, value is the type of property
   * value
   */
  auto CollectPropertyKeysOnVertices()
      -> bl::result<std::map<std::string, folly::dynamic::Type>> {
    auto inner_vertices = grape::VertexRange<vid_t>(0, ivnum_);
    std::map<std::string, folly::dynamic::Type> prop_keys;

    for (auto v : inner_vertices) {
      if (IsAliveInnerVertex(v)) {
        auto data = vdata_[v.GetValue()];

        CHECK(data.isObject());
        for (auto& k : data.keys()) {
          auto s_k = k.asString();

          if (prop_keys.find(s_k) == prop_keys.end()) {
            prop_keys[s_k] = data[k].type();
          } else {
            auto seen_type = prop_keys[s_k];
            auto curr_type = data[k].type();

            if (seen_type != curr_type) {
              std::stringstream ss;
              ss << "OID: " << GetId(v) << " has key " << s_k << " with type "
                 << getTypeName(curr_type)
                 << " but previous type is: " << getTypeName(seen_type);
              RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError, ss.str());
            }
          }
        }
      }
    }

    return prop_keys;
  }

  auto CollectPropertyKeysOnEdges()
      -> bl::result<std::map<std::string, folly::dynamic::Type>> {
    auto inner_vertices = grape::VertexRange<vid_t>(0, ivnum_);
    std::map<std::string, folly::dynamic::Type> prop_keys;

    auto extract_keys = [this, &prop_keys](
                            const vertex_t& u,
                            size_t edge_pos) -> bl::result<void> {
      auto& adj_list = inner_edge_space_[edge_pos];

      for (auto& e : adj_list) {
        auto& nbr = e.second;
        auto data = nbr.data();

        CHECK(data.isObject());
        for (auto& k : data.keys()) {
          auto s_k = k.asString();

          if (prop_keys.find(s_k) == prop_keys.end()) {
            prop_keys[s_k] = data[k].type();
          } else {
            auto seen_type = prop_keys[s_k];
            auto curr_type = data[k].type();

            if (seen_type != curr_type) {
              std::stringstream ss;
              ss << "Edge (OID): " << GetId(u) << " " << GetId(nbr.neighbor())
                 << " has key " << s_k << " with type "
                 << getTypeName(curr_type)
                 << " but previous type is: " << getTypeName(seen_type);
              RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError, ss.str());
            }
          }
        }
      }
      return {};
    };

    for (const auto& v : inner_vertices) {
      if (IsAliveInnerVertex(v)) {
        if (load_strategy_ == grape::LoadStrategy::kOnlyIn ||
            load_strategy_ == grape::LoadStrategy::kBothOutIn) {
          auto ie_pos = inner_ie_pos_[v.GetValue()];

          if (ie_pos != -1) {
            BOOST_LEAF_CHECK(extract_keys(v, ie_pos));
          }
        }

        if (load_strategy_ == grape::LoadStrategy::kOnlyOut ||
            load_strategy_ == grape::LoadStrategy::kBothOutIn) {
          auto oe_pos = inner_oe_pos_[v.GetValue()];

          if (oe_pos != -1) {
            BOOST_LEAF_CHECK(extract_keys(v, oe_pos));
          }
        }
      }
    }

    return prop_keys;
  }

  std::vector<std::vector<oid_t>> GetAllOids(
      const grape::CommSpec& comm_spec) const {
    auto dead_gids = allGatherDeadVertices(comm_spec);
    std::vector<std::vector<oid_t>> all_oids(fnum_);

    for (fid_t fid = 0; fid < fnum_; fid++) {
      for (vid_t lid = 0; lid < vm_ptr_->GetInnerVertexSize(fid); lid++) {
        auto gid = vm_ptr_->Lid2Gid(fid, lid);
        if (dead_gids.find(gid) == dead_gids.end()) {
          oid_t oid;
          CHECK(vm_ptr_->GetOid(fid, lid, oid));
          all_oids[fid].push_back(oid);
        }
      }
    }
    return all_oids;
  }

  bl::result<folly::dynamic::Type> GetOidType(
      const grape::CommSpec& comm_spec) const {
    auto oid_type = folly::dynamic::Type::NULLT;
    auto all_oids = GetAllOids(comm_spec);

    for (const auto& oids : all_oids) {
      for (const auto& oid : oids) {
        if (oid_type == folly::dynamic::Type::NULLT) {
          oid_type = oid.type();
        } else if (oid.type() != oid_type) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError,
                          "Previous oid type is " +
                              std::string(getTypeName(oid_type)) +
                              ", but the current is " +
                              std::string(getTypeName(oid.type())));
        }
      }
    }
    if (oid_type != folly::dynamic::Type::INT64 &&
        oid_type != folly::dynamic::Type::STRING &&
        oid_type != folly::dynamic::Type::NULLT) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kDataTypeError,
          "Unsupported oid type: " + std::string(getTypeName(oid_type)));
    }
    return oid_type;
  }

  std::shared_ptr<vertex_map_t> GetVertexMap() { return vm_ptr_; }

  inline bool IsAliveVertex(const vertex_t& v) const {
    return IsInnerVertex(v) ? IsAliveInnerVertex(v) : IsAliveOuterVertex(v);
  }

  inline bool IsAliveInnerVertex(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return inner_vertex_alive_[v.GetValue()];
  }

  inline bool IsAliveOuterVertex(const vertex_t& v) const {
    assert(IsOuterVertex(v));
    return outer_vertex_alive_[v.GetValue() - ivnum_];
  }

 private:
  inline bool isAlive(vid_t lid) const {
    if (lid < ivnum_) {
      return inner_vertex_alive_[lid];
    } else if (lid < tvnum_) {
      return outer_vertex_alive_[lid - ivnum_];
    }
    return false;
  }

  void InvalidCache() {
    alive_inner_vertices_.first = false;
    alive_outer_vertices_.first = false;
    alive_vertices_.first = false;
  }

  void Insert(std::vector<internal_vertex_t>& vertices,
              std::vector<edge_t>& edges) {
    std::vector<vid_t> outer_vertices =
        getOuterVerticesAndInvalidEdges(edges, load_strategy_);
    std::vector<vid_t> new_outer_vertices;
    vid_t new_ivnum_ = vm_ptr_->GetInnerVertexSize(fid_);
    vid_t new_ovnum = ovnum_;

    grape::DistinctSort(outer_vertices);
    for (auto gid : outer_vertices) {
      auto iter = ovg2i_.find(gid);
      if (iter == ovg2i_.end()) {
        new_outer_vertices.push_back(gid);
        ovg2i_.emplace(gid, new_ovnum);
        new_ovnum++;
      }
    }

    ovgid_.resize(new_ovnum);
    memcpy(&ovgid_[ovnum_], &new_outer_vertices[0],
           sizeof(vid_t) * new_outer_vertices.size());

    alive_ivnum_ += new_ivnum_ - ivnum_;
    alive_ovnum_ += new_ovnum - ovnum_;
    ivnum_ = new_ivnum_;
    ovnum_ = new_ovnum;
    tvnum_ = ivnum_ + ovnum_;

    inner_ie_pos_.resize(ivnum_, -1);
    inner_oe_pos_.resize(ivnum_, -1);
    inner_vertex_alive_.resize(ivnum_, true);
    outer_vertex_alive_.resize(ovnum_, true);

    AddEdges(edges, load_strategy_);

    initOuterVerticesOfFragment();

    vdata_.resize(ivnum_, folly::dynamic::object);
    if (sizeof(internal_vertex_t) > sizeof(vid_t)) {
      for (auto& v : vertices) {
        vid_t gid = v.vid();
        if (gid >> fid_offset_ == fid_) {
          vdata_[(gid & id_mask_)].update(v.vdata());
        }
      }
    }

    initMessageDestination(message_strategy_);
  }

  void Update(std::vector<internal_vertex_t>& vertices,
              std::vector<edge_t>& edges) {
    for (auto& v : vertices) {
      // the vertex exist
      vdata_[(v.vid() & id_mask_)] = v.vdata();
    }

    switch (load_strategy_) {
    case grape::LoadStrategy::kOnlyOut: {
      for (auto& e : edges) {
        vid_t dst;
        if (!is_iv_gid(e.src()))
          continue;
        if (is_iv_gid(e.dst())) {
          dst = iv_gid_to_lid(e.dst());
        } else {
          dst = ov_gid_to_lid(e.dst());
        }
        e.SetEndpoint(iv_gid_to_lid(e.src()), dst);
        int pos = inner_oe_pos_[e.src()];
        inner_edge_space_.set_data(pos, e.dst(), e.edata());
      }
      break;
    }
    case grape::LoadStrategy::kBothOutIn: {
      for (auto& e : edges) {
        if (is_iv_gid(e.src()) && is_iv_gid(e.dst())) {
          e.SetEndpoint(iv_gid_to_lid(e.src()), iv_gid_to_lid(e.dst()));
          int pos = inner_oe_pos_[e.src()];
          inner_edge_space_.set_data(pos, e.dst(), e.edata());
          pos = inner_ie_pos_[e.dst()];
          inner_edge_space_.set_data(pos, e.src(), e.edata());
        } else if (is_iv_gid(e.src())) {
          vid_t dst;
          if (is_iv_gid(e.dst())) {
            dst = iv_gid_to_lid(e.dst());
          } else {
            dst = ov_gid_to_lid(e.dst());
          }
          e.SetEndpoint(iv_gid_to_lid(e.src()), dst);
          int pos = inner_oe_pos_[e.src()];
          inner_edge_space_.set_data(pos, e.dst(), e.edata());
        } else if (is_iv_gid(e.dst())) {
          vid_t src;
          if (is_iv_gid(e.src())) {
            src = iv_gid_to_lid(e.src());
          } else {
            src = ov_gid_to_lid(e.src());
          }
          e.SetEndpoint(src, iv_gid_to_lid(e.dst()));
          int pos = inner_ie_pos_[e.dst()];
          inner_edge_space_.set_data(pos, e.src(), e.edata());
        } else {
          CHECK(false);
        }
      }
      break;
    }
    default:
      assert(false);
    }
  }

  void Delete(std::vector<grape::internal::Vertex<vid_t, vdata_t>>& vertices,
              std::vector<grape::Edge<vid_t, edata_t>>& edges) {
    std::unordered_set<vid_t> to_remove_lid_set;
    // remove vertices and attached edges
    for (auto& v : vertices) {
      if (is_iv_gid(v.vid())) {
        auto lid = iv_gid_to_lid(v.vid());
        assert(lid < ivnum_);
        if (inner_vertex_alive_[lid]) {
          if (load_strategy_ == grape::LoadStrategy::kOnlyIn ||
              load_strategy_ == grape::LoadStrategy::kBothOutIn) {
            auto ie_pos = inner_ie_pos_[lid];

            if (ie_pos != -1) {
              ienum_ -= inner_edge_space_[ie_pos].size();
              inner_edge_space_.remove_edges(ie_pos);
              inner_ie_pos_[lid] = -1;
            }
          }

          if (load_strategy_ == grape::LoadStrategy::kOnlyOut ||
              load_strategy_ == grape::LoadStrategy::kBothOutIn) {
            auto oe_pos = inner_oe_pos_[lid];

            if (oe_pos != -1) {
              oenum_ -= inner_edge_space_[oe_pos].size();
              inner_edge_space_.remove_edges(oe_pos);
              inner_oe_pos_[lid] = -1;
            }
          }

          inner_vertex_alive_[lid] = false;
          to_remove_lid_set.insert(lid);
          alive_ivnum_--;
        }
      } else {
        auto iter = ovg2i_.find(v.vid());
        if (iter != ovg2i_.end() && outer_vertex_alive_[iter->second]) {
          outer_vertex_alive_[iter->second] = false;
          to_remove_lid_set.insert(ov_gid_to_lid(v.vid()));
          alive_ovnum_--;
        }
      }
    }

    auto inner_vertices = grape::VertexRange<vid_t>(0, ivnum_);
    for (auto v : inner_vertices) {
      if (IsAliveInnerVertex(v)) {
        auto lid = v.GetValue();
        if (load_strategy_ == grape::LoadStrategy::kOnlyIn ||
            load_strategy_ == grape::LoadStrategy::kBothOutIn) {
          auto ie_pos = inner_ie_pos_[lid];

          if (ie_pos != -1) {
            for (auto to_remove_lid : to_remove_lid_set) {
              ienum_ -= inner_edge_space_.remove_edge(ie_pos, to_remove_lid);
            }
          }
        }

        if (load_strategy_ == grape::LoadStrategy::kOnlyOut ||
            load_strategy_ == grape::LoadStrategy::kBothOutIn) {
          auto oe_pos = inner_oe_pos_[lid];

          if (oe_pos != -1) {
            for (auto to_remove_lid : to_remove_lid_set) {
              oenum_ -= inner_edge_space_.remove_edge(oe_pos, to_remove_lid);
            }
          }
        }
      }
    }

    switch (load_strategy_) {
    case grape::LoadStrategy::kOnlyIn: {
      for (auto& e : edges) {
        if (is_iv_gid(e.dst())) {
          auto dst_lid = iv_gid_to_lid(e.dst());
          auto src_lid = gid_to_lid(e.src());
          auto ie_pos = inner_ie_pos_[dst_lid];

          if (ie_pos != -1) {
            ienum_ -= inner_edge_space_.remove_edge(ie_pos, src_lid);
          }
        }
      }
      break;
    }
    case grape::LoadStrategy::kOnlyOut: {
      for (auto& e : edges) {
        if (is_iv_gid(e.src())) {
          auto src_lid = iv_gid_to_lid(e.src());
          auto dst_lid = gid_to_lid(e.dst());
          auto oe_pos = inner_oe_pos_[src_lid];

          if (oe_pos != -1) {
            oenum_ -= inner_edge_space_.remove_edge(oe_pos, dst_lid);
          }
        }
      }
      break;
    }
    case grape::LoadStrategy::kBothOutIn: {
      for (auto& e : edges) {
        if (is_iv_gid(e.src()) && is_iv_gid(e.dst())) {
          auto src_lid = iv_gid_to_lid(e.src());
          auto dst_lid = iv_gid_to_lid(e.dst());
          auto ie_pos = inner_ie_pos_[dst_lid];
          auto oe_pos = inner_oe_pos_[src_lid];

          if (ie_pos != -1) {
            ienum_ -= inner_edge_space_.remove_edge(ie_pos, src_lid);
          }

          if (oe_pos != -1) {
            oenum_ -= inner_edge_space_.remove_edge(oe_pos, dst_lid);
          }
        } else if (is_iv_gid(e.src())) {
          auto src_lid = iv_gid_to_lid(e.src());
          auto dst_lid = gid_to_lid(e.dst());
          auto oe_pos = inner_oe_pos_[src_lid];

          if (oe_pos != -1) {
            oenum_ -= inner_edge_space_.remove_edge(oe_pos, dst_lid);
          }
        } else if (is_iv_gid(e.dst())) {
          auto src_lid = gid_to_lid(e.src());
          auto dst_lid = iv_gid_to_lid(e.dst());
          auto ie_pos = inner_ie_pos_[dst_lid];

          if (ie_pos != -1) {
            ienum_ -= inner_edge_space_.remove_edge(ie_pos, src_lid);
          }
        } else {
          CHECK(false);
        }
      }
      break;
    }
    default:
      CHECK(false);
    }

    initMessageDestination(message_strategy_);
  }

  std::unordered_set<vid_t> allGatherDeadVertices(
      const grape::CommSpec& comm_spec) const {
    auto inner_vertices = grape::VertexRange<vid_t>(0, ivnum_);
    std::vector<vid_t> local_dead_gids;
    std::vector<std::vector<vid_t>> all_dead_gids;

    for (auto v : inner_vertices) {
      if (!IsAliveInnerVertex(v)) {
        local_dead_gids.push_back(GetInnerVertexGid(v));
      }
    }

    vineyard::GlobalAllGatherv(local_dead_gids, all_dead_gids, comm_spec);
    std::size_t total_size = 0;
    for (const auto& gids : all_dead_gids) {
      total_size += gids.size();
    }
    std::unordered_set<vid_t> result;
    result.reserve(total_size);
    for (const auto& gids : all_dead_gids)
      result.insert(gids.begin(), gids.end());
    return result;
  }

  void initMessageDestination(const grape::MessageStrategy& msg_strategy) {
    if (msg_strategy ==
        grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
      initDestFidList(false, true, odst_, odoffset_);
    } else if (msg_strategy ==
               grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex) {
      initDestFidList(true, false, idst_, idoffset_);
    } else if (msg_strategy ==
               grape::MessageStrategy::kAlongEdgeToOuterVertex) {
      initDestFidList(true, true, iodst_, iodoffset_);
    }
  }

  void initOuterVerticesOfFragment() {
    outer_vertices_of_frag_.clear();
    outer_vertices_of_frag_.resize(fnum());
    for (auto e : ovg2i_) {
      auto gid = e.first;
      auto idx = e.second;
      auto fid = gid >> fid_offset_;

      CHECK_NE(fid, fid_);
      // mapped gid -> ivnum_ + idx of outer vertex;
      outer_vertices_of_frag_[fid].emplace_back(ivnum_ + idx);
    }
  }

  std::vector<vid_t> getOuterVerticesAndInvalidEdges(
      std::vector<edge_t>& edges, grape::LoadStrategy strategy) {
    std::vector<vid_t> outer_vertices;

    switch (strategy) {
    case grape::LoadStrategy::kOnlyIn: {
      for (auto& e : edges) {
        if (is_iv_gid(e.dst())) {
          if (!is_iv_gid(e.src())) {
            outer_vertices.push_back(e.src());
          }
        } else {
          e.SetEndpoint(invalid_vid, invalid_vid);
        }
      }
      break;
    }
    case grape::LoadStrategy::kOnlyOut: {
      for (auto& e : edges) {
        if (is_iv_gid(e.src())) {
          if (!is_iv_gid(e.dst())) {
            outer_vertices.push_back(e.dst());
          }
        } else {
          e.SetEndpoint(invalid_vid, invalid_vid);
        }
      }
      break;
    }
    case grape::LoadStrategy::kBothOutIn: {
      for (auto& e : edges) {
        if (is_iv_gid(e.src())) {
          if (!is_iv_gid(e.dst())) {
            outer_vertices.push_back(e.dst());
          }
        } else if (is_iv_gid(e.dst())) {
          outer_vertices.push_back(e.src());
        } else {
          e.SetEndpoint(invalid_vid, invalid_vid);
        }
      }
      break;
    }
    default:
      assert(false);
    }
    return outer_vertices;
  }

  void AddEdges(std::vector<edge_t>& edges, grape::LoadStrategy strategy) {
    switch (strategy) {
    case grape::LoadStrategy::kOnlyIn: {
      for (auto& e : edges) {
        if (e.src() != invalid_vid && is_iv_gid(e.dst())) {
          vid_t src;
          if (is_iv_gid(e.src())) {
            src = iv_gid_to_lid(e.src());
          } else {
            src = ov_gid_to_lid(e.src());
          }
          e.SetEndpoint(src, iv_gid_to_lid(e.dst()));

          if (addIncomingEdge(e.src(), e.dst(), e.edata())) {
            ++ienum_;
          }
        }
      }
      break;
    }
    case grape::LoadStrategy::kOnlyOut: {
      for (auto& e : edges) {
        if (e.src() != invalid_vid && is_iv_gid(e.src())) {
          vid_t dst;
          if (is_iv_gid(e.dst())) {
            dst = iv_gid_to_lid(e.dst());
          } else {
            dst = ov_gid_to_lid(e.dst());
          }
          e.SetEndpoint(iv_gid_to_lid(e.src()), dst);

          if (addOutgoingEdge(e.src(), e.dst(), e.edata())) {
            ++oenum_;
          }
        }
      }
      break;
    }
    case grape::LoadStrategy::kBothOutIn: {
      for (auto& e : edges) {
        if (e.src() != invalid_vid) {
          if (is_iv_gid(e.src()) && is_iv_gid(e.dst())) {
            e.SetEndpoint(iv_gid_to_lid(e.src()), iv_gid_to_lid(e.dst()));
            if (addOutgoingEdge(e.src(), e.dst(), e.edata())) {
              ++oenum_;
            }

            if (addIncomingEdge(e.src(), e.dst(), e.edata())) {
              ++ienum_;
            }
          } else if (is_iv_gid(e.src())) {
            vid_t dst;
            if (is_iv_gid(e.dst())) {
              dst = iv_gid_to_lid(e.dst());
            } else {
              dst = ov_gid_to_lid(e.dst());
            }
            e.SetEndpoint(iv_gid_to_lid(e.src()), dst);

            if (addOutgoingEdge(e.src(), e.dst(), e.edata())) {
              ++oenum_;
            }
          } else if (is_iv_gid(e.dst())) {
            vid_t src;
            if (is_iv_gid(e.src())) {
              src = iv_gid_to_lid(e.src());
            } else {
              src = ov_gid_to_lid(e.src());
            }
            e.SetEndpoint(src, iv_gid_to_lid(e.dst()));

            if (addIncomingEdge(e.src(), e.dst(), e.edata())) {
              ++ienum_;
            }
          } else {
            CHECK(false);
          }
        }
      }
      break;
    }
    default:
      assert(false);
    }
  }

  void initDestFidList(
      bool in_edge, bool out_edge,
      Array<fid_t, grape::Allocator<fid_t>>& fid_list,
      Array<fid_t*, grape::Allocator<fid_t*>>& fid_list_offset) {
    std::set<fid_t> dstset;
    std::vector<fid_t> tmp_fids;
    std::vector<int> id_num(ivnum_, 0);

    for (vid_t i = 0; i < ivnum_; ++i) {
      dstset.clear();
      if (in_edge) {
        auto pos = inner_ie_pos_[i];

        if (inner_vertex_alive_[i] && pos != -1) {
          for (auto& e : inner_edge_space_[pos]) {
            vid_t src = e.first;
            if (src >= ivnum_) {
              fid_t f = ovgid_[id_mask_ - src] >> fid_offset_;
              dstset.insert(f);
            }
          }
        }
      }

      if (out_edge) {
        auto pos = inner_oe_pos_[i];

        if (inner_vertex_alive_[i] && pos != -1) {
          for (auto& e : inner_edge_space_[pos]) {
            vid_t dst = e.first;
            if (dst >= ivnum_) {
              fid_t f = ovgid_[id_mask_ - dst] >> fid_offset_;
              dstset.insert(f);
            }
          }
        }
      }
      id_num[i] = dstset.size();
      for (auto fid : dstset) {
        tmp_fids.push_back(fid);
      }
    }

    fid_list.resize(tmp_fids.size());
    fid_list_offset.resize(ivnum_ + 1);

    memcpy(&fid_list[0], &tmp_fids[0], sizeof(fid_t) * fid_list.size());
    fid_list_offset[0] = fid_list.data();
    for (vid_t i = 0; i < ivnum_; ++i) {
      fid_list_offset[i + 1] = fid_list_offset[i] + id_num[i];
    }
  }

  template <typename T>
  void calcFidBitWidth(fid_t fnum, T& id_mask, fid_t& fid_offset) {
    fid_t maxfid = fnum - 1;
    if (maxfid == 0) {
      fid_offset = (sizeof(T) * 8) - 1;
    } else {
      int i = 0;
      while (maxfid) {
        maxfid >>= 1u;
        ++i;
      }
      fid_offset = (sizeof(T) * 8) - i;
    }
    id_mask = ((T) 1 << fid_offset) - (T) 1;
  }

  bool addIncomingEdge(vid_t src_lid, vid_t dst_lid, const edata_t& edata) {
    auto pos = inner_ie_pos_[dst_lid];

    inner_vertex_alive_[dst_lid] = true;
    if (src_lid < ivnum_) {
      inner_vertex_alive_[src_lid] = true;
    } else if (id_mask_ - src_lid < ovnum_) {
      outer_vertex_alive_[id_mask_ - src_lid] = true;
    } else {
      assert(false);
    }

    if (pos == -1) {
      inner_ie_pos_[dst_lid] = inner_edge_space_.emplace(src_lid, edata);
      return true;
    } else {
      bool created = false;
      inner_ie_pos_[dst_lid] =
          inner_edge_space_.emplace(pos, src_lid, edata, created);
      return created;
    }
  }

  bool addOutgoingEdge(vid_t src_lid, vid_t dst_lid, const edata_t& edata) {
    inner_vertex_alive_[src_lid] = true;
    if (dst_lid < ivnum_) {
      inner_vertex_alive_[dst_lid] = true;
    } else if (id_mask_ - dst_lid < ovnum_) {
      outer_vertex_alive_[id_mask_ - dst_lid] = true;
    } else {
      assert(false);
    }

    int pos = inner_oe_pos_[src_lid];
    if (pos == -1) {
      inner_oe_pos_[src_lid] = inner_edge_space_.emplace(dst_lid, edata);
      return true;
    } else {
      bool created = false;
      inner_oe_pos_[src_lid] =
          inner_edge_space_.emplace(pos, dst_lid, edata, created);
      return created;
    }
  }

  inline const char* getTypeName(folly::dynamic::Type type) const {
    switch (type) {
    case folly::dynamic::Type::INT64:
      return "int64";
    case folly::dynamic::Type::STRING:
      return "string";
    case folly::dynamic::Type::DOUBLE:
      return "double";
    case folly::dynamic::Type::BOOL:
      return "bool";
    case folly::dynamic::Type::NULLT:
      return "null";
    case folly::dynamic::Type::ARRAY:
      return "array";
    case folly::dynamic::Type::OBJECT:
      return "object";
    default:
      return "unknown";
    }
  }

  std::shared_ptr<vertex_map_t> vm_ptr_;
  vid_t ivnum_{}, ovnum_{}, tvnum_{}, id_mask_{};
  vid_t alive_ivnum_{}, alive_ovnum_{};
  size_t ienum_{}, oenum_{};
  fid_t fid_offset_{};
  fid_t fid_{}, fnum_{};
  bool directed_{};
  grape::LoadStrategy load_strategy_{};

  // vertices cache
  std::pair<bool, std::vector<vertex_t>> alive_inner_vertices_;
  std::pair<bool, std::vector<vertex_t>> alive_outer_vertices_;
  std::pair<bool, std::vector<vertex_t>> alive_vertices_;

  ska::flat_hash_map<vid_t, vid_t>
      ovg2i_;  // <outer vertex gid, idx of outer vertex>
  Array<vid_t, grape::Allocator<vid_t>>
      ovgid_;  // idx is index of outer vertex, the content is gid
  Array<vdata_t, grape::Allocator<vdata_t>> vdata_;

  // first idx is fid, second idx is index of outer vertex, the content is
  // mapped vid ([ivnum, ovnum)) of outer vertex nested array also contains dead
  // vertices
  std::vector<std::vector<vertex_t>> outer_vertices_of_frag_;

  std::vector<std::vector<vertex_t>> mirrors_of_frag_;

  Array<fid_t, grape::Allocator<fid_t>> idst_, odst_, iodst_;
  Array<fid_t*, grape::Allocator<fid_t*>> idoffset_, odoffset_, iodoffset_;
  Array<bool> inner_vertex_alive_;
  Array<bool> outer_vertex_alive_;

  grape::MessageStrategy message_strategy_{};
  // ie_pos_[lid]/oe_pos_[lid] stores the inner index representation of
  // NbrMapSpace DO NOT use unsigned type, because negative numbers have
  // internal meaning
  Array<int32_t, grape::Allocator<int32_t>> inner_ie_pos_;
  Array<int32_t, grape::Allocator<int32_t>> inner_oe_pos_;
  dynamic_fragment_impl::NbrMapSpace<edata_t> inner_edge_space_;

  const vid_t invalid_vid = std::numeric_limits<vid_t>::max();

  inline bool is_iv_gid(vid_t id) const { return (id >> fid_offset_) == fid_; }

  inline vid_t gid_to_lid(vid_t gid) const {
    return ((gid >> fid_offset_) == fid_) ? (gid & id_mask_)
                                          : (id_mask_ - ovg2i_.at(gid));
  }

  inline vid_t iv_gid_to_lid(vid_t gid) const { return gid & id_mask_; }
  inline vid_t ov_gid_to_lid(vid_t gid) const {
    return id_mask_ - ovg2i_.at(gid);
  }

  template <typename _FRAG_T, typename _PARTITIONER_T, typename _IOADAPTOR_T,
            typename _Enable>
  friend class BasicFragmentLoader;

  template <typename _VDATA_T, typename _EDATA_T>
  friend class DynamicProjectedFragment;
};
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_H_
