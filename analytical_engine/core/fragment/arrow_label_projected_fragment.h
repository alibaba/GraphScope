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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_LABEL_PROJECTED_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_LABEL_PROJECTED_FRAGMENT_H_

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "vineyard/graph/fragment/arrow_fragment.h"

namespace gs {

namespace arrow_label_projected_fragment_impl {
template <typename T>
class UnionVertexRange {
 public:
  UnionVertexRange() {}
  explicit UnionVertexRange(
      const std::vector<grape::VertexRange<T>>& vertex_ranges)
      : vertex_ranges_(std::move(vertex_ranges)) {}

  class iterator {
    using pointer_type = grape::Vertex<T>*;
    using reference_type = grape::Vertex<T>&;

   public:
    iterator() = default;
    explicit iterator(const std::vector<grape::VertexRange<T>>& vertex_ranges,
                      bool is_end = false)
        : vertex_ranges_(vertex_ranges) {
      if (is_end) {
        curr_vertex_ = vertex_ranges_.get().back().end();
        curr_range_index_ = vertex_ranges_.get().size();
      } else {
        curr_vertex_ = vertex_ranges_.get().front().begin();
        curr_range_index_ = 0;
      }
    }

    reference_type operator*() noexcept { return curr_vertex_; }

    pointer_type operator->() noexcept { return &curr_vertex_; }

    iterator& operator++() {
      if (++curr_vertex_ == vertex_ranges_.get()[curr_range_index_].end()) {
        ++curr_range_index_;
        if (curr_range_index_ < vertex_ranges_.get().size()) {
          curr_vertex_ = vertex_ranges_.get()[curr_range_index_].begin();
        }
      }
      return *this;
    }

    iterator& operator++(int) {
      if (++curr_vertex_ >= vertex_ranges_.get()[curr_range_index_].end()) {
        ++curr_range_index_;
        if (curr_range_index_ < vertex_ranges_.get().size()) {
          curr_vertex_ = vertex_ranges_.get()[curr_range_index_].begin();
        }
      }
      return *this;
    }

    bool operator==(const iterator& rhs) noexcept {
      return curr_vertex_ == rhs.curr_vertex_;
    }

    bool operator!=(const iterator& rhs) noexcept {
      return curr_vertex_ != rhs.curr_vertex_;
    }

   private:
    grape::Vertex<T> curr_vertex_;
    int curr_range_index_;
    std::reference_wrapper<const std::vector<grape::VertexRange<T>>>
        vertex_ranges_;
  };

  iterator begin() const { return iterator(vertex_ranges_); }

  iterator end() const { return iterator(vertex_ranges_, true); }

  const std::vector<grape::VertexRange<T>>& GetVertexRanges() const {
    return vertex_ranges_;
  }

 private:
  std::vector<grape::VertexRange<T>> vertex_ranges_;
};

template <typename VID_T, typename EID_T>
class UnionAdjList {
 public:
  using nbr_t = vineyard::property_graph_utils::Nbr<VID_T, EID_T>;
  using adj_list_t = vineyard::property_graph_utils::AdjList<VID_T, EID_T>;

  UnionAdjList() {}
  explicit UnionAdjList(const std::vector<adj_list_t>& adj_lists)
      : adj_lists_(adj_lists) {}

  class iterator {
    using pointer_type = nbr_t*;
    using reference_type = nbr_t&;

   public:
    iterator() = default;
    explicit iterator(const std::vector<adj_list_t>& adj_lists,
                      bool is_end = false)
        : adj_lists_(adj_lists) {
      if (is_end) {
        if (!adj_lists.empty()) {
          curr_nbr_ = adj_lists_.get().back().end();
          curr_list_index_ = adj_lists_.get().size();
        }
      } else {
        if (!adj_lists.empty()) {
          curr_nbr_ = adj_lists_.get().front().begin();
          curr_list_index_ = 0;
        }
      }
    }

    reference_type operator*() noexcept { return curr_nbr_; }

    pointer_type operator->() noexcept { return &curr_nbr_; }

    iterator& operator++() {
      if (++curr_nbr_ == adj_lists_.get()[curr_list_index_].end()) {
        ++curr_list_index_;
        if (curr_list_index_ < adj_lists_.get().size()) {
          curr_nbr_ = adj_lists_.get()[curr_list_index_].begin();
        }
      }
      return *this;
    }

    iterator& operator++(int) {
      if (++curr_list_index_ == adj_lists_.get()[curr_list_index_].end()) {
        ++curr_list_index_;
        if (curr_list_index_ < adj_lists_.get().size()) {
          curr_nbr_ = adj_lists_.get()[curr_list_index_].begin();
        }
      }
      return *this;
    }

    bool operator==(const iterator& rhs) noexcept {
      return curr_nbr_ == rhs.curr_nbr_;
    }

    bool operator!=(const iterator& rhs) noexcept {
      return curr_nbr_ != rhs.curr_nbr_;
    }

   private:
    nbr_t curr_nbr_;
    int curr_list_index_;
    std::reference_wrapper<const std::vector<adj_list_t>> adj_lists_;
  };

  iterator begin() { return iterator(adj_lists_); }

  iterator end() { return iterator(adj_lists_, true); }

  inline bool Empty() const { return adj_lists_.empty(); }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const {
    size_t size = 0;
    for (auto& adj_list : adj_lists_) {
      size += adj_list.Size();
    }
    return size;
  }

 private:
  std::vector<adj_list_t> adj_lists_;
};

template <typename T, typename VID_T>
class UnionVertexArray {
 public:
  UnionVertexArray() {}
  explicit UnionVertexArray(const UnionVertexRange<VID_T>& vertices) {
    Init(vertices);
  }

  UnionVertexArray(const UnionVertexRange<VID_T>& vertices, const T& value) {
    Init(vertices, value);
  }

  ~UnionVertexArray() = default;

  void Init(const UnionVertexRange<VID_T>& vertices) {
    ranges_ = vertices.GetVertexRanges();
    vertex_arrays_.resize(ranges_.size());
    for (size_t i = 0; i < ranges_.size(); i++) {
      vertex_arrays_[i].Init(ranges_[i]);
    }
  }

  void Init(const UnionVertexRange<VID_T>& vertices, const T& value) {
    ranges_ = vertices.GetVertexRanges();
    vertex_arrays_.resize(ranges_.size());
    for (size_t i = 0; i < ranges_.size(); i++) {
      vertex_arrays_[i].Init(ranges_[i], value);
    }
  }

  void SetValue(const UnionVertexRange<VID_T>& vertices, const T& value) {
    ranges_ = vertices.GetVertexRanges();
    vertex_arrays_.resize(ranges_.size());
    for (size_t i = 0; i < ranges_.size(); i++) {
      vertex_arrays_[i].SetValue(ranges_[i], value);
    }
  }

  void SetValue(const T& value) {
    for (size_t i = 0; i < vertex_arrays_.size(); i++) {
      vertex_arrays_[i].SetValue(value);
    }
  }

  inline T& operator[](const grape::Vertex<VID_T>& loc) {
    auto range_index = getRangeIndex(loc);
    return vertex_arrays_[range_index][loc];
  }

  inline const T& operator[](const grape::Vertex<VID_T>& loc) const {
    auto range_index = getRangeIndex(loc);
    return vertex_arrays_[range_index][loc];
  }

  void Swap(UnionVertexArray& rhs) {
    ranges_.swap(rhs.ranges_);
    vertex_arrays_.swap(rhs.vertex_arrays_);
  }

  void Clear() {
    ranges_._clear();
    vertex_arrays_.clear();
  }

  UnionVertexRange<VID_T> GetVertexRange() const {
    return UnionVertexRange<VID_T>(ranges_);
  }

 private:
  size_t getRangeIndex(const grape::Vertex<VID_T>& loc) const {
    const auto& value = loc.GetValue();
    for (size_t i = 0; i < ranges_.size(); ++i) {
      // LOG(INFO) << "range-" << i << ": begin=" <<
      // ranges_[i].begin().GetValue() << ", end=" <<
      // ranges_[i].end().GetValue();
      if (value >= ranges_[i].begin().GetValue() &&
          value < ranges_[i].end().GetValue()) {
        return i;
      }
    }
    return 0;
  }

  std::vector<grape::VertexRange<VID_T>> ranges_;
  std::vector<grape::VertexArray<T, VID_T>> vertex_arrays_;
};

class UnionDestList {
  // FIXME: tricky solution.
 public:
  explicit UnionDestList(const std::vector<grape::DestList>& dest_lists) {
    std::set<grape::fid_t> dstset;
    for (auto& dsts : dest_lists) {
      grape::fid_t* ptr = dsts.begin;
      while (ptr != dsts.end) {
        dstset.insert(*(ptr++));
      }
    }
    for (auto fid : dstset) {
      fid_list_.push_back(fid);
    }

    begin = fid_list_.data();
    end = fid_list_.data() + fid_list_.size();
  }

  grape::fid_t* begin;
  grape::fid_t* end;

 private:
  std::vector<grape::fid_t> fid_list_;
};
}  // namespace arrow_label_projected_fragment_impl

/**
 * @brief A label projected wrapper of arrow property fragment.
 *
 */
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ArrowLabelProjectedFragment {
 public:
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T>;
  using oid_t = OID_T;
  using vid_t = VID_T;
  using eid_t = typename fragment_t::eid_t;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using vertex_t = typename fragment_t::vertex_t;
  using fid_t = grape::fid_t;
  using vertex_range_t =
      arrow_label_projected_fragment_impl::UnionVertexRange<vid_t>;
  template <typename DATA_T>
  using vertex_array_t =
      arrow_label_projected_fragment_impl::UnionVertexArray<DATA_T, vid_t>;
  using adj_list_t =
      arrow_label_projected_fragment_impl::UnionAdjList<vid_t, eid_t>;
  using label_id_t = typename fragment_t::label_id_t;
  using dest_list_t = arrow_label_projected_fragment_impl::UnionDestList;

  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  ArrowLabelProjectedFragment() = default;

  explicit ArrowLabelProjectedFragment(fragment_t* frag,
                                       rpc::graph::GraphTypePb host_graph_type,
                                       const std::string& v_prop,
                                       const std::string& e_prop)
      : fragment_(frag),
        v_prop_key_(std::move(v_prop)),
        e_prop_key_(std::move(e_prop)) {
    if (host_graph_type == rpc::graph::ARROW_PROPERTY) {
      v_prop_id_ = boost::lexical_cast<int>(v_prop_key_);
      e_prop_id_ = boost::lexical_cast<int>(e_prop_key_);
    }
  }

  virtual ~ArrowLabelProjectedFragment() = default;

  static std::shared_ptr<
      ArrowLabelProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>
  Project(const std::shared_ptr<fragment_t>& frag,
          rpc::graph::GraphTypePb host_graph_type, const std::string& v_prop,
          const std::string& e_prop) {
    return std::make_shared<ArrowLabelProjectedFragment>(
        frag.get(), host_graph_type, v_prop, e_prop);
  }

  void PrepareToRunApp(grape::MessageStrategy strategy, bool need_split_edges) {
    fragment_->PrepareToRunApp(strategy, need_split_edges);
  }

  inline fid_t fid() const { return fragment_->fid(); }

  inline fid_t fnum() const { return fragment_->fnum(); }

  inline bool directed() const { return fragment_->directed(); }

  inline vertex_range_t Vertices() const {
    std::vector<grape::VertexRange<vid_t>> vertex_ranges;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      vertex_ranges.push_back(fragment_->Vertices(v_label));
    }
    return vertex_range_t(vertex_ranges);
  }

  inline vertex_range_t InnerVertices() const {
    std::vector<grape::VertexRange<vid_t>> vertex_ranges;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      vertex_ranges.push_back(fragment_->InnerVertices(v_label));
    }
    return vertex_range_t(vertex_ranges);
  }

  inline vertex_range_t OuterVertices() const {
    std::vector<grape::VertexRange<vid_t>> vertex_ranges;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      vertex_ranges.push_back(fragment_->OuterVertices(v_label));
    }
    return vertex_range_t(vertex_ranges);
  }

  inline label_id_t vertex_label(const vertex_t& v) const {
    return fragment_->vertex_label(v);
  }

  // FIXME
  // inline const std::vector<vertex_t>& MirrorVertices(fid_t fid) const {
  //   return fragment->MirrorVertices(fid);
  // }

  // FIXME
  // inline const vid_t* GetOuterVerticesGid() const {
  //   return fragment_->GetOuterVerticesGid();
  // }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      if (fragment_->GetVertex(v_label, oid, v)) {
        return true;
      }
    }
    return false;
  }

  inline oid_t GetId(const vertex_t& v) const { return fragment_->GetId(v); }

  inline fid_t GetFragId(const vertex_t& u) const {
    return fragment_->GetFragId(u);
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return fragment_->Gid2Vertex(gid, v);
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const {
    return fragment_->Vertex2Gid(v);
  }

  inline vdata_t GetData(const vertex_t& v) const {
    return fragment_->template GetData<vdata_t>(v, v_prop_id_);
  }

  inline vid_t GetInnerVerticesNum() const {
    vid_t ivnum = 0;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      ivnum += fragment_->GetInnerVerticesNum(v_label);
    }
    return ivnum;
  }

  inline vid_t GetOuterVerticesNum() const {
    vid_t ovnum = 0;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      ovnum += fragment_->GetOuterVerticesNum(v_label);
    }
    return ovnum;
  }

  inline vid_t GetVerticesNum() const {
    vid_t tvnum = 0;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      tvnum += fragment_->GetVerticesNum(v_label);
    }
    return tvnum;
  }

  inline size_t GetTotalVerticesNum() const {
    return fragment_->GetTotalVerticesNum();
  }

  inline size_t GetEdgeNum() const { return fragment_->GetEdgeNum(); }

  inline bool IsInnerVertex(const vertex_t& v) const {
    return fragment_->IsInnerVertex(v);
  }

  inline bool IsOuterVertex(const vertex_t& v) const {
    return fragment_->IsOuterVertex(v);
  }

  inline bool GetInnerVertex(const oid_t& oid, vertex_t& v) const {
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      if (fragment_->GetInnerVertex(v_label, oid, v)) {
        return true;
      }
    }
    return false;
  }

  inline bool GetOuterVertex(const oid_t& oid, vertex_t& v) const {
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      if (fragment_->GetOuterVertex(v_label, oid, v)) {
        return true;
      }
    }
    return false;
  }

  inline oid_t GetInnerVertexId(const vertex_t& v) const {
    return fragment_->GetInnerVertexId(v);
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    return fragment_->GetOuterVertexId(v);
  }

  inline oid_t Gid2Oid(const vid_t& gid) const {
    return fragment_->Gid2Oid(gid);
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    for (label_id_t label = 0; label < fragment_->vertex_label_num(); label++) {
      if (fragment_->Oid2Gid(label, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  inline bool InnerVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    return fragment_->InnerVertexGid2Vertex(gid, v);
  }

  inline bool OuterVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    return fragment_->OuterVertexGid2Vertex(gid, v);
  }

  inline vid_t GetOuterVertexGid(const vertex_t& v) const {
    return fragment_->GetOuterVertexGid(v);
  }

  inline vid_t GetInnerVertexGid(const vertex_t& v) const {
    return fragment_->GetInnerVertexGid(v);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    std::vector<vineyard::property_graph_utils::AdjList<vid_t, eid_t>>
        adj_lists;
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      auto adj_list = fragment_->GetOutgoingAdjList(v, e_label);
      if (adj_list.NotEmpty()) {
        adj_lists.push_back(adj_list);
      }
    }
    return adj_list_t(adj_lists);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    std::vector<vineyard::property_graph_utils::AdjList<vid_t, eid_t>>
        adj_lists;
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      auto adj_list = fragment_->GetOutgoingAdjList(v, e_label);
      if (adj_list.NotEmpty()) {
        adj_lists.push_back(adj_list);
      }
    }
    return adj_list_t(adj_lists);
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    int local_out_degree = 0;
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      local_out_degree += fragment_->GetLocalOutDegree(v, e_label);
    }
    return local_out_degree;
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    int local_in_degree = 0;
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      local_in_degree += fragment_->GetLocalInDegree(v, e_label);
    }
    return local_in_degree;
  }

  inline dest_list_t IEDests(const vertex_t& v) const {
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      dest_lists.push_back(fragment_->IEDests(v, e_label));
    }
    return dest_list_t(dest_lists);
  }

  inline dest_list_t OEDests(const vertex_t& v) const {
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      dest_lists.push_back(fragment_->OEDests(v, e_label));
    }
    return dest_list_t(dest_lists);
  }

  inline dest_list_t IOEDests(const vertex_t& v) const {
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      dest_lists.push_back(fragment_->IOEDests(v, e_label));
    }
    return dest_list_t(dest_lists);
  }

 private:
  fragment_t* fragment_;
  int v_prop_id_;
  int e_prop_id_;
  std::string v_prop_key_;
  std::string e_prop_key_;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_LABEL_PROJECTED_FRAGMENT_H_
