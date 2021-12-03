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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_H_

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "vineyard/graph/fragment/arrow_fragment.h"

namespace gs {

namespace arrow_flattened_fragment_impl {

/**
 * @brief  A union collection of continuous vertex ranges. The vertex ranges
 * must be non-empty to construct the UnionVertexRange.
 *
 * @tparam T Vertex ID type.
 */
template <typename T>
class UnionVertexRange {
 public:
  UnionVertexRange() {}
  explicit UnionVertexRange(
      const std::vector<grape::VertexRange<T>>& vertex_ranges)
      : vertex_ranges_(std::move(vertex_ranges)) {}

  class iterator {
   private:
    std::reference_wrapper<const std::vector<grape::VertexRange<T>>>
        vertex_ranges_;
    grape::Vertex<T> curr_vertex_;
    size_t curr_range_index_;

   public:
    iterator() = default;
    explicit iterator(const std::vector<grape::VertexRange<T>>& vertex_ranges,
                      const grape::Vertex<T>& c, size_t range_index)
        : vertex_ranges_(vertex_ranges),
          curr_vertex_(c),
          curr_range_index_(range_index) {}

    const grape::Vertex<T>& operator*() const { return curr_vertex_; }

    iterator& operator++() {
      if (++curr_vertex_ == vertex_ranges_.get()[curr_range_index_].end()) {
        ++curr_range_index_;
        if (curr_range_index_ < vertex_ranges_.get().size()) {
          curr_vertex_ = vertex_ranges_.get()[curr_range_index_].begin();
        }
      }
      return *this;
    }

    iterator operator++(int) {
      iterator ret(vertex_ranges_.get(), curr_vertex_, curr_range_index_);
      ++(*this);
      return ret;
    }

    iterator operator+(size_t offset) const {
      if (vertex_ranges_.get().empty()) {
        return iterator(vertex_ranges_.get(), grape::Vertex<T>(0), 0);
      }

      grape::Vertex<T> new_vertex(curr_vertex_);
      size_t new_range_index = curr_range_index_;
      while (offset) {
        if (new_vertex + offset < vertex_ranges_.get()[new_range_index].end()) {
          new_vertex.SetValue(new_vertex.GetValue() + offset);
          break;
        } else if (new_range_index == vertex_ranges_.get().size() - 1) {
          new_vertex = vertex_ranges_.get()[new_range_index].end();
          break;
        } else {
          offset -= (vertex_ranges_.get()[new_range_index].end().GetValue() -
                     new_vertex.GetValue());
          new_vertex = vertex_ranges_.get()[++new_range_index].begin();
        }
      }
      return iterator(vertex_ranges_.get(), new_vertex, new_range_index);
    }

    inline bool operator==(const iterator& rhs) const {
      return curr_vertex_ == rhs.curr_vertex_;
    }

    inline bool operator!=(const iterator& rhs) const {
      return curr_vertex_ != rhs.curr_vertex_;
    }

    inline bool operator<(const iterator& rhs) const {
      return curr_vertex_ < rhs.curr_vertex_;
    }
  };

  iterator begin() const {
    if (vertex_ranges_.empty()) {
      return iterator(vertex_ranges_, grape::Vertex<T>(0), 0);
    }
    return iterator(vertex_ranges_, vertex_ranges_.front().begin(), 0);
  }
  iterator end() const {
    if (vertex_ranges_.empty()) {
      return iterator(vertex_ranges_, grape::Vertex<T>(0), 0);
    }
    return iterator(vertex_ranges_, vertex_ranges_.back().end(),
                    vertex_ranges_.size());
  }

  const std::vector<grape::VertexRange<T>>& GetVertexRanges() const {
    return vertex_ranges_;
  }

 private:
  std::vector<grape::VertexRange<T>> vertex_ranges_;
};

/**
 * @brief  A wrapper of vineyard::property_graph_utils::Nbr with default
 * property id to access data.
 *
 * @tparam VID_T.
 * @tparam EID_T.
 * @tparam EDATA_T.
 */
template <typename VID_T, typename EID_T, typename EDATA_T>
struct NbrDefault {
  using nbr_t = vineyard::property_graph_utils::Nbr<VID_T, EID_T>;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<VID_T, EID_T>;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;

 public:
  explicit NbrDefault(const prop_id_t& default_prop_id)
      : default_prop_id_(default_prop_id) {}
  NbrDefault(const nbr_t& nbr, const prop_id_t& default_prop_id)
      : nbr_(nbr), default_prop_id_(default_prop_id) {}
  NbrDefault(const NbrDefault& rhs)
      : nbr_(rhs.nbr_), default_prop_id_(rhs.default_prop_id_) {}
  NbrDefault(NbrDefault&& rhs)
      : nbr_(rhs.nbr_), default_prop_id_(rhs.default_prop_id_) {}

  NbrDefault& operator=(const NbrDefault& rhs) {
    nbr_ = rhs.nbr_;
    default_prop_id_ = rhs.default_prop_id_;
    return *this;
  }

  NbrDefault& operator=(NbrDefault&& rhs) {
    nbr_ = std::move(rhs.nbr_);
    default_prop_id_ = rhs.default_prop_id_;
    return *this;
  }

  NbrDefault& operator=(const nbr_t& nbr) {
    nbr_ = nbr;
    return *this;
  }

  NbrDefault& operator=(nbr_t&& nbr) {
    nbr_ = std::move(nbr);
    return *this;
  }

  grape::Vertex<VID_T> neighbor() const { return nbr_.neighbor(); }

  grape::Vertex<VID_T> get_neighbor() const { return nbr_.get_neighbor(); }

  EID_T edge_id() const { return nbr_.edge_id(); }

  EDATA_T get_data() const {
    return nbr_.template get_data<EDATA_T>(default_prop_id_);
  }

  std::string get_str() const { return nbr_.get_str(default_prop_id_); }

  double get_double() const { return nbr_.get_double(default_prop_id_); }

  int64_t get_int() const { return nbr_.get_int(default_prop_id_); }

  inline const NbrDefault& operator++() const {
    ++nbr_;
    return *this;
  }

  inline NbrDefault operator++(int) const {
    NbrDefault ret(nbr_, default_prop_id_);
    ++(*this);
    return ret;
  }

  inline const NbrDefault& operator--() const {
    --nbr_;
    return *this;
  }

  inline NbrDefault operator--(int) const {
    NbrDefault ret(nbr_, default_prop_id_);
    --(*this);
    return ret;
  }

  inline bool operator==(const NbrDefault& rhs) const {
    return nbr_ == rhs.nbr_;
  }
  inline bool operator!=(const NbrDefault& rhs) const {
    return nbr_ != rhs.nbr_;
  }
  inline bool operator<(const NbrDefault& rhs) const { return nbr_ < rhs.nbr_; }

  inline bool operator==(const nbr_t& nbr) const { return nbr_ == nbr; }
  inline bool operator!=(const nbr_t& nbr) const { return nbr_ != nbr; }
  inline bool operator<(const nbr_t& nbr) const { return nbr_ < nbr; }

  inline const NbrDefault& operator*() const { return *this; }

 private:
  nbr_t nbr_;
  prop_id_t default_prop_id_;
};

/**
 * @brief Union of all iteratable adjencent lists of a vertex. The union
 * list contains all neighbors in format of NbrDefault, which contains the other
 * Node and the data on the Edge. The lists must be non-empty to construct the
 * UnionAdjList.
 *
 * @tparam VID_T
 * @tparam EID_T
 * @tparam EDATA_T
 */
template <typename VID_T, typename EID_T, typename EDATA_T>
class UnionAdjList {
 public:
  using nbr_t = NbrDefault<VID_T, EID_T, EDATA_T>;
  using nbr_unit_t = vineyard::property_graph_utils::Nbr<VID_T, EID_T>;
  using adj_list_t = vineyard::property_graph_utils::AdjList<VID_T, EID_T>;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;

  UnionAdjList() : size_(0) {}
  explicit UnionAdjList(const std::vector<adj_list_t>& adj_lists,
                        const prop_id_t& default_prop_id)
      : adj_lists_(adj_lists), default_prop_id_(default_prop_id) {
    size_ = 0;
    for (auto& adj_list : adj_lists) {
      size_ += adj_list.Size();
    }
  }

  class iterator {
    using pointer_type = nbr_t*;
    using reference_type = nbr_t&;

   public:
    iterator() = default;
    explicit iterator(const std::vector<adj_list_t>& adj_lists,
                      const nbr_unit_t& nbr, const prop_id_t& default_prop_id,
                      size_t index)
        : adj_lists_(adj_lists),
          curr_nbr_(default_prop_id),
          curr_list_index_(index) {
      curr_nbr_ = nbr;
    }
    explicit iterator(const std::vector<adj_list_t>& adj_lists,
                      const nbr_t& nbr, size_t list_index)
        : adj_lists_(adj_lists), curr_nbr_(nbr), curr_list_index_(list_index) {}

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

    iterator operator++(int) {
      iterator ret(adj_lists_.get(), curr_nbr_, curr_list_index_);
      ++(*this);
      return ret;
    }

    bool operator==(const iterator& rhs) noexcept {
      return curr_nbr_ == rhs.curr_nbr_;
    }

    bool operator!=(const iterator& rhs) noexcept {
      return curr_nbr_ != rhs.curr_nbr_;
    }

   private:
    std::reference_wrapper<const std::vector<adj_list_t>> adj_lists_;
    nbr_t curr_nbr_;
    size_t curr_list_index_;
  };

  iterator begin() {
    if (size_ == 0) {
      nbr_unit_t nbr;
      return iterator(adj_lists_, nbr, default_prop_id_, 0);
    } else {
      return iterator(adj_lists_, adj_lists_.front().begin(), default_prop_id_,
                      0);
    }
  }

  iterator end() {
    if (size_ == 0) {
      nbr_unit_t nbr;
      return iterator(adj_lists_, nbr, default_prop_id_, 0);
    } else {
      return iterator(adj_lists_, adj_lists_.back().end(), default_prop_id_,
                      adj_lists_.size());
    }
  }

  inline bool Empty() const { return adj_lists_.empty(); }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const { return size_; }

 private:
  std::vector<adj_list_t> adj_lists_;
  prop_id_t default_prop_id_;
  size_t size_;
};

/**
 * @brief Union of a set of VertexArray. UnionVertexArray is construct with
 * UnionVertexRange.
 *
 * @tparam T
 * @tparam VID_T
 */
template <typename T, typename VID_T>
class UnionVertexArray {
 public:
  UnionVertexArray() = default;
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
    for (size_t i = 0; i < ranges_.size(); ++i) {
      vertex_arrays_[i].Init(ranges_[i]);
    }
  }

  void Init(const UnionVertexRange<VID_T>& vertices, const T& value) {
    ranges_ = vertices.GetVertexRanges();
    vertex_arrays_.resize(ranges_.size());
    for (size_t i = 0; i < ranges_.size(); ++i) {
      vertex_arrays_[i].Init(ranges_[i], value);
    }
  }

  void SetValue(const UnionVertexRange<VID_T>& vertices, const T& value) {
    ranges_ = vertices.GetVertexRanges();
    vertex_arrays_.resize(ranges_.size());
    for (size_t i = 0; i < ranges_.size(); ++i) {
      vertex_arrays_[i].SetValue(ranges_[i], value);
    }
  }

  void SetValue(const T& value) {
    for (auto& array : vertex_arrays_) {
      array.SetValue(value);
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
}  // namespace arrow_flattened_fragment_impl

/**
 * @brief This class represents the fragment flattened from ArrowFragment.
 * Different from ArrowProjectedFragment, an ArrowFlattenedFragment derives from
 * an ArrowFragment, but flattens all the labels to one type, result in a graph
 * with a single type of vertices and a single type of edges. Optionally,
 * a common property across labels of vertices(reps., edges) in the
 * ArrowFragment will be reserved as vdata(resp, edata).
 * ArrowFlattenedFragment usually used as a wrapper for ArrowFragment to run the
 * applications/algorithms defined in NetworkX or Analytical engine,
 * since these algorithms need the topology of the whole (property) graph.
 *
 * @tparam OID_T
 * @tparam VID_T
 * @tparam VDATA_T
 * @tparam EDATA_T
 */
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ArrowFlattenedFragment {
 public:
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T>;
  using oid_t = OID_T;
  using vid_t = VID_T;
  using eid_t = typename fragment_t::eid_t;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using vertex_t = typename fragment_t::vertex_t;
  using fid_t = grape::fid_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using vertex_range_t = arrow_flattened_fragment_impl::UnionVertexRange<vid_t>;
  template <typename DATA_T>
  using vertex_array_t =
      arrow_flattened_fragment_impl::UnionVertexArray<DATA_T, vid_t>;
  using adj_list_t =
      arrow_flattened_fragment_impl::UnionAdjList<vid_t, eid_t, edata_t>;
  using dest_list_t = arrow_flattened_fragment_impl::UnionDestList;

  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  ArrowFlattenedFragment() = default;

  explicit ArrowFlattenedFragment(fragment_t* frag, prop_id_t v_prop_id,
                                  prop_id_t e_prop_id)
      : fragment_(frag), v_prop_id_(v_prop_id), e_prop_id_(e_prop_id) {
    ivnum_ = ovnum_ = tvnum_ = 0;
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      ivnum_ += fragment_->GetInnerVerticesNum(v_label);
      ovnum_ += fragment_->GetOuterVerticesNum(v_label);
      tvnum_ += fragment_->GetVerticesNum(v_label);
    }
  }

  virtual ~ArrowFlattenedFragment() = default;

  static std::shared_ptr<ArrowFlattenedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>
  Project(const std::shared_ptr<fragment_t>& frag, const std::string& v_prop,
          const std::string& e_prop) {
    prop_id_t v_prop_id = boost::lexical_cast<int>(v_prop);
    prop_id_t e_prop_id = boost::lexical_cast<int>(e_prop);
    return std::make_shared<ArrowFlattenedFragment>(frag.get(), v_prop_id,
                                                    e_prop_id);
  }

  void PrepareToRunApp(grape::MessageStrategy strategy, bool need_split_edges) {
    fragment_->PrepareToRunApp(strategy, need_split_edges);
  }

  inline fid_t fid() const { return fragment_->fid(); }

  inline fid_t fnum() const { return fragment_->fnum(); }

  inline bool directed() const { return fragment_->directed(); }

  inline vertex_range_t Vertices() const {
    std::vector<grape::VertexRange<vid_t>> vertex_ranges;
    vertex_ranges.reserve(fragment_->vertex_label_num());
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      auto range = fragment_->Vertices(v_label);
      if (range.size() != 0) {
        vertex_ranges.push_back(range);
      }
    }
    return vertex_range_t(vertex_ranges);
  }

  inline vertex_range_t InnerVertices() const {
    std::vector<grape::VertexRange<vid_t>> vertex_ranges;
    vertex_ranges.reserve(fragment_->vertex_label_num());
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      auto range = fragment_->InnerVertices(v_label);
      if (range.size() != 0) {
        vertex_ranges.push_back(range);
      }
    }
    return vertex_range_t(vertex_ranges);
  }

  inline vertex_range_t OuterVertices() const {
    std::vector<grape::VertexRange<vid_t>> vertex_ranges;
    vertex_ranges.reserve(fragment_->vertex_label_num());
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      auto range = fragment_->OuterVertices(v_label);
      if (range.size() != 0) {
        vertex_ranges.push_back(range);
      }
    }
    return vertex_range_t(vertex_ranges);
  }

  inline label_id_t vertex_label(const vertex_t& v) const {
    return fragment_->vertex_label(v);
  }

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

  inline vid_t GetInnerVerticesNum() const { return ivnum_; }

  inline vid_t GetOuterVerticesNum() const { return ovnum_; }

  inline vid_t GetVerticesNum() const { return tvnum_; }

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
    adj_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      auto adj_list = fragment_->GetOutgoingAdjList(v, e_label);
      if (adj_list.NotEmpty()) {
        adj_lists.push_back(adj_list);
      }
    }
    return adj_list_t(adj_lists, e_prop_id_);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    std::vector<vineyard::property_graph_utils::AdjList<vid_t, eid_t>>
        adj_lists;
    adj_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      auto adj_list = fragment_->GetIncomingAdjList(v, e_label);
      if (adj_list.NotEmpty()) {
        adj_lists.push_back(adj_list);
      }
    }
    return adj_list_t(adj_lists, e_prop_id_);
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
  prop_id_t v_prop_id_;
  prop_id_t e_prop_id_;
  vid_t ivnum_;
  vid_t ovnum_;
  vid_t tvnum_;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_H_
