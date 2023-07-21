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

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "grape/fragment/fragment_base.h"
#include "grape/graph/adj_list.h"
#include "grape/types.h"
#include "grape/utils/vertex_array.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/config.h"

namespace grape {
class CommSpec;
}

namespace gs {

namespace arrow_flattened_fragment_impl {

template <typename ID_TYPE>
class UnionIdParser {
  using fid_t = unsigned;
  using LabelIDT = int;

 public:
  UnionIdParser() : ivnum_(0) {}
  ~UnionIdParser() {}

  void Init(fid_t fnum, LabelIDT vertex_label_num,
            std::vector<ID_TYPE>& vertex_range_offset,
            std::vector<ID_TYPE>& ivnums, std::vector<ID_TYPE>& ovnums) {
    fnum_ = fnum;
    vertex_label_num_ = vertex_label_num;
    vertex_range_offset_ = vertex_range_offset;
    ivnums_ = ivnums;
    ovnums_ = ovnums;
    vid_parser_.Init(fnum_, vertex_label_num_);

    for (auto n : ivnums_) {
      ivnum_ += n;
    }
  }

  LabelIDT GetLabelId(ID_TYPE v) const {
    return (getVertexRangeOffsetIndex(v) - 1) % vertex_label_num_;
  }

  int64_t GetOffset(ID_TYPE v) const {
    size_t index = getVertexRangeOffsetIndex(v);
    if (v < ivnum_) {
      // inner vertex
      return v - vertex_range_offset_[index - 1];
    } else {
      return v - vertex_range_offset_[index - 1] + ivnums_[GetLabelId(v)];
    }
  }

  ID_TYPE GenerateContinuousLid(ID_TYPE lid) const {
    LabelIDT label_id = vid_parser_.GetLabelId(lid);
    int64_t offset = vid_parser_.GetOffset(lid);

    if (offset < static_cast<int64_t>(ivnums_[label_id])) {
      return vertex_range_offset_[label_id] + offset;
    } else {
      return vertex_range_offset_[label_id + vertex_label_num_] + offset -
             ivnums_[label_id];
    }
  }

  ID_TYPE ParseContinuousLid(ID_TYPE cont_lid) const {
    return vid_parser_.GenerateId(0, GetLabelId(cont_lid), GetOffset(cont_lid));
  }

 private:
  size_t getVertexRangeOffsetIndex(ID_TYPE v) const {
    size_t index = 0;
    for (size_t i = 0; i < vertex_range_offset_.size(); ++i) {
      if (vertex_range_offset_[i] > v) {
        index = i;
        break;
      }
    }
    CHECK_NE(index, 0);
    return index;
  }

 private:
  fid_t fnum_;
  LabelIDT vertex_label_num_;
  std::vector<ID_TYPE> vertex_range_offset_;
  ID_TYPE ivnum_;
  std::vector<ID_TYPE> ivnums_;
  std::vector<ID_TYPE> ovnums_;
  vineyard::IdParser<ID_TYPE> vid_parser_;
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
  explicit NbrDefault(const prop_id_t& default_prop_id,
                      const UnionIdParser<VID_T>& union_id_parser)
      : default_prop_id_(default_prop_id), union_id_parser_(union_id_parser) {}
  NbrDefault(const nbr_t& nbr, const prop_id_t& default_prop_id,
             const UnionIdParser<VID_T>& union_id_parser)
      : nbr_(nbr),
        default_prop_id_(default_prop_id),
        union_id_parser_(union_id_parser) {}
  NbrDefault(const NbrDefault& rhs)
      : nbr_(rhs.nbr_),
        default_prop_id_(rhs.default_prop_id_),
        union_id_parser_(rhs.union_id_parser_) {}
  NbrDefault(NbrDefault&& rhs)
      : nbr_(rhs.nbr_),
        default_prop_id_(rhs.default_prop_id_),
        union_id_parser_(rhs.union_id_parser_) {}

  NbrDefault& operator=(const NbrDefault& rhs) {
    nbr_ = rhs.nbr_;
    default_prop_id_ = rhs.default_prop_id_;
    union_id_parser_ = rhs.union_id_parser_;
    return *this;
  }

  NbrDefault& operator=(NbrDefault&& rhs) {
    nbr_ = std::move(rhs.nbr_);
    default_prop_id_ = rhs.default_prop_id_;
    union_id_parser_ = std::move(rhs.union_id_parser_);
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

  grape::Vertex<VID_T> neighbor() const {
    return grape::Vertex<VID_T>(
        union_id_parser_.GenerateContinuousLid(nbr_.neighbor().GetValue()));
  }

  grape::Vertex<VID_T> get_neighbor() const {
    return grape::Vertex<VID_T>(
        union_id_parser_.GenerateContinuousLid(nbr_.get_neighbor().GetValue()));
  }

  grape::Vertex<VID_T> raw_neighbor() const { return nbr_.neighbor(); }

  grape::Vertex<VID_T> get_raw_neighbor() const { return nbr_.neighbor(); }

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
  UnionIdParser<VID_T> union_id_parser_;
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
template <typename VID_T, typename EID_T, typename EDATA_T, typename FRAGMENT_T>
class UnionAdjList {
 public:
  using nbr_t = NbrDefault<VID_T, EID_T, EDATA_T>;
  using nbr_unit_t = vineyard::property_graph_utils::Nbr<VID_T, EID_T>;
  using adj_list_t = vineyard::property_graph_utils::AdjList<VID_T, EID_T>;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;

  UnionAdjList() : size_(0) {}
  explicit UnionAdjList(const std::vector<adj_list_t>& adj_lists,
                        const prop_id_t& default_prop_id,
                        const UnionIdParser<VID_T>& union_id_parser,
                        const FRAGMENT_T* fragment)
      : adj_lists_(adj_lists),
        default_prop_id_(default_prop_id),
        union_id_parser_(union_id_parser),
        fragment_(fragment) {
    size_ = 0;
    for (auto& adj_list : adj_lists_) {
      size_ += adj_list.Size();
    }
  }

  explicit UnionAdjList(std::vector<adj_list_t>&& adj_lists,
                        const prop_id_t& default_prop_id,
                        const UnionIdParser<VID_T>& union_id_parser,
                        const FRAGMENT_T* fragment)
      : adj_lists_(std::move(adj_lists)),
        default_prop_id_(default_prop_id),
        union_id_parser_(union_id_parser),
        fragment_(fragment) {
    size_ = 0;
    for (auto& adj_list : adj_lists_) {
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
                      size_t index, const UnionIdParser<VID_T>& union_id_parser,
                      const FRAGMENT_T* fragment)
        : adj_lists_(adj_lists),
          fragment_(fragment),
          curr_nbr_(default_prop_id, union_id_parser),
          curr_list_index_(index) {
      curr_nbr_ = nbr;
      move_to_next_valid_nbr();
    }
    explicit iterator(const std::vector<adj_list_t>& adj_lists,
                      const nbr_t& nbr, size_t list_index)
        : adj_lists_(adj_lists), curr_nbr_(nbr), curr_list_index_(list_index) {}

    reference_type operator*() noexcept { return curr_nbr_; }

    pointer_type operator->() noexcept { return &curr_nbr_; }

    // The only the interator's `operator++()` is exposed to the external
    // programs so we only need to check the validity here, and nothing
    // to do with the `operator++()` of NbrDefault.
    inline void move_to_next_valid_nbr() {
      // move to the next valid nbr
      while (curr_list_index_ < adj_lists_.get().size()) {
        if (curr_nbr_ == adj_lists_.get()[curr_list_index_].end()) {
          ++curr_list_index_;
          if (curr_list_index_ < adj_lists_.get().size()) {
            curr_nbr_ = adj_lists_.get()[curr_list_index_].begin();
          }
        } else {
          if (fragment_->is_valid_vertex(curr_nbr_.raw_neighbor())) {
            break;
          }
          ++curr_nbr_;
        }
      }
    }

    iterator& operator++() {
      ++curr_nbr_;
      move_to_next_valid_nbr();
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
    const FRAGMENT_T* fragment_;
    nbr_t curr_nbr_;
    size_t curr_list_index_;
  };

  class const_iterator {
    using pointer_type = const nbr_t*;
    using reference_type = const nbr_t&;

   public:
    const_iterator() = default;
    explicit const_iterator(const std::vector<adj_list_t>& adj_lists,
                            const nbr_unit_t& nbr,
                            const prop_id_t& default_prop_id, size_t index,
                            const UnionIdParser<VID_T>& union_id_parser,
                            const FRAGMENT_T* fragment)
        : adj_lists_(adj_lists),
          fragment_(fragment),
          curr_nbr_(default_prop_id, union_id_parser),
          curr_list_index_(index) {
      curr_nbr_ = nbr;
      move_to_next_valid_nbr();
    }

    explicit const_iterator(const std::vector<adj_list_t>& adj_lists,
                            const nbr_t& nbr, size_t list_index)
        : adj_lists_(adj_lists), curr_nbr_(nbr), curr_list_index_(list_index) {}

    reference_type operator*() noexcept { return curr_nbr_; }

    pointer_type operator->() noexcept { return curr_nbr_; }

    // The only the interator's `operator++()` is exposed to the external
    // programs so we only need to check the validity here, and nothing
    // to do with the `operator++()` of NbrDefault.
    inline void move_to_next_valid_nbr() {
      // move to the next valid nbr
      while (curr_list_index_ < adj_lists_.get().size()) {
        if (curr_nbr_ == adj_lists_.get()[curr_list_index_].end()) {
          ++curr_list_index_;
          if (curr_list_index_ < adj_lists_.get().size()) {
            curr_nbr_ = adj_lists_.get()[curr_list_index_].begin();
          }
        } else {
          if (fragment_->is_valid_vertex(curr_nbr_.raw_neighbor())) {
            break;
          }
          ++curr_nbr_;
        }
      }
    }

    const_iterator& operator++() {
      ++curr_nbr_;
      move_to_next_valid_nbr();
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator ret(adj_lists_, curr_nbr_, curr_list_index_);
      ++(*this);
      return ret;
    }

    bool operator==(const const_iterator& rhs) noexcept {
      return curr_nbr_ == rhs.curr_nbr_;
    }

    bool operator!=(const const_iterator& rhs) noexcept {
      return curr_nbr_ != rhs.curr_nbr_;
    }

   private:
    std::reference_wrapper<const std::vector<adj_list_t>> adj_lists_;
    const FRAGMENT_T* fragment_;
    nbr_t curr_nbr_;
    size_t curr_list_index_;
  };

  iterator begin() {
    if (size_ == 0) {
      nbr_unit_t nbr;
      return iterator(adj_lists_, nbr, default_prop_id_, 0, union_id_parser_,
                      fragment_);
    } else {
      return iterator(adj_lists_, adj_lists_.front().begin(), default_prop_id_,
                      0, union_id_parser_, fragment_);
    }
  }

  iterator end() {
    if (size_ == 0) {
      nbr_unit_t nbr;
      return iterator(adj_lists_, nbr, default_prop_id_, 0, union_id_parser_,
                      fragment_);
    } else {
      return iterator(adj_lists_, adj_lists_.back().end(), default_prop_id_,
                      adj_lists_.size(), union_id_parser_, fragment_);
    }
  }

  const_iterator begin() const {
    if (size_ == 0) {
      nbr_unit_t nbr;
      return const_iterator(adj_lists_, nbr, default_prop_id_, 0,
                            union_id_parser_, fragment_);
    } else {
      return const_iterator(adj_lists_, adj_lists_.front().begin(),
                            default_prop_id_, 0, union_id_parser_, fragment_);
    }
  }

  const_iterator end() const {
    if (size_ == 0) {
      nbr_unit_t nbr;
      return const_iterator(adj_lists_, nbr, default_prop_id_, 0,
                            union_id_parser_, fragment_);
    } else {
      return const_iterator(adj_lists_, adj_lists_.back().end(),
                            default_prop_id_, adj_lists_.size(),
                            union_id_parser_, fragment_);
    }
  }

  inline bool Empty() const { return adj_lists_.empty(); }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const { return size_; }

 private:
  std::vector<adj_list_t> adj_lists_;
  prop_id_t default_prop_id_;
  UnionIdParser<VID_T> union_id_parser_;
  const FRAGMENT_T* fragment_;
  size_t size_;
};

class UnionDestList {
 public:
  explicit UnionDestList(const std::vector<grape::DestList>& dest_lists) {
    std::set<grape::fid_t> dstset;
    for (auto& dsts : dest_lists) {
      const grape::fid_t* ptr = dsts.begin;
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
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename VERTEX_MAP_T = vineyard::ArrowVertexMap<
              typename vineyard::InternalType<OID_T>::type, VID_T>>
class ArrowFlattenedFragment {
 public:
  // TODO(tao): ArrowFragment with compact edges cannot be flattened.
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T, VERTEX_MAP_T, false>;
  using flatten_fragment_t =
      ArrowFlattenedFragment<OID_T, VID_T, VDATA_T, EDATA_T, VERTEX_MAP_T>;
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using eid_t = typename fragment_t::eid_t;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using vertex_t = typename fragment_t::vertex_t;
  using fid_t = grape::fid_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using vertex_range_t = grape::VertexRange<vid_t>;
  using inner_vertices_t = vertex_range_t;
  using outer_vertices_t = vertex_range_t;
  using vertices_t = vertex_range_t;

  template <typename DATA_T>
  using vertex_array_t = grape::VertexArray<vertices_t, DATA_T>;

  template <typename DATA_T>
  using inner_vertex_array_t = grape::VertexArray<inner_vertices_t, DATA_T>;

  template <typename DATA_T>
  using outer_vertex_array_t = grape::VertexArray<outer_vertices_t, DATA_T>;

  using adj_list_t =
      arrow_flattened_fragment_impl::UnionAdjList<vid_t, eid_t, edata_t,
                                                  flatten_fragment_t>;
  using dest_list_t = arrow_flattened_fragment_impl::UnionDestList;

  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  ArrowFlattenedFragment() = default;

  explicit ArrowFlattenedFragment(fragment_t* frag, prop_id_t v_prop_id,
                                  prop_id_t e_prop_id)
      : fragment_(frag),
        schema_(fragment_->schema()),
        v_prop_id_(v_prop_id),
        e_prop_id_(e_prop_id) {
    ivnum_ = ovnum_ = tvnum_ = 0;
    label_id_t vertex_label_num =
        static_cast<label_id_t>(schema_.AllVertexEntries().size());
    for (label_id_t v_label = 0; v_label < vertex_label_num; v_label++) {
      vid_t ivnum = 0, ovnum = 0, tvnum = 0;
      if (schema_.IsVertexValid(v_label)) {
        ivnum = fragment_->GetInnerVerticesNum(v_label);
        ovnum = fragment_->GetOuterVerticesNum(v_label);
        tvnum = fragment_->GetVerticesNum(v_label);
      }
      ivnums_.push_back(ivnum);
      ovnums_.push_back(ovnum);
      tvnums_.push_back(tvnum);
      ivnum_ += ivnum;
      ovnum_ += ovnum;
      tvnum_ += tvnum;
    }
    // init union_vertex_range_offset_
    // e.g. vertex_label_num is 2
    // [0,
    //  l0_ivnum,
    //  l0_ivnum + l1_ivnum,
    //  l0_ivnum + l1_ivnum + l0_ovnum,
    //  l0_ivnum + l1_ivnum + l0_ovnum + l1_ovnum
    // ]
    union_vertex_range_offset_.resize(2 * vertex_label_num + 1, 0);
    for (label_id_t v_label = 0; v_label < vertex_label_num; v_label++) {
      union_vertex_range_offset_[v_label + 1] =
          union_vertex_range_offset_[v_label];
      if (schema_.IsVertexValid(v_label)) {
        union_vertex_range_offset_[v_label + 1] +=
            fragment_->GetInnerVerticesNum(v_label);
      }
    }
    for (label_id_t v_label = 0; v_label < vertex_label_num; v_label++) {
      union_vertex_range_offset_[v_label + vertex_label_num + 1] =
          union_vertex_range_offset_[v_label + vertex_label_num];
      if (schema_.IsVertexValid(v_label)) {
        union_vertex_range_offset_[v_label + vertex_label_num + 1] +=
            fragment_->GetOuterVerticesNum(v_label);
      }
    }
    // init id parser
    union_id_parser_.Init(fragment_->fnum(), vertex_label_num,
                          union_vertex_range_offset_, ivnums_, ovnums_);
  }

  virtual ~ArrowFlattenedFragment() = default;

  static std::shared_ptr<
      ArrowFlattenedFragment<OID_T, VID_T, VDATA_T, EDATA_T, VERTEX_MAP_T>>
  Project(const std::shared_ptr<fragment_t>& frag, const std::string& v_prop,
          const std::string& e_prop) {
    prop_id_t v_prop_id = boost::lexical_cast<int>(v_prop);
    prop_id_t e_prop_id = boost::lexical_cast<int>(e_prop);
    return std::make_shared<ArrowFlattenedFragment>(frag.get(), v_prop_id,
                                                    e_prop_id);
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) {
    fragment_->PrepareToRunApp(comm_spec, conf);
  }

  inline fid_t fid() const { return fragment_->fid(); }

  inline fid_t fnum() const { return fragment_->fnum(); }

  inline bool directed() const { return fragment_->directed(); }

  inline vertex_range_t Vertices() const { return vertex_range_t(0, tvnum_); }

  inline vertex_range_t InnerVertices() const {
    return vertex_range_t(0, ivnum_);
  }

  inline vertex_range_t OuterVertices() const {
    return vertex_range_t(ivnum_, tvnum_);
  }

  inline label_id_t vertex_label(const vertex_t& v) const {
    return union_id_parser_.GetLabelId(v.GetValue());
  }

  // Check if a given vertex is valid in the underlying arrow fragment
  //
  // The argument is the vid in the original arrow fragment.
  inline bool is_valid_vertex(const vertex_t& v) const {
    return schema_.IsVertexValid(fragment_->vertex_label(v));
  }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      if (fragment_->GetVertex(v_label, oid, v)) {
        // generate continuous lid
        v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
        return true;
      }
    }
    return false;
  }

  inline oid_t GetId(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->GetId(v_);
  }

  inline internal_oid_t GetInternalId(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->GetInternalId(v_);
  }

  inline fid_t GetFragId(const vertex_t& u) const {
    vertex_t u_(union_id_parser_.ParseContinuousLid(u.GetValue()));
    return fragment_->GetFragId(u_);
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    if (fragment_->Gid2Vertex(gid, v)) {
      v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
      return true;
    }
    return false;
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->Vertex2Gid(v_);
  }

  inline vdata_t GetData(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->template GetData<vdata_t>(v_, v_prop_id_);
  }

  inline vid_t GetInnerVerticesNum() const { return ivnum_; }

  inline vid_t GetOuterVerticesNum() const { return ovnum_; }

  inline vid_t GetVerticesNum() const { return tvnum_; }

  inline size_t GetTotalVerticesNum() const {
    return fragment_->GetTotalVerticesNum();
  }

  inline size_t GetEdgeNum() const { return fragment_->GetEdgeNum(); }

  inline bool IsInnerVertex(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->IsInnerVertex(v_);
  }

  inline bool IsOuterVertex(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->IsOuterVertex(v_);
  }

  inline bool GetInnerVertex(const oid_t& oid, vertex_t& v) const {
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      if (fragment_->GetInnerVertex(v_label, oid, v)) {
        // generate continuous lid
        v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
        return true;
      }
    }
    return false;
  }

  inline bool GetOuterVertex(const oid_t& oid, vertex_t& v) const {
    for (label_id_t v_label = 0; v_label < fragment_->vertex_label_num();
         v_label++) {
      if (fragment_->GetOuterVertex(v_label, oid, v)) {
        // generate continuous lid
        v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
        return true;
      }
    }
    return false;
  }

  inline oid_t GetInnerVertexId(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->GetInnerVertexId(v_);
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->GetOuterVertexId(v_);
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
    if (fragment_->InnerVertexGid2Vertex(gid, v)) {
      v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
      return true;
    }
    return false;
  }

  inline bool OuterVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    if (fragment_->OuterVertexGid2Vertex(gid, v)) {
      v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
      return true;
    }
    return false;
  }

  inline vid_t GetOuterVertexGid(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->GetOuterVertexGid(v_);
  }

  inline vid_t GetInnerVertexGid(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->GetInnerVertexGid(v_);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<vineyard::property_graph_utils::AdjList<vid_t, eid_t>>
        adj_lists;
    adj_lists.reserve(fragment_->edge_label_num());
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      auto adj_list = fragment_->GetOutgoingAdjList(v_, e_label);
      if (adj_list.NotEmpty()) {
        adj_lists.push_back(adj_list);
      }
    }
    return adj_list_t(std::move(adj_lists), e_prop_id_, union_id_parser_, this);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<vineyard::property_graph_utils::AdjList<vid_t, eid_t>>
        adj_lists;
    adj_lists.reserve(fragment_->edge_label_num());
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      auto adj_list = fragment_->GetIncomingAdjList(v_, e_label);
      if (adj_list.NotEmpty()) {
        adj_lists.push_back(adj_list);
      }
    }
    return adj_list_t(std::move(adj_lists), e_prop_id_, union_id_parser_, this);
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    int local_out_degree = 0;
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      local_out_degree += fragment_->GetLocalOutDegree(v_, e_label);
    }
    return local_out_degree;
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    int local_in_degree = 0;
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      local_in_degree += fragment_->GetLocalInDegree(v_, e_label);
    }
    return local_in_degree;
  }

  inline dest_list_t IEDests(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      dest_lists.push_back(fragment_->IEDests(v_, e_label));
    }
    return dest_list_t(dest_lists);
  }

  inline dest_list_t OEDests(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      dest_lists.push_back(fragment_->OEDests(v_, e_label));
    }
    return dest_list_t(dest_lists);
  }

  inline dest_list_t IOEDests(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    auto& schema = fragment_->schema();
    label_id_t edge_label_num =
        static_cast<label_id_t>(schema.AllEdgeEntries().size());
    for (label_id_t e_label = 0; e_label < edge_label_num; e_label++) {
      if (!schema.IsEdgeValid(e_label)) {
        continue;
      }
      dest_lists.push_back(fragment_->IOEDests(v_, e_label));
    }
    return dest_list_t(dest_lists);
  }

 private:
  fragment_t* fragment_;
  const vineyard::PropertyGraphSchema& schema_;
  prop_id_t v_prop_id_;
  prop_id_t e_prop_id_;

  vid_t ivnum_;
  vid_t ovnum_;
  vid_t tvnum_;
  std::vector<vid_t> ivnums_, ovnums_, tvnums_;

  arrow_flattened_fragment_impl::UnionIdParser<vid_t> union_id_parser_;
  std::vector<vid_t> union_vertex_range_offset_;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_H_
