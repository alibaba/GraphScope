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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_PROJECTED_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_PROJECTED_FRAGMENT_H_

#ifdef NETWORKX

#include <cassert>
#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "grape/fragment/fragment_base.h"
#include "grape/graph/adj_list.h"
#include "grape/types.h"

#include "core/config.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/object/dynamic.h"

namespace grape {
class CommSpec;
}

namespace gs {
namespace dynamic_projected_fragment_impl {
/**
 * @brief A specialized UnpackDynamicVData for int32_t type
 */
template <typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type unpack_dynamic(
    const dynamic::Value& data, const std::string& v_prop_key) {
  return data[v_prop_key].GetInt64();
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
unpack_dynamic(const dynamic::Value& data, const std::string& v_prop_key) {
  return data[v_prop_key].GetDouble();
}

template <typename T>
typename std::enable_if<std::is_same<T, bool>::value, T>::type unpack_dynamic(
    const dynamic::Value& data, const std::string& v_prop_key) {
  return data[v_prop_key].GetBool();
}

template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type
unpack_dynamic(const dynamic::Value& data, const std::string& v_prop_key) {
  return data[v_prop_key].GetString();
}

template <typename T>
typename std::enable_if<std::is_same<T, grape::EmptyType>::value, T>::type
unpack_dynamic(const dynamic::Value& data, const std::string& v_prop_key) {
  return grape::EmptyType();
}

template <typename VID_T, typename T>
typename std::enable_if<std::is_integral<T>::value>::type unpack_nbr(
    grape::Nbr<VID_T, T>& nbr, const dynamic::Value& d, const char* key) {
  nbr.data = d[key].GetInt64();
}

template <typename VID_T, typename T>
typename std::enable_if<std::is_floating_point<T>::value>::type unpack_nbr(
    grape::Nbr<VID_T, T>& nbr, const dynamic::Value& d, const char* key) {
  nbr.data = d[key].GetDouble();
}

template <typename VID_T, typename T>
typename std::enable_if<std::is_same<std::string, T>::value>::type unpack_nbr(
    grape::Nbr<VID_T, T>& nbr, const dynamic::Value& d, const char* key) {
  nbr.data = d[key].GetString();
}

template <typename VID_T, typename T>
typename std::enable_if<std::is_same<bool, T>::value>::type unpack_nbr(
    grape::Nbr<VID_T, T>& nbr, const dynamic::Value& d, const char* key) {
  nbr.data = d[key].GetBool();
}

template <typename VID_T, typename T>
typename std::enable_if<std::is_same<grape::EmptyType, T>::value>::type
unpack_nbr(grape::Nbr<VID_T, T>& nbr, const dynamic::Value& d,
           const std::string& key) {
  return;
}

#define SET_PROJECTED_POC_NBR                                            \
  void set_nbr() {                                                       \
    unpack_nbr<VID_T, EDATA_T>(internal_nbr, current_->data, prop_key_); \
    internal_nbr.neighbor = current_->neighbor;                          \
  }

/**
 * @brief This is an internal representation of neighbor vertices using
 * std::map.
 *
 * @tparam EDATA_T Data type of edge
 */
template <typename VID_T, typename EDATA_T>
class AdjList {
  using NbrT = grape::Nbr<VID_T, dynamic::Value>;
  using InternalNbrT = grape::Nbr<VID_T, EDATA_T>;

 public:
  AdjList() = default;
  AdjList(NbrT* b, NbrT* e, const std::string& prop_key)
      : begin_(b), end_(e), prop_key_(prop_key.data()) {}

  ~AdjList() = default;

  inline bool Empty() const { return begin_ == end_; }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const { return end_ - begin_; }

  class iterator {
    using pointer_type = InternalNbrT*;
    using reference_type = InternalNbrT&;

   private:
    NbrT* current_;
    const char* prop_key_;
    InternalNbrT internal_nbr;

    SET_PROJECTED_POC_NBR

   public:
    iterator() = default;
    iterator(NbrT* c, const char* prop_key) noexcept
        : current_(c), prop_key_(prop_key) {}

    reference_type operator*() noexcept {
      set_nbr();
      return internal_nbr;
    }

    pointer_type operator->() noexcept {
      set_nbr();
      return &internal_nbr;
    }

    iterator& operator++() noexcept {
      ++current_;
      return *this;
    }

    iterator operator++(int) noexcept {
      return iterator(current_++, prop_key_);
    }

    iterator& operator--() noexcept {
      --current_;
      return *this;
    }

    iterator operator--(int) noexcept {
      return iterator(current_--, prop_key_);
    }

    iterator operator+(size_t offset) noexcept {
      return iterator(current_ + offset, prop_key_);
    }

    bool operator==(const iterator& rhs) noexcept {
      return current_ == rhs.current_;
    }

    bool operator!=(const iterator& rhs) noexcept {
      return current_ != rhs.current_;
    }
  };

  class const_iterator {
    using pointer_type = const InternalNbrT*;
    using reference_type = const InternalNbrT&;

   private:
    const NbrT* current_;
    const char* prop_key_;
    InternalNbrT internal_nbr;

    SET_PROJECTED_POC_NBR

   public:
    const_iterator() = default;
    const_iterator(const NbrT* c, const char* prop_key) noexcept
        : current_(c), prop_key_(prop_key) {}

    reference_type operator*() noexcept {
      set_nbr();
      return internal_nbr;
    }

    pointer_type operator->() noexcept {
      set_nbr();
      return &internal_nbr;
    }

    const_iterator& operator++() noexcept {
      ++current_;
      return *this;
    }

    const_iterator operator++(int) noexcept {
      return const_iterator(current_++, prop_key_);
    }

    const_iterator& operator--() noexcept {
      --current_;
      return *this;
    }

    const_iterator operator--(int) noexcept {
      return const_iterator(current_--, prop_key_);
    }

    const_iterator operator+(size_t offset) noexcept {
      return const_iterator(current_ + offset, prop_key_);
    }

    bool operator==(const const_iterator& rhs) noexcept {
      return current_ == rhs.current_;
    }

    bool operator!=(const const_iterator& rhs) noexcept {
      return current_ != rhs.current_;
    }
  };

  iterator begin() { return iterator(begin_, prop_key_); }

  iterator end() { return iterator(end_, prop_key_); }

  iterator begin() const { return const_iterator(begin_, prop_key_); }

  iterator end() const { return const_iterator(end_, prop_key_); }

  bool empty() const { return begin_ == end_; }

 private:
  NbrT* begin_;
  NbrT* end_;
  const char* prop_key_;
};

/**
 * @brief primitive adj linked list
 * @tparam EDATA_T
 */
template <typename VID_T, typename EDATA_T>
class ConstAdjList {
  using NbrT = grape::Nbr<VID_T, dynamic::Value>;
  using InternalNbrT = grape::Nbr<VID_T, EDATA_T>;

 public:
  ConstAdjList() = default;
  ConstAdjList(const NbrT* b, const NbrT* e, const std::string& prop_key)
      : begin_(b), end_(e), prop_key_(prop_key.data()) {}

  ~ConstAdjList() = default;

  inline bool Empty() const { return begin_ == end_; }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const { return end_ - begin_; }

  class const_iterator {
    using pointer_type = const InternalNbrT*;
    using reference_type = const InternalNbrT&;

   private:
    const NbrT* current_;
    const char* prop_key_;
    InternalNbrT internal_nbr;

    SET_PROJECTED_POC_NBR

   public:
    const_iterator() = default;
    const_iterator(const NbrT* c, const char* prop_key) noexcept
        : current_(c), prop_key_(prop_key) {}

    reference_type operator*() noexcept {
      set_nbr();
      return internal_nbr;
    }

    pointer_type operator->() noexcept {
      set_nbr();
      return &internal_nbr;
    }

    const_iterator& operator++() noexcept {
      ++current_;
      return *this;
    }

    const_iterator operator++(int) noexcept {
      return const_iterator(current_++, prop_key_);
    }

    const_iterator& operator--() noexcept {
      --current_;
      return *this;
    }

    const_iterator operator--(int) noexcept {
      return const_iterator(current_--, prop_key_);
    }

    const_iterator operator+(size_t offset) noexcept {
      return const_iterator(current_ + offset, prop_key_);
    }

    bool operator==(const const_iterator& rhs) noexcept {
      return current_ == rhs.current_;
    }

    bool operator!=(const const_iterator& rhs) noexcept {
      return current_ != rhs.current_;
    }
  };

  const_iterator begin() { return const_iterator(begin_, prop_key_); }

  const_iterator end() { return const_iterator(end_, prop_key_); }

  bool empty() const { return begin_ == end_; }

 private:
  const NbrT* begin_;
  const NbrT* end_;
  const char* prop_key_;
};

}  // namespace dynamic_projected_fragment_impl

/**
 * @brief A wrapper class of DynamicFragment.
 * Inheritance does not work because of different return type of some methods.
 * We forward most of methods to DynamicFragment but enact
 * GetIncoming(Outgoing)AdjList, Get(Set)Data...
 *
 * @tparam VDATA_T The type of data attached with the vertex
 * @tparam EDATA_T The type of data attached with the edge
 */
template <typename VDATA_T, typename EDATA_T>
class DynamicProjectedFragment {
 public:
  using fragment_t = DynamicFragment;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using vertex_map_t = fragment_t::vertex_map_t;
  using adj_list_t = dynamic_projected_fragment_impl::AdjList<vid_t, edata_t>;
  using const_adj_list_t =
      dynamic_projected_fragment_impl::ConstAdjList<vid_t, edata_t>;
  using inner_vertices_t = typename fragment_t::inner_vertices_t;
  using outer_vertices_t = typename fragment_t::outer_vertices_t;
  using vertices_t = typename fragment_t::vertices_t;
  using sub_vertices_t = typename fragment_t::sub_vertices_t;
  template <typename DATA_T>
  using vertex_array_t = typename fragment_t::vertex_array_t<DATA_T>;

  template <typename DATA_T>
  using inner_vertex_array_t =
      typename fragment_t::inner_vertex_array_t<DATA_T>;

  template <typename DATA_T>
  using outer_vertex_array_t =
      typename fragment_t::outer_vertex_array_t<DATA_T>;

  using vertex_range_t = inner_vertices_t;

  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  DynamicProjectedFragment(fragment_t* frag, const std::string& v_prop_key,
                           const std::string& e_prop_key)
      : fragment_(frag),
        v_prop_key_(std::move(v_prop_key)),
        e_prop_key_(std::move(e_prop_key)) {}

  static std::shared_ptr<DynamicProjectedFragment<VDATA_T, EDATA_T>> Project(
      const std::shared_ptr<DynamicFragment>& frag, const std::string& v_prop,
      const std::string& e_prop) {
    return std::make_shared<DynamicProjectedFragment>(frag.get(), v_prop,
                                                      e_prop);
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) {
    fragment_->PrepareToRunApp(comm_spec, conf);
  }

  inline fid_t fid() const { return fragment_->fid(); }

  inline fid_t fnum() const { return fragment_->fnum(); }

  inline bool directed() const { return fragment_->directed(); }

  std::shared_ptr<vertex_map_t> GetVertexMap() const {
    return fragment_->GetVertexMap();
  }

  inline const vertices_t& Vertices() const { return fragment_->Vertices(); }

  inline const inner_vertices_t& InnerVertices() const {
    return fragment_->InnerVertices();
  }

  inline const outer_vertices_t& OuterVertices() const {
    return fragment_->OuterVertices();
  }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    return fragment_->GetVertex(oid, v);
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
    assert(fragment_->IsInnerVertex(v));
    auto data = fragment_->GetData(v);
    return dynamic_projected_fragment_impl::unpack_dynamic<vdata_t>(
        data, v_prop_key_);
  }

  inline vid_t GetInnerVerticesNum() const {
    return fragment_->GetInnerVerticesNum();
  }

  inline vid_t GetOuterVerticesNum() const {
    return fragment_->GetOuterVerticesNum();
  }

  inline vid_t GetVerticesNum() const { return fragment_->GetVerticesNum(); }

  size_t GetTotalVerticesNum() const {
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
    return fragment_->GetInnerVertex(oid, v);
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    return fragment_->GetOuterVertexId(v);
  }

  inline oid_t Gid2Oid(const vid_t& gid) const {
    return fragment_->Gid2Oid(gid);
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

  inline bool IsAliveInnerVertex(const vertex_t& v) const {
    return fragment_->IsAliveInnerVertex(v);
  }

  inline bool HasChild(const vertex_t& v) const {
    return fragment_->HasChild(v);
  }

  inline bool HasParent(const vertex_t& v) const {
    return fragment_->HasParent(v);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return adj_list_t(fragment_->get_oe_begin(v), fragment_->get_oe_end(v),
                        e_prop_key_);
    }
    return adj_list_t(fragment_->get_ie_begin(v), fragment_->get_ie_end(v),
                      e_prop_key_);
  }

  inline const_adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return const_adj_list_t(fragment_->get_oe_begin(v),
                              fragment_->get_oe_end(v), e_prop_key_);
    }
    return const_adj_list_t(fragment_->get_ie_begin(v),
                            fragment_->get_ie_end(v), e_prop_key_);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return adj_list_t(fragment_->get_oe_begin(v), fragment_->get_oe_end(v),
                      e_prop_key_);
  }

  inline const_adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return const_adj_list_t(fragment_->get_oe_begin(v),
                            fragment_->get_oe_end(v), e_prop_key_);
  }

  inline adj_list_t GetIncomingInnerVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return adj_list_t(fragment_->get_oe_begin(v), fragment_->oespliter_[v],
                        e_prop_key_);
    }
    return adj_list_t(fragment_->get_ie_begin(v), fragment_->iespliter_[v],
                      e_prop_key_);
  }

  inline const_adj_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return const_adj_list_t(fragment_->get_oe_begin(v),
                              fragment_->oespliter_[v], e_prop_key_);
    }
    return const_adj_list_t(fragment_->get_ie_begin(v),
                            fragment_->iespliter_[v], e_prop_key_);
  }

  inline adj_list_t GetIncomingOuterVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return adj_list_t(fragment_->oespliter_[v], fragment_->get_oe_end(v),
                        e_prop_key_);
    }
    return adj_list_t(fragment_->iespliter_[v], fragment_->get_ie_end(v),
                      e_prop_key_);
  }

  inline const_adj_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return const_adj_list_t(fragment_->oespliter_[v],
                              fragment_->get_oe_end(v), e_prop_key_);
    }
    return const_adj_list_t(fragment_->iespliter_[v], fragment_->get_ie_end(v),
                            e_prop_key_);
  }

  inline adj_list_t GetOutgoingInnerVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return adj_list_t(fragment_->get_oe_begin(v), fragment_->oespliter_[v],
                      e_prop_key_);
  }

  inline const_adj_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return const_adj_list_t(fragment_->get_oe_begin(v),
                            fragment_->oespliter_[v], e_prop_key_);
  }

  inline adj_list_t GetOutgoingOuterVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return adj_list_t(fragment_->oespliter_[v], fragment_->get_oe_end(v),
                      e_prop_key_);
  }

  inline const_adj_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return const_adj_list_t(fragment_->oespliter_[v], fragment_->get_oe_end(v),
                            e_prop_key_);
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    return fragment_->GetLocalOutDegree(v);
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    return fragment_->GetLocalInDegree(v);
  }

  inline grape::DestList IEDests(const vertex_t& v) const {
    return fragment_->IEDests(v);
  }

  inline grape::DestList OEDests(const vertex_t& v) const {
    return fragment_->OEDests(v);
  }

  inline grape::DestList IOEDests(const vertex_t& v) const {
    return fragment_->IOEDests(v);
  }

  inline const std::vector<vertex_t>& MirrorVertices(fid_t fid) const {
    return fragment_->MirrorVertices(fid);
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    return fragment_->Oid2Gid(oid, gid);
  }

  inline bool HasNode(const oid_t& node) const {
    return fragment_->HasNode(node);
  }

 private:
  fragment_t* fragment_;
  std::string v_prop_key_;
  std::string e_prop_key_;

  static_assert(std::is_same<int, VDATA_T>::value ||
                    std::is_same<int64_t, VDATA_T>::value ||
                    std::is_same<double, VDATA_T>::value ||
                    std::is_same<std::string, VDATA_T>::value ||
                    std::is_same<grape::EmptyType, VDATA_T>::value,
                "unsupported type");

  static_assert(std::is_same<int, EDATA_T>::value ||
                    std::is_same<int64_t, EDATA_T>::value ||
                    std::is_same<double, EDATA_T>::value ||
                    std::is_same<std::string, EDATA_T>::value ||
                    std::is_same<grape::EmptyType, EDATA_T>::value,
                "unsupported type");
};

template <>
class DynamicProjectedFragment<grape::EmptyType, grape::EmptyType> {
 public:
  using fragment_t = DynamicFragment;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using vdata_t = grape::EmptyType;
  using edata_t = grape::EmptyType;
  using vertex_map_t = fragment_t::vertex_map_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using const_adj_list_t = typename fragment_t::const_adj_list_t;
  using inner_vertices_t = typename fragment_t::inner_vertices_t;
  using outer_vertices_t = typename fragment_t::outer_vertices_t;
  using vertices_t = typename fragment_t::vertices_t;
  using sub_vertices_t = typename fragment_t::sub_vertices_t;
  template <typename DATA_T>
  using vertex_array_t = typename fragment_t::vertex_array_t<DATA_T>;

  template <typename DATA_T>
  using inner_vertex_array_t =
      typename fragment_t::inner_vertex_array_t<DATA_T>;

  template <typename DATA_T>
  using outer_vertex_array_t =
      typename fragment_t::outer_vertex_array_t<DATA_T>;

  using vertex_range_t = inner_vertices_t;

  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  DynamicProjectedFragment(fragment_t* frag, const std::string& v_prop_key,
                           const std::string& e_prop_key)
      : fragment_(frag),
        v_prop_key_(std::move(v_prop_key)),
        e_prop_key_(std::move(e_prop_key)) {}

  static std::shared_ptr<DynamicProjectedFragment<vdata_t, edata_t>> Project(
      const std::shared_ptr<DynamicFragment>& frag, const std::string& v_prop,
      const std::string& e_prop) {
    return std::make_shared<DynamicProjectedFragment>(frag.get(), v_prop,
                                                      e_prop);
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) {
    fragment_->PrepareToRunApp(comm_spec, conf);
  }

  inline fid_t fid() const { return fragment_->fid(); }

  inline fid_t fnum() const { return fragment_->fnum(); }

  inline bool directed() const { return fragment_->directed(); }

  std::shared_ptr<vertex_map_t> GetVertexMap() const {
    return fragment_->GetVertexMap();
  }

  inline const vertices_t& Vertices() const { return fragment_->Vertices(); }

  inline const inner_vertices_t& InnerVertices() const {
    return fragment_->InnerVertices();
  }

  inline const outer_vertices_t& OuterVertices() const {
    return fragment_->OuterVertices();
  }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    return fragment_->GetVertex(oid, v);
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
    assert(fragment_->IsInnerVertex(v));
    return grape::EmptyType();
  }

  inline vid_t GetInnerVerticesNum() const {
    return fragment_->GetInnerVerticesNum();
  }

  inline vid_t GetOuterVerticesNum() const {
    return fragment_->GetOuterVerticesNum();
  }

  inline vid_t GetVerticesNum() const { return fragment_->GetVerticesNum(); }

  size_t GetTotalVerticesNum() const {
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
    return fragment_->GetInnerVertex(oid, v);
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    return fragment_->GetOuterVertexId(v);
  }

  inline oid_t Gid2Oid(const vid_t& gid) const {
    return fragment_->Gid2Oid(gid);
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

  inline bool IsAliveInnerVertex(const vertex_t& v) const {
    return fragment_->IsAliveInnerVertex(v);
  }

  inline bool HasChild(const vertex_t& v) const {
    return fragment_->HasChild(v);
  }

  inline bool HasParent(const vertex_t& v) const {
    return fragment_->HasParent(v);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) {
    if (!fragment_->directed_) {
      return adj_list_t(fragment_->get_oe_begin(v), fragment_->get_oe_end(v));
    }
    return adj_list_t(fragment_->get_ie_begin(v), fragment_->get_ie_end(v));
  }

  inline const_adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    if (!fragment_->directed()) {
      return const_adj_list_t(fragment_->get_oe_begin(v),
                              fragment_->get_oe_end(v));
    }
    return const_adj_list_t(fragment_->get_ie_begin(v),
                            fragment_->get_ie_end(v));
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) {
    return adj_list_t(fragment_->get_oe_begin(v), fragment_->get_oe_end(v));
  }

  inline const_adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    return const_adj_list_t(fragment_->get_oe_begin(v),
                            fragment_->get_oe_end(v));
  }

  inline adj_list_t GetIncomingInnerVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return adj_list_t(fragment_->get_oe_begin(v), fragment_->oespliter_[v]);
    }
    return adj_list_t(fragment_->get_ie_begin(v), fragment_->iespliter_[v]);
  }

  inline const_adj_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return const_adj_list_t(fragment_->get_oe_begin(v),
                              fragment_->oespliter_[v]);
    }
    return const_adj_list_t(fragment_->get_ie_begin(v),
                            fragment_->iespliter_[v]);
  }

  inline adj_list_t GetIncomingOuterVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return adj_list_t(fragment_->oespliter_[v], fragment_->get_oe_end(v));
    }
    return adj_list_t(fragment_->iespliter_[v], fragment_->get_ie_end(v));
  }

  inline const_adj_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    if (!fragment_->directed()) {
      return const_adj_list_t(fragment_->oespliter_[v],
                              fragment_->get_oe_end(v));
    }
    return const_adj_list_t(fragment_->iespliter_[v], fragment_->get_ie_end(v));
  }

  inline adj_list_t GetOutgoingInnerVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return adj_list_t(fragment_->get_oe_begin(v), fragment_->oespliter_[v]);
  }

  inline const_adj_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return const_adj_list_t(fragment_->get_oe_begin(v),
                            fragment_->oespliter_[v]);
  }

  inline adj_list_t GetOutgoingOuterVertexAdjList(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return adj_list_t(fragment_->oespliter_[v], fragment_->get_oe_end(v));
  }

  inline const_adj_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) const {
    assert(IsInnerVertex(v));
    return const_adj_list_t(fragment_->oespliter_[v], fragment_->get_oe_end(v));
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    return fragment_->GetLocalOutDegree(v);
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    return fragment_->GetLocalInDegree(v);
  }

  inline grape::DestList IEDests(const vertex_t& v) const {
    return fragment_->IEDests(v);
  }

  inline grape::DestList OEDests(const vertex_t& v) const {
    return fragment_->OEDests(v);
  }

  inline grape::DestList IOEDests(const vertex_t& v) const {
    return fragment_->IOEDests(v);
  }

  inline const std::vector<vertex_t>& MirrorVertices(fid_t fid) const {
    return fragment_->MirrorVertices(fid);
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    return fragment_->Oid2Gid(oid, gid);
  }

  inline bool HasNode(const oid_t& node) const {
    return fragment_->HasNode(node);
  }

 private:
  fragment_t* fragment_;
  std::string v_prop_key_;
  std::string e_prop_key_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_PROJECTED_FRAGMENT_H_
