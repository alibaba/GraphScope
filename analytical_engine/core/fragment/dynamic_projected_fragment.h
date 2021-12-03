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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/config.h"
#include "core/fragment/dynamic_fragment.h"
#include "proto/types.pb.h"

namespace gs {

namespace dynamic_projected_fragment_impl {
template <typename T>
typename std::enable_if<!std::is_same<T, grape::EmptyType>::value>::type
pack_dynamic(folly::dynamic& d, const T& val) {
  d = folly::dynamic(val);
}

template <typename T>
typename std::enable_if<std::is_same<T, grape::EmptyType>::value>::type
pack_dynamic(folly::dynamic& d, const T& val) {
  d = folly::dynamic(nullptr);
}

/**
 * @brief A specialized UnpackDynamicVData for int32_t type
 */
template <typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type unpack_dynamic(
    const folly::dynamic& data, const std::string& v_prop_key) {
  return data.at(v_prop_key).asInt();
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
unpack_dynamic(const folly::dynamic& data, const std::string& v_prop_key) {
  return data.at(v_prop_key).asDouble();
}

template <typename T>
typename std::enable_if<std::is_same<T, bool>::value, T>::type unpack_dynamic(
    const folly::dynamic& data, const std::string& v_prop_key) {
  return data.at(v_prop_key).asBool();
}

template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type
unpack_dynamic(const folly::dynamic& data, const std::string& v_prop_key) {
  return data.at(v_prop_key).asString();
}

template <typename T>
typename std::enable_if<std::is_same<T, grape::EmptyType>::value, T>::type
unpack_dynamic(const folly::dynamic& data, const std::string& v_prop_key) {
  return grape::EmptyType();
}

template <typename T>
typename std::enable_if<std::is_integral<T>::value>::type unpack_nbr(
    dynamic_fragment_impl::Nbr<T>& nbr, const folly::dynamic& d,
    const std::string& key) {
  nbr.set_data(d.at(key).asInt());
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value>::type unpack_nbr(
    dynamic_fragment_impl::Nbr<T>& nbr, const folly::dynamic& d,
    const std::string& key) {
  nbr.set_data(d.at(key).asDouble());
}

template <typename T>
typename std::enable_if<std::is_same<std::string, T>::value>::type unpack_nbr(
    dynamic_fragment_impl::Nbr<T>& nbr, const folly::dynamic& d,
    const std::string& key) {
  nbr.set_data(d.at(key).asString());
}

template <typename T>
typename std::enable_if<std::is_same<bool, T>::value>::type unpack_nbr(
    dynamic_fragment_impl::Nbr<T>& nbr, const folly::dynamic& d,
    const std::string& key) {
  nbr.set_data(d.at(key).asBool());
}

template <typename T>
typename std::enable_if<std::is_same<grape::EmptyType, T>::value>::type
unpack_nbr(dynamic_fragment_impl::Nbr<T>& nbr, const folly::dynamic& d,
           const std::string& key) {
  nbr.set_data(grape::EmptyType());
}

#define SET_PROJECTED_NBR                               \
  void set_nbr() {                                      \
    auto original_nbr = map_current_->second;           \
    auto& data = original_nbr.data();                   \
                                                        \
    unpack_nbr<EDATA_T>(internal_nbr, data, prop_key_); \
    internal_nbr.set_neighbor(original_nbr.neighbor()); \
    auto v = internal_nbr.neighbor();                   \
    if (v.GetValue() >= ivnum_) {                       \
      v.SetValue(ivnum_ + id_mask_ - v.GetValue());     \
    }                                                   \
    internal_nbr.set_neighbor(v);                       \
  }

/**
 * @brief This is an internal representation of neighbor vertices using
 * std::map.
 *
 * @tparam EDATA_T Data type of edge
 */
template <typename EDATA_T>
class ProjectedAdjLinkedList {
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using NbrT = dynamic_fragment_impl::Nbr<folly::dynamic>;
  using ProjectedNbrT = dynamic_fragment_impl::Nbr<EDATA_T>;

 public:
  ProjectedAdjLinkedList() = default;
  ProjectedAdjLinkedList(
      VID_T id_mask, VID_T ivnum, std::string prop_key,
      typename std::map<VID_T, NbrT>::iterator map_iter_begin,
      typename std::map<VID_T, NbrT>::iterator map_iter_end)
      : id_mask_(id_mask),
        ivnum_(ivnum),
        prop_key_(std::move(prop_key)),
        map_iter_begin_(map_iter_begin),
        map_iter_end_(map_iter_end) {}
  ~ProjectedAdjLinkedList() = default;

  inline bool Empty() const { return map_iter_begin_ == map_iter_end_; }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const {
    return std::distance(map_iter_begin_, map_iter_end_);
  }

  class iterator {
    using pointer_type = ProjectedNbrT*;
    using reference_type = ProjectedNbrT&;

    SET_PROJECTED_NBR

   public:
    iterator() = default;
    iterator(VID_T id_mask, VID_T ivnum, std::string prop_key,
             typename std::map<VID_T, NbrT>::iterator map_current) noexcept
        : id_mask_(id_mask),
          ivnum_(ivnum),
          prop_key_(std::move(prop_key)),
          map_current_(map_current) {}

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
    VID_T id_mask_;
    VID_T ivnum_;
    std::string prop_key_;
    ProjectedNbrT internal_nbr;
    typename std::map<VID_T, NbrT>::iterator map_current_;
  };

  class const_iterator {
    using pointer_type = const ProjectedNbrT*;
    using reference_type = const ProjectedNbrT&;

    SET_PROJECTED_NBR

   public:
    const_iterator() = default;
    const_iterator(
        VID_T id_mask, VID_T ivnum, std::string prop_key,
        typename std::map<VID_T, NbrT>::iterator map_current) noexcept
        : id_mask_(id_mask),
          ivnum_(ivnum),
          prop_key_(std::move(prop_key)),
          map_current_(map_current) {}

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
    std::string prop_key_;
    ProjectedNbrT internal_nbr;
    typename std::map<VID_T, NbrT>::iterator map_current_;
  };

  iterator begin() {
    return iterator(id_mask_, ivnum_, prop_key_, map_iter_begin_);
  }

  iterator end() {
    return iterator(id_mask_, ivnum_, prop_key_, map_iter_end_);
  }

  const_iterator cbegin() const {
    return const_iterator(id_mask_, ivnum_, prop_key_, map_iter_begin_);
  }
  const_iterator cend() const {
    return const_iterator(id_mask_, ivnum_, prop_key_, map_iter_end_);
  }

  bool empty() const { return map_iter_begin_ == map_iter_end_; }

 private:
  VID_T id_mask_{};
  VID_T ivnum_{};
  std::string prop_key_;
  typename std::map<VID_T, NbrT>::iterator map_iter_begin_;
  typename std::map<VID_T, NbrT>::iterator map_iter_end_;
};

/**
 * @brief primitive adj linked list
 * @tparam EDATA_T
 */
template <typename EDATA_T>
class ConstProjectedAdjLinkedList {
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using NbrT = dynamic_fragment_impl::Nbr<folly::dynamic>;
  using ProjectedNbrT = dynamic_fragment_impl::Nbr<EDATA_T>;

 public:
  ConstProjectedAdjLinkedList() = default;
  ConstProjectedAdjLinkedList(
      VID_T id_mask, VID_T ivnum, std::string prop_key,
      typename std::map<VID_T, NbrT>::const_iterator map_iter_begin,
      typename std::map<VID_T, NbrT>::const_iterator map_iter_end)
      : id_mask_(id_mask),
        ivnum_(ivnum),
        prop_key_(std::move(prop_key)),
        map_iter_begin_(map_iter_begin),
        map_iter_end_(map_iter_end) {}
  ~ConstProjectedAdjLinkedList() = default;

  inline bool Empty() const { return map_iter_begin_ == map_iter_end_; }

  inline bool NotEmpty() const { return !Empty(); }

  inline size_t Size() const {
    return std::distance(map_iter_begin_, map_iter_end_);
  }

  class const_iterator {
    using pointer_type = const ProjectedNbrT*;
    using reference_type = const ProjectedNbrT&;

    SET_PROJECTED_NBR

   public:
    const_iterator() = default;
    const_iterator(
        VID_T id_mask, VID_T ivnum, std::string prop_key,
        typename std::map<VID_T, NbrT>::const_iterator map_current) noexcept
        : id_mask_(id_mask),
          ivnum_(ivnum),
          prop_key_(std::move(prop_key)),
          map_current_(map_current) {}

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
    VID_T id_mask_{};
    VID_T ivnum_{};
    std::string prop_key_;
    ProjectedNbrT internal_nbr;
    typename std::map<VID_T, NbrT>::const_iterator map_current_;
  };

  const_iterator begin() const {
    return const_iterator(id_mask_, ivnum_, prop_key_, map_iter_begin_);
  }
  const_iterator end() const {
    return const_iterator(id_mask_, ivnum_, prop_key_, map_iter_end_);
  }

  bool empty() const { return map_iter_begin_ == map_iter_end_; }

 private:
  VID_T id_mask_{};
  VID_T ivnum_{};
  std::string prop_key_;
  typename std::map<VID_T, NbrT>::const_iterator map_iter_begin_;
  typename std::map<VID_T, NbrT>::const_iterator map_iter_end_;
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
  using projected_adj_linked_list_t =
      dynamic_projected_fragment_impl::ProjectedAdjLinkedList<edata_t>;
  using const_projected_adj_linked_list_t =
      dynamic_projected_fragment_impl::ConstProjectedAdjLinkedList<edata_t>;
  using vertex_range_t = typename fragment_t::vertex_range_t;
  template <typename DATA_T>
  using vertex_array_t = typename fragment_t::vertex_array_t<DATA_T>;
  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  DynamicProjectedFragment(fragment_t* frag, std::string v_prop_key,
                           std::string e_prop_key)
      : fragment_(frag),
        v_prop_key_(std::move(v_prop_key)),
        e_prop_key_(std::move(e_prop_key)) {}

  static std::shared_ptr<DynamicProjectedFragment<VDATA_T, EDATA_T>> Project(
      const std::shared_ptr<DynamicFragment>& frag, const std::string& v_prop,
      const std::string& e_prop) {
    return std::make_shared<DynamicProjectedFragment>(frag.get(), v_prop,
                                                      e_prop);
  }

  void PrepareToRunApp(grape::MessageStrategy strategy, bool need_split_edges) {
    fragment_->PrepareToRunApp(strategy, need_split_edges);
  }

  inline fid_t fid() const { return fragment_->fid_; }

  inline fid_t fnum() const { return fragment_->fnum_; }

  inline vid_t id_mask() const { return fragment_->id_mask_; }

  inline int fid_offset() const { return fragment_->fid_offset_; }

  inline bool directed() const { return fragment_->directed(); }

  inline vertex_range_t Vertices() const { return fragment_->Vertices(); }

  inline vertex_range_t InnerVertices() const {
    return fragment_->InnerVertices();
  }

  inline vertex_range_t OuterVertices() const {
    return fragment_->OuterVertices();
  }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    return fragment_->GetVertex(oid, v);
  }

  inline const vid_t* GetOuterVerticesGid() const {
    return fragment_->GetOuterVerticesGid();
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
    auto data = fragment_->vdata()[v.GetValue()];
    return dynamic_projected_fragment_impl::unpack_dynamic<vdata_t>(
        data, v_prop_key_);
  }

  inline void SetData(const vertex_t& v, const vdata_t& val) {
    assert(fragment_->IsInnerVertex(v));
    dynamic_projected_fragment_impl::pack_dynamic(
        fragment_->vdata()[v.GetValue()][v_prop_key_], val);
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

  inline bool GetOuterVertex(const oid_t& oid, vertex_t& v) const {
    return fragment_->GetOuterVertex(oid, v);
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
    return fragment_->Oid2Gid(oid, gid);
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

  inline bool IsAliveVertex(const vertex_t& v) const {
    return fragment_->IsAliveVertex(v);
  }

  inline bool IsAliveInnerVertex(const vertex_t& v) const {
    return fragment_->IsAliveInnerVertex(v);
  }

  inline bool IsAliveOuterVertex(const vertex_t& v) const {
    return fragment_->IsAliveOuterVertex(v);
  }

  inline bool HasChild(const vertex_t& v) const {
    return fragment_->HasChild(v);
  }

  inline bool HasParent(const vertex_t& v) const {
    return fragment_->HasParent(v);
  }

  inline projected_adj_linked_list_t GetIncomingAdjList(const vertex_t& v) {
    int32_t ie_pos;
    if (fragment_->duplicated() && fragment_->IsOuterVertex(v)) {
      ie_pos = fragment_->outer_ie_pos()[v.GetValue() - fragment_->ivnum()];
    } else {
      ie_pos = fragment_->inner_ie_pos()[v.GetValue()];
    }
    if (ie_pos == -1) {
      return projected_adj_linked_list_t();
    }
    return projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space()[ie_pos].begin(),
        fragment_->inner_edge_space()[ie_pos].end());
  }

  inline const_projected_adj_linked_list_t GetIncomingAdjList(
      const vertex_t& v) const {
    int32_t ie_pos;
    if (fragment_->duplicated() && fragment_->IsOuterVertex(v)) {
      ie_pos = fragment_->outer_ie_pos()[v.GetValue() - fragment_->ivnum()];
    } else {
      ie_pos = fragment_->inner_ie_pos()[v.GetValue()];
    }
    if (ie_pos == -1) {
      return const_projected_adj_linked_list_t();
    }
    return const_projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space()[ie_pos].cbegin(),
        fragment_->inner_edge_space()[ie_pos].cend());
  }

  inline projected_adj_linked_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) {
    auto ie_pos = fragment_->inner_ie_pos()[v.GetValue()];
    if (ie_pos == -1) {
      return projected_adj_linked_list_t();
    }
    return projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().InnerNbr(ie_pos).begin(),
        fragment_->inner_edge_space().InnerNbr(ie_pos).end());
  }

  inline const_projected_adj_linked_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) const {
    auto ie_pos = fragment_->inner_ie_pos()[v.GetValue()];
    if (ie_pos == -1) {
      return const_projected_adj_linked_list_t();
    }
    return const_projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().InnerNbr(ie_pos).cbegin(),
        fragment_->inner_edge_space().InnerNbr(ie_pos).cend());
  }

  inline projected_adj_linked_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) {
    auto ie_pos = fragment_->inner_ie_pos()[v.GetValue()];
    if (ie_pos == -1) {
      return projected_adj_linked_list_t();
    }
    return projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().OuterNbr(ie_pos).begin(),
        fragment_->inner_edge_space().OuterNbr(ie_pos).end());
  }

  inline const_projected_adj_linked_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) const {
    auto ie_pos = fragment_->inner_ie_pos()[v.GetValue()];
    if (ie_pos == -1) {
      return const_projected_adj_linked_list_t();
    }
    return const_projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().OuterNbr(ie_pos).cbegin(),
        fragment_->inner_edge_space().OuterNbr(ie_pos).cend());
  }

  inline projected_adj_linked_list_t GetOutgoingAdjList(const vertex_t& v) {
    int32_t oe_pos;
    if (fragment_->duplicated() && fragment_->IsOuterVertex(v)) {
      oe_pos = fragment_->outer_oe_pos()[v.GetValue() - fragment_->ivnum()];
    } else {
      oe_pos = fragment_->inner_oe_pos()[v.GetValue()];
    }
    if (oe_pos == -1) {
      return projected_adj_linked_list_t();
    }
    return projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space()[oe_pos].begin(),
        fragment_->inner_edge_space()[oe_pos].end());
  }

  inline const_projected_adj_linked_list_t GetOutgoingAdjList(
      const vertex_t& v) const {
    int32_t oe_pos;
    if (fragment_->duplicated() && fragment_->IsOuterVertex(v)) {
      oe_pos = fragment_->outer_oe_pos()[v.GetValue() - fragment_->ivnum()];
    } else {
      oe_pos = fragment_->inner_oe_pos()[v.GetValue()];
    }
    if (oe_pos == -1) {
      return const_projected_adj_linked_list_t();
    }
    return const_projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space()[oe_pos].cbegin(),
        fragment_->inner_edge_space()[oe_pos].cend());
  }

  inline projected_adj_linked_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) {
    auto oe_pos = fragment_->inner_oe_pos()[v.GetValue()];
    if (oe_pos == -1) {
      return projected_adj_linked_list_t();
    }
    return projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().InnerNbr(oe_pos).begin(),
        fragment_->inner_edge_space().InnerNbr(oe_pos).end());
  }

  inline const_projected_adj_linked_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) const {
    auto oe_pos = fragment_->inner_oe_pos()[v.GetValue()];
    if (oe_pos == -1) {
      return const_projected_adj_linked_list_t();
    }
    return const_projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().InnerNbr(oe_pos).cbegin(),
        fragment_->inner_edge_space().InnerNbr(oe_pos).cend());
  }

  inline projected_adj_linked_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) {
    auto oe_pos = fragment_->inner_oe_pos()[v.GetValue()];
    if (oe_pos == -1) {
      return projected_adj_linked_list_t();
    }
    return projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().OuterNbr(oe_pos).begin(),
        fragment_->inner_edge_space().OuterNbr(oe_pos).end());
  }

  inline const_projected_adj_linked_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) const {
    auto oe_pos = fragment_->inner_oe_pos()[v.GetValue()];
    if (oe_pos == -1) {
      return const_projected_adj_linked_list_t();
    }
    return const_projected_adj_linked_list_t(
        fragment_->id_mask(), fragment_->ivnum(), e_prop_key_,
        fragment_->inner_edge_space().OuterNbr(oe_pos).cbegin(),
        fragment_->inner_edge_space().OuterNbr(oe_pos).cend());
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

  bl::result<folly::dynamic::Type> GetOidType(
      const grape::CommSpec& comm_spec) const {
    return fragment_->GetOidType(comm_spec);
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

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_PROJECTED_FRAGMENT_H_
