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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_VIEW_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_VIEW_H_

#ifdef NETWORKX

#include <memory>
#include <string>
#include <vector>

#include "core/fragment/dynamic_fragment.h"

namespace gs {

enum class FragmentViewType { REVERSED, DIRECTED, UNDIRECTED };

FragmentViewType parse_fragment_view_type(const std::string& view_type) {
  if (view_type == "reversed") {
    return FragmentViewType::REVERSED;
  } else if (view_type == "directed") {
    return FragmentViewType::DIRECTED;
  } else if (view_type == "undirected") {
    return FragmentViewType::UNDIRECTED;
  } else {
    throw std::runtime_error("Invalid fragment view type" + view_type);
  }
}

/**
 * @brief A wrapper class of DynamicFragment to behaves as a view.
 * here implement three type of views.
 * - reversed: view of original graph with edge directions reverse, original
 *             graph is directed.
 * - directed: view of original graph with edge directed, original graph is
 *             undirected.
 * - undirected: view of original graph with edge undirected, original graph is
 *               directed.
 */
class DynamicFragmentView : public DynamicFragment {
 public:
  using fragment_t = DynamicFragment;

  DynamicFragmentView() = default;

  explicit DynamicFragmentView(fragment_t* frag,
                               const FragmentViewType& view_type)
      : fragment_(frag), view_type_(view_type) {}

  virtual ~DynamicFragmentView() = default;

  inline fid_t fid() const { return fragment_->fid(); }

  inline fid_t fnum() const { return fragment_->fnum(); }

  inline vid_t id_mask() const { return fragment_->id_mask(); }

  inline int fid_offset() const { return fragment_->fid_offset(); }

  inline size_t selfloops_num() const { return fragment_->selfloops_num(); }

  inline bool directed() const {
    switch (view_type_) {
    case FragmentViewType::DIRECTED: {
      return true;
    }
    case FragmentViewType::UNDIRECTED: {
      return false;
    }
    default:
      return fragment_->directed();
    }

    return fragment_->directed();
  }

  inline bool duplicated() const { return fragment_->duplicated(); }

  inline const vid_t* GetOuterVerticesGid() const {
    return fragment_->GetOuterVerticesGid();
  }

  inline size_t GetEdgeNum() const { return fragment_->GetEdgeNum(); }

  inline vid_t GetVerticesNum() const { return fragment_->GetVerticesNum(); }

  size_t GetTotalVerticesNum() const {
    return fragment_->GetTotalVerticesNum();
  }

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

  inline oid_t GetId(const vertex_t& v) const { return fragment_->GetId(v); }

  inline fid_t GetFragId(const vertex_t& u) const {
    return fragment_->GetFragId(u);
  }

  inline const vdata_t& GetData(const vertex_t& v) const {
    return fragment_->GetData(v);
  }

  inline void SetData(const vertex_t& v, const vdata_t& val) {
    return fragment_->SetData(v, val);
  }

  inline bool HasChild(const vertex_t& v) const {
    return fragment_->HasChild(v);
  }

  inline bool HasParent(const vertex_t& v) const {
    return fragment_->HasParent(v);
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    if (view_type_ == FragmentViewType::REVERSED) {
      return fragment_->GetLocalInDegree(v);
    }
    return fragment_->GetLocalOutDegree(v);
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    if (view_type_ == FragmentViewType::REVERSED ||
        view_type_ == FragmentViewType::DIRECTED) {
      return fragment_->GetLocalOutDegree(v);
    }
    return fragment_->GetLocalInDegree(v);
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return fragment_->Gid2Vertex(gid, v);
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const {
    return fragment_->Vertex2Gid(v);
  }

  inline vid_t GetInnerVerticesNum() const {
    return fragment_->GetInnerVerticesNum();
  }

  inline vid_t GetOuterVerticesNum() const {
    return fragment_->GetOuterVerticesNum();
  }

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

  inline grape::DestList IEDests(const vertex_t& v) const {
    return fragment_->IEDests(v);
  }

  inline grape::DestList OEDests(const vertex_t& v) const {
    return fragment_->OEDests(v);
  }

  inline grape::DestList IOEDests(const vertex_t& v) const {
    return fragment_->IOEDests(v);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) {
    if (view_type_ == FragmentViewType::REVERSED) {
      return fragment_->GetIncomingAdjList(v);
    }
    return fragment_->GetOutgoingAdjList(v);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) {
    if (view_type_ == FragmentViewType::REVERSED ||
        view_type_ == FragmentViewType::DIRECTED) {
      return fragment_->GetOutgoingAdjList(v);
    }
    return fragment_->GetIncomingAdjList(v);
  }

  inline const std::vector<vertex_t>& MirrorVertices(fid_t fid) const {
    return fragment_->MirrorVertices(fid);
  }

  void PrepareToRunApp(grape::MessageStrategy strategy, bool need_split_edges) {
    fragment_->PrepareToRunApp(strategy, need_split_edges);
  }

  inline bool HasNode(const oid_t& node) const {
    return fragment_->HasNode(node);
  }

  inline bool HasEdge(const oid_t& u, const oid_t& v) {
    if (view_type_ == FragmentViewType::REVERSED) {
      return fragment_->HasEdge(v, u);
    }
    return fragment_->HasEdge(u, v);
  }

  inline bool GetEdgeData(const oid_t& u, const oid_t& v, edata_t& ret) {
    if (view_type_ == FragmentViewType::REVERSED) {
      return fragment_->GetEdgeData(v, u, ret);
    }
    return fragment_->GetEdgeData(u, v, ret);
  }

  bl::result<folly::dynamic::Type> GetOidType(
      const grape::CommSpec& comm_spec) const {
    return fragment_->GetOidType(comm_spec);
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

 private:
  inline vid_t ivnum() { return fragment_->ivnum(); }

  inline Array<vdata_t, grape::Allocator<vdata_t>>& vdata() {
    return fragment_->vdata();
  }

  inline Array<int32_t, grape::Allocator<int32_t>>& inner_ie_pos() {
    if (view_type_ == FragmentViewType::REVERSED ||
        view_type_ == FragmentViewType::DIRECTED) {
      return fragment_->inner_oe_pos();
    }
    return fragment_->inner_ie_pos();
  }

  inline Array<int32_t, grape::Allocator<int32_t>>& inner_oe_pos() {
    if (view_type_ == FragmentViewType::REVERSED) {
      return fragment_->inner_ie_pos();
    }
    return fragment_->inner_oe_pos();
  }

  dynamic_fragment_impl::NbrMapSpace<edata_t>& inner_edge_space() {
    return fragment_->inner_edge_space();
  }

 private:
  fragment_t* fragment_;
  FragmentViewType view_type_;
};

}  // namespace gs
#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_VIEW_H_
