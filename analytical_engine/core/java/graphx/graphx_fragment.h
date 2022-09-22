
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H_

#define WITH_PROFILING

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"

#include "grape/grape.h"
#include "grape/utils/vertex_array.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/hashmap.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/utils/error.h"
#include "vineyard/graph/utils/table_shuffler.h"

#include "core/config.h"
#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/edge_data.h"
#include "core/java/graphx/graphx_csr.h"
#include "core/java/graphx/graphx_vertex_map.h"
#include "core/java/graphx/vertex_data.h"

/**
 * @brief only stores local vertex mapping, construct global vertex map in mpi
 *
 */
namespace gs {

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class GraphXFragment
    : public vineyard::Registered<GraphXFragment<OID_T, VID_T, VD_T, ED_T>> {
 public:
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using edata_t = ED_T;
  using csr_t = GraphXCSR<VID_T>;
  using vm_t = GraphXVertexMap<OID_T, VID_T>;
  using graphx_vdata_t = VertexData<VID_T, VD_T>;
  using graphx_edata_t = EdgeData<VID_T, ED_T>;
  using vertices_t = grape::VertexRange<VID_T>;
  using vertex_t = grape::Vertex<VID_T>;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using adj_list_t =
      arrow_projected_fragment_impl::AdjList<vid_t, eid_t, edata_t>;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  template <typename T>
  using vertex_array_t = grape::VertexArray<vertices_t, T>;

  GraphXFragment() {}
  ~GraphXFragment() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXFragment<OID_T, VID_T, VD_T, ED_T>>{
            new GraphXFragment<OID_T, VID_T, VD_T, ED_T>()});
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) {}

  void Construct(const vineyard::ObjectMeta& meta) override {
    auto time = -grape::GetCurrentTime();
    this->meta_ = meta;
    this->id_ = meta.GetId();

    this->fnum_ = meta.GetKeyValue<fid_t>("fnum");
    this->fid_ = meta.GetKeyValue<fid_t>("fid");

    this->csr_.Construct(meta.GetMemberMeta("csr"));
    this->vm_.Construct(meta.GetMemberMeta("vm"));
    this->vdata_.Construct(meta.GetMemberMeta("vdata"));
    this->edata_.Construct(meta.GetMemberMeta("edata"));
    CHECK_EQ(vm_.GetVertexSize(), vdata_.VerticesNum());
    this->inner_vertices_.SetRange(0, vm_.GetInnerVertexSize());
    this->outer_vertices_.SetRange(vm_.GetInnerVertexSize(),
                                   vm_.GetVertexSize());
    this->vertices_.SetRange(0, vm_.GetVertexSize());
    time += grape::GetCurrentTime();
    VLOG(10) << "GraphXFragment finish construction : " << fid_
             << ", took: " << time << "s";
  }
  inline fid_t fid() const { return fid_; }
  inline fid_t fnum() const { return fnum_; }

  csr_t& GetCSR() { return csr_; }

  vm_t& GetVM() { return vm_; }

  graphx_vdata_t GetVdata() { return vdata_; }

  graphx_edata_t GetEdata() { return edata_; }

  inline int64_t GetEdgeNum() const { return csr_.GetTotalEdgesNum(); }
  inline int64_t GetInEdgeNum() const { return csr_.GetInEdgesNum(); }
  inline int64_t GetOutEdgeNum() const { return csr_.GetOutEdgesNum(); }

  inline VID_T GetInnerVerticesNum() const { return vm_.GetInnerVertexSize(); }
  inline VID_T GetOuterVerticesNum() const { return vm_.GetOuterVertexSize(); }
  inline VID_T GetVerticesNum() const { return vm_.GetVertexSize(); }
  inline VID_T GetTotalVerticesNum() const { return vm_.GetTotalVertexSize(); }

  inline vertices_t Vertices() const { return vertices_; }
  inline vertices_t InnerVertices() const { return inner_vertices_; }
  inline vertices_t OuterVertices() const { return outer_vertices_; }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) {
    return vm_.GetVertex(oid, v);
  }

  inline OID_T GetId(const vertex_t& v) { return vm_.GetId(v); }

  inline fid_t GetFragId(const vertex_t& v) const { return vm_.GetFragId(v); }

  inline int GetLocalInDegree(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return csr_.GetInDegree(v.GetValue());
  }
  inline int GetLocalOutDegree(const vertex_t& v) {
    assert(IsInnerVertex(v));
    return csr_.GetOutDegree(v.GetValue());
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return vm_.Gid2Vertex(gid, v);
  }

  inline vid_t Vertex2Gid(const vertex_t& v) { return vm_.Vertex2Gid(v); }

  inline bool IsInnerVertex(const vertex_t& v) {
    return inner_vertices_.Contain(v);
  }
  inline bool IsOuterVertex(const vertex_t& v) {
    return outer_vertices_.Contain(v);
  }

  // Try to get iv lid from oid
  inline bool GetInnerVertex(const OID_T& oid, vertex_t& v) {
    return vm_.GetInnerVertex(oid, v);
  }

  inline bool GetOuterVertex(const OID_T& oid, vertex_t& v) {
    return vm_.GetOuterVertex(oid, v);
  }

  inline OID_T GetInnerVertexId(const vertex_t& v) const {
    return vm_.GetInnerVertexId(v);
  }

  inline OID_T GetOuterVertexId(const vertex_t& v) const {
    return vm_.GetOuterVertexId(v);
  }

  inline bool InnerVertexGid2Vertex(const vid_t& gid, vertex_t& v) {
    return vm_.InnerVertexGid2Vertex(gid, v);
  }
  inline bool OuterVertexGid2Vertex(const vid_t& gid, vertex_t& v) {
    return vm_.OuterVertexGid2Vertex(gid, v);
  }

  inline VID_T GetInnerVertexGid(const vertex_t& v) const {
    return vm_.GetInnerVertexGid(v);
  }
  inline VID_T GetOuterVertexGid(const vertex_t& v) const {
    return vm_.GetOuterVertexGid(v);
  }

  inline vdata_t GetData(const vertex_t& v) { return vdata_.GetData(v); }

  inline nbr_t* GetIEBegin(const vertex_t& v) {
    return csr_.GetIEBegin(v.GetValue());
  }
  inline nbr_t* GetOEBegin(const vertex_t& v) {
    return csr_.GetOEBegin(v.GetValue());
  }
  inline nbr_t* GetIEEnd(const vertex_t& v) {
    return csr_.GetIEEnd(v.GetValue());
  }
  inline nbr_t* GetOEEnd(const vertex_t& v) {
    return csr_.GetOEEnd(v.GetValue());
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    return adj_list_t(GetIEBegin(v), GetIEEnd(v), edata_.GetEdataArray());
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    return adj_list_t(GetOEBegin(v), GetOEEnd(v), edata_.GetEdataArray());
  }

  gs::arrow_projected_fragment_impl::TypedArray<edata_t>& GetEdataArray() {
    return edata_.GetEdataArray();
  }
  gs::arrow_projected_fragment_impl::TypedArray<vdata_t>& GetVdataArray() {
    return vdata_.GetVdataArray();
  }

 private:
  grape::fid_t fnum_, fid_;
  vertices_t inner_vertices_, outer_vertices_, vertices_;
  csr_t csr_;
  vm_t vm_;
  graphx_vdata_t vdata_;
  graphx_edata_t edata_;

  template <typename _OID_T, typename _VID_T, typename _VD_T, typename _ED_T>
  friend class GraphXFragmentBuilder;
};

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class GraphXFragmentBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using edata_t = ED_T;
  using csr_t = GraphXCSR<VID_T>;
  using vm_t = GraphXVertexMap<OID_T, VID_T>;
  using graphx_vdata_t = VertexData<VID_T, VD_T>;
  using graphx_edata_t = EdgeData<VID_T, ED_T>;

 public:
  explicit GraphXFragmentBuilder(vineyard::Client& client,
                                 GraphXVertexMap<OID_T, VID_T>& vm,
                                 GraphXCSR<VID_T>& csr,
                                 VertexData<VID_T, VD_T>& vdata,
                                 EdgeData<VID_T, ED_T>& edata)
      : client_(client) {
    fid_ = vm.fid();
    fnum_ = vm.fnum();
    vm_ = vm;
    csr_ = csr;
    vdata_ = vdata;
    edata_ = edata;
  }

  explicit GraphXFragmentBuilder(vineyard::Client& client,
                                 vineyard::ObjectID vm_id,
                                 vineyard::ObjectID csr_id,
                                 vineyard::ObjectID vdata_id,
                                 vineyard::ObjectID edata_id)
      : client_(client) {
    vm_ = *std::dynamic_pointer_cast<vm_t>(client.GetObject(vm_id));
    fid_ = vm_.fid();
    fnum_ = vm_.fnum();
    csr_ = *std::dynamic_pointer_cast<csr_t>(client.GetObject(csr_id));
    vdata_ =
        *std::dynamic_pointer_cast<graphx_vdata_t>(client.GetObject(vdata_id));
    edata_ =
        *std::dynamic_pointer_cast<graphx_edata_t>(client.GetObject(edata_id));
  }

  std::shared_ptr<GraphXFragment<oid_t, vid_t, vdata_t, edata_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<
        GraphXFragment<oid_t, vid_t, vdata_t, edata_t>>(this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto fragment =
        std::make_shared<GraphXFragment<oid_t, vid_t, vdata_t, edata_t>>();
    fragment->meta_.SetTypeName(
        type_name<GraphXFragment<oid_t, vid_t, vdata_t, edata_t>>());

    fragment->fid_ = fid_;
    fragment->fnum_ = fnum_;
    fragment->csr_ = csr_;
    fragment->vm_ = vm_;
    fragment->vdata_ = vdata_;
    fragment->edata_ = edata_;

    fragment->meta_.AddKeyValue("fid", fid_);
    fragment->meta_.AddKeyValue("fnum", fnum_);
    fragment->meta_.AddMember("vdata", vdata_.meta());
    fragment->meta_.AddMember("csr", csr_.meta());
    fragment->meta_.AddMember("vm", vm_.meta());
    fragment->meta_.AddMember("edata", edata_.meta());

    fragment->inner_vertices_.SetRange(0, vm_.GetInnerVertexSize());
    fragment->outer_vertices_.SetRange(vm_.GetInnerVertexSize(),
                                       vm_.GetVertexSize());
    fragment->vertices_.SetRange(0, vm_.GetVertexSize());

    size_t nBytes = 0;
    nBytes += vdata_.nbytes();
    nBytes += csr_.nbytes();
    nBytes += vm_.nbytes();
    nBytes += edata_.nbytes();
    LOG(INFO) << "total bytes: " << nBytes;
    fragment->meta_.SetNBytes(nBytes);

    VINEYARD_CHECK_OK(client.CreateMetaData(fragment->meta_, fragment->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(fragment);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    LOG(INFO) << "no need for build";
    return vineyard::Status::OK();
  }

 private:
  grape::fid_t fnum_, fid_;
  csr_t csr_;
  vm_t vm_;
  graphx_vdata_t vdata_;
  graphx_edata_t edata_;
  vineyard::Client& client_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H_
