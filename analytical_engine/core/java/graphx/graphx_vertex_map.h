
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_VERTEX_MAP_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_VERTEX_MAP_H_

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
#include "grape/util.h"
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
#include "core/java/graphx/local_vertex_map.h"

/**
 * @brief only stores local vertex mapping, construct global vertex map in mpi
 *
 */
namespace gs {

template <typename OID_T, typename VID_T>
class GraphXVertexMap
    : public vineyard::Registered<GraphXVertexMap<OID_T, VID_T>> {
 public:
  static constexpr int thread_num = 16;
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_vid_array_t =
      typename vineyard::InternalType<vid_t>::vineyard_array_type;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using vertex_t = grape::Vertex<VID_T>;

  GraphXVertexMap() {}
  ~GraphXVertexMap() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXVertexMap<OID_T, VID_T>>{
            new GraphXVertexMap<OID_T, VID_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    this->fnum_ = meta.GetKeyValue<fid_t>("fnum");
    this->fid_ = meta.GetKeyValue<fid_t>("fid");
    this->graphx_pid_ = meta.GetKeyValue<int>("graphx_pid");
    this->shuffle_num_ = meta.GetKeyValue<int>("shuffle_num");

    id_parser_.init(fnum_);

    l2o_.resize(fnum_);
    l2o_accessor_.resize(fnum_);
    o2l_.resize(fnum_);
    for (fid_t i = 0; i < fnum_; ++i) {
      vineyard_array_t array;
      array.Construct(meta.GetMemberMeta("l2o_" + std::to_string(i)));
      l2o_[i] = array.GetArray();
      l2o_accessor_[i].Init(l2o_[i]);

      o2l_[i].Construct(meta.GetMemberMeta("o2l_" + std::to_string(i)));
    }
    {
      vineyard::NumericArray<int32_t> array;
      array.Construct(meta.GetMemberMeta("fid_to_pid"));
      fid_to_pid_ = array.GetArray();
    }
    {
      vineyard::NumericArray<int32_t> array;
      array.Construct(meta.GetMemberMeta("pid_to_fid"));
      pid_to_fid_ = array.GetArray();
    }
    {
      vineyard::NumericArray<int32_t> array;
      array.Construct(meta.GetMemberMeta("pid_array"));
      pid_array_ = array.GetArray();
    }
    {
      vineyard_vid_array_t array;
      array.Construct(meta.GetMemberMeta("ov_l2g"));
      ov_l2g_ = array.GetArray();
      ov_l2g_accessor_.Init(ov_l2g_);
    }

    this->ivnum_ = l2o_[fid_]->length();
    this->ovnum_ = ov_l2g_->length();
    this->tvnum_ = this->ivnum_ + this->ovnum_;

    ov_g2l_.resize(thread_num);
    for (int i = 0; i < thread_num; ++i) {
      std::string str = "ov_g2l_" + std::to_string(i);
      if (meta.Haskey(str)) {
        ov_g2l_[i].Construct(meta.GetMemberMeta(str));
      }
    }

    VLOG(10) << "Finish constructing global vertex map, ivnum: " << ivnum_
             << "ovnum: " << ovnum_ << " tvnum: " << tvnum_;
  }
  fid_t fid() const { return fid_; }
  fid_t fnum() const { return fnum_; }

  int32_t Fid2GraphxPid(fid_t fid) {
    CHECK_LT(fid, fnum_);
    return fid_to_pid_->Value(fid);
  }

  int32_t GraphXPid2Fid(fid_t pid) {
    CHECK_LT(pid, fnum_);
    return pid_to_fid_->Value(pid);
  }

  inline fid_t Oid2FragId(const oid_t& oid) const {
    int32_t shuffle_pid = static_cast<uint64_t>(oid) % shuffle_num_;
    int32_t pid = pid_array_->Value(shuffle_pid);
    fid_t fid = pid_to_fid_->Value(pid);
    return fid;
  }

  inline fid_t GetFragId(const vertex_t& v) const {
    if (v.GetValue() > ivnum_) {
      auto gid = ov_l2g_->Value(v.GetValue());
      return id_parser_.get_fragment_id(gid);
    }
    return fid_;
  }

  inline fid_t GetFragId(vid_t lid) const {
    if (lid >= ivnum_) {
      auto gid = ov_l2g_accessor_[lid - ivnum_];
      return id_parser_.get_fragment_id(gid);
    }
    return fid_;
  }

  inline VID_T GetTotalVertexSize() const {
    size_t size = 0;
    for (const auto& v : o2l_) {
      size += v.size();
    }
    return size;
  }

  VID_T GetInnerVertexSize(fid_t fid) const { return o2l_[fid].size(); }

  VID_T GetInnerVertexSize() const { return ivnum_; }

  VID_T GetOuterVertexSize() const { return ovnum_; }

  VID_T GetVertexSize() const { return tvnum_; }

  bool GetVertex(const oid_t& oid, vertex_t& v) {
    vid_t gid;
    if (!GetGid(oid, gid)) {
      LOG(ERROR) << "worker " << fid_ << "Get gid from oid faild: oid" << oid;
      return false;
    }
    return Gid2Vertex(gid, v);
  }

  bool GetInnerVertex(const oid_t& oid, vertex_t& v) {
    auto iter = o2l_[fid_].find(oid);
    if (iter == o2l_[fid_].end()) {
      LOG(ERROR) << "No match for oid " << oid << "found in frag: " << fid_;
      return false;
    }
    v.SetValue(iter->second);
    return true;
  }

  bool GetOuterVertex(const oid_t& oid, vertex_t& v) {
    vid_t gid, lid;
    assert(GetGid(oid, gid));
    assert(OuterVertexGid2Lid(gid, lid));
    v.SetValue(lid);
    return true;
  }

  bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return IsInnerVertexGid(gid) ? InnerVertexGid2Vertex(gid, v)
                                 : OuterVertexGid2Vertex(gid, v);
  }
  inline bool IsInnerVertexGid(const VID_T& gid) const {
    return id_parser_.get_fragment_id(gid) == fid();
  }

  inline bool InnerVertexGid2Vertex(const VID_T& gid, vertex_t& v) const {
    v.SetValue(id_parser_.get_local_id(gid));
    return true;
  }

  inline bool OuterVertexGid2Vertex(const VID_T& gid, vertex_t& v) const {
    vid_t lid;
    if (OuterVertexGid2Lid(gid, lid)) {
      v.SetValue(lid);
      return true;
    }
    return false;
  }

  inline bool OuterVertexGid2Lid(const VID_T gid, VID_T& lid) const {
    int32_t tid = gid % thread_num;
    auto iter = ov_g2l_[tid].find(gid);
    if (iter != ov_g2l_[tid].end()) {
      lid = iter->second;
      return true;
    }
    LOG(ERROR) << "worker [" << fid_ << "find no lid for outer gid" << gid;
    return false;
  }

  inline VID_T Vertex2Gid(const vertex_t& v) {
    return IsInnerVertex(v) ? GetInnerVertexGid(v) : GetOuterVertexGid(v);
  }
  inline VID_T GetInnerVertexGid(const vertex_t& v) const {
    return id_parser_.generate_global_id(fid(), v.GetValue());
  }
  inline VID_T GetOuterVertexGid(const vertex_t& v) const {
    return ov_l2g_->Value(v.GetValue() - ivnum_);
  }
  inline VID_T GetOuterVertexGid(const VID_T& lid) const {
    CHECK_GE(lid, ivnum_);
    return ov_l2g_accessor_[lid - ivnum_];
  }

  inline OID_T GetInnerVertexId(const vertex_t& v) const {
    assert(v.GetValue() < ivnum_);
    return l2o_accessor_[fid_][v.GetValue()];
  }
  inline OID_T GetOuterVertexId(const vertex_t& v) const {
    assert(v.GetValue() >= ivnum_);
    return OuterVertexLid2Oid(v.GetValue());
  }

  inline bool IsInnerVertex(const vertex_t& v) { return v.GetValue() < ivnum_; }

  inline bool InnerVertexGid2Lid(VID_T gid, VID_T& lid) const {
    lid = id_parser_.get_local_id(gid);
    return true;
  }

  inline VID_T GetInnererVertexGid(const vertex_t& v) {
    assert(v.GetValue() < ivnum_);
    return id_parser_.generate_global_id(fid_, v.GetValue());
  }

  OID_T GetId(const vertex_t& v) const {
    if (v.GetValue() >= ivnum_) {
      return OuterVertexLid2Oid(v.GetValue());
    } else {
      return InnerVertexLid2Oid(v.GetValue());
    }
  }
  OID_T GetId(const vid_t lid) const {
    if (lid >= ivnum_) {
      return OuterVertexLid2Oid(lid);
    } else {
      return InnerVertexLid2Oid(lid);
    }
  }

  inline bool GetOid(const VID_T& gid, OID_T& oid) const {
    fid_t fid = GetFidFromGid(gid);
    VID_T lid = id_parser_.get_local_id(gid);
    return GetOid(fid, lid, oid);
  }

  bool GetOid(fid_t fid, const VID_T& lid, OID_T& oid) const {
    if (lid >= l2o_[fid]->length()) {
      return false;
    }
    oid = l2o_accessor_[fid][lid];
    return true;
  }

  /**
   * @brief For a oid, get the lid in this frag.
   *
   * @param oid
   * @return VID_T
   */
  inline VID_T GetLid(const OID_T& oid) const {
    vid_t gid;
    CHECK(GetGid(oid, gid));
    if (GetFidFromGid(gid) == fid_) {
      return id_parser_.get_local_id(gid);
    } else {
      vid_t vid;
      CHECK(OuterVertexGid2Lid(gid, vid));
      CHECK(vid < tvnum_);
      return vid;
    }
  }

  inline OID_T InnerVertexLid2Oid(const VID_T& lid) const {
    CHECK_LT(lid, ivnum_);
    return l2o_accessor_[fid_][lid];
  }
  inline OID_T OuterVertexLid2Oid(const VID_T& lid) const {
    auto gid = ov_l2g_accessor_[lid - ivnum_];
    oid_t oid;
    CHECK(GetOid(gid, oid));
    return oid;
  }

  inline bool GetGid(fid_t fid, const OID_T& oid, VID_T& gid) const {
    auto& rm = o2l_[fid];
    auto iter = rm.find(oid);
    if (iter == rm.end()) {
      return false;
    } else {
      gid = Lid2Gid(fid, iter->second);
      return true;
    }
  }

  VID_T InnerOid2Gid(const OID_T& oid) const {
    VID_T gid;
    CHECK(GetGid(fid_, oid, gid));
    return gid;
  }

  inline bool GetGid(const OID_T& oid, VID_T& gid) const {
    // judge the fid from partition.
    int32_t shuffle_pid = static_cast<uint64_t>(oid) % shuffle_num_;
    int32_t pid = pid_array_->Value(shuffle_pid);
    fid_t fid = pid_to_fid_->Value(pid);
    bool res = GetGid(fid, oid, gid);
    if (!res) {
      LOG(ERROR) << "Fail to get gid for oid " << oid << ", shuffle_pid "
                 << shuffle_pid << " pid " << pid << " fid " << fid;
      for (fid_t i = 1; i < fnum_; ++i) {
        fid_t try_fid = (fid + i) % fnum_;
        if (GetGid(try_fid, oid, gid)) {
          LOG(ERROR) << "Found dst fid for " << oid << " at " << try_fid;
          return true;
        }
      }
      return false;
    }
    return res;
  }

  inline fid_t GetFidFromGid(const VID_T& gid) const {
    return id_parser_.get_fragment_id(gid);
  }
  inline VID_T Lid2Gid(fid_t fid, const VID_T& lid) const {
    return id_parser_.generate_global_id(fid, lid);
  }

  inline gs::arrow_projected_fragment_impl::TypedArray<oid_t>&
  GetLid2OidsAccessor(fid_t fid) {
    return l2o_accessor_[fid];
  }

  inline gs::arrow_projected_fragment_impl::TypedArray<vid_t>&
  GetOuterLid2GidsAccessor() {
    return ov_l2g_accessor_;
  }

 private:
  grape::fid_t fnum_, fid_;
  int graphx_pid_;
  int32_t shuffle_num_;
  vid_t ivnum_, ovnum_, tvnum_;
  grape::IdParser<vid_t> id_parser_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> o2l_;
  std::vector<std::shared_ptr<oid_array_t>> l2o_;
  std::vector<gs::arrow_projected_fragment_impl::TypedArray<oid_t>>
      l2o_accessor_;
  gs::arrow_projected_fragment_impl::TypedArray<vid_t> ov_l2g_accessor_;
  std::shared_ptr<vid_array_t> ov_l2g_;
  std::vector<vineyard::Hashmap<vid_t, vid_t>> ov_g2l_;
  std::shared_ptr<arrow::Int32Array> fid_to_pid_, pid_to_fid_;
  std::shared_ptr<arrow::Int32Array> pid_array_;

  template <typename _OID_T, typename _VID_T>
  friend class GraphXVertexMapBuilder;
};

template <typename OID_T, typename VID_T>
class GraphXVertexMapBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_oid_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_vid_array_t =
      typename vineyard::InternalType<vid_t>::vineyard_array_type;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  explicit GraphXVertexMapBuilder(vineyard::Client& client, grape::fid_t fnum,
                                  grape::fid_t fid, int graphx_pid,
                                  int32_t local_num)
      : client_(client) {
    l2o_.resize(fnum);
    o2l_.resize(fnum);
    fnum_ = fnum;
    fid_ = fid;
    graphx_pid_ = graphx_pid;
    local_num_ = local_num;
    id_parser_.init(fnum);
  }

  void setOuterLid2Oid(std::shared_ptr<oid_array_t> outer_oids) {
    this->outer_oid_array_ = outer_oids;
  }

  void setPidArray(vineyard::NumericArray<int32_t>& pid_array) {
    this->pid_array_ = pid_array;
    this->pid_array_accessor_ = pid_array.GetArray();
    this->shuffle_num_ = pid_array_accessor_->length();
  }

  void SetFid2GraphXPids(
      const vineyard::NumericArray<int32_t>& graphx_pids_array) {
    this->fid_to_pid_ = graphx_pids_array;
  }

  void SetGraphXPid2Fid(const vineyard::NumericArray<int32_t>& pid_to_fid) {
    this->pid_to_fid_ = pid_to_fid;
    this->pid_to_fid_accessor_ = pid_to_fid_.GetArray();
  }

  void SetOidArray(grape::fid_t fid, const vineyard_oid_array_t& oid_arrays) {
    this->l2o_[fid] = oid_arrays;
  }

  void SetOid2Lid(grape::fid_t fid, const vineyard::Hashmap<oid_t, vid_t>& rm) {
    this->o2l_[fid] = rm;
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif

    auto vertex_map = std::make_shared<GraphXVertexMap<oid_t, vid_t>>();
    vertex_map->fnum_ = fnum_;
    vertex_map->fid_ = fid_;
    vertex_map->shuffle_num_ = shuffle_num_;
    vertex_map->id_parser_.init(fnum_);
    vertex_map->graphx_pid_ = graphx_pid_;
    vertex_map->ivnum_ = l2o_[fid_].GetArray()->length();
    vertex_map->ovnum_ = outer_oid_array_->length();
    vertex_map->tvnum_ = vertex_map->ivnum_ + vertex_map->ovnum_;

    vertex_map->l2o_.resize(fnum_);
    vertex_map->l2o_accessor_.resize(fnum_);
    for (grape::fid_t i = 0; i < fnum_; ++i) {
      auto& array = vertex_map->l2o_[i];
      array = l2o_[i].GetArray();
      vertex_map->l2o_accessor_[i].Init(array);
    }

    vertex_map->o2l_ = o2l_;
    size_t nbytes = 0;
    // Builder ovg2l and ovgids
    int64_t ovnum = vertex_map->ovnum_;
    VLOG(10) << "ivnum: " << vertex_map->ivnum_ << ", " << ovnum;
#if defined(WITH_PROFILING)
    auto time0 = grape::GetCurrentTime();
#endif
    {
      std::atomic<int64_t> current_ind(0);
      int thread_num =
          (std::thread::hardware_concurrency() + local_num_ - 1) / local_num_;
      std::vector<std::thread> threads(thread_num);
      vid_array_builder_t gid_builder;
      gid_builder.Resize(ovnum);
      const oid_t* outer_lid2Oids_accessor_ = outer_oid_array_->raw_values();
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread(
            [&](int fid) {
              int64_t begin, end;
              while (true) {
                begin = std::min(ovnum, current_ind.fetch_add(
                                            4096, std::memory_order_relaxed));
                end = std::min(begin + 4096, ovnum);
                if (begin >= end) {
                  break;
                }
                for (int64_t j = begin; j < end; ++j) {
                  CHECK(getGid(outer_lid2Oids_accessor_[j], gid_builder[j]));
                }
              }
            },
            i);
      }
      for (int i = 0; i < thread_num; ++i) {
        threads[i].join();
      }
      gid_builder.Advance(ovnum);
      gid_builder.Finish(&vertex_map->ov_l2g_);
      vertex_map->ov_l2g_accessor_.Init(vertex_map->ov_l2g_);
    }
#if defined(WITH_PROFILING)
    auto time1 = grape::GetCurrentTime();
    VLOG(10) << "Build gid array len: " << vertex_map->ov_l2g_->length()
             << " cost" << (time1 - time0) << " seconds";
#endif
    {
      typename vineyard::InternalType<vid_t>::vineyard_builder_type
          array_builder(client, vertex_map->ov_l2g_);
      auto vineyard_gid_array =
          *std::dynamic_pointer_cast<vineyard::NumericArray<vid_t>>(
              array_builder.Seal(client));
      nbytes += vineyard_gid_array.nbytes();
      vertex_map->meta_.AddMember("ov_l2g", vineyard_gid_array.meta());
    }
    {
      int thread_num = GraphXVertexMap<oid_t, vid_t>::thread_num;
      vertex_map->ov_g2l_.resize(thread_num);
      std::vector<std::thread> threads(thread_num);
      auto& gid_accessor = vertex_map->ov_l2g_accessor_;
      int64_t chunk_size = (ovnum + thread_num - 1) / thread_num;
      int64_t ivnum = static_cast<int64_t>(vertex_map->ivnum_);
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread(
            [&](int fid) {
              vineyard::HashmapBuilder<vid_t, vid_t> builder(client);
              builder.reserve(static_cast<size_t>(chunk_size));
              for (int j = 0; j < ovnum; ++j) {
                int pid = gid_accessor[j] % thread_num;
                if (pid == fid) {
                  builder.emplace(gid_accessor[j], j + ivnum);
                }
              }
              if (builder.size() > 0) {
                vertex_map->ov_g2l_[fid] =
                    *std::dynamic_pointer_cast<vineyard::Hashmap<vid_t, vid_t>>(
                        builder.Seal(client));
              }
            },
            i);
      }
      for (int i = 0; i < thread_num; ++i) {
        threads[i].join();
      }
      for (int i = 0; i < thread_num; ++i) {
        if (vertex_map->ov_g2l_[i].size() > 0) {
          nbytes += vertex_map->ov_g2l_[i].nbytes();
          vertex_map->meta_.AddMember("ov_g2l_" + std::to_string(i),
                                      vertex_map->ov_g2l_[i].meta());
        }
      }
    }
#if defined(WITH_PROFILING)
    auto time2 = grape::GetCurrentTime();
    VLOG(10) << "building gid2lid cost" << (time2 - time1) << " seconds";
#endif
    vertex_map->fid_to_pid_ = fid_to_pid_.GetArray();
    vertex_map->pid_to_fid_ = pid_to_fid_.GetArray();
    vertex_map->pid_array_ = pid_array_.GetArray();

    vertex_map->meta_.SetTypeName(type_name<GraphXVertexMap<oid_t, vid_t>>());

    vertex_map->meta_.AddKeyValue("fnum", fnum_);
    vertex_map->meta_.AddKeyValue("fid", fid_);
    vertex_map->meta_.AddKeyValue("shuffle_num", shuffle_num_);
    vertex_map->meta_.AddKeyValue("graphx_pid", graphx_pid_);

    for (grape::fid_t i = 0; i < fnum_; ++i) {
      vertex_map->meta_.AddMember("o2l_" + std::to_string(i), o2l_[i].meta());
      nbytes += o2l_[i].nbytes();

      vertex_map->meta_.AddMember("l2o_" + std::to_string(i), l2o_[i].meta());
      nbytes += l2o_[i].nbytes();
    }
    vertex_map->meta_.AddMember("fid_to_pid", fid_to_pid_.meta());
    nbytes += fid_to_pid_.nbytes();
    vertex_map->meta_.AddMember("pid_to_fid", pid_to_fid_.meta());
    nbytes += pid_to_fid_.nbytes();
    vertex_map->meta_.AddMember("pid_array", pid_array_.meta());
    nbytes += pid_array_.nbytes();

    vertex_map->meta_.SetNBytes(nbytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_map->meta_, vertex_map->id_));
    this->set_sealed(true);
#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    VLOG(10) << "Sealing GraphX vertex map cost" << (finish_seal_ts - start_ts)
             << " seconds";
#endif

    return std::static_pointer_cast<vineyard::Object>(vertex_map);
  }

 protected:
  int graphx_pid_;
  vineyard::NumericArray<int32_t> pid_array_;
  std::shared_ptr<arrow::Int32Array> pid_array_accessor_;
  int32_t shuffle_num_;  // num part when do graphx shuffling

 private:
  inline bool getGid(const oid_t& oid, vid_t& gid) const {
    int32_t shuffle_pid = static_cast<uint64_t>(oid) % shuffle_num_;
    int32_t pid = pid_array_accessor_->Value(shuffle_pid);
    fid_t fid = pid_to_fid_accessor_->Value(pid);
    return getGid(fid, oid, gid);
  }

  inline bool getGid(fid_t fid, const oid_t& oid, vid_t& gid) const {
    auto& rm = o2l_[fid];
    auto iter = rm.find(oid);
    if (iter == rm.end()) {
      return false;
    } else {
      gid = lid2Gid(fid, iter->second);
      return true;
    }
  }
  inline vid_t lid2Gid(fid_t fid, const vid_t& lid) const {
    return id_parser_.generate_global_id(fid, lid);
  }

  grape::fid_t fnum_, fid_;
  int32_t local_num_;
  grape::IdParser<vid_t> id_parser_;
  vineyard::Client& client_;
  std::vector<vineyard_oid_array_t> l2o_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> o2l_;
  std::shared_ptr<oid_array_t> outer_oid_array_;
  vineyard::NumericArray<int32_t> fid_to_pid_, pid_to_fid_;
  std::shared_ptr<arrow::Int32Array> pid_to_fid_accessor_;
};

template <typename OID_T, typename VID_T>
class BasicGraphXVertexMapBuilder
    : public GraphXVertexMapBuilder<OID_T, VID_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;
  using base_t = GraphXVertexMapBuilder<oid_t, vid_t>;

 public:
  BasicGraphXVertexMapBuilder(vineyard::Client& client,
                              grape::CommSpec& comm_spec, int graphx_pid,
                              vineyard::ObjectID localVertexMapID)
      : GraphXVertexMapBuilder<oid_t, vid_t>(client, comm_spec.worker_num(),
                                             comm_spec.worker_id(), graphx_pid,
                                             comm_spec.local_num()),
        comm_spec_(comm_spec) {
    comm_spec_.Dup();
    partial_vmap_ = std::dynamic_pointer_cast<LocalVertexMap<oid_t, vid_t>>(
        client.GetObject(localVertexMapID));
    VLOG(10) << "Worer [" << comm_spec.worker_id() << " got partial vmap id "
             << localVertexMapID
             << ", local vnum: " << partial_vmap_->GetInnerVerticesNum()
             << ", graphx pid: " << graphx_pid;
    base_t::setOuterLid2Oid(partial_vmap_->GetOuterLid2Oid().GetArray());
    base_t::setPidArray(partial_vmap_->GetPidArray());
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    std::vector<std::shared_ptr<oid_array_t>> collected_oids;
    std::shared_ptr<oid_array_t> our_oids =
        partial_vmap_->GetInnerLid2Oid().GetArray();

    CHECK(vineyard::FragmentAllGatherArray<oid_t>(comm_spec_, our_oids,
                                                  collected_oids)
              .ok());
    CHECK_EQ(collected_oids.size(), comm_spec_.worker_num());
#if defined(WITH_PROFILING)
    auto shuffle_ts = grape::GetCurrentTime();
    VLOG(10) << "Shuffle inner oids cost " << (shuffle_ts - start_ts)
             << " seconds";
#endif

    grape::fid_t curFid = comm_spec_.fid();

    int thread_num =
        (std::thread::hardware_concurrency() + comm_spec_.local_num() - 1) /
        comm_spec_.local_num();
    {
      std::atomic<grape::fid_t> current_fid(0);
      std::vector<std::thread> threads(thread_num);
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread(
            [&](int fid) {
              grape::fid_t cur_fid;
              while (true) {
                cur_fid = current_fid.fetch_add(1, std::memory_order_relaxed);
                if (cur_fid >= comm_spec_.fnum()) {
                  break;
                }
                if (cur_fid == curFid) {
                  this->SetOidArray(cur_fid, partial_vmap_->GetInnerLid2Oid());
                } else {
                  typename vineyard::InternalType<oid_t>::vineyard_builder_type
                      array_builder(client, collected_oids[cur_fid]);
                  this->SetOidArray(
                      cur_fid,
                      *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
                          array_builder.Seal(client)));
                }
              }
            },
            i);
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
    }
#if defined(WITH_PROFILING)
    auto oidArrayTime = grape::GetCurrentTime();
    VLOG(10) << "Buillding GraphX vertex map oid array"
             << (oidArrayTime - start_ts) << " seconds";
#endif
    {
      std::atomic<grape::fid_t> current_fid(0);
      std::vector<std::thread> threads(thread_num);
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread(
            [&](int fid) {
              grape::fid_t cur_fid;
              while (true) {
                cur_fid = current_fid.fetch_add(1, std::memory_order_relaxed);
                if (cur_fid >= comm_spec_.fnum()) {
                  break;
                }
                vineyard::HashmapBuilder<oid_t, vid_t> builder(client);
                auto array = collected_oids[cur_fid]->raw_values();
                {
                  int64_t vnum = collected_oids[cur_fid]->length();
                  builder.reserve(static_cast<size_t>(vnum));
                  for (int64_t k = 0; k < vnum; ++k) {
                    builder.emplace(array[k], k);
                  }
                }
                {
                  this->SetOid2Lid(cur_fid,
                                   *std::dynamic_pointer_cast<
                                       vineyard::Hashmap<oid_t, vid_t>>(
                                       builder.Seal(client)));
                }
              }
            },
            i);
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
    }
#if defined(WITH_PROFILING)
    auto oid2LidTime = grape::GetCurrentTime();
    VLOG(10) << "Buillding oid2Lid cost" << (oid2LidTime - oidArrayTime)
             << " seconds";
#endif

    {
      // gather grape pid <-> graphx pid matching.
      std::vector<int32_t> fid_to_graphx_pids;
      fid_to_graphx_pids.resize(comm_spec_.fnum());
      int32_t tmp_graphx_pid = base_t::graphx_pid_;
      MPI_Allgather(&tmp_graphx_pid, 1, MPI_INT, fid_to_graphx_pids.data(), 1,
                    MPI_INT, comm_spec_.comm());

      {
        arrow::Int32Builder builder;
        CHECK(builder.AppendValues(fid_to_graphx_pids).ok());
        std::shared_ptr<arrow::Int32Array> graphx_pids_array;
        CHECK(builder.Finish(&graphx_pids_array).ok());
        vineyard::NumericArrayBuilder<int32_t> v6d_graphx_pids_builder(
            client, graphx_pids_array);
        this->SetFid2GraphXPids(
            *std::dynamic_pointer_cast<vineyard::NumericArray<int32_t>>(
                v6d_graphx_pids_builder.Seal(client)));
      }

      std::vector<int32_t> graphx_pid_to_fid;
      graphx_pid_to_fid.resize(comm_spec_.fnum());
      for (grape::fid_t i = 0; i < comm_spec_.fnum(); ++i) {
        graphx_pid_to_fid[fid_to_graphx_pids[i]] = i;
      }
      {
        arrow::Int32Builder builder;
        CHECK(builder.AppendValues(graphx_pid_to_fid).ok());
        std::shared_ptr<arrow::Int32Array> graphx_pid_to_fid_array;
        CHECK(builder.Finish(&graphx_pid_to_fid_array).ok());
        vineyard::NumericArrayBuilder<int32_t> v6d_graphx_pid2fid_builder(
            client, graphx_pid_to_fid_array);
        this->SetGraphXPid2Fid(
            *std::dynamic_pointer_cast<vineyard::NumericArray<int32_t>>(
                v6d_graphx_pid2fid_builder.Seal(client)));
      }
    }

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    VLOG(10) << "Buillding GraphX vertex map cost"
             << (finish_seal_ts - start_ts) << " seconds";
#endif
    return vineyard::Status::OK();
  }

 private:
  grape::CommSpec comm_spec_;
  std::shared_ptr<LocalVertexMap<OID_T, VID_T>> partial_vmap_;
};

template <typename OID_T, typename VID_T>
class GraphXVertexMapGetter {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  GraphXVertexMapGetter() {}
  ~GraphXVertexMapGetter() {}
  std::shared_ptr<GraphXVertexMap<oid_t, vid_t>> Get(
      vineyard::Client& client, vineyard::ObjectID global_vm_id) {
    auto global_vm = std::dynamic_pointer_cast<GraphXVertexMap<oid_t, vid_t>>(
        client.GetObject(global_vm_id));
    VLOG(10) << "Got global vm: " << global_vm_id
             << " total vnum: " << global_vm->GetTotalVertexSize();
    return global_vm;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_VERTEX_MAP_H_
