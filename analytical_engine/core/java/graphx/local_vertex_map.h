
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_LOCAL_VERTEX_MAP_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_LOCAL_VERTEX_MAP_H_

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
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/hashmap.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"

#include "core/config.h"

/**
 * @brief only stores local vertex mapping.
 *
 */
namespace gs {

template <typename OID_T, typename VID_T>
class LocalVertexMap
    : public vineyard::Registered<LocalVertexMap<OID_T, VID_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_oid_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;

 public:
  LocalVertexMap() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<LocalVertexMap<OID_T, VID_T>>{
            new LocalVertexMap<OID_T, VID_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->ivnum_ = meta.GetKeyValue<grape::fid_t>("ivnum");
    this->ovnum_ = meta.GetKeyValue<grape::fid_t>("ovnum");
    VLOG(10) << "ivnum: " << ivnum_ << "ovnum: " << ovnum_;

    inner_lid2Oid_.Construct(meta.GetMemberMeta("inner_lid2Oid"));
    outer_lid2Oid_.Construct(meta.GetMemberMeta("outer_lid2Oid"));

    pid_array_.Construct(meta.GetMemberMeta("pid_array"));

    VLOG(10) << "Finish construct local_vertex_map,  ivnum" << ivnum_
             << "ovnum: " << ovnum_;
  }

  int64_t GetInnerVerticesNum() { return ivnum_; }

  vineyard_oid_array_t& GetInnerLid2Oid() { return inner_lid2Oid_; }

  vineyard_oid_array_t& GetOuterLid2Oid() { return outer_lid2Oid_; }

  vineyard::NumericArray<int32_t>& GetPidArray() { return pid_array_; }

 private:
  vid_t ivnum_, ovnum_;
  vineyard_oid_array_t inner_lid2Oid_, outer_lid2Oid_;
  vineyard::NumericArray<int32_t> pid_array_;

  template <typename _OID_T, typename _VID_T>
  friend class LocalVertexMapBuilder;
};

template <typename OID_T, typename VID_T>
class LocalVertexMapBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_oid_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;

 public:
  explicit LocalVertexMapBuilder(vineyard::Client& client) : client_(client) {}

  void SetInnerOidArray(const vineyard_oid_array_t& oid_array) {
    this->inner_lid2Oid_ = oid_array;
  }
  void SetOuterOidArray(const vineyard_oid_array_t& oid_array) {
    this->outer_lid2Oid_ = oid_array;
  }

  void SetPidArray(const vineyard::NumericArray<int32_t>& pid_array) {
    this->pid_array_ = pid_array;
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);

    VINEYARD_CHECK_OK(this->Build(client));

    auto vertex_map = std::make_shared<LocalVertexMap<oid_t, vid_t>>();
    vertex_map->meta_.SetTypeName(type_name<LocalVertexMap<oid_t, vid_t>>());

    {
      vertex_map->inner_lid2Oid_ = inner_lid2Oid_;
      vertex_map->ivnum_ = inner_lid2Oid_.GetArray()->length();
      vertex_map->meta_.AddKeyValue("ivnum", vertex_map->ivnum_);

      vertex_map->outer_lid2Oid_ = outer_lid2Oid_;
      vertex_map->ovnum_ = outer_lid2Oid_.GetArray()->length();
      vertex_map->meta_.AddKeyValue("ovnum", vertex_map->ovnum_);
    }
    size_t nbytes = 0;

    vertex_map->meta_.AddMember("inner_lid2Oid", inner_lid2Oid_.meta());
    nbytes += inner_lid2Oid_.nbytes();
    vertex_map->meta_.AddMember("outer_lid2Oid", outer_lid2Oid_.meta());
    nbytes += outer_lid2Oid_.nbytes();
    vertex_map->meta_.AddMember("pid_array", pid_array_.meta());
    nbytes += pid_array_.nbytes();

    VLOG(10) << "total bytes: " << nbytes;
    vertex_map->meta_.SetNBytes(nbytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_map->meta_, vertex_map->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(vertex_map);
  }

 private:
  vineyard::Client& client_;
  vineyard_oid_array_t inner_lid2Oid_, outer_lid2Oid_;
  vineyard::NumericArray<int32_t> pid_array_;
};

template <typename OID_T, typename VID_T>
class BasicLocalVertexMapBuilder : public LocalVertexMapBuilder<OID_T, VID_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;
  using pid_array_builder_t =
      typename vineyard::ConvertToArrowType<int32_t>::BuilderType;

 public:
  BasicLocalVertexMapBuilder(vineyard::Client& client,
                             oid_array_builder_t& inner_oids_builder,
                             oid_array_builder_t& outer_oids_builder,
                             pid_array_builder_t& pid_array_builder)
      : LocalVertexMapBuilder<oid_t, vid_t>(client) {
    CHECK(inner_oids_builder.Finish(&inner_oids_).ok());
    CHECK(outer_oids_builder.Finish(&outer_oids_).ok());
    CHECK(pid_array_builder.Finish(&pid_array_).ok());
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif

    typename vineyard::InternalType<oid_t>::vineyard_builder_type
        inner_array_builder(client, inner_oids_);
    this->SetInnerOidArray(
        *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
            inner_array_builder.Seal(client)));
    typename vineyard::InternalType<oid_t>::vineyard_builder_type
        outer_array_builder(client, outer_oids_);
    this->SetOuterOidArray(
        *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
            outer_array_builder.Seal(client)));

    typename vineyard::InternalType<int32_t>::vineyard_builder_type
        pid_array_builder(client, pid_array_);
    this->SetPidArray(
        *std::dynamic_pointer_cast<vineyard::NumericArray<int32_t>>(
            pid_array_builder.Seal(client)));
    LOG(INFO) << "Finish setting inner and outer oids";

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Seal hashmaps uses " << (finish_seal_ts - start_ts)
              << " seconds";
#endif
    return vineyard::Status::OK();
  }
  std::shared_ptr<LocalVertexMap<oid_t, vid_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<LocalVertexMap<oid_t, vid_t>>(
        this->Seal(client));
  }

 private:
  std::shared_ptr<oid_array_t> inner_oids_, outer_oids_;
  std::shared_ptr<arrow::Int32Array> pid_array_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_LOCAL_VERTEX_MAP_H_
