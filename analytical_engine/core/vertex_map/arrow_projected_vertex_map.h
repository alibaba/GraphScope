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

#ifndef ANALYTICAL_ENGINE_CORE_VERTEX_MAP_ARROW_PROJECTED_VERTEX_MAP_H_
#define ANALYTICAL_ENGINE_CORE_VERTEX_MAP_ARROW_PROJECTED_VERTEX_MAP_H_

#include <memory>
#include <vector>

#include "vineyard/common/util/version.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/vertex_map/arrow_vertex_map.h"

#include "core/config.h"

namespace gs {
/**
 * @brief This class represents the mapping between oid and vid.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T, typename VID_T>
class ArrowProjectedVertexMap
    : public vineyard::Registered<ArrowProjectedVertexMap<OID_T, VID_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
#if defined(VINEYARD_VERSION) && defined(VINEYARD_VERSION_MAJOR)
#if VINEYARD_VERSION >= 2007
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<ArrowProjectedVertexMap<oid_t, vid_t>>{
            new ArrowProjectedVertexMap<oid_t, vid_t>()});
  }
#endif
#else
  static std::shared_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::make_shared<ArrowProjectedVertexMap<oid_t, vid_t>>());
  }
#endif

  static std::shared_ptr<ArrowProjectedVertexMap<OID_T, VID_T>> Project(
      std::shared_ptr<vineyard::ArrowVertexMap<OID_T, VID_T>> vm,
      label_id_t v_label) {
    vineyard::Client& client =
        *dynamic_cast<vineyard::Client*>(vm->meta().GetClient());

    vineyard::ObjectMeta meta;
    meta.SetTypeName(type_name<ArrowProjectedVertexMap<oid_t, vid_t>>());

    meta.AddKeyValue("projected_label", v_label);
    meta.AddMember("arrow_vertex_map", vm->meta());

    meta.SetNBytes(0);

    vineyard::ObjectID id;
    VINEYARD_CHECK_OK(client.CreateMetaData(meta, id));

    return std::dynamic_pointer_cast<ArrowProjectedVertexMap<OID_T, VID_T>>(
        client.GetObject(id));
  }

  void Construct(const vineyard::ObjectMeta& meta) {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    vertex_map_ = std::make_shared<vineyard::ArrowVertexMap<oid_t, vid_t>>();
    vertex_map_->Construct(meta.GetMemberMeta("arrow_vertex_map"));

    fnum_ = vertex_map_->fnum_;
    label_num_ = vertex_map_->label_num_;
    label_id_ = meta.GetKeyValue<label_id_t>("projected_label");
    id_parser_.Init(fnum_, label_num_);
    oid_arrays_.resize(fnum_);
    o2g_.resize(fnum_);
    for (fid_t i = 0; i < fnum_; ++i) {
      oid_arrays_[i] = vertex_map_->oid_arrays_[i][label_id_];
      o2g_[i] = vertex_map_->o2g_[i][label_id_];
    }
  }

  bool GetOid(vid_t gid, oid_t& oid) const {
    if (id_parser_.GetLabelId(gid) == label_id_) {
      int64_t offset = id_parser_.GetOffset(gid);
      fid_t fid = id_parser_.GetFid(gid);
      if (offset < oid_arrays_[fid]->length()) {
        oid = oid_arrays_[fid]->GetView(offset);
        return true;
      }
    }
    return false;
  }

  bool GetGid(fid_t fid, oid_t oid, vid_t& gid) const {
    if (fid < fnum_) {
      auto& hm = o2g_[fid];
      auto iter = hm.find(oid);
      if (iter != hm.end()) {
        gid = iter->second;
        if (id_parser_.GetLabelId(gid) == label_id_) {
          return true;
        }
      }
    }
    return false;
  }

  bool GetGid(oid_t oid, vid_t& gid) const {
    for (fid_t i = 0; i < fnum_; ++i) {
      if (GetGid(i, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  size_t GetTotalVerticesNum() const {
    int64_t ret = 0;
    for (auto oid_array : oid_arrays_) {
      ret += oid_array->length();
    }
    return static_cast<size_t>(ret);
  }

 private:
  fid_t fnum_;
  label_id_t label_num_;
  label_id_t label_id_;

  vineyard::IdParser<vid_t> id_parser_;
  std::vector<std::shared_ptr<oid_array_t>> oid_arrays_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> o2g_;

  std::shared_ptr<vineyard::ArrowVertexMap<oid_t, vid_t>> vertex_map_;
};

template <typename VID_T>
class ArrowProjectedVertexMap<arrow::util::string_view, VID_T>
    : public vineyard::Registered<
          ArrowProjectedVertexMap<arrow::util::string_view, VID_T>> {
  using oid_t = arrow::util::string_view;
  using vid_t = VID_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using oid_array_t = arrow::LargeStringArray;

 public:
#if defined(VINEYARD_VERSION) && defined(VINEYARD_VERSION_MAJOR)
#if VINEYARD_VERSION >= 2007
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<ArrowProjectedVertexMap<oid_t, vid_t>>{
            new ArrowProjectedVertexMap<oid_t, vid_t>()});
  }
#endif
#else
  static std::shared_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::make_shared<ArrowProjectedVertexMap<oid_t, vid_t>>());
  }
#endif

  static std::shared_ptr<ArrowProjectedVertexMap<oid_t, VID_T>> Project(
      std::shared_ptr<vineyard::ArrowVertexMap<oid_t, VID_T>> vm,
      label_id_t v_label) {
    vineyard::Client& client =
        *dynamic_cast<vineyard::Client*>(vm->meta().GetClient());

    vineyard::ObjectMeta meta;
    meta.SetTypeName(type_name<ArrowProjectedVertexMap<oid_t, vid_t>>());

    meta.AddKeyValue("projected_label", v_label);
    meta.AddMember("arrow_vertex_map", vm->meta());

    meta.SetNBytes(0);

    vineyard::ObjectID id;
    VINEYARD_CHECK_OK(client.CreateMetaData(meta, id));

    return std::dynamic_pointer_cast<ArrowProjectedVertexMap<oid_t, VID_T>>(
        client.GetObject(id));
  }

  void Construct(const vineyard::ObjectMeta& meta) {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    vertex_map_ = std::make_shared<vineyard::ArrowVertexMap<oid_t, vid_t>>();
    vertex_map_->Construct(meta.GetMemberMeta("arrow_vertex_map"));

    fnum_ = vertex_map_->fnum_;
    label_num_ = vertex_map_->label_num_;
    label_id_ = meta.GetKeyValue<label_id_t>("projected_label");
    id_parser_.Init(fnum_, label_num_);
    oid_arrays_.resize(fnum_);
    o2g_ptrs_.resize(fnum_);
    for (fid_t i = 0; i < fnum_; ++i) {
      oid_arrays_[i] = vertex_map_->oid_arrays_[i][label_id_];
      o2g_ptrs_[i] = &vertex_map_->o2g_[i][label_id_];
    }
  }

  bool GetOid(vid_t gid, oid_t& oid) const {
    if (id_parser_.GetLabelId(gid) == label_id_) {
      int64_t offset = id_parser_.GetOffset(gid);
      fid_t fid = id_parser_.GetFid(gid);
      if (offset < oid_arrays_[fid]->length()) {
        oid = oid_arrays_[fid]->GetView(offset);
        return true;
      }
    }
    return false;
  }

  bool GetGid(fid_t fid, oid_t oid, vid_t& gid) const {
    if (fid < fnum_) {
      auto& hm = *o2g_ptrs_[fid];
      auto iter = hm.find(oid);
      if (iter != hm.end()) {
        gid = iter->second;
        if (id_parser_.GetLabelId(gid) == label_id_) {
          return true;
        }
      }
    }
    return false;
  }

  bool GetGid(oid_t oid, vid_t& gid) const {
    for (fid_t i = 0; i < fnum_; ++i) {
      if (GetGid(i, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  size_t GetTotalVerticesNum() const {
    int64_t ret = 0;
    for (auto oid_array : oid_arrays_) {
      ret += oid_array->length();
    }
    return static_cast<size_t>(ret);
  }

 private:
  fid_t fnum_;
  label_id_t label_num_;
  label_id_t label_id_;

  vineyard::IdParser<vid_t> id_parser_;
  std::vector<std::shared_ptr<oid_array_t>> oid_arrays_;
  std::vector<ska::flat_hash_map<oid_t, vid_t>*> o2g_ptrs_;

  std::shared_ptr<vineyard::ArrowVertexMap<oid_t, vid_t>> vertex_map_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_ARROW_PROJECTED_VERTEX_MAP_H_
