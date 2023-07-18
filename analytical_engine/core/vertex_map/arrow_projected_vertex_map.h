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

#include "arrow/array/array_binary.h"
#include "flat_hash_map/flat_hash_map.hpp"
#include "string_view/string_view.hpp"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/hashmap.h"
#include "vineyard/client/client.h"
#include "vineyard/client/ds/i_object.h"
#include "vineyard/client/ds/object_meta.h"
#include "vineyard/common/util/config.h"
#include "vineyard/common/util/status.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/common/util/uuid.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"

#include "core/config.h"

namespace vineyard {
template <typename OID_T, typename VID_T>
class ArrowVertexMap;
}

namespace gs {
/**
 * @brief This class represents the mapping between oid and vid.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T, typename VID_T,
          typename VERTEX_MAP_T = vineyard::ArrowVertexMap<OID_T, VID_T>>
class ArrowProjectedVertexMap
    : public vineyard::Registered<
          ArrowProjectedVertexMap<OID_T, VID_T, VERTEX_MAP_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using property_vertex_map_t = VERTEX_MAP_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
#if defined(VINEYARD_VERSION) && defined(VINEYARD_VERSION_MAJOR)
#if VINEYARD_VERSION >= 2007
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<
            ArrowProjectedVertexMap<oid_t, vid_t, property_vertex_map_t>>{
            new ArrowProjectedVertexMap<oid_t, vid_t,
                                        property_vertex_map_t>()});
  }
#endif
#else
  static std::shared_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::make_shared<
            ArrowProjectedVertexMap<oid_t, vid_t, property_vertex_map_t>>());
  }
#endif

  static std::shared_ptr<ArrowProjectedVertexMap<OID_T, VID_T, VERTEX_MAP_T>>
  Project(std::shared_ptr<property_vertex_map_t> vm, label_id_t v_label) {
    vineyard::Client& client =
        *dynamic_cast<vineyard::Client*>(vm->meta().GetClient());

    vineyard::ObjectMeta meta;
    meta.SetTypeName(
        type_name<
            ArrowProjectedVertexMap<oid_t, vid_t, property_vertex_map_t>>());

    meta.AddKeyValue("projected_label", v_label);
    meta.AddMember("arrow_vertex_map", vm->meta());

    meta.SetNBytes(0);

    vineyard::ObjectID id;
    VINEYARD_CHECK_OK(client.CreateMetaData(meta, id));

    return std::dynamic_pointer_cast<
        ArrowProjectedVertexMap<OID_T, VID_T, VERTEX_MAP_T>>(
        client.GetObject(id));
  }

  void Construct(const vineyard::ObjectMeta& meta) {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    vertex_map_ = std::make_shared<property_vertex_map_t>();
    vertex_map_->Construct(meta.GetMemberMeta("arrow_vertex_map"));

    fnum_ = vertex_map_->fnum();
    label_num_ = vertex_map_->label_num();
    label_id_ = meta.GetKeyValue<label_id_t>("projected_label");
    id_parser_.Init(fnum_, label_num_);
  }

  bool GetOid(vid_t gid, oid_t& oid) const {
    if (id_parser_.GetLabelId(gid) == label_id_) {
      return vertex_map_->GetOid(gid, oid);
    }
    return false;
  }

  bool GetGid(fid_t fid, oid_t oid, vid_t& gid) const {
    if (fid < fnum_) {
      return vertex_map_->GetGid(fid, label_id_, oid, gid);
    }
    return false;
  }

  bool GetGid(oid_t oid, vid_t& gid) const {
    for (fid_t i = 0; i < fnum_; ++i) {
      if (vertex_map_->GetGid(i, label_id_, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  vid_t Offset2Lid(const vid_t& offset) {
    return id_parser_.GenerateId(label_id_, offset);
  }

  vid_t GetOffsetFromLid(vid_t lid) { return id_parser_.GetOffset(lid); }

  size_t GetTotalVerticesNum() const {
    return vertex_map_->GetTotalNodesNum(label_id_);
  }

  VID_T GetInnerVertexSize(fid_t fid) const {
    return vertex_map_->GetInnerVertexSize(fid, label_id_);
  }

  VID_T GetLidFromGid(vid_t gid) const { return id_parser_.GetLid(gid); }

  fid_t GetFidFromGid(vid_t gid) const { return id_parser_.GetFid(gid); }

  bool use_perfect_hash() const { return vertex_map_->use_perfect_hash(); }

 private:
  fid_t fnum_;
  label_id_t label_num_;
  label_id_t label_id_;

  vineyard::IdParser<vid_t> id_parser_;
  std::shared_ptr<property_vertex_map_t> vertex_map_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_ARROW_PROJECTED_VERTEX_MAP_H_
