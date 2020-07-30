/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_CORE_VERTEX_MAP_EXTRA_VERTEX_MAP_H_
#define ANALYTICAL_ENGINE_CORE_VERTEX_MAP_EXTRA_VERTEX_MAP_H_

#include <memory>
#include <vector>

#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/vertex_map/arrow_vertex_map.h"

#include "core/config.h"

namespace gs {
/**
 * @brief A VertexMap for later appended vertices
 * @tparam OID_T
 * @tparam VID_T
 */
template <typename OID_T, typename VID_T>
class ExtraVertexMap {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;

 public:
  void Init(
      std::shared_ptr<vineyard::ArrowVertexMap<internal_oid_t, vid_t>> vm_ptr) {
    fnum_ = vm_ptr->fnum();
    label_num_ = vm_ptr->label_num();

    extra_oid_arrays_.resize(fnum_);
    extra_o2g_.resize(fnum_);
    base_size_.resize(fnum_);

    for (fid_t fid = 0; fid < fnum_; fid++) {
      extra_oid_arrays_[fid].resize(label_num_);
      base_size_[fid].resize(label_num_);
    }
    id_parser_.Init(fnum_, label_num_);

    for (fid_t fid = 0; fid < fnum_; fid++) {
      for (label_id_t v_label = 0; v_label < label_num_; v_label++) {
        base_size_[fid][v_label] = vm_ptr->GetInnerVertexSize(fid, v_label);
      }
    }
  }

  bool AddVertex(fid_t fid, label_id_t v_label, const oid_t& oid, vid_t& gid) {
    auto& o2g = extra_o2g_[fid];

    if (o2g.find(oid) == o2g.end()) {
      auto& oid_array = extra_oid_arrays_[fid][v_label];
      auto base = base_size_[fid][v_label];
      auto idx = base + oid_array.size();

      gid = id_parser_.GenerateId(fid, v_label, idx);
      oid_array.push_back(oid);
      o2g[oid] = gid;
      return true;
    }
    return false;
  }

  bool GetOid(vid_t gid, oid_t& oid) const {
    fid_t fid = id_parser_.GetFid(gid);
    label_id_t label = id_parser_.GetLabelId(gid);

    if (fid < fnum_ && label < label_num_ && label >= 0) {
      auto base = base_size_[fid][label];
      auto offset = id_parser_.GetOffset(gid) - base;
      auto& oid_array = extra_oid_arrays_[fid][label];

      // offset should greater equal 0,
      // otherwise the oid is stored in vertex map
      if (offset >= 0 && offset < oid_array.size()) {
        oid = oid_array[offset];
        return true;
      }
    }
    return false;
  }

  bool GetGid(fid_t fid, oid_t oid, vid_t& gid) const {
    auto iter = extra_o2g_[fid].find(oid);
    if (iter != extra_o2g_[fid].end()) {
      gid = iter->second;
      return true;
    }
    return false;
  }

  bool GetGid(oid_t oid, vid_t& gid) const {
    for (fid_t fid = 0; fid < fnum_; fid++) {
      if (GetGid(fid, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  size_t GetTotalNodesNum() const {
    size_t num = 0;
    for (auto& m : extra_o2g_) {
      num += m.size();
    }
    return num;
  }

 private:
  // The members below are related to append only features
  std::vector<std::vector<std::vector<oid_t>>> extra_oid_arrays_;
  std::vector<ska::flat_hash_map<oid_t, vid_t>> extra_o2g_;
  vineyard::IdParser<vid_t> id_parser_;
  std::vector<std::vector<size_t>> base_size_;
  fid_t fnum_;
  label_id_t label_num_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_EXTRA_VERTEX_MAP_H_
