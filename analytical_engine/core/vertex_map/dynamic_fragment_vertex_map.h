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

#ifndef ANALYTICAL_ENGINE_CORE_VERTEX_MAP_DYNAMIC_FRAGMENT_VERTEX_MAP_H_
#define ANALYTICAL_ENGINE_CORE_VERTEX_MAP_DYNAMIC_FRAGMENT_VERTEX_MAP_H_

#include "grape/vertex_map/global_vertex_map.h"

namespace gs {

/**
 * @brief a kind of VertexMap designed for DynamicFragment which can holds
 * global mapping information in each worker or hold whole graph mapping
 * information as one fragment.
 *
 * @tparam OID_T
 * @tparam VID_T
 */
template <typename OID_T, typename VID_T>
class DynamicFragmentVertexMap : public grape::GlobalVertexMap<OID_T, VID_T> {

  using Base = grape::GlobalVertexMap<OID_T, VID_T>;

 public:
  explicit DynamicFragmentVertexMap(const grape::CommSpec& comm_spec) :
      Base(comm_spec), duplicated_load_(false) {}
  ~DynamicFragmentVertexMap() = default;

  void Init() override {
    Base::Init();
  }

  void Init(fid_t fnum) override {
    Base::Init(fnum);
    duplicated_load_ = true;
  }

  void Construct() override {
    if (!duplicated_load_) {
      // only the multiple fragments vertex map need to construct.
      Base::Construct();
    }
  }

 private:
  bool duplicated_load_;  // duplicated load (whole) graph flag
};

}  // namespace gs


#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_DYNAMIC_FRAGMENT_VERTEX_MAP_H_
