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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_K_CENTER_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_K_CENTER_H_

#include <memory>
#include <utility>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class KCenterFlash : public FlashAppBase<FRAG_T, BFS_TYPE> {
 public:
  INSTALL_FLASH_WORKER(KCenterFlash<FRAG_T>, BFS_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, BFS_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw, int k) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run K-center with Flash, total vertices: " << n_vertex
              << ", k = " << k << std::endl;

    std::pair<int, int> v_loc = std::make_pair(0, 0), v_glb;
    DefineMapV(init) {
      if (Deg(id) > v_loc.first)
        v_loc = std::make_pair(Deg(id), id);
      v.dis = INT_MAX;
    };
    VertexMapSeq(All, CTrueV, init);

    for (int i = 0; i < k; ++i) {
      GetMax(v_loc, v_glb);
      int sid = v_glb.second;
      LOG(INFO) << "Round " << i << ": max_min_dis=" << v_glb.first
                << std::endl;

      DefineFV(filter) { return id == sid; };
      DefineMapV(local) { v.dis = 0; };
      vset_t A = VertexMap(All, filter, local);

      for (int len = VSize(A), j = 1; len > 0; len = VSize(A), ++j) {
        DefineFE(check) { return d.dis > j; };
        DefineMapE(update) { d.dis = j; };
        A = EdgeMapSparse(A, EU, check, update, CTrueV, update);
      }

      v_loc = std::make_pair(0, 0);
      TraverseLocal(
          if (v.dis > v_loc.first) { v_loc = std::make_pair(v.dis, id); });
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_K_CENTER_H_
