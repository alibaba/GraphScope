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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_DENSEST_SUB_2_APPROX_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_DENSEST_SUB_2_APPROX_H_

#include <algorithm>
#include <memory>
#include <vector>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class DensestFlash : public FlashAppBase<FRAG_T, DENSEST_TYPE> {
 public:
  INSTALL_FLASH_WORKER(DensestFlash<FRAG_T>, DENSEST_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, DENSEST_TYPE, double>;

  bool sync_all_ = false;
  double density;

  double GlobalRes() { return density; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run densest-sub-2-approx with Flash, total vertices: "
              << n_vertex << std::endl;

    DefineMapV(init) {
      v.core = std::min(SHRT_MAX, Deg(id));
      v.t = v.core;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    std::vector<int> cnt(SHRT_MAX);
    DefineMapV(update) {
      int nowcnt = 0;
      for_nb(if (nb.core >= v.core) { ++nowcnt; });
      if (nowcnt >= v.core)
        return;
      memset(cnt.data(), 0, sizeof(int) * (v.core + 1));
      for_nb(++cnt[std::min(v.core, nb.core)];);
      for (int s = 0; s + cnt[v.core] < v.core; --v.core)
        s += cnt[v.core];
    };

    DefineFV(filter) { return v.core != v.t; };
    DefineMapV(local) { v.t = v.core; };

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Round " << i << ", len = " << len << std::endl;
      A = VertexMapSeq(All, CTrueV, update, false);
      A = VertexMap(All, filter, local);
    }

    int cloc = 0, cmax = 0;
    TraverseLocal(cloc = std::max(cloc, v.core););
    GetMax(cloc, cmax);

    int nvloc = 0, neloc = 0, nv, ne;

    DefineFV(check) { return v.core == cmax; };
    DefineMapV(local2) {
      for_nb(if (nb.core == cmax) { ++neloc; });
      ++nvloc;
    };
    VertexMapSeq(All, check, local2, false);
    GetSum(nvloc, nv);
    GetSum(neloc, ne);

    density = ne * 1.0 / nv;
    LOG(INFO) << "density = " << density << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_DENSEST_SUB_2_APPROX_H_
