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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CORE_DEGENERACY_ORDERING_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CORE_DEGENERACY_ORDERING_H_

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
class DegeneracyFlash : public FlashAppBase<FRAG_T, DEGENERACY_TYPE> {
 public:
  INSTALL_FLASH_WORKER(DegeneracyFlash<FRAG_T>, DEGENERACY_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, DEGENERACY_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->rank); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Degeneracy Ordering with Flash, total vertices: "
              << n_vertex << std::endl;

    DefineMapV(init) {
      v.core = std::min(SHRT_MAX, Deg(id));
      v.rank = -1;
      v.d = Deg(id);
      v.tmp = 0;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    DefineMapE(none){};

    std::vector<int> cnt(SHRT_MAX);
    DefineMapV(local) {
      v.old = v.core;
      int nowcnt = 0;
      for_nb(if (nb.core >= v.core) { ++nowcnt; });
      if (nowcnt >= v.core)
        return;
      memset(cnt.data(), 0, sizeof(int) * (v.core + 1));
      for_nb(++cnt[std::min(v.core, nb.core)];);
      for (int s = 0; s + cnt[v.core] < v.core; --v.core)
        s += cnt[v.core];
    };

    DefineFV(filter) { return v.old != v.core; };

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Core Round " << i << ": size=" << len << std::endl;
      if (len < THRESHOLD) {
        A = EdgeMapSparse(A, EU, CTrueE, none, CTrueV);
        A = VertexMapSeq(A, CTrueV, local);
      } else {
        A = VertexMapSeq(All, CTrueV, local);
      }
      A = VertexMap(A, filter);
    }

    int max_core = 0, dg;
    TraverseLocal(max_core = std::max(max_core, v.core););
    GetMax(max_core, dg);
    LOG(INFO) << "degeneracy = " << dg << std::endl;

    A = All;
    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Ranking Round " << i << ": size=" << len << std::endl;
      DefineFV(filterd) { return v.d <= dg; };
      DefineMapV(localr) { v.rank = i; };
      A = VertexMap(A, filterd, localr);

      DefineFV(cond) { return v.rank == -1; };
      DefineMapE(update) { d.tmp++; };
      DefineMapE(reduce) { d.tmp += s.tmp; };
      A = EdgeMapSparse(A, EU, CTrueE, update, cond, reduce);

      DefineMapV(locald) {
        v.d -= v.tmp;
        v.tmp = 0;
      };
      A = VertexMap(A, CTrueV, locald);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CORE_DEGENERACY_ORDERING_H_
