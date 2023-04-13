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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MM_OPT_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MM_OPT_H_

#include <algorithm>
#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class MMOptFlash : public FlashAppBase<FRAG_T, MM_TYPE> {
 public:
  INSTALL_FLASH_WORKER(MMOptFlash<FRAG_T>, MM_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, MM_TYPE, int>;

  bool sync_all_ = false;
  int n_match;

  int GlobalRes() { return n_match; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Maximal Matching with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.s = -1;
      v.p = -1;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    DefineFE(check1) { return s.s == -1; };
    DefineMapE(update1) { d.p = std::max(d.p, static_cast<int>(sid)); };
    DefineFV(cond) { return v.s == -1; };

    DefineOutEdges(edges) { VjoinP(p); };
    DefineFE(check2) { return s.p != -1 && d.p == sid; };
    DefineMapE(update2) { d.s = d.p; };

    DefineFE(check3) { return d.p == sid; };
    DefineMapE(update3) { d.p = -1; };

    for (int i = 0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;

      A = EdgeMapDense(All, EjoinV(EU, A), check1, update1, cond);
      A = EdgeMapSparse(A, edges, check2, update2, cond);
      vset_t B = EdgeMapSparse(A, edges, check2, update2, cond);

      A = A.Union(B);
      A = EdgeMapSparse(A, EU, check3, update3, cond);
    }

    DefineFV(filter) { return v.s >= 0; };
    A = VertexMap(All, filter);
    n_match = VSize(A) / 2;
    LOG(INFO) << "number of matching pairs = " << n_match << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MM_OPT_H_
