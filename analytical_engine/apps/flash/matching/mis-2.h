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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIS_2_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIS_2_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class MIS2Flash : public FlashAppBase<FRAG_T, MIS_2_TYPE> {
 public:
  INSTALL_FLASH_WORKER(MIS2Flash<FRAG_T>, MIS_2_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, MIS_2_TYPE, int>;

  bool sync_all_ = false;
  int n_mis;

  int GlobalRes() { return n_mis; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int64_t n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run MIS with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) { v.d = false; };

    vset_t A = VertexMap(All, CTrueV, init);

    DefineMapV(local) { v.b = true; };

    DefineFE(check) { return !s.d && sid > did; };
    DefineMapE(update) { d.b = false; };
    DefineFV(filter) { return v.b; };

    DefineMapE(update2){};
    DefineFV(cond) { return !v.d; };
    DefineMapE(reduce) { d.d = true; };

    DefineFV(filter2) { return !v.b; };

    for (int i = 0, len = VSize(A); len > 0; ++i) {
      A = VertexMap(A, CTrueV, local);
      EdgeMapDense(All, EjoinV(EU, A), check, update, filter);

      vset_t B = VertexMap(A, filter);
      vset_t C = EdgeMapSparse(B, EU, CTrueE, update2, cond, reduce);
      A = A.Minus(C);
      A = VertexMap(A, filter2);

      int num = VSize(B);
      len = VSize(A);
      LOG(INFO) << "Round " << i << ": size=" << len << ", selected=" << num
                << std::endl;
    }

    A = VertexMap(All, filter);
    n_mis = VSize(A);
    LOG(INFO) << "size of max independent set = " << n_mis << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIS_2_H_
