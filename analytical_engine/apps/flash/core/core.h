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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CORE_CORE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CORE_CORE_H_

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
class CoreFlash : public FlashAppBase<FRAG_T, CORE_TYPE> {
 public:
  INSTALL_FLASH_WORKER(CoreFlash<FRAG_T>, CORE_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CORE_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->core); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int64_t n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run K-core with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) { v.core = std::min(SHRT_MAX, Deg(id)); };
    vset_t A = VertexMap(All, CTrueV, init);

    DefineMapV(local1) {
      v.cnt = 0;
      v.s.clear();
    };

    DefineFE(check1) { return s.core >= d.core; };
    DefineMapE(update1) { d.cnt++; };

    DefineFV(filter) { return v.cnt < v.core; };

    DefineMapE(update2) { d.s.push_back(s.core); };

    DefineMapV(local2) {
      std::vector<int> cnt((v.core + 1));
      memset(cnt.data(), 0, sizeof(int) * (v.core + 1));
      for (auto& i : v.s) {
        ++cnt[std::min(v.core, i)];
      }
      for (int s = 0; s + cnt[v.core] < v.core; --v.core)
        s += cnt[v.core];
    };

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
      A = VertexMap(All, CTrueV, local1, false);
      EdgeMapDense(All, EU, check1, update1, CTrueV, false);
      A = VertexMap(All, filter);
      EdgeMapDense(All, EjoinV(EU, A), CTrueE, update2, CTrueV, false);
      A = VertexMap(A, CTrueV, local2);
    }

    int64_t sum_core = 0, tot_sum_core = 0;
    TraverseLocal(sum_core += v.core;);
    GetSum(sum_core, tot_sum_core);
    LOG(INFO) << "sum_core=" << tot_sum_core << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CORE_CORE_H_
