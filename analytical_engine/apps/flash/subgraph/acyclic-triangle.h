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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_ACYCLIC_TRIANGLE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_ACYCLIC_TRIANGLE_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class AcyclicTriangleFlash : public FlashAppBase<FRAG_T, TRIANGLE_TYPE> {
 public:
  INSTALL_FLASH_WORKER(AcyclicTriangleFlash<FRAG_T>, TRIANGLE_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, TRIANGLE_TYPE, int64_t>;

  bool sync_all_ = false;
  int64_t cnt_all;

  int64_t GlobalRes() { return cnt_all; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Acyclic Triangle Counting with Flash, total vertices: "
              << n_vertex << std::endl;

    DefineMapV(init) {
      v.count = 0;
      v.out.clear();
    };
    VertexMap(All, CTrueV, init);

    DefineMapE(update) { d.out.insert(sid); };

    DefineMapE(update2) {
      for (auto& x : s.out) {
        if (d.out.find(x) != d.out.end())
          d.count++;
      }
    };

    EdgeMapDense(All, ER, CTrueE, update, CTrueV);
    EdgeMapDense(All, ED, CTrueE, update2, CTrueV, false);

    int64_t cnt = 0;
    cnt_all = 0;
    TraverseLocal(cnt += v.count;);
    GetSum(cnt, cnt_all);
    LOG(INFO) << "number of acyclic triangles = " << cnt_all << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_ACYCLIC_TRIANGLE_H_
