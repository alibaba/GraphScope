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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_EIGENVEC_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_EIGENVEC_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class EigenvecFlash : public FlashAppBase<FRAG_T, KATZ_TYPE> {
 public:
  INSTALL_FLASH_WORKER(EigenvecFlash<FRAG_T>, KATZ_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, KATZ_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->val); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run eigenvec-centrality with Flash, total vertices: "
              << n_vertex << std::endl;

    DefineMapV(init) { v.val = 1.0 / n_vertex; };
    VertexMap(All, CTrueV, init);

    double s = 0, s_tot = 0;
    DefineMapE(update) { d.next += s.val; };
    DefineMapV(local1) {
      v.val = v.next;
      v.next = 0;
    };
    DefineMapV(local2) { v.val /= s_tot; };

    for (int i = 0; i < 10; i++) {
      LOG(INFO) << "Round " << i << std::endl;
      s = 0;
      s_tot = 0;
      EdgeMapDense(All, EU, CTrueE, update, CTrueV, false);
      VertexMap(All, CTrueV, local1, false);
      TraverseLocal(s += v.val;);
      GetSum(s, s_tot);
      s_tot = sqrt(s_tot);
      VertexMap(All, CTrueV, local2);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_EIGENVEC_H_
