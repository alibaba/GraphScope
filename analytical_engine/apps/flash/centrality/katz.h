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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_KATZ_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_KATZ_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class KATZFlash : public FlashAppBase<FRAG_T, KATZ_TYPE> {
 public:
  INSTALL_FLASH_WORKER(KATZFlash<FRAG_T>, KATZ_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, KATZ_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->val); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run katz-centrality with Flash, total vertices: " << n_vertex
              << std::endl;

    double alpha = 0.1;
    DefineMapV(init) {
      v.val = 1.0;
      v.next = 0;
    };
    VertexMap(All, CTrueV, init);

    DefineMapE(update) { d.next += s.val + 1; };
    DefineMapV(local) {
      v.val = v.next * alpha;
      v.next = 0;
    };

    for (int i = 0; i < 10; i++) {
      LOG(INFO) << "Round " << i << std::endl;
      EdgeMapDense(All, ER, CTrueE, update, CTrueV, false);
      VertexMap(All, CTrueV, local);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_KATZ_H_
