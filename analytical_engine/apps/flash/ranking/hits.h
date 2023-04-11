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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_RANKING_HITS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_RANKING_HITS_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class HITSFlash : public FlashAppBase<FRAG_T, HITS_TYPE> {
 public:
  INSTALL_FLASH_WORKER(HITSFlash<FRAG_T>, HITS_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, HITS_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->auth); };

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
           int max_iters) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run HITS with Flash, max_iters: " << max_iters
              << ", total vertices: " << n_vertex << std::endl;

    DefineMapV(init_v) {
      v.auth = 1.0;
      v.hub = 1.0;
      v.auth1 = 0;
      v.hub1 = 0;
    };
    VertexMap(All, CTrueV, init_v);

    double sa = 0, sh = 0, sa_all = 0, sh_all = 0, sa_tot = 0, sh_tot = 0;

    DefineMapE(update1) { d.auth1 += s.hub; };
    DefineMapE(update2) { d.hub1 += s.auth; };
    DefineMapV(local) {
      v.auth = v.auth1 / sa_all;
      v.hub = v.hub1 / sh_all;
      v.auth1 = 0;
      v.hub1 = 0;
    };

    for (int i = 0; i < max_iters; ++i) {
      LOG(INFO) << "Round " << i << std::endl;
      sa = 0, sh = 0, sa_all = 0, sh_all = 0;

      EdgeMapDense(All, ED, CTrueE, update1, CTrueV, false);
      EdgeMapDense(All, ER, CTrueE, update2, CTrueV, false);

      TraverseLocal(sa += v.auth1 * v.auth1; sh += v.hub1 * v.hub1;);
      GetSum(sa, sa_tot);
      GetSum(sh, sh_tot);
      sa_all = sqrt(sa_tot);
      sh_all = sqrt(sh_tot);
      VertexMap(All, CTrueV, local);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_RANKING_HITS_H_
