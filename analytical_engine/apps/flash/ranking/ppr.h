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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_RANKING_PPR_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_RANKING_PPR_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class PPRFlash : public FlashAppBase<FRAG_T, PR_TYPE> {
 public:
  INSTALL_FLASH_WORKER(PPRFlash<FRAG_T>, PR_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, PR_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->val); };

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
           oid_t o_source, int max_iters) {
    int n_vertex = graph.GetTotalVerticesNum();
    vid_t source = Oid2FlashId(o_source);
    LOG(INFO) << "Run PPR with Flash, max_iters: " << max_iters
              << ", total vertices: " << n_vertex << std::endl;

    DefineMapV(init_v) {
      v.val = 0;
      v.next = (id == source ? 0.5 : 0);
      v.deg = Deg(id);
    };
    VertexMap(All, CTrueV, init_v);
    LOG(INFO) << "Init complete" << std::endl;

    DefineFV(filter) { return id == source; };
    DefineMapV(local) { v.val = 1.0f; };
    vset_t A = VertexMap(All, filter, local);

    for (int i = 0; i < max_iters; i++) {
      LOG(INFO) << "Round " << i << std::endl;
      DefineMapE(update) { d.next += 0.5 * s.val / s.deg; };
      A = EdgeMapDense(All, EU, CTrueE, update, CTrueV, false);

      DefineMapV(local2) {
        v.val = v.next;
        v.next = (id == source ? 0.5 : 0);
      };
      A = VertexMap(All, CTrueV, local2);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_RANKING_PPR_H_
