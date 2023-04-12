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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_DOMINATING_SET_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_DOMINATING_SET_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class MinDominatingSetFlash
    : public FlashAppBase<FRAG_T, MIN_DOMINATING_SET_TYPE> {
 public:
  INSTALL_FLASH_WORKER(MinDominatingSetFlash<FRAG_T>, MIN_DOMINATING_SET_TYPE,
                       FRAG_T)
  using context_t =
      FlashGlobalDataContext<FRAG_T, MIN_DOMINATING_SET_TYPE, int>;

  bool sync_all_ = false;
  int n_mc;

  int GlobalRes() { return n_mc; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Min Dominating Set with Flash, total vertices: "
              << n_vertex << std::endl;

    DefineMapV(init) {
      v.max_cnt = Deg(id);
      v.d = false;
      v.b = false;
      v.max_id = id;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    DefineMapV(local) {
      for_nb(if (!nb.d && (nb.max_cnt > v.max_cnt ||
                           (nb.max_cnt == v.max_cnt && nb.max_id > v.max_id))) {
        v.max_cnt = nb.max_cnt;
        v.max_id = nb.max_id;
      })
    };

    for (int i = 0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
      VertexMap(A, CTrueV, local);
      VertexMap(A, CTrueV, local);

      DefineFV(filter1) { return id == v.max_id; };
      DefineMapV(local1) {
        v.b = true;
        v.d = true;
      };
      vset_t B = VertexMap(A, filter1, local1);
      int cnt = VSize(B);
      LOG(INFO) << "Round " << i << ": len=" << len << ", " << cnt << " added"
                << std::endl;

      DefineFE(check) { return !d.d; };
      DefineMapE(update) { d.d = true; };
      EdgeMapSparse(B, EU, check, update, CTrueV, update);

      DefineFV(filter2) { return !v.d; };
      DefineMapV(local2) {
        v.max_id = id;
        v.max_cnt = 0;
        for_nb(if (!nb.d) { ++v.max_cnt; });
      };
      A = VertexMap(A, filter2, local2);
    }

    DefineFV(filter) { return v.b; };
    A = VertexMap(All, filter);
    n_mc = VSize(A);
    LOG(INFO) << "size of min dominating set = " << n_mc << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_DOMINATING_SET_H_
