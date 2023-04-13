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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_COVER_GREEDY_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_COVER_GREEDY_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class MinCoverGreedyFlash : public FlashAppBase<FRAG_T, MIN_COVER_TYPE> {
 public:
  INSTALL_FLASH_WORKER(MinCoverGreedyFlash<FRAG_T>, MIN_COVER_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, MIN_COVER_TYPE, int>;

  bool sync_all_ = false;
  int n_mc;

  int GlobalRes() { return n_mc; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Min Cover with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.c = false;
      v.d = Deg(id);
      v.tmp = 0;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    for (int i = 0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;

      DefineFV(filter1) {
        for_nb(if (!nb.c && (nb.d > v.d || (nb.d == v.d && nb_id > id))) {
          return false;
        });
        return true;
      };
      DefineMapV(local1) { v.c = true; };
      vset_t B = VertexMap(A, filter1, local1);
      int cnt = VSize(B);
      LOG(INFO) << "selected=" << cnt << std::endl;

      DefineFE(check) { return !d.c; };
      DefineMapE(update) { d.tmp++; };
      DefineMapE(reduce) { d.tmp += s.tmp; };
      B = EdgeMapSparse(B, EU, check, update, CTrueV, reduce);

      DefineMapV(local2) {
        v.d -= v.tmp;
        v.tmp = 0;
      };
      VertexMap(B, CTrueV, local2);

      DefineFV(filter2) { return !v.c && v.d > 0; };
      A = VertexMap(A, filter2);
    }

    DefineFV(filter) { return v.c; };
    A = VertexMap(All, filter);
    n_mc = VSize(A);
    LOG(INFO) << "size of vertex-cover = " << n_mc << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_COVER_GREEDY_H_
