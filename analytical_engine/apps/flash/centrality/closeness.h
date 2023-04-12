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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_CLOSENESS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_CLOSENESS_H_

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
class ClosenessFlash : public FlashAppBase<FRAG_T, CLOSENESS_TYPE> {
 public:
  INSTALL_FLASH_WORKER(ClosenessFlash<FRAG_T>, CLOSENESS_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CLOSENESS_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->val); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run closeness-centrality with Flash, total vertices: "
              << n_vertex << std::endl;

    std::vector<int> s;
    int64_t one = 1, n_sample = 500;

    DefineMapV(init) {
      v.val = 0;
      v.cnt = 0;
    };
    VertexMap(All, CTrueV, init);

    DefineMapV(local1) { v.seen = 0; };
    DefineFV(filter2) { return find(s, static_cast<int>(id)); };
    DefineMapV(local2) {
      int p = locate(s, static_cast<int>(id));
      v.seen |= one << p;
    };

    uint32_t seed = time(NULL);
    for (int p = 0; p < n_sample; p += 64) {
      LOG(INFO) << "Phase" << p / 64 + 1 << std::endl;
      s.clear();
      for (int i = p; i < n_sample && i < p + 64; ++i)
        s.push_back(rand_r(&seed) % n_vertex);

      int l = s.size();
      vset_t C = All;
      vset_t S = VertexMap(All, CTrueV, local1);
      S = VertexMap(S, filter2, local2);

      for (int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
        DefineFE(check) { return s.seen & (~d.seen); };
        DefineMapE(update) {
          int64_t b = s.seen & (~d.seen);
          d.seen |= b;
          for (int p = 0; p < l; ++p)
            if (b & (one << p)) {
              d.val += i;
              ++d.cnt;
            }
        };
        LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
        S = EdgeMapDense(All, EU, check, update, CTrueV);
      }
    }

    DefineMapV(final) {
      if (v.cnt)
        v.val /= v.cnt;
    };
    VertexMap(All, CTrueV, final, false);

    double best_local = n_vertex * 1.0, best;
    TraverseLocal(if (v.cnt && v.val < best_local) { best_local = v.val; });
    GetMin(best_local, best);
    LOG(INFO) << "best_val=" << best << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_CLOSENESS_H_
