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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_CLUSTERING_COEFF_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_CLUSTERING_COEFF_H_

#include <memory>
#include <vector>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

#define COND nb.deg > v.deg || (nb.deg == v.deg && nb_id > id)

namespace gs {

template <typename FRAG_T>
class ClusteringCoeffFlash : public FlashAppBase<FRAG_T, K_CLIQUE_2_TYPE> {
 public:
  INSTALL_FLASH_WORKER(ClusteringCoeffFlash<FRAG_T>, K_CLIQUE_2_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, K_CLIQUE_2_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->count); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int64_t n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run clustering-coeff with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.count = 0;
      v.deg = Deg(id);
    };
    VertexMap(All, CTrueV, init);

    LOG(INFO) << "Loading..." << std::endl;
    DefineMapV(local) {
      v.out.clear();
      for_nb(if (COND) { v.out.push_back(nb_id); });
    };
    VertexMap(All, CTrueV, local);

    LOG(INFO) << "Computing..." << std::endl;

    std::vector<bool> p(n_vertex, false);
    std::vector<int64_t> cnt(n_vertex, 0);

    DefineMapV(update) {
      for (auto u : v.out)
        p[u] = true;
      for_nb(if (COND) {
        for (auto& u : nb.out)
          if (p[u]) {
            cnt[id]++;
            cnt[nb_id]++;
            cnt[u]++;
          }
      });
      for (auto u : v.out)
        p[u] = false;
    };

    VertexMapSeq(All, CTrueV, update);

    DefineMapV(final) { v.count = cnt[id]; };
    VertexMap(All, CTrueV, final);

    int64_t cnt_loc = 0, cnt_all;
    Traverse(cnt_loc += cnt[id];);
    GetSum(cnt_loc, cnt_all);
    LOG(INFO) << "Totol count = " << cnt_all << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_CLUSTERING_COEFF_H_
