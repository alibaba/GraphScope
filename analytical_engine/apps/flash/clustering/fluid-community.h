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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_FLUID_COMMUNITY_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_FLUID_COMMUNITY_H_

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
class FluidCommunityFlash : public FlashAppBase<FRAG_T, FLUID_TYPE> {
 public:
  INSTALL_FLASH_WORKER(FluidCommunityFlash<FRAG_T>, FLUID_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, FLUID_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->lab); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run fluid-community with Flash, total vertices: " << n_vertex
              << std::endl;

    int s = 10, iter_max = 100;
    std::vector<int> c(s), cnt(s, 0), cnt_loc(s, 0);
    uint32_t seed = time(NULL);
    for (int i = 0; i < s; ++i)
      c[i] = rand_r(&seed) % n_vertex;
    std::sort(c.begin(), c.end());

    DefineMapV(init) {
      v.lab = locate(c, static_cast<int>(id));
      if (v.lab == s)
        v.lab = -1;
      else
        ++cnt_loc[v.lab];
      v.l1 = -2;
      v.l2 = -2;
    };
    VertexMapSeq(All, CTrueV, init);
    DefineFV(filter) { return v.lab >= 0; };

    vset_t A = VertexMap(All, filter);

    std::vector<double> d(s);
    DefineMapV(update) {
      v.old = v.lab;
      if (v.lab >= 0) {
        v.l2 = v.l1;
        v.l1 = v.lab;
      }
      int pre = v.lab;
      memset(d.data(), 0, sizeof(double) * s);
      if (v.lab >= 0)
        d[v.lab] = 1.0 / cnt[v.lab];
      for_nb(if (nb.lab >= 0) { d[nb.lab] += 1.0 / cnt[nb.lab]; });
      for (int i = 0; i < s; ++i)
        if (d[i] > 1e-10 && (v.lab == -1 || d[i] > d[v.lab] + 1e-10))
          v.lab = i;
      if (v.lab >= 0)
        ++cnt_loc[v.lab];
      if (pre >= 0)
        --cnt_loc[pre];
    };

    for (int len = VSize(A), j = 0; len > 0 && j < iter_max;
         len = VSize(A), ++j) {
      Reduce(cnt_loc, cnt, for_i(cnt[i] += cnt_loc[i]));
      int t_cnt = 0;
      for (int i = 0; i < s; ++i)
        t_cnt += cnt[i];
      LOG(INFO) << "Round " << j << ": size=" << len << ", t_cnt=" << t_cnt
                << std::endl;
      VertexMapSeq(All, CTrueV, update, false);

      DefineFV(filter1) { return v.lab != v.old; };
      DefineMapV(local) { v.old = v.lab; };
      A = VertexMap(All, filter1, local);

      DefineFV(filter2) { return v.lab != v.l2; };
      A = VertexMap(A, filter2);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_FLUID_COMMUNITY_H_
