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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_K_CLIQUE_2_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_K_CLIQUE_2_H_

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
class KClique2Flash : public FlashAppBase<FRAG_T, K_CLIQUE_2_TYPE> {
 public:
  INSTALL_FLASH_WORKER(KClique2Flash<FRAG_T>, K_CLIQUE_2_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, K_CLIQUE_2_TYPE, int64_t>;

  bool sync_all_ = false;
  int64_t cnt_all;

  int64_t GlobalRes() { return cnt_all; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw, int k) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run K-clique Counting with Flash, total vertices: "
              << n_vertex << ", k: " << k << std::endl;

    LOG(INFO) << "Loading..." << std::endl;
    DefineMapV(init) {
      v.deg = Deg(id);
      v.count = 0;
      v.out.clear();
    };
    VertexMap(All, CTrueV, init);

    DefineFE(check) {
      return (s.deg > d.deg) || (s.deg == d.deg && sid > did);
    };
    DefineMapE(update) { d.out.push_back(sid); };
    EdgeMapDense(All, EU, check, update, CTrueV);

    LOG(INFO) << "Computing..." << std::endl;
    std::vector<std::vector<int>> c(k - 1),
        s(k - 1, std::vector<int>((n_vertex + 31) / 32, 0));
    std::function<void(std::vector<int>&, int, int&)> compute =
        [&](std::vector<int>& cand, int nowk, int& res) {
          if (nowk == k) {
            res++;
            return;
          }
          for (auto& u : cand)
            s[nowk - 1][u / 32] |= 1 << (u % 32);
          for (auto& u : cand) {
            int len = 0;
            c[nowk - 1].resize(cand.size());
            for (auto& w : GetV(u)->out)
              if (s[nowk - 1][w / 32] & (1 << (w % 32)))
                c[nowk - 1][len++] = w;
            if (len < k - nowk - 1)
              continue;
            c[nowk - 1].resize(len);
            compute(c[nowk - 1], nowk + 1, res);
          }
          for (auto& u : cand)
            s[nowk - 1][u / 32] = 0;
        };

    DefineMapV(local) {
      if (static_cast<int>(v.out.size()) < k - 1)
        return;
      compute(v.out, 1, v.count);
    };
    VertexMapSeq(All, CTrueV, local, false);

    int64_t cnt = 0;
    cnt_all = 0;
    TraverseLocal(cnt += v.count;);
    GetSum(cnt, cnt_all);
    LOG(INFO) << "number of k-cliques = " << cnt_all << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_K_CLIQUE_2_H_
