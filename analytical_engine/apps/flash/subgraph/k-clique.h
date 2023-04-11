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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_K_CLIQUE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_K_CLIQUE_H_

#include <memory>
#include <set>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class KCliqueFlash : public FlashAppBase<FRAG_T, TRIANGLE_TYPE> {
 public:
  INSTALL_FLASH_WORKER(KCliqueFlash<FRAG_T>, TRIANGLE_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, TRIANGLE_TYPE, int64_t>;

  bool sync_all_ = false;
  int64_t cnt_all;

  int64_t GlobalRes() { return cnt_all; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw, int32_t k) {
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
    DefineMapE(update) { d.out.insert(sid); };
    EdgeMapDense(All, EU, check, update, CTrueV);

    LOG(INFO) << "Computing..." << std::endl;
    DefineFV(filter) { return v.out.size() >= k - 1; };
    std::function<void(std::set<int>&, int, int&)> compute =
        [&](std::set<int>& cand, int nowk, int& res) {
          if (nowk == k) {
            res++;
            return;
          }
          std::set<int> c;
          for (auto& u : cand) {
            int len = 0;
            c.clear();
            for (auto& o : GetV(u)->out) {
              if (cand.find(o) != cand.end()) {
                len++;
                c.insert(o);
              }
            }
            if (len < k - nowk - 1)
              continue;
            compute(c, nowk + 1, res);
          }
        };

    DefineMapV(local) { compute(v.out, 1, v.count); };
    VertexMapSeq(All, filter, local, false);

    int64_t cnt = 0;
    cnt_all = 0;
    TraverseLocal(cnt += v.count;);
    GetSum(cnt, cnt_all);
    LOG(INFO) << "number of k-cliques = " << cnt_all << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_SUBGRAPH_K_CLIQUE_H_
