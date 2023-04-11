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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_DIAMETER_APPROX_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_DIAMETER_APPROX_H_

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class DiameterApproxFlash : public FlashAppBase<FRAG_T, DIAMETER_TYPE> {
 public:
  INSTALL_FLASH_WORKER(DiameterApproxFlash<FRAG_T>, DIAMETER_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, DIAMETER_TYPE, int>;

  bool sync_all_ = false;
  int dd, rr;

  int GlobalRes() { return dd; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run diameter-approx with Flash, total vertices: " << n_vertex
              << std::endl;

    std::vector<vid_t> s;
    int64_t one = 1;
    int k = 64;
    uint32_t seed = time(NULL);
    for (int i = 0; i < k; ++i)
      s.push_back(rand_r(&seed) % n_vertex);

    DefineMapV(init) { v.ecc = 0; };
    VertexMap(All, CTrueV, init);

    auto multi_source_bfs = [&]() {
      DefineMapV(local1) { v.seen = 0; };
      vset_t C = All, S = VertexMap(All, CTrueV, local1);

      DefineFV(filter2) { return find(s, id); };
      DefineMapV(local2) {
        int p = locate(s, id);
        v.seen |= one << p;
      };
      S = VertexMapSeq(S, filter2, local2);

      for (int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
        LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
        DefineFE(check) { return (s.seen & (~d.seen)); };
        DefineMapE(update) {
          d.seen |= (s.seen & (~d.seen));
          d.ecc = std::max(d.ecc, i);
        };
        S = EdgeMapDense(S, EjoinV(EU, C), check, update, CTrueV);
      }
    };

    multi_source_bfs();

    std::vector<std::pair<int, int>> c(k, std::make_pair(-1, -1)), t(k);
    DefineMapV(local) {
      int p = 0;
      for (int i = 1; i < k; ++i)
        if (c[i] < c[p])
          p = i;
      if (v.ecc > c[p].first)
        c[p] = std::make_pair(v.ecc, static_cast<int>(id));
    };
    VertexMapSeq(All, CTrueV, local);
    std::sort(c.begin(), c.end());
    Reduce(c, t, std::reverse(t, t + len); for_i(t[i] = std::max(t[i], c[i]));
           std::sort(t, t + len));
    s.clear();
    for (int i = 0; i < k; ++i)
      s.push_back(t[i].second);

    multi_source_bfs();

    int d = 0, r = n_vertex;
    TraverseLocal(int e = v.ecc; d = std::max(d, e);
                  if (e != 0) { r = std::min(r, e); });
    GetMax(d, dd);
    GetMin(r, rr);
    LOG(INFO) << "diameter=" << dd << ", radius=" << rr << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_DIAMETER_APPROX_H_
