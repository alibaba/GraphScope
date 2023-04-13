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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_RANDOM_MULTI_BFS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_RANDOM_MULTI_BFS_H_

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
class RandomMultiBFSFlash : public FlashAppBase<FRAG_T, MULTI_BFS_TYPE> {
 public:
  INSTALL_FLASH_WORKER(RandomMultiBFSFlash<FRAG_T>, MULTI_BFS_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, MULTI_BFS_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->res); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run random multi-source BFS with Flash, total vertices: "
              << n_vertex << std::endl;

    std::vector<vid_t> s;
    int64_t one = 1;
    int k = 64;
    uint32_t seed = time(NULL);
    for (int i = 0; i < k; ++i)
      s.push_back(rand_r(&seed) % n_vertex);

    DefineMapV(init) {
      v.d.resize(k);
      for (int i = 0; i < k; i++)
        v.d[i] = -1;
      v.seen = 0;
    };
    vset_t C = VertexMap(All, CTrueV, init);

    DefineFV(filter) { return find(s, id); };
    DefineMapV(local) {
      int p = locate(s, id);
      v.seen |= one << p;
      v.d[p] = 0;
    };
    vset_t S = VertexMap(All, filter, local);

    for (int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
      DefineFE(check) { return (s.seen & (~d.seen)); };
      DefineMapE(update) {
        int64_t b = (s.seen & (~d.seen));
        if (b)
          d.seen |= b;
        for (int p = 0; p < k; ++p)
          if (b & (one << p))
            d.d[p] = i;
      };
      S = EdgeMapDense(All, ED, check, update, CTrueV);
    }

    DefineMapV(final) {
      v.res = -1;
      for (int i = 0; i < k; ++i)
        v.res = std::max(v.res, v.d[i]);
    };
    VertexMap(All, CTrueV, final);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_RANDOM_MULTI_BFS_H_
