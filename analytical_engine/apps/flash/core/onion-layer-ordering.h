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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CORE_ONION_LAYER_ORDERING_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CORE_ONION_LAYER_ORDERING_H_

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
class OnionFlash : public FlashAppBase<FRAG_T, ONION_TYPE> {
 public:
  INSTALL_FLASH_WORKER(OnionFlash<FRAG_T>, ONION_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, ONION_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->rank); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run onion-layer ordering with Flash, total vertices: "
              << n_vertex << std::endl;

    int maxd = 30000;
    DefineMapV(init) { v.core = std::min(maxd, Deg(id)); };
    vset_t A = VertexMap(All, CTrueV, init);

    std::vector<int> cnt(30000);
    DefineMapV(local) {
      v.old = v.core;
      int nowcnt = 0;
      for_nb(if (nb.core >= v.core) { ++nowcnt; });
      if (nowcnt >= v.core)
        return;
      memset(cnt.data(), 0, sizeof(int) * (v.core + 1));
      for_nb(++cnt[std::min(v.core, nb.core)];);
      for (int s = 0; s + cnt[v.core] < v.core; --v.core)
        s += cnt[v.core];
    };

    DefineFV(filter) { return v.old != v.core; };
    DefineMapV(update) { v.old = v.core; };

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Core Round " << i << ": size=" << len << std::endl;
      VertexMapSeq(All, CTrueV, local, false);
      A = VertexMap(All, filter, update);
    }

    DefineMapV(initrank) {
      v.rank = -1;
      v.d = 0;
      v.tmp = 0;
      for_nb(if (nb.core >= v.core) { ++v.d; });
    };
    A = VertexMap(All, CTrueV, initrank);

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Ranking Round " << i << ": size=" << len << std::endl;

      DefineFV(filter2) { return v.d <= v.core; };
      DefineMapV(local2) { v.rank = i; };
      A = VertexMap(A, filter2, local2);

      DefineFE(check) { return d.core == s.core && d.rank == -1; };
      DefineMapE(updated) { d.tmp++; };
      DefineMapE(reduce) { d.tmp += s.tmp; };
      A = EdgeMapSparse(A, EU, check, updated, CTrueV, reduce);

      DefineMapV(local3) {
        v.d -= v.tmp;
        v.tmp = 0;
      };
      A = VertexMap(A, CTrueV, local3);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CORE_ONION_LAYER_ORDERING_H_
