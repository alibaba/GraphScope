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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_LPA_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_LPA_H_

#include <map>
#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class LPAFlash : public FlashAppBase<FRAG_T, LPA_TYPE> {
 public:
  INSTALL_FLASH_WORKER(LPAFlash<FRAG_T>, LPA_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, LPA_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->c); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run LPA with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.c = id;
      v.cc = -1;
      v.s.clear();
    };
    vset_t A = VertexMap(All, CTrueV, init);

    DefineMapE(update) { d.s.push_back(s.c); };

    DefineMapV(local1) {
      std::map<int, int> cnt;
      int max_cnt = 0;
      for (auto& i : v.s) {
        cnt[i]++;
        if (cnt[i] > max_cnt) {
          max_cnt = cnt[i];
          v.cc = i;
        }
      }
      v.s.clear();
    };

    DefineFV(filter) { return v.cc != v.c; };
    DefineMapV(local2) { v.c = v.cc; };

    for (int len = VSize(A), i = 0; i<10 & len> 0; len = VSize(A), ++i) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
      A = EdgeMapDense(All, EU, CTrueE, update, CTrueV, false);
      A = VertexMap(All, CTrueV, local1, false);
      A = VertexMap(All, filter, local2);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CLUSTERING_LPA_H_
