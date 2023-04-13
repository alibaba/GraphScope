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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_BC_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_BC_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class BCFlash : public FlashAppBase<FRAG_T, BC_TYPE> {
 public:
  INSTALL_FLASH_WORKER(BCFlash<FRAG_T>, BC_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, BC_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->b); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
           oid_t o_source) {
    vid_t s = Oid2FlashId(o_source);
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run BC with Flash, total vertices: " << n_vertex << std::endl;

    vset_t a = All;

    int curLevel;

    DefineMapV(init) {
      if (id == s) {
        v.d = 0;
        v.c = 1;
        v.b = 0;
      } else {
        v.d = -1;
        v.c = 0;
        v.b = 0;
      }
    };
    DefineFV(filter) { return id == s; };

    DefineMapE(update1) { d.c += s.c; };
    DefineFV(cond) { return v.d == -1; };
    DefineMapE(reduce1) { d.c += s.c; };
    DefineMapV(local) { v.d = curLevel; };

    DefineMapE(update2) { d.b += d.c / s.c * (1 + s.b); };

    std::function<void(vset_t&, int)> bn = [&](vset_t& S, int h) {
      curLevel = h;
      int sz = VSize(S);
      if (sz == 0)
        return;
      LOG(INFO) << "size= " << sz << std::endl;
      vset_t T = EdgeMap(S, ED, CTrueE, update1, cond, reduce1);
      T = VertexMap(T, CTrueV, local);
      bn(T, h + 1);
      LOG(INFO) << "-size= " << sz << std::endl;
      curLevel = h;
      EdgeMap(T, EjoinV(ER, S), CTrueE, update2, CTrueV);
    };

    vset_t S = VertexMap(All, CTrueV, init);
    S = VertexMap(S, filter);

    bn(S, 1);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CENTRALITY_BC_H_
