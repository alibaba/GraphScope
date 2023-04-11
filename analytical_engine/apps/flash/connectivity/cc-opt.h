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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_CC_OPT_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_CC_OPT_H_

#include <algorithm>
#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class CCOptFlash : public FlashAppBase<FRAG_T, CC_OPT_TYPE> {
 public:
  INSTALL_FLASH_WORKER(CCOptFlash<FRAG_T>, CC_OPT_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CC_OPT_TYPE, int64_t>;

  bool sync_all_ = false;

  int64_t* Res(value_t* v) { return &(v->cid); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int64_t n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run CC with Flash, total vertices: " << n_vertex << std::endl;
    int64_t v_loc = 0, v_glb = 0;

    DefineMapV(init) {
      v.cid = Deg(id) * n_vertex + id;
      v_loc = std::max(v_loc, v.cid);
    };
    DefineFV(filter) { return v.cid == v_glb; };

    VertexMap(All, CTrueV, init);
    GetMax(v_loc, v_glb);
    vset_t A = VertexMap(All, filter);

    DefineFV(cond) { return v.cid != v_glb; };
    DefineMapE(update) { d.cid = v_glb; };
    DefineMapE(reduce) { d = s; };

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Round 0." << i << ": size = " << len << std::endl;
      A = EdgeMap(A, EU, CTrueE, update, cond, reduce);
    }

    DefineFV(filter2) { return v.cid != v_glb; };
    A = VertexMap(All, filter2);

    DefineFE(check2) { return s.cid > d.cid; };
    DefineMapE(update2) { d.cid = std::max(d.cid, s.cid); };

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "Round 1." << i << ": size = " << len << std::endl;
      A = EdgeMap(A, EU, check2, update2, CTrueV, update2);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_CC_OPT_H_
