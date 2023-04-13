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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_BFS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_BFS_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class BFSFlash : public FlashAppBase<FRAG_T, BFS_TYPE> {
 public:
  INSTALL_FLASH_WORKER(BFSFlash<FRAG_T>, BFS_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, BFS_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
           oid_t o_source) {
    vid_t source = Oid2FlashId(o_source);
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run BFS with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init_v) { v.dis = (id == source) ? 0 : -1; };
    vset_t a = All;
    a = VertexMap(a, CTrueV, init_v);

    DefineFV(f_filter) { return id == source; };
    a = VertexMap(a, f_filter);

    DefineMapE(update) { d.dis = s.dis + 1; };
    DefineFV(cond) { return v.dis == -1; };

    for (int len = VSize(a), i = 1; len > 0; len = VSize(a), ++i) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
      a = EdgeMap(a, ED, CTrueE, update, cond);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_BFS_H_
