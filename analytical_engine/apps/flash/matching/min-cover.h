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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_COVER_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_COVER_H_

#include <memory>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class MinCoverFlash : public FlashAppBase<FRAG_T, MIN_COVER_TYPE> {
 public:
  INSTALL_FLASH_WORKER(MinCoverFlash<FRAG_T>, MIN_COVER_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, MIN_COVER_TYPE, int>;

  bool sync_all_ = false;
  int n_mc;

  int GlobalRes() { return n_mc; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Min Cover with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.c = false;
      v.s = false;
      v.d = Deg(id);
      v.tmp = 0;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    for (int i = 0, len = n_vertex, nowd = n_vertex / 2; len > 0;
         len = VSize(A), ++i, nowd /= 2) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;

      DefineFV(filter1) { return v.d >= nowd; };
      DefineMapV(local1) { v.c = true; };
      vset_t B = VertexMap(A, filter1, local1);

      DefineMapE(update) { d.tmp++; };
      DefineMapE(reduce) { d.tmp += s.tmp; };
      B = EdgeMapSparse(B, EU, CTrueE, update, CTrueV, reduce);

      DefineMapV(local2) {
        v.d -= v.tmp;
        v.tmp = 0;
      };
      VertexMap(B, CTrueV, local2);

      DefineFV(filter2) { return !v.c && v.d > 0; };
      A = VertexMap(A, filter2);
    }

    DefineFV(filter) { return v.c; };
    for (int len = n_vertex, i = 0; len > 0; ++i) {
      A = VertexMap(All, filter);

      DefineFV(filter2) {
        for_nb(if (!nb.c) { return false; });
        return true;
      };
      DefineMapV(local2) { v.s = true; };
      vset_t B = VertexMap(A, filter2, local2);

      DefineFV(filter3) {
        if (!v.s)
          return false;
        for_nb(if (nb.s && nb_id > id) { return false; });
        return true;
      };
      DefineMapV(local3) { v.c = false; };
      A = VertexMap(A, filter3, local3);
      len = VSize(A);

      DefineMapV(reset) { v.s = false; };
      VertexMap(B, CTrueV, reset);
      LOG(INFO) << "Refining round " << i + 1 << ": len=" << len << std::endl;
    }

    A = VertexMap(All, filter);
    n_mc = VSize(A);
    LOG(INFO) << "size of vertex-cover = " << n_mc << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MATCHING_MIN_COVER_H_
