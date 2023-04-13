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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_CC_LOG_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_CC_LOG_H_

#include <algorithm>
#include <memory>
#include <vector>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

#define jump(A) VertexMap(A, checkj, updatej)

#define star(A)                                               \
  S = VertexMap(A, CTrueV, locals);                           \
  S = VertexMap(S, checkj, locals2);                          \
  EdgeMapSparse(S, edges2, CTrueE, updates, CTrueV, updates); \
  VertexMap(A, checks, locals2);

#define hook(A)                             \
  S = VertexMap(A, filterh1);               \
  VertexMap(S, CTrueV, localh1);            \
  EdgeMapSparse(S, EU, f2, h2, CTrueV, h2); \
  VertexMap(S, filterh2, localh2);

namespace gs {

template <typename FRAG_T>
class CCLogFlash : public FlashAppBase<FRAG_T, CC_LOG_TYPE> {
 public:
  INSTALL_FLASH_WORKER(CCLogFlash<FRAG_T>, CC_LOG_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CC_LOG_TYPE, int>;

  bool sync_all_ = true;

  int* Res(value_t* v) { return &(v->p); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run CC-log with Flash, total vertices: " << n_vertex
              << std::endl;
    vset_t S;
    bool c = true;

    DefineMapV(init) {
      v.p = id;
      v.s = false;
      v.f = id;
    };
    VertexMap(All, CTrueV, init);

    DefineFE(check1) { return sid < d.p; };
    DefineMapE(update1) { d.p = std::min(d.p, static_cast<int>(sid)); };
    vset_t A = EdgeMapDense(All, EU, check1, update1, CTrueV);

    DefineOutEdges(edges){VjoinP(p)};
    DefineMapE(update2) { d.s = true; };
    EdgeMapSparse(A, edges, CTrueE, update2, CTrueV, update2);

    DefineFV(filter1) { return v.p == id && (!v.s); };
    DefineMapV(local1) { v.p = INT_MAX; };
    A = VertexMap(All, filter1, local1);
    EdgeMapDense(All, EjoinV(EU, A), check1, update1, CTrueV);

    DefineFV(filter2) { return v.p != INT_MAX; };
    A = VertexMap(All, filter2);

    DefineFV(checkj) { return GetV(v.p)->p != v.p; };
    DefineMapV(updatej) { v.p = GetV(v.p)->p; };

    DefineOutEdges(edges2) {
      std::vector<vid_t> res;
      res.clear();
      res.push_back(GetV(v.p)->p);
      return res;
    };
    DefineMapV(locals) { v.s = true; };
    DefineMapV(locals2) { v.s = false; };
    DefineMapE(updates) { d.s = false; };
    DefineFV(checks) { return (v.s && !(GetV(v.p)->s)); };

    DefineFV(filterh1) { return v.s; };
    DefineMapV(localh1) {
      v.f = c ? v.p : INT_MAX;
      for_nb(if (nb.p != v.p) { v.f = std::min(v.f, nb.p); })
    };
    DefineFE(f2) {
      return s.p != sid && s.f != INT_MAX && s.f != s.p && s.p == did;
    };
    DefineMapE(h2) { d.f = std::min(d.f, s.f); };

    DefineFV(filterh2) { return v.p == id && v.f != INT_MAX && v.f != v.p; };
    DefineMapV(localh2) { v.p = v.f; };

    for (int i = 0, len = 0; VSize(A) > 0; ++i) {
      S = jump(A);
      len = VSize(S);
      if (len == 0)
        break;
      LOG(INFO) << "Round " << i << ": len = " << len << std::endl;
      jump(A);
      jump(A);
      star(A);
      c = true;
      hook(A);
      star(A);
      c = false;
      hook(A);
    }

    DefineFV(filter3) { return v.p == INT_MAX; };
    DefineMapV(local3) { v.p = id; };
    VertexMap(All, filter3, local3);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_CC_LOG_H_
