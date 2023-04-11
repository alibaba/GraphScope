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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_DIAMETER_APPROX_2_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_DIAMETER_APPROX_2_H_

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
class DiameterApprox2Flash : public FlashAppBase<FRAG_T, DIAMETER_2_TYPE> {
 public:
  INSTALL_FLASH_WORKER(DiameterApprox2Flash<FRAG_T>, DIAMETER_2_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, DIAMETER_2_TYPE, int>;

  bool sync_all_ = false;
  int dd, rr;

  int GlobalRes() { return dd; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run diameter-approx with Flash, total vertices: " << n_vertex
              << std::endl;

    int64_t v_loc = 0, v_glb = 0;
    DefineMapV(init) {
      int64_t v_now = Deg(id) * (int64_t) n_vertex + id;
      v_loc = std::max(v_loc, v_now);
      v.dis = -1;
      v.ecc = 0;
    };
    VertexMapSeq(All, CTrueV, init);
    GetMax(v_loc, v_glb);

    DefineFV(filter) { return id == v_glb % n_vertex; };
    DefineMapV(local) { v.dis = 0; };
    vset_t A = VertexMap(All, filter, local);

    int dt = 0;
    for (int len = VSize(A), i = 1; len > 0; len = VSize(A), dt = i++) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
      DefineFV(cond) { return v.dis == -1; };
      DefineFE(check) { return s.dis != -1; };
      DefineMapE(update) { d.dis = i; };
      A = EdgeMap(A, EU, check, update, cond, update);
    }

    DefineFV(filter2) { return v.dis != -1; };
    DefineMapV(local2) { v.ecc = std::max(v.dis, dt - v.dis); };
    A = VertexMap(All, filter2, local2);

    std::vector<vid_t> s;
    int64_t one = 1;
    int k = 64;
    std::vector<std::pair<int, int>> c(k, std::make_pair(-1, -1)), t(k);
    DefineMapV(cal_c) {
      int p = 0;
      for (int i = 1; i < k; ++i)
        if (c[i] < c[p])
          p = i;
      if (v.dis > c[p].first)
        c[p] = std::make_pair(v.dis, static_cast<int>(id));
    };
    VertexMapSeq(A, CTrueV, cal_c);
    std::sort(c.begin(), c.end());
    Reduce(c, t, std::reverse(t, t + len); for_i(t[i] = std::max(t[i], c[i]));
           std::sort(t, t + len));
    s.clear();
    for (int i = 0; i < k; ++i)
      s.push_back(t[i].second);

    DefineMapV(local3) { v.seen = 0; };
    vset_t S = VertexMap(A, CTrueV, local3);
    DefineFV(filter4) { return find(s, id); };
    DefineMapV(local4) {
      int p = locate(s, id);
      v.seen |= one << p;
    };
    S = VertexMapSeq(S, filter4, local4);

    for (int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
      LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
      DefineFE(check) { return (s.seen & (~d.seen)); };
      DefineMapE(update) {
        d.seen |= (s.seen & (~d.seen));
        d.ecc = std::max(d.ecc, std::max(i, dt - i));
      };
      S = EdgeMapDense(S, EjoinV(EU, A), check, update, CTrueV);
    }

    int d = 0, r = n_vertex;
    TraverseLocal(int e = v.ecc; d = std::max(d, e);
                  if (e != 0) { r = std::min(r, e); });
    GetMax(d, dd);
    GetMin(r, rr);
    LOG(INFO) << "diameter=" << dd << ", radius=" << rr << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_DIAMETER_APPROX_2_H_
