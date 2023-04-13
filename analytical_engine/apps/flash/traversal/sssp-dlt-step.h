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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_SSSP_DLT_STEP_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_SSSP_DLT_STEP_H_

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
class SSSPDltStepFlash : public FlashAppBase<FRAG_T, SSSP_TYPE> {
 public:
  INSTALL_FLASH_WORKER(SSSPDltStepFlash<FRAG_T>, SSSP_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, SSSP_TYPE, double>;

  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
           oid_t o_source) {
    vid_t source = Oid2FlashId(o_source);
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run delta-stepping SSSP with Flash, total vertices: "
              << n_vertex << std::endl;

    double dlt = 0, m = 0, dlt_tot, m_tot;
    DefineMapV(init_v) {
      v.dis = (id == source) ? 0 : -1;
      for_nb(dlt += weight; m += 1;);
    };
    vset_t a = VertexMapSeq(All, CTrueV, init_v);
    GetSum(dlt, dlt_tot);
    GetSum(m, m_tot);
    dlt = dlt_tot * 2 / m_tot;
    LOG(INFO) << "dlt=" << dlt << std::endl;

    vset_t B = All;
    for (double a = 0, b = dlt, maxd = -1; a < maxd || maxd < 0;
         a += dlt, b += dlt) {
      LOG(INFO) << "a=" << a << ", b=" << b << std::endl;
      DefineFV(filter1) { return v.dis >= a - (1e-10) || v.dis < -0.5; };
      DefineFV(filter2) { return v.dis >= a - (1e-10) && v.dis < b; };
      B = VertexMap(B, filter1);
      vset_t A = VertexMap(B, filter2);

      for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
        LOG(INFO) << "Round " << i << ": size=" << len << std::endl;
        DefineFE(check) {
          return s.dis >= a - (1e-10) &&
                 (d.dis < -0.5 || s.dis + weight < d.dis);
        };
        DefineMapE(update) {
          if (d.dis < -0.5 || s.dis + weight < d.dis)
            d.dis = s.dis + weight;
        };
        A = EdgeMapDense(A, EjoinV(ED, B), check, update, CTrueV);
        A = VertexMap(A, filter2);
      }

      maxd = 0;
      double maxd_glb = 0;
      DefineMapV(find_max) { maxd = std::max(maxd, v.dis); };
      VertexMapSeq(All, CTrueV, find_max);
      GetMax(maxd, maxd_glb);
      maxd = maxd_glb;
      LOG(INFO) << "maxd=" << maxd << std::endl;
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_TRAVERSAL_SSSP_DLT_STEP_H_
