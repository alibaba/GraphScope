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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_BRIDGE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_BRIDGE_H_

#include <memory>
#include <vector>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

#define GT(A, B) (A.d > B.d || (A.d == B.d && A.cid > B.cid))

namespace gs {

template <typename FRAG_T>
class BridgeFlash : public FlashAppBase<FRAG_T, BCC_TYPE> {
 public:
  INSTALL_FLASH_WORKER(BridgeFlash<FRAG_T>, BCC_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, BCC_TYPE, int>;

  bool sync_all_ = true;
  int cnt;

  int GlobalRes() { return cnt; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Bridge Detection with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.cid = id;
      v.d = Deg(id);
      v.dis = -1;
      v.p = -1;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    for (int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "CC Round " << i << ": size = " << len << std::endl;

      DefineFE(check1) { return GT(s, d); };
      DefineMapE(update1) {
        d.cid = s.cid;
        d.d = s.d;
      };
      DefineMapE(reduce1) {
        if (GT(s, d)) {
          d.cid = s.cid;
          d.d = s.d;
        }
      };
      A = EdgeMap(A, EU, check1, update1, CTrueV, reduce1);
    }

    std::vector<vset_t> v_bfs;

    DefineFV(filter1) { return v.cid == id; };
    DefineMapV(local1) { v.dis = 0; };
    A = VertexMap(All, filter1, local1);

    for (int len = VSize(A), i = 1; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "BFS Round " << i << ": size = " << len << std::endl;
      v_bfs.push_back(A);

      DefineMapE(update2) { d.dis = i; };
      DefineFV(cond2) { return (v.dis == -1); };
      A = EdgeMap(A, EU, CTrueE, update2, cond2, update2);
    }

    DefineMapV(local) {
      v.p = -1;
      for_nb(if (nb.dis == v.dis - 1) {
        v.p = nb_id;
        break;
      });
    };
    VertexMap(All, CTrueV, local);

    DefineFE(check3) { return (s.dis == d.dis - 1); };
    DefineMapE(update3) { d.p = sid; };
    DefineFV(cond3) { return (v.p == -1); };
    DefineMapE(reduce3) { d = s; };
    EdgeMap(All, EU, check3, update3, cond3, reduce3);

    LOG(INFO) << "Joining Edges..." << std::endl;
    std::vector<int> bd(n_vertex), bd_all(n_vertex);
    Traverse(bd[id] = (v.p != -1););

    DefineMapV(join_edges) {
      for_nb(if (nb_id > id && v.p != nb_id && nb.p != id) {
        int a = nb_id, b = id;
        bd[a] = false;
        bd[b] = false;
        while (a != b) {
          int da = GetV(a)->dis, db = GetV(b)->dis, pa = GetV(a)->p,
              pb = GetV(b)->p;
          if (da >= db) {
            if (pa != pb) {
              bd[a] = false;
              bd[pa] = false;
            }
            a = pa;
          }
          if (db >= da) {
            if (pa != pb) {
              bd[b] = false;
              bd[pb] = false;
            }
            b = pb;
          }
        }
      });
    };
    VertexMapSeq(All, CTrueV, join_edges, false);

    LOG(INFO) << "Reducing..." << std::endl;
    Reduce(bd, bd_all, for_i(bd_all[i] = bd_all[i] && bd[i]), true);

    cnt = 0;
    for (int i = 0; i < n_vertex; ++i) {
      if (bd_all[i])
        ++cnt;
    }

    LOG(INFO) << "Num of bridges = " << cnt << std::endl;

    DefineMapV(final) { v.bcc = bd_all[id]; };
    VertexMapSeq(All, CTrueV, final, false);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_BRIDGE_H_
