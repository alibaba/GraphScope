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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_BRIDGE_2_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_BRIDGE_2_H_

#include <algorithm>
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
class Bridge2Flash : public FlashAppBase<FRAG_T, BCC_2_TYPE> {
 public:
  INSTALL_FLASH_WORKER(Bridge2Flash<FRAG_T>, BCC_2_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, BCC_2_TYPE, int>;

  bool sync_all_ = false;

  int tot_cnt;
  int GlobalRes() { return tot_cnt; }

  void RunCC(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    DefineMapV(init) {
      v.cid = id;
      v.d = Deg(id);
      v.dis = -1;
      v.tmp = 0;
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
  }

  void RunBFS(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    std::vector<vset_t> v_bfs;
    DefineFV(filter1) { return v.cid == id; };
    DefineMapV(local1) { v.dis = 0; };
    vset_t A = VertexMap(All, filter1, local1);

    for (int len = VSize(A), i = 1; len > 0; len = VSize(A), ++i) {
      LOG(INFO) << "BFS Round " << i << ": size = " << len << std::endl;
      v_bfs.push_back(A);

      DefineMapE(update2) { d.dis = i; };
      DefineFV(cond2) { return (v.dis == -1); };
      A = EdgeMap(A, EU, CTrueE, update2, cond2, update2);
    }

    DefineMapV(pre_init) {
      v.nd = 1;
      v.p = -1;
      v.pre = 0;
      for_nb(if (nb.dis == v.dis - 1) {
        v.p = nb_id;
        break;
      });
    };
    VertexMap(All, CTrueV, pre_init);

    for (int i = static_cast<int>(v_bfs.size()) - 1; i >= 0; --i) {
      DefineFV(filter) { return v.p >= 0; };
      vset_t B = VertexMap(v_bfs[i], filter);

      DefineOutEdges(edges){VjoinP(p)};
      DefineMapE(update) { d.tmp += s.nd; };
      DefineMapE(reduce) { d.tmp += s.tmp; };
      B = EdgeMapSparse(B, edges, CTrueE, update, CTrueV, reduce);

      DefineMapV(local) {
        v.nd += v.tmp;
        v.tmp = 0;
      };
      B = VertexMap(B, CTrueV, local);
    }

    for (int i = 0; i < static_cast<int>(v_bfs.size()); ++i) {
      int ss = -1, pre = 0;
      DefineFE(check) { return d.p == sid; };
      DefineMapE(update) {
        if (sid != ss)
          pre = s.pre + 1;
        ss = sid;
        d.pre = pre;
        pre += d.nd;
      };
      DefineMapE(reduce) { d.pre = s.pre; };
      EdgeMapSparse(v_bfs[i], EU, check, update, CTrueV, reduce);
    }

    DefineMapV(mm_init) {
      v.minp = v.pre;
      v.maxp = v.pre;
    };
    VertexMap(All, CTrueV, mm_init);
    for (int i = static_cast<int>(v_bfs.size()) - 1; i >= 0; --i) {
      DefineMapV(compute_mm) {
        for_nb(
            if (nb.p == id) {
              v.minp = std::min(v.minp, nb.minp);
              v.maxp = std::max(v.maxp, nb.maxp);
            } else if (v.p != nb_id) {
              v.minp = std::min(v.minp, nb.pre);
              v.maxp = std::max(v.maxp, nb.pre);
            })
      };
      VertexMap(v_bfs[i], CTrueV, compute_mm);
    }
  }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run Bridge Detection with Flash, total vertices: " << n_vertex
              << std::endl;
    RunCC(graph, fw);
    RunBFS(graph, fw);

    DefineMapV(bridge) {
      v.d = (v.p == -1 ? 0 : 1);
      for_nb(
          if (v.p == nb_id) {
            if (v.minp < nb.pre || v.maxp >= nb.pre + nb.nd)
              v.d = 0;
          } else if (id == nb.p) {
            if (nb.minp < v.pre || nb.maxp >= v.pre + v.nd)
              v.d = 0;
          } else { v.d = 0; })
    };
    VertexMap(All, CTrueV, bridge);
    int cnt = 0;
    tot_cnt = 0;
    TraverseLocal(cnt += v.d;);
    GetSum(cnt, tot_cnt);
    LOG(INFO) << "Num of bridges = " << tot_cnt << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_BRIDGE_2_H_
