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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_TC_PREGEL_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_TC_PREGEL_H_

#include <set>

#include "core/app/pregel/i_vertex_program.h"
#include "core/app/pregel/pregel_compute_context.h"
#include "core/app/pregel/pregel_vertex.h"

namespace gs {
template <typename FRAG_T>
class PregelTC
    : public IPregelProgram<PregelVertex<FRAG_T, uint32_t, uint32_t>,
                            PregelComputeContext<FRAG_T, uint32_t, uint32_t>> {
 public:
  using oid_t = int64_t;
  using vid_t = uint32_t;
  using vd_t = uint32_t;
  using md_t = uint32_t;
  using fragment_t = FRAG_T;
  using pregel_vertex_t = PregelVertex<fragment_t, vd_t, md_t>;
  using compute_context_t = PregelComputeContext<fragment_t, vd_t, md_t>;

 public:
  void Init(pregel_vertex_t& v, compute_context_t& context) override {
    v.set_value(0);
  }

  void Compute(grape::IteratorPair<md_t*> messages, pregel_vertex_t& v,
               compute_context_t& ctx) override {
    if (ctx.superstep() == 0) {
      const auto& oe = v.outgoing_edges();
      const auto& ie = v.incoming_edges();

      for (auto& eb : oe) {
        std::set<vid_t> vst_c;
        for (auto& ec : ie) {
          if (vst_c.insert(ec.get_neighbor().GetValue()).second) {
            v.send(ec.get_neighbor(), ctx.GetId(eb.get_neighbor()));
          }
        }

        for (auto& ec : oe) {
          if (vst_c.insert(ec.get_neighbor().GetValue()).second) {
            v.send(ec.get_neighbor(), ctx.GetId(eb.get_neighbor()));
          }
        }
      }
    } else if (ctx.superstep() == 1) {
      int counter = 0;
      auto oe = v.outgoing_edges();
      auto ie = v.incoming_edges();

      for (auto& oid : messages) {
        for (auto& e : oe) {
          if (oid == ctx.GetId(e.get_neighbor())) {
            counter++;
            break;
          }
        }
      }
      v.set_value(counter / 2);
      v.vote_to_halt();
    } else {
      CHECK(false);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_TC_PREGEL_H_
