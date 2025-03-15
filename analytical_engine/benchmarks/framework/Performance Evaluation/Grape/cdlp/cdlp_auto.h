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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_AUTO_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_AUTO_H_

#include <grape/grape.h>

#include "cdlp/cdlp_auto_context.h"
#include "cdlp/cdlp_utils.h"

namespace grape {

/**
 * @brief An implementation of CDLP(Community detection using label propagation)
 * without using explicit message-passing APIs, the version in LDBC, which only
 * works on the undirected graph.
 *
 * This is the auto-parallel version inherit AutoAppBase. In this version, users
 * plug sequential algorithms for PEval and IncEval, libgrape-lite parallelizes
 * them in the distributed setting. Users are not aware of messages.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class CDLPAuto : public AutoAppBase<FRAG_T, CDLPAutoContext<FRAG_T>> {
  INSTALL_AUTO_WORKER(CDLPAuto<FRAG_T>, CDLPAutoContext<FRAG_T>, FRAG_T)

 private:
  using vertex_t = typename fragment_t::vertex_t;
  using label_t = typename context_t::label_t;
  using vid_t = typename context_t::vid_t;

  void PropagateLabel(const fragment_t& frag, context_t& ctx) {
#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    auto inner_vertices = frag.InnerVertices();
    typename FRAG_T::template inner_vertex_array_t<label_t> new_ilabels;
    new_ilabels.Init(inner_vertices);

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // propagate new label to neighbor
    for (auto& v : inner_vertices) {
      auto ie = frag.GetIncomingAdjList(v);
      if (ie.Empty()) {
        ctx.changed[v] = false;
      } else {
        label_t new_label = update_label_fast<label_t>(ie, ctx.labels);
        if (ctx.labels[v] != new_label) {
          new_ilabels[v] = new_label;
          ctx.changed[v] = true;
        } else {
          ctx.changed[v] = false;
        }
      }
    }

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    for (auto& v : inner_vertices) {
      if (ctx.changed[v]) {
        ctx.labels.SetValue(v, new_ilabels[v]);
      }
    }

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

 public:
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx) {
    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    }

    PropagateLabel(frag, ctx);
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    }

    PropagateLabel(frag, ctx);
  }
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_AUTO_H_
