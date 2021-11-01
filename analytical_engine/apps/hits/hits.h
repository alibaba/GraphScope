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

#ifndef ANALYTICAL_ENGINE_APPS_HITS_HITS_H_
#define ANALYTICAL_ENGINE_APPS_HITS_HITS_H_

#include <algorithm>
#include <limits>
#include <utility>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/worker/default_worker.h"
#include "hits/hits_context.h"

namespace gs {
/**
 * @brief Hyperlink-Induced Topic Search. The algorithm computes authorities and
 * hubs for a vertex.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class HITS : public grape::ParallelAppBase<FRAG_T, HitsContext<FRAG_T>>,
             public grape::ParallelEngine,
             public grape::Communicator {
 public:
  INSTALL_PARALLEL_WORKER(HITS<FRAG_T>, HitsContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    auto& channel_0 = messages.Channels()[0];
    auto inner_vertices = frag.InnerVertices();
    auto& auth = ctx.auth;
    auto& hub_last = ctx.hub_last;
    auto& hub = ctx.hub;

    hub_last.Swap(hub);

    for (auto u : inner_vertices) {
      auth[u] = 0.0;
      for (auto& nbr : frag.GetIncomingAdjList(u)) {
        auth[u] += hub_last[nbr.get_neighbor()];
      }
      channel_0.SendMsgThroughEdges<fragment_t, double>(frag, u, auth[u]);
    }

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    }

    ctx.stage = HubIteration;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();
    auto& hub = ctx.hub;
    auto& auth = ctx.auth;
    auto& hub_last = ctx.hub_last;
    double tolerance = ctx.tolerance;
    int thrd_num = thread_num();

    if (ctx.stage == AuthIteration) {
      hub_last.Swap(hub);

      ForEach(
          inner_vertices.begin(), inner_vertices.end(),
          [&auth, &hub_last, &frag, &messages](int tid, vertex_t u) {
            auth[u] = 0.0;
            for (auto& nbr : frag.GetIncomingAdjList(u)) {
              auth[u] += hub_last[nbr.get_neighbor()];
            }
            messages.Channels()[tid].SendMsgThroughEdges<fragment_t, double>(
                frag, u, auth[u]);
          });

      ctx.stage = HubIteration;
      if (frag.fnum() == 1) {
        messages.ForceContinue();
      }
    } else if (ctx.stage == HubIteration) {
      messages.ParallelProcess<fragment_t, double>(
          thrd_num, frag, [&auth](int tid, vertex_t v, double auth_val) {
            auth[v] = auth_val;
          });

      ForEach(
          inner_vertices.begin(), inner_vertices.end(),
          [&hub, &auth, &frag, &messages](int tid, vertex_t u) {
            hub[u] = 0.0;
            for (auto& nbr : frag.GetOutgoingAdjList(u)) {
              hub[u] += auth[nbr.get_neighbor()];
            }

            messages.Channels()[tid].SendMsgThroughEdges<fragment_t, double>(
                frag, u, hub[u]);
          });

      ctx.stage = Normalize;
      if (frag.fnum() == 1) {
        messages.ForceContinue();
      }
    } else if (ctx.stage == Normalize) {
      messages.ParallelProcess<fragment_t, double>(
          thrd_num, frag,
          [&hub](int tid, vertex_t v, double hub_val) { hub[v] = hub_val; });

      double local_max_h = -std::numeric_limits<double>::max();
      double local_max_a = -std::numeric_limits<double>::max();

      for (auto& u : inner_vertices) {
        local_max_h = std::max(local_max_h, hub[u]);
        local_max_a = std::max(local_max_a, auth[u]);
      }

      {
        double global_max = -std::numeric_limits<double>::max();
        Max(local_max_h, global_max);

        double s = 1.0 / global_max;
        for (auto& u : vertices) {
          hub[u] *= s;
        }
      }

      {
        double global_max = -std::numeric_limits<double>::max();
        Max(local_max_a, global_max);

        double s = 1.0 / global_max;
        for (auto& u : vertices) {
          auth[u] *= s;
        }
      }
      ctx.stage = AuthIteration;

      ++ctx.step;

      double eps = 0.0;
      for (auto& u : inner_vertices) {
        eps += fabs(hub[u] - hub_last[u]);
      }
      double total_eps = 0.0;
      Sum(eps, total_eps);
      VLOG(1) << "[step - " << ctx.step << " ] Diff: " << total_eps;
      if (total_eps <= tolerance || ctx.step >= ctx.max_round) {
        VLOG(1) << "HITS terminates after " << ctx.step
                << " iterations. Diff: " << total_eps;

        // normalize result
        if (ctx.normalized) {
          double local_sum_a = 0;
          double local_sum_h = 0;
          for (auto& u : inner_vertices) {
            local_sum_a += auth[u];
            local_sum_h += hub[u];
          }
          Sum(local_sum_a, ctx.sum_a);
          Sum(local_sum_h, ctx.sum_h);
        }

        auto hub_idx = ctx.add_column("hub", ContextDataType::kDouble);
        auto auth_idx = ctx.add_column("auth", ContextDataType::kDouble);
        double s_a = 1.0 / ctx.sum_a;
        double s_h = 1.0 / ctx.sum_h;
        auto col_hub = ctx.template get_typed_column<double>(hub_idx);
        auto col_auth = ctx.template get_typed_column<double>(auth_idx);

        for (auto& u : inner_vertices) {
          if (ctx.normalized) {
            hub[u] *= s_h;
            auth[u] *= s_a;
          }
          col_hub->at(u) = hub[u];
          col_auth->at(u) = auth[u];
        }
        return;
      }
      // this stage does not produce any messages
      messages.ForceContinue();
    }
  }
};
};  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_HITS_HITS_H_
