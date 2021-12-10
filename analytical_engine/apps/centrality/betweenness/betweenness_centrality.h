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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_H_

#include <utility>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"
#include "core/worker/default_worker.h"

#include "apps/centrality/betweenness/betweenness_centrality_context.h"

namespace gs {
/**
 * @brief Compute the shortest-path betweenness centrality for nodes.
 * Betweenness centrality of a node v is the sum of the fraction of
 * all-pairs shortest paths that pass through v.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BetweennessCentrality
    : public AppBase<FRAG_T, BetweennessCentralityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(BetweennessCentrality<FRAG_T>,
                         BetweennessCentralityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;
  using forward_msg_t = typename std::pair<int, int>;
  using backward_msg_t = typename std::pair<int, double>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    if (ctx.round < ctx.max_round)
      messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if ("Forward" == ctx.phase) {
      forward(frag, ctx, messages);
    } else if ("Backward" == ctx.phase) {
      backward(frag, ctx, messages);
    } else {
      // TODO(mengke): more formal error handling
      LOG(ERROR) << "bad phase parameter!" << std::endl;
    }
  }

 private:
  void forward(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto vertices = frag.Vertices();

    // nested PEval
    if (ctx.PEval) {
      ctx.PEval = false;
      ctx.curr_depth = -1;
      ctx.depth.SetValue(-100);
      ctx.number_of_path.SetValue(0);
      generateNextSource(frag, ctx, messages);
      vertex_t src;
      bool native_source =
          frag.Gid2Vertex(ctx.source, src) & frag.IsInnerVertex(src);
      if (native_source) {
        ctx.depth[src] = 0;
        ctx.number_of_path[src] = 1;
      }
    }

    // process messages
    forward_msg_t msg;
    vertex_t u;
    while (messages.GetMessage<fragment_t, forward_msg_t>(frag, u, msg)) {
      int depth_v = msg.first;
      int nsp_v = msg.second;
      if (depth_v + 1 == ctx.depth[u] || ctx.depth[u] < 0) {
        ctx.depth[u] = depth_v + 1;
        ctx.number_of_path[u] += nsp_v;
      }
    }

    ++ctx.curr_depth;

    // emit messages
    int epoch_tasks = 0;
    for (auto& v : inner_vertices) {
      if (ctx.depth[v] == ctx.curr_depth) {
        ++epoch_tasks;
        auto oes = frag.GetOutgoingAdjList(v);
        for (auto& e : oes) {
          auto u = e.get_neighbor();
          if (!frag.IsOuterVertex(u)) {
            if (ctx.depth[v] + 1 == ctx.depth[u] || ctx.depth[u] < 0) {
              ctx.depth[u] = ctx.depth[v] + 1;
              ctx.number_of_path[u] += ctx.number_of_path[v];
            }
          } else {
            messages.SyncStateOnOuterVertex(
                frag, u, std::make_pair(ctx.depth[v], ctx.number_of_path[v]));
          }
        }
      }
    }

    Sum(epoch_tasks, ctx.epoch_tasks);
    if (0 == ctx.epoch_tasks) {
      // shift to Backward
      ctx.phase = "Backward";
      ctx.PEval = true;
    }

    // Always
    messages.ForceContinue();
  }

  void backward(const fragment_t& frag, context_t& ctx,
                message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    // nested PEval
    if (ctx.PEval) {
      ctx.PEval = false;
      ctx.pair_dependency.SetValue(0.0);
    }

    // process messages
    backward_msg_t msg;
    vertex_t u;
    while (messages.GetMessage<fragment_t, backward_msg_t>(frag, u, msg)) {
      int nsp_v = msg.first;
      double pair_dependency_v = msg.second;
      if (ctx.depth[u] + 1 == ctx.curr_depth) {
        ctx.pair_dependency[u] +=
            ctx.number_of_path[u] * (pair_dependency_v + 1) / nsp_v;
      }
    }

    --ctx.curr_depth;

    int epoch_tasks = 0;
    for (auto& v : inner_vertices) {
      if (ctx.depth[v] == ctx.curr_depth) {
        ++epoch_tasks;
        auto ies = frag.directed() ? frag.GetIncomingAdjList(v)
                                   : frag.GetOutgoingAdjList(v);
        for (auto& e : ies) {
          auto u = e.get_neighbor();
          if (!frag.IsOuterVertex(u)) {
            if (ctx.depth[u] + 1 == ctx.curr_depth) {
              ctx.pair_dependency[u] += ctx.number_of_path[u] *
                                        (ctx.pair_dependency[v] + 1) /
                                        ctx.number_of_path[v];
            }
          } else {
            messages.SyncStateOnOuterVertex(
                frag, u,
                std::make_pair(ctx.number_of_path[v], ctx.pair_dependency[v]));
          }
        }
      }
    }

    Sum(epoch_tasks, ctx.epoch_tasks);
    if (0 == ctx.epoch_tasks) {
      for (auto v : inner_vertices) {
        if (frag.Vertex2Gid(v) != ctx.source) {
          ctx.centrality[v] += ctx.pair_dependency[v];
          if (ctx.endpoints && ctx.depth[v] >= 0)
            ctx.centrality[v]++;
        } else if (ctx.endpoints) {
          ctx.centrality[v] += ctx.pair_dependency[v];
        }
      }
      // try next source
      if (ctx.round < ctx.max_round) {
        ctx.round++;
        ctx.phase = "Forward";
        ctx.PEval = true;
        messages.ForceContinue();
      } else {
        for (auto v : inner_vertices) {
          ctx.centrality[v] = ctx.norm * ctx.centrality[v];
        }
      }
      return;
    }

    messages.ForceContinue();
  }

  void generateNextSource(const fragment_t& frag, context_t& ctx,
                          message_manager_t& messages) {
    auto fid = frag.fid();
    auto fnum = frag.fnum();
    auto vote = fid;
    auto master = fid;

    if (ctx.remain_source == 0)
      vote = fnum;
    Min(vote, master);
    vid_t src_gid;
    if (fid == master) {
      vertex_t tmp = vertex_t(frag.GetInnerVerticesNum() - ctx.remain_source);
      ctx.remain_source--;
      src_gid = frag.Vertex2Gid(tmp);
      for (fid_t f = fid_t(0); f < fnum; ++f) {
        if (f != master)
          SendTo(f, src_gid);
      }
    } else {
      RecvFrom(master, src_gid);
    }
    ctx.source = src_gid;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_H_
