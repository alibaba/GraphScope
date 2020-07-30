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

#ifndef ANALYTICAL_ENGINE_APPS_SSSP_SSSP_HAS_PATH_H_
#define ANALYTICAL_ENGINE_APPS_SSSP_SSSP_HAS_PATH_H_
#include <queue>
#include <vector>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/worker/default_worker.h"
#include "sssp/sssp_has_path_context.h"

namespace gs {
/**
 * Compute whether have a path between source and destination
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSPHasPath : public AppBase<FRAG_T, SSSPHasPathContext<FRAG_T>>,
                    public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(SSSPHasPath<FRAG_T>, SSSPHasPathContext<FRAG_T>,
                         FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);
    ctx.has_target = frag.GetVertex(ctx.target_id, ctx.target);

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    std::queue<vertex_t> next_queue;
    if (native_source) {
      next_queue.push(source);
      ctx.visited[source] = true;
    }
    while (!next_queue.empty()) {
      vertex_t v = next_queue.front();
      next_queue.pop();
      vertexProcess(v, next_queue, frag, ctx, messages);
      if (ctx.has_path)
        break;
    }

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.post_time -= GetCurrentTime();
#endif

    messages.ForceContinue();

#ifdef PROFILING
    ctx.post_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
#ifdef PROFILING
    auto t1 = GetCurrentTime(), t2 = GetCurrentTime();
#endif

    std::queue<vertex_t> next_queue;
    if (!ctx.has_path) {
      vertex_t v, u;
      vid_t v_vid;
      while (messages.GetMessage<fragment_t, vid_t>(frag, u, v_vid)) {
        frag.Gid2Vertex(v_vid, v);
        if (ctx.has_target && v == ctx.target) {
          ctx.has_path = true;
          break;
        }
        ctx.visited[v] = true;
        ctx.visited[u] = true;
        next_queue.push(u);

        while (!next_queue.empty()) {
          vertex_t v = next_queue.front();
          next_queue.pop();
          vertexProcess(v, next_queue, frag, ctx, messages);
          if (ctx.has_path)
            break;
        }
        if (ctx.has_path)
          break;
      }
    }

    {
      if (frag.GetInnerVertex(ctx.target_id, ctx.target)) {
        std::vector<size_t> shape{1};

        ctx.set_shape(shape);
        ctx.assign(ctx.has_path);
      }
    }

#ifdef PROFILING
    t2 = GetCurrentTime();
    ctx.exec_time += t2 - t1;
#endif
  }

 private:
  void vertexProcess(vertex_t v, std::queue<vertex_t>& next_queue,
                     const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    auto oes = frag.GetOutgoingAdjList(v);
    vid_t v_vid = frag.Vertex2Gid(v);
    if (ctx.has_target && v == ctx.target) {
      for (auto& e : oes)
        if (frag.IsOuterVertex(e.get_neighbor()))
          messages.SyncStateOnOuterVertex<fragment_t, vid_t>(
              frag, e.get_neighbor(), v_vid);
      ctx.has_path = true;
      return;
    }

    for (auto& e : oes) {
      auto u = e.get_neighbor();
      if (ctx.visited[u] == false) {
        if (frag.IsOuterVertex(u))
          messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, u, v_vid);
        else
          next_queue.push(u);
        ctx.visited[u] = true;
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SSSP_SSSP_HAS_PATH_H_
