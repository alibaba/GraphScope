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

#ifndef ANALYTICAL_ENGINE_APPS_BFS_BFS_GENERIC_H_
#define ANALYTICAL_ENGINE_APPS_BFS_BFS_GENERIC_H_

#include <utility>
#include <vector>

#include "bfs/bfs_generic_context.h"

#include "core/app/app_base.h"
#include "core/config.h"
#include "core/worker/default_worker.h"

namespace gs {

/**
 * @brief Breadth-first search. The predecessor or successor will be found and
 * hold in the context. The behavior of the algorithm can be controlled by a
 * source vertex and depth limit.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BFSGeneric : public AppBase<FRAG_T, BFSGenericContext<FRAG_T>>,
                   public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(BFSGeneric<FRAG_T>, BFSGenericContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    ctx.depth = 0;
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    if (native_source) {
      ctx.visited[source] = true;
      ctx.predecessor[source] = frag.Vertex2Gid(source);
      vertexProcess(source, frag, ctx, messages);
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
    ctx.pre_time -= GetCurrentTime();
#endif

    vid_t msg;
    vertex_t u;
    while (messages.GetMessage<fragment_t, vid_t>(frag, u, msg)) {
      // Update predecessor of mirror u in this fragment.
      if (ctx.visited[u] == false) {
        ctx.predecessor[u] = msg;
        ctx.next_level_inner.push(u);
        ctx.visited[u] = true;
      }
    }
    ctx.curr_level_inner.swap(ctx.next_level_inner);

#ifdef PROFILING
    ctx.pre_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif
    ctx.depth++;
    if (ctx.depth < ctx.depth_limit) {
      while (!ctx.curr_level_inner.empty()) {
        vertex_t v = ctx.curr_level_inner.front();
        ctx.curr_level_inner.pop();
        vertexProcess(v, frag, ctx, messages);
      }
    }

    // Make sure IncEval continues when all activated neighbor is local.
    fid_t terminate_worker_num = 0;
    if (ctx.next_level_inner.empty() || ctx.depth == ctx.depth_limit) {
      Sum(1u, terminate_worker_num);
    } else {
      Sum(0u, terminate_worker_num);
      messages.ForceContinue();
    }
    if (terminate_worker_num == frag.fnum()) {
      writeToCtx(frag, ctx);
    }
#ifdef PROFILING
    ctx.post_time += GetCurrentTime();
#endif
  }

 private:
  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    auto& output_format = ctx.output_format;
    auto inner_vertices = frag.InnerVertices();
    auto& visited = ctx.visited;
    auto& predecessor = ctx.predecessor;
    auto& source_id = ctx.source_id;
    std::vector<std::pair<vid_t, vid_t>> bfs_edges;

    if (output_format == "edges") {
      for (auto v : inner_vertices) {
        if (visited[v] && frag.GetId(v) != source_id) {
          bfs_edges.emplace_back(predecessor[v], frag.Vertex2Gid(v));
        }
      }
    } else if (output_format == "predecessors") {
      for (auto v : inner_vertices) {
        if (visited[v] && frag.GetId(v) != source_id) {
          bfs_edges.emplace_back(frag.Vertex2Gid(v), predecessor[v]);
        }
      }
    } else if (output_format == "successors") {
      for (auto v : inner_vertices) {
        if (visited[v]) {
          vid_t v_vid = frag.Vertex2Gid(v);
          auto oes = frag.GetOutgoingAdjList(v);
          for (auto& e : oes) {
            if (predecessor[e.get_neighbor()] == v_vid) {
              bfs_edges.emplace_back(v_vid, frag.Vertex2Gid(e.get_neighbor()));
            }
          }
        }
      }
    }
    std::vector<std::pair<vid_t, vid_t>> all_bfs_edges;
    AllReduce(bfs_edges, all_bfs_edges,
              [](std::vector<std::pair<vid_t, vid_t>>& out,
                 const std::vector<std::pair<vid_t, vid_t>>& in) {
                for (auto& e : in) {
                  out.push_back(e);
                }
              });
    bfs_edges.clear();

    if (frag.fid() == 0) {
      std::vector<typename fragment_t::oid_t> data;
      for (auto& e : all_bfs_edges) {
        data.push_back(frag.Gid2Oid(e.first));
        data.push_back(frag.Gid2Oid(e.second));
      }
      std::vector<size_t> shape{data.size() / 2, 2};
      ctx.assign(data, shape);
    }
  }

  void vertexProcess(vertex_t v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    vid_t v_vid = frag.Vertex2Gid(v);
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t u = e.get_neighbor();
      if (ctx.visited[u] == false) {
        if (!frag.IsOuterVertex(u)) {
          ctx.predecessor[u] = v_vid;
          ctx.next_level_inner.push(u);
        } else {
          messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, u, v_vid);
        }
        ctx.visited[u] = true;
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BFS_BFS_GENERIC_H_
