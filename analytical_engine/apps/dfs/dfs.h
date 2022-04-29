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

#ifndef ANALYTICAL_ENGINE_APPS_DFS_DFS_H_
#define ANALYTICAL_ENGINE_APPS_DFS_DFS_H_

#include <tuple>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/config.h"
#include "dfs/dfs_context.h"

namespace gs {
/**
 * @brief Depth-first search. The predecessor or successor will be found and
 * hold in the context. The behavior of the algorithm can be controlled by a
 * source vertex.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class DFS : public AppBase<FRAG_T, DFSContext<FRAG_T>>,
            public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(DFS<FRAG_T>, DFSContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto& is_in_frag = ctx.is_in_frag;
    if (ctx.output_stage == false) {
      std::tuple<std::pair<vid_t, vid_t>, int, bool> msg;
      while (messages.GetMessage(msg)) {
        if (std::get<1>(msg) == -1) {
          std::vector<std::pair<oid_t, int>> send_msg;
          send_msg.reserve(frag.GetInnerVerticesNum());
          for (auto v : inner_vertices) {
            send_msg.emplace_back(std::make_pair(frag.GetId(v), ctx.rank[v]));
          }
          messages.SendToFragment(0, send_msg);
          ctx.output_stage = true;
          break;
        }
        is_in_frag = true;
        vid_t gid = std::get<0>(msg).second;
        vertex_t v;
        frag.Gid2Vertex(gid, v);
        if (ctx.is_visited[v] == false) {
          ctx.max_rank = std::get<1>(msg) + 1;
          ctx.rank[v] = ctx.max_rank;
          ctx.is_visited[v] = true;
          ctx.current_vertex = v;
          ctx.parent[v] = std::get<0>(msg).first;
          vertex_t parent_v;
          frag.Gid2Vertex(std::get<0>(msg).first, parent_v);
          ctx.is_visited[parent_v] = true;
        } else if (ctx.is_visited[v] == true && std::get<2>(msg) == false) {
          ctx.current_vertex = v;
          ctx.max_rank = std::get<1>(msg);
        } else {
          is_in_frag = false;
          std::tuple<std::pair<vid_t, vid_t>, int, bool> send_msg;
          send_msg = std::make_tuple(
              std::make_pair(std::get<0>(msg).second, std::get<0>(msg).first),
              std::get<1>(msg), false);
          vertex_t tmp_v;
          frag.Gid2Vertex(std::get<0>(msg).first, tmp_v);
          fid_t fid = frag.GetFragId(tmp_v);
          messages.SendToFragment(fid, send_msg);
        }
      }
    } else if (ctx.output_stage == true) {
      std::vector<std::pair<oid_t, int>> msg;
      while (messages.GetMessage(msg)) {
        for (size_t i = 0; i < msg.size(); i++) {
          if (msg[i].second != -1) {
            ctx.results[msg[i].second] = msg[i].first;
            ctx.total_num++;
          }
        }
      }
    }
    if (is_in_frag) {
      auto current_vertex = ctx.current_vertex;
      while (true) {
        bool message_send = false;
        int num_visited = 0;
        auto es = frag.GetOutgoingAdjList(current_vertex);
        for (auto& e : es) {
          auto u = e.neighbor;
          if (ctx.is_visited[u] == true) {
            num_visited++;
          }
        }
        if (num_visited == frag.GetLocalOutDegree(current_vertex) &&
            frag.Vertex2Gid(current_vertex) != ctx.source_gid) {
          vid_t gid = ctx.parent[current_vertex];
          vertex_t parent_vertex;
          frag.Gid2Vertex(gid, parent_vertex);
          if (frag.IsInnerVertex(parent_vertex)) {
            current_vertex = parent_vertex;
          } else {
            std::tuple<std::pair<vid_t, vid_t>, int, bool> msg =
                std::make_tuple(
                    std::make_pair(frag.Vertex2Gid(current_vertex), gid),
                    ctx.max_rank, false);
            fid_t fid = frag.GetFragId(parent_vertex);
            messages.SendToFragment(fid, msg);
            break;
          }
        } else if (num_visited == frag.GetLocalOutDegree(current_vertex) &&
                   frag.Vertex2Gid(current_vertex) == ctx.source_gid) {
          std::tuple<std::pair<vid_t, vid_t>, int, bool> msg =
              std::make_tuple(std::make_pair(0, 0), -1, false);
          for (fid_t fid = 0; fid < frag.fnum(); fid++) {
            messages.SendToFragment(fid, msg);
          }
          break;
        } else {
          auto es = frag.GetOutgoingAdjList(current_vertex);
          for (auto& e : es) {
            auto u = e.neighbor;
            if (ctx.is_visited[u] == false) {
              ctx.is_visited[u] = true;
              if (frag.IsInnerVertex(u)) {
                ctx.parent[u] = frag.Vertex2Gid(current_vertex);
                ctx.rank[u] = ctx.max_rank + 1;
                ctx.max_rank++;
                current_vertex = u;
              } else {
                vid_t gid = frag.Vertex2Gid(u);
                std::tuple<std::pair<vid_t, vid_t>, int, bool> msg =
                    std::make_tuple(
                        std::make_pair(frag.Vertex2Gid(current_vertex), gid),
                        ctx.max_rank, true);
                fid_t fid = frag.GetFragId(u);
                messages.SendToFragment(fid, msg);
                message_send = true;
              }
              break;
            }
          }
          if (message_send) {
            break;
          }
        }
      }
      ctx.is_in_frag = false;
    }

    auto& output_format = ctx.output_format;
    auto total_num = ctx.total_num;

    if (output_format == "edges" || output_format == "successors") {
      if (frag.fid() == 0) {
        auto& results = ctx.results;
        std::vector<size_t> shape{(size_t) total_num - 1, 2};

        ctx.set_shape(shape);

        auto* data = ctx.tensor().data();
        size_t idx = 0;

        for (int i = 0; i < total_num - 1; i++) {
          data[idx++] = results[i];
          data[idx++] = results[i + 1];
        }
      }
    } else if (output_format == "predecessors") {
      if (frag.fid() == 0) {
        auto& results = ctx.results;
        std::vector<size_t> shape{(size_t) total_num - 1, 2};

        ctx.set_shape(shape);

        auto* data = ctx.tensor().data();
        size_t idx = 0;

        for (int i = 1; i < total_num; i++) {
          data[idx++] = results[i];
          data[idx++] = results[i + 1];
        }
      }
    } else {
      auto& rank = ctx.rank;
      std::vector<size_t> shape{inner_vertices.size(), 2};

      ctx.set_shape(shape);
      auto* data = ctx.tensor().data();
      size_t idx = 0;

      for (auto& u : inner_vertices) {
        data[idx++] = frag.GetId(u);
        data[idx++] = rank[u];
      }
    }
  }
};
};  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_DFS_DFS_H_
