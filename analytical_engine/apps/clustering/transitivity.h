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

#ifndef ANALYTICAL_ENGINE_APPS_CLUSTERING_TRANSITIVITY_H_
#define ANALYTICAL_ENGINE_APPS_CLUSTERING_TRANSITIVITY_H_

#include <utility>
#include <vector>

#include "grape/grape.h"

#include "clustering/transitivity_context.h"

namespace gs {
/**
 * Compute the fraction of triangles in the graph.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class Transitivity
    : public grape::ParallelAppBase<FRAG_T, TransitivityContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(Transitivity<FRAG_T>, TransitivityContext<FRAG_T>,
                          FRAG_T);
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    messages.InitChannels(thread_num());
    ctx.stage = 0;
    ForEach(inner_vertices.begin(), inner_vertices.end(),
            [&messages, &frag, &ctx](int tid, vertex_t v) {
              ctx.global_degree[v] =
                  frag.GetLocalOutDegree(v) + frag.GetLocalInDegree(v);
              messages.SendMsgThroughEdges<fragment_t, int>(
                  frag, v, ctx.global_degree[v], tid);
            });
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using vid_t = typename context_t::vid_t;
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();
    if (ctx.stage == 0) {
      ctx.stage = 1;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag,
          [&ctx](int tid, vertex_t u, int msg) { ctx.global_degree[u] = msg; });

      ForEach(inner_vertices.begin(), inner_vertices.end(),
              [&frag, &ctx, &messages, &vertices](int tid, vertex_t v) {
                vid_t u_gid, v_gid;
                auto& nbr_vec = ctx.complete_neighbor[v];
                int degree = ctx.global_degree[v];
                nbr_vec.reserve(degree);
                std::vector<std::pair<vid_t, uint32_t>> msg_vec;
                msg_vec.reserve(degree);

                typename FRAG_T::template vertex_array_t<uint32_t> is_rec;
                is_rec.Init(vertices, 0);
                auto es = frag.GetOutgoingAdjList(v);
                for (auto& e : es) {
                  auto u = e.get_neighbor();
                  is_rec[u]++;
                }
                es = frag.GetIncomingAdjList(v);
                for (auto& e : es) {
                  auto u = e.get_neighbor();
                  is_rec[u]++;
                  if (is_rec[u] == 2) {
                    ctx.rec_degree[v]++;
                  }
                }

                es = frag.GetOutgoingAdjList(v);
                for (auto& e : es) {
                  auto u = e.get_neighbor();
                  if (ctx.global_degree[u] < ctx.global_degree[v]) {
                    std::pair<vid_t, uint32_t> msg;
                    msg.first = frag.Vertex2Gid(u);
                    if (is_rec[u] == 2) {
                      msg.second = 2;
                    } else {
                      msg.second = 1;
                    }
                    msg_vec.push_back(msg);
                    nbr_vec.push_back(std::make_pair(u, msg.second));
                  } else if (ctx.global_degree[u] == ctx.global_degree[v]) {
                    u_gid = frag.Vertex2Gid(u);
                    v_gid = frag.GetInnerVertexGid(v);
                    if (v_gid > u_gid) {
                      std::pair<vid_t, uint32_t> msg;
                      msg.first = frag.Vertex2Gid(u);
                      if (is_rec[u] == 2) {
                        msg.second = 2;
                      } else {
                        msg.second = 1;
                      }
                      nbr_vec.push_back(std::make_pair(u, msg.second));
                      msg_vec.push_back(msg);
                    }
                  }
                }

                es = frag.GetIncomingAdjList(v);
                for (auto& e : es) {
                  auto u = e.get_neighbor();
                  if (ctx.global_degree[u] < ctx.global_degree[v]) {
                    std::pair<vid_t, uint32_t> msg;
                    msg.first = frag.Vertex2Gid(u);
                    if (is_rec[u] == 1) {
                      msg.second = 1;
                      msg_vec.push_back(msg);
                      nbr_vec.push_back(std::make_pair(u, 1));
                    }
                  } else if (ctx.global_degree[u] == ctx.global_degree[v]) {
                    u_gid = frag.Vertex2Gid(u);
                    v_gid = frag.GetInnerVertexGid(v);
                    if (v_gid > u_gid) {
                      std::pair<vid_t, uint32_t> msg;
                      msg.first = frag.Vertex2Gid(u);
                      if (is_rec[u] == 1) {
                        msg.second = 1;
                        msg_vec.push_back(msg);
                        nbr_vec.push_back(std::make_pair(u, 1));
                      }
                    }
                  }
                }

                messages.SendMsgThroughEdges<
                    fragment_t, std::vector<std::pair<vid_t, uint32_t>>>(
                    frag, v, msg_vec, tid);
              });
      messages.ForceContinue();
    } else if (ctx.stage == 1) {
      ctx.stage = 2;
      messages
          .ParallelProcess<fragment_t, std::vector<std::pair<vid_t, uint32_t>>>(
              thread_num(), frag,
              [&frag, &ctx](
                  int tid, vertex_t u,
                  const std::vector<std::pair<vid_t, uint32_t>>& msg) {
                auto& nbr_vec = ctx.complete_neighbor[u];
                for (auto m : msg) {
                  auto gid = m.first;
                  vertex_t v;
                  if (frag.Gid2Vertex(gid, v)) {
                    nbr_vec.push_back(std::make_pair(v, m.second));
                  }
                }
              });

      ForEach(inner_vertices.begin(), inner_vertices.end(),
              [&frag, &ctx, &messages](int tid, vertex_t v) {
                auto& outer_nbr_vec = ctx.complete_outer_neighbor[v];
                int degree = frag.GetLocalOutDegree(v);
                outer_nbr_vec.reserve(degree);
                auto es = frag.GetOutgoingAdjList(v);
                std::vector<vid_t> msg_vec;
                msg_vec.reserve(degree);
                for (auto& e : es) {
                  auto u = e.get_neighbor();
                  outer_nbr_vec.push_back(u);
                  msg_vec.push_back(frag.Vertex2Gid(u));
                }
                messages.SendMsgThroughEdges<fragment_t, std::vector<vid_t>>(
                    frag, v, msg_vec, tid);
              });
      messages.ForceContinue();
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      messages.ParallelProcess<fragment_t, std::vector<vid_t>>(
          thread_num(), frag,
          [&frag, &ctx](int tid, vertex_t u, const std::vector<vid_t>& msg) {
            auto& outer_nbr_vec = ctx.complete_outer_neighbor[u];
            for (auto gid : msg) {
              vertex_t v;
              if (frag.Gid2Vertex(gid, v)) {
                outer_nbr_vec.push_back(v);
              }
            }
          });

      typename FRAG_T::template vertex_array_t<uint32_t> v0_nbr_set(vertices,
                                                                    0);
      for (auto v : inner_vertices) {
        auto& v0_nbr_vec = ctx.complete_neighbor[v];
        for (auto u : v0_nbr_vec) {
          v0_nbr_set[u.first] = u.second;
        }
        for (auto u : v0_nbr_vec) {
          auto& v1_nbr_vec = ctx.complete_neighbor[u.first];
          for (auto w : v1_nbr_vec) {
            if (v0_nbr_set[w.first] != 0) {
              int flag = 0;
              for (size_t i = 0; i < ctx.complete_outer_neighbor[v].size();
                   i++) {
                if (ctx.complete_outer_neighbor[v][i] == u.first ||
                    ctx.complete_outer_neighbor[v][i] == w.first) {
                  flag++;
                }
                if (flag == 2) {
                  break;
                }
              }
              if (flag == 2) {
                ctx.tricnt[v] += w.second;
              }
              flag = 0;
              for (size_t i = 0;
                   i < ctx.complete_outer_neighbor[u.first].size(); i++) {
                if (ctx.complete_outer_neighbor[u.first][i] == v ||
                    ctx.complete_outer_neighbor[u.first][i] == w.first) {
                  flag++;
                }
                if (flag == 2) {
                  break;
                }
              }
              if (flag == 2) {
                ctx.tricnt[u.first] += v0_nbr_set[w.first];
              }
              flag = 0;
              for (size_t i = 0;
                   i < ctx.complete_outer_neighbor[w.first].size(); i++) {
                if (ctx.complete_outer_neighbor[w.first][i] == v ||
                    ctx.complete_outer_neighbor[w.first][i] == u.first) {
                  flag++;
                }
                if (flag == 2) {
                  break;
                }
              }
              if (flag == 2) {
                ctx.tricnt[w.first] += v0_nbr_set[u.first];
              }
            }
          }
        }
        for (auto u : v0_nbr_vec) {
          v0_nbr_set[u.first] = 0;
        }
      }

      ForEach(outer_vertices.begin(), outer_vertices.end(),
              [&messages, &frag, &ctx](int tid, vertex_t v) {
                if (ctx.tricnt[v] != 0) {
                  messages.SyncStateOnOuterVertex<fragment_t, int>(
                      frag, v, ctx.tricnt[v], tid);
                }
              });
      messages.ForceContinue();
    } else if (ctx.stage == 3) {
      ctx.stage = 4;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, int deg) {
            grape::atomic_add(ctx.tricnt[u], deg);
          });
      messages.ForceContinue();
    } else if (ctx.stage == 4) {
      ctx.stage = 5;
      int total_triangles = 0;
      int total_trids = 0;
      for (auto v : inner_vertices) {
        total_triangles += ctx.tricnt[v];
        total_trids +=
            frag.GetLocalOutDegree(v) * (frag.GetLocalOutDegree(v) - 1);
      }
      std::pair<int, int> msg = std::make_pair(total_triangles, total_trids);
      grape::InArchive in_archive;
      in_archive << msg;
      messages.SendRawMsgByFid(0, std::move(in_archive));
      messages.ForceContinue();
    } else if (ctx.stage == 5) {
      messages.ParallelProcess<std::pair<int, int>>(
          thread_num(), [&ctx](int tid, const std::pair<int, int>& msg) {
            grape::atomic_add(ctx.total_triangles, msg.first);
            grape::atomic_add(ctx.total_trids, msg.second);
          });

      if (frag.fid() == 0) {
        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        ctx.assign(1.0 * ctx.total_triangles / ctx.total_trids);
      }
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CLUSTERING_TRANSITIVITY_H_
