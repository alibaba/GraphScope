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

#ifndef ANALYTICAL_ENGINE_APPS_CLUSTERING_AVG_CLUSTERING_H_
#define ANALYTICAL_ENGINE_APPS_CLUSTERING_AVG_CLUSTERING_H_

#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "clustering/avg_clustering_context.h"

namespace gs {
/**
 * @brief Compute the average clustering coefficient for the directed graph.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class AvgClustering
    : public grape::ParallelAppBase<FRAG_T, AvgClusteringContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(AvgClustering<FRAG_T>, AvgClusteringContext<FRAG_T>,
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
    ForEach(inner_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      ctx.global_degree[v] =
          frag.GetLocalOutDegree(v) + frag.GetLocalInDegree(v);
      messages.SendMsgThroughEdges<fragment_t, int>(frag, v,
                                                    ctx.global_degree[v], tid);
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

      ForEach(inner_vertices, [this, &frag, &ctx, &messages](int tid,
                                                             vertex_t v) {
        if (filterByDegree(frag, ctx, v)) {
          return;
        }
        vid_t u_gid, v_gid;
        auto& nbr_vec = ctx.complete_neighbor[v];
        int degree = ctx.global_degree[v];
        nbr_vec.reserve(degree);
        std::vector<std::pair<vid_t, uint32_t>> msg_vec;
        msg_vec.reserve(degree);

        std::unordered_map<vid_t, uint32_t> is_rec;
        auto es = frag.GetOutgoingAdjList(v);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          is_rec[u.GetValue()]++;
        }
        es = frag.GetIncomingAdjList(v);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          is_rec[u.GetValue()]++;
          if (is_rec[u.GetValue()] == 2) {
            ctx.rec_degree[v]++;
          }
        }

        es = frag.GetOutgoingAdjList(v);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.global_degree[u] < ctx.global_degree[v]) {
            std::pair<vid_t, uint32_t> msg;
            msg.first = frag.Vertex2Gid(u);
            if (is_rec[u.GetValue()] == 2) {
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
              if (is_rec[u.GetValue()] == 2) {
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
            if (is_rec[u.GetValue()] == 1) {
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
              if (is_rec[u.GetValue()] == 1) {
                msg.second = 1;
                msg_vec.push_back(msg);
                nbr_vec.push_back(std::make_pair(u, 1));
              }
            }
          }
        }

        messages.SendMsgThroughEdges<fragment_t,
                                     std::vector<std::pair<vid_t, uint32_t>>>(
            frag, v, msg_vec, tid);
      });
      messages.ForceContinue();
    } else if (ctx.stage == 1) {
      ctx.stage = 2;
      messages
          .ParallelProcess<fragment_t, std::vector<std::pair<vid_t, uint32_t>>>(
              thread_num(), frag,
              [this, &frag, &ctx](
                  int tid, vertex_t u,
                  const std::vector<std::pair<vid_t, uint32_t>>& msg) {
                if (frag.IsInnerVertex(u) && filterByDegree(frag, ctx, u)) {
                  return;
                }
                auto& nbr_vec = ctx.complete_neighbor[u];
                for (auto m : msg) {
                  auto gid = m.first;
                  vertex_t v;
                  if (frag.Gid2Vertex(gid, v)) {
                    nbr_vec.push_back(std::make_pair(v, m.second));
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
              ctx.tricnt[u.first] += v0_nbr_set[w.first] * u.second * w.second;
              ctx.tricnt[v] += v0_nbr_set[w.first] * u.second * w.second;
              ctx.tricnt[w.first] += v0_nbr_set[w.first] * u.second * w.second;
            }
          }
        }
        for (auto u : v0_nbr_vec) {
          v0_nbr_set[u.first] = 0;
        }
      }

      ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
        if (ctx.tricnt[v] != 0) {
          messages.SyncStateOnOuterVertex<fragment_t, int>(frag, v,
                                                           ctx.tricnt[v], tid);
        }
      });
      messages.ForceContinue();
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, int deg) {
            grape::atomic_add(ctx.tricnt[u], deg);
          });
      messages.ForceContinue();
    } else if (ctx.stage == 3) {
      ctx.stage = 4;
      float total_clustering = 0;
      for (auto v : inner_vertices) {
        int degree = ctx.global_degree[v] * (ctx.global_degree[v] - 1) -
                     2 * ctx.rec_degree[v];
        if (degree != 0) {
          total_clustering += 1.0 * ctx.tricnt[v] / degree;
        }
      }
      float msg = total_clustering;
      grape::InArchive in_archive;
      in_archive << msg;
      messages.SendRawMsgByFid(0, std::move(in_archive));
      messages.ForceContinue();
    } else if (ctx.stage == 4) {
      messages.ParallelProcess<float>(
          thread_num(), [&ctx](int tid, const float& msg) {
            grape::atomic_add(ctx.total_clustering, msg);
          });

      if (frag.fid() == 0) {
        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        ctx.assign(ctx.total_clustering / frag.GetTotalVerticesNum());
      }
    }
  }

  bool filterByDegree(const fragment_t& frag, context_t& ctx, vertex_t v) {
    int degree = frag.GetLocalOutDegree(v);
    if (frag.directed()) {
      degree += frag.GetLocalInDegree(v);
    }
    if (degree > ctx.degree_threshold) {
      return true;
    }
    return false;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CLUSTERING_AVG_CLUSTERING_H_
