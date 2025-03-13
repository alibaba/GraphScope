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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_BETA_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_BETA_H_

#include <grape/grape.h>

#include <vector>

#include "grape/utils/varint.h"
#include "lcc/lcc.h"
#include "lcc/lcc_beta_context.h"

namespace grape {

/**
 * @brief An implementation of LCC (Local CLustering Coefficient), the version
 * in LDBC, which only works on undirected graphs.
 *
 * This version of LCC inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T, typename COUNT_T = uint32_t>
class LCCBeta : public ParallelAppBase<FRAG_T, LCCBetaContext<FRAG_T, COUNT_T>,
                                       ParallelMessageManagerOpt>,
                public ParallelEngine {
  using VecOutType = DeltaVarintEncoder<typename FRAG_T::vid_t>;
  using VecInType = DeltaVarintDecoder<typename FRAG_T::vid_t>;

 public:
  using fragment_t = FRAG_T;
  using context_t = LCCBetaContext<FRAG_T, COUNT_T>;
  using message_manager_t = ParallelMessageManagerOpt;
  using worker_t = ParallelWorkerOpt<LCCBeta<FRAG_T, COUNT_T>>;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using count_t = COUNT_T;
  using tricnt_list_t = typename fragment_t::template vertex_array_t<count_t>;

  virtual ~LCCBeta() {}

  static std::shared_ptr<worker_t> CreateWorker(
      std::shared_ptr<LCCBeta<FRAG_T, COUNT_T>> app,
      std::shared_ptr<FRAG_T> frag) {
    return std::shared_ptr<worker_t>(new worker_t(app, frag));
  }

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    messages.InitChannels(thread_num());

    ctx.stage = 0;

    // Each vertex scatter its own out degree.
    ForEach(inner_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      ctx.global_degree[v] = frag.GetLocalOutDegree(v);
      messages.SendMsgThroughOEdges<fragment_t, int>(frag, v,
                                                     ctx.global_degree[v], tid);
    });

    // Just in case we are running on single process and no messages will
    // be send. ForceContinue() ensure the computation
    messages.ForceContinue();
  }

  count_t intersect_with_bs(const std::vector<vertex_t>& small,
                            const std::vector<vertex_t>& large,
                            tricnt_list_t& result) {
    count_t ret = 0;
    auto from = large.begin();
    auto to = large.end();
    for (auto v : small) {
      from = std::lower_bound(from, to, v);
      if (from == to) {
        return ret;
      }
      if (*from == v) {
        ++ret;
        ++from;
        atomic_add(result[v], static_cast<count_t>(1));
      }
    }
    return ret;
  }

  count_t intersect(const std::vector<vertex_t>& lhs,
                    const std::vector<vertex_t>& rhs, tricnt_list_t& result) {
    if (lhs.empty() || rhs.empty()) {
      return 0;
    }
    vid_t v_size = lhs.size();
    vid_t u_size = rhs.size();
    if (static_cast<double>(v_size + u_size) <
        std::min<double>(v_size, u_size) *
            ilogb(std::max<double>(v_size, u_size))) {
      count_t count = 0;
      vid_t i = 0, j = 0;
      while (i < v_size && j < u_size) {
        if (lhs[i] == rhs[j]) {
          atomic_add(result[lhs[i]], static_cast<count_t>(1));
          ++count;
          ++i;
          ++j;
        } else if (lhs[i] < rhs[j]) {
          ++i;
        } else {
          ++j;
        }
      }
      return count;
    } else {
      if (v_size > u_size) {
        return intersect_with_bs(rhs, lhs, result);
      } else {
        return intersect_with_bs(lhs, rhs, result);
      }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using vid_t = typename context_t::vid_t;

    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    if (ctx.stage == 0) {
      ctx.stage = 1;

      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag,
          [&ctx](int tid, vertex_t u, int msg) { ctx.global_degree[u] = msg; });

      std::vector<size_t> max_degrees(thread_num(), 0);
      ForEach(inner_vertices, [&frag, &ctx, &messages, &max_degrees](
                                  int tid, vertex_t v) {
        vid_t v_gid_hash = IdHasher<vid_t>::hash(frag.GetInnerVertexGid(v));
        auto& inner_nbr_vec = ctx.complete_inner_neighbor[v];
        auto& outer_nbr_vec = ctx.complete_outer_neighbor[v];
        int degree = ctx.global_degree[v];
        auto es = frag.GetOutgoingAdjList(v);
        static thread_local VecOutType msg_vec;
        msg_vec.clear();
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.global_degree[u] > degree) {
            if (frag.IsInnerVertex(u)) {
              inner_nbr_vec.push_back(u);
              msg_vec.push_back(frag.GetInnerVertexGid(u));
            } else {
              outer_nbr_vec.push_back(u);
              msg_vec.push_back(frag.GetOuterVertexGid(u));
            }
          } else if (ctx.global_degree[u] == degree) {
            if (frag.IsInnerVertex(u)) {
              vid_t u_gid = frag.GetInnerVertexGid(u);
              if (v_gid_hash > IdHasher<vid_t>::hash(u_gid)) {
                inner_nbr_vec.push_back(u);
                msg_vec.push_back(frag.GetInnerVertexGid(u));
              }
            } else {
              vid_t u_gid = frag.GetOuterVertexGid(u);
              if (v_gid_hash > IdHasher<vid_t>::hash(u_gid)) {
                outer_nbr_vec.push_back(u);
                msg_vec.push_back(frag.GetOuterVertexGid(u));
              }
            }
          }
        }
        if (msg_vec.empty()) {
          return;
        }
        std::sort(inner_nbr_vec.begin(), inner_nbr_vec.end());
        std::sort(outer_nbr_vec.begin(), outer_nbr_vec.end());
        if (msg_vec.size() > max_degrees[tid]) {
          max_degrees[tid] = msg_vec.size();
        }
        messages.SendMsgThroughOEdges<fragment_t, VecOutType>(frag, v, msg_vec,
                                                              tid);
      });
      size_t max_degree = 0;
      for (auto x : max_degrees) {
        max_degree = std::max(x, max_degree);
      }
      ctx.degree_x = max_degree * 4 / 10;
      messages.ForceContinue();
    } else if (ctx.stage == 1) {
      ctx.stage = 2;
      messages.ParallelProcess<fragment_t, VecInType>(
          thread_num(), frag,
          [&frag, &ctx](int tid, vertex_t u, VecInType& msg) {
            auto& inner_nbr_vec = ctx.complete_inner_neighbor[u];
            auto& outer_nbr_vec = ctx.complete_outer_neighbor[u];
            vid_t gid;
            while (msg.pop(gid)) {
              vertex_t v;
              if (frag.Gid2Vertex(gid, v)) {
                if (frag.IsInnerVertex(v)) {
                  inner_nbr_vec.push_back(v);
                } else {
                  outer_nbr_vec.push_back(v);
                }
              }
            }
            std::sort(inner_nbr_vec.begin(), inner_nbr_vec.end());
            std::sort(outer_nbr_vec.begin(), outer_nbr_vec.end());
          });
      std::vector<DenseVertexSet<typename FRAG_T::inner_vertices_t>>
          inner_vertexsets(thread_num());
      std::vector<DenseVertexSet<typename FRAG_T::outer_vertices_t>>
          outer_vertexsets(thread_num());
      for (auto& vs : inner_vertexsets) {
        vs.Init(frag.InnerVertices());
      }
      for (auto& vs : outer_vertexsets) {
        vs.Init(frag.OuterVertices());
      }
      ForEach(inner_vertices, [this, &ctx, &inner_vertexsets,
                               &outer_vertexsets](int tid, vertex_t v) {
        auto& v0_inner_nbr_vec = ctx.complete_inner_neighbor[v];
        auto& v0_outer_nbr_vec = ctx.complete_outer_neighbor[v];
        size_t deg = v0_inner_nbr_vec.size() + v0_outer_nbr_vec.size();
        if (deg <= 1) {
          return;
        } else if (deg <= 10) {
          count_t v_count = 0;
          for (auto u : v0_inner_nbr_vec) {
            auto& v1_inner_nbr_vec = ctx.complete_inner_neighbor[u];
            auto& v1_outer_nbr_vec = ctx.complete_outer_neighbor[u];
            count_t u_count =
                intersect(v0_inner_nbr_vec, v1_inner_nbr_vec, ctx.tricnt);
            u_count +=
                intersect(v0_outer_nbr_vec, v1_outer_nbr_vec, ctx.tricnt);
            atomic_add(ctx.tricnt[u], u_count);
            v_count += u_count;
          }
          for (auto u : v0_outer_nbr_vec) {
            auto& v1_inner_nbr_vec = ctx.complete_inner_neighbor[u];
            auto& v1_outer_nbr_vec = ctx.complete_outer_neighbor[u];
            count_t u_count =
                intersect(v0_inner_nbr_vec, v1_inner_nbr_vec, ctx.tricnt);
            u_count +=
                intersect(v0_outer_nbr_vec, v1_outer_nbr_vec, ctx.tricnt);
            atomic_add(ctx.tricnt[u], u_count);
            v_count += u_count;
          }
          atomic_add(ctx.tricnt[v], v_count);
        } else {
          auto& v0_inner_nbr_set = inner_vertexsets[tid];
          for (auto u : v0_inner_nbr_vec) {
            v0_inner_nbr_set.Insert(u);
          }
          count_t v_count = 0;
          for (auto u : v0_inner_nbr_vec) {
            count_t u_count = 0;
            auto& v1_nbr_vec = ctx.complete_inner_neighbor[u];
            for (auto w : v1_nbr_vec) {
              if (v0_inner_nbr_set.Exist(w)) {
                ++u_count;
                atomic_add(ctx.tricnt[w], static_cast<count_t>(1));
              }
            }
            v_count += u_count;
            atomic_add(ctx.tricnt[u], u_count);
          }
          for (auto u : v0_outer_nbr_vec) {
            count_t u_count = 0;
            auto& v1_nbr_vec = ctx.complete_inner_neighbor[u];
            for (auto w : v1_nbr_vec) {
              if (v0_inner_nbr_set.Exist(w)) {
                ++u_count;
                atomic_add(ctx.tricnt[w], static_cast<count_t>(1));
              }
            }
            v_count += u_count;
            atomic_add(ctx.tricnt[u], u_count);
          }
          atomic_add(ctx.tricnt[v], v_count);
          for (auto u : v0_inner_nbr_vec) {
            v0_inner_nbr_set.Erase(u);
          }

          auto& v0_outer_nbr_set = outer_vertexsets[tid];
          for (auto u : v0_outer_nbr_vec) {
            v0_outer_nbr_set.Insert(u);
          }

          for (auto u : v0_inner_nbr_vec) {
            count_t u_count = 0;
            auto& v1_nbr_vec = ctx.complete_outer_neighbor[u];
            for (auto w : v1_nbr_vec) {
              if (v0_outer_nbr_set.Exist(w)) {
                ++u_count;
                atomic_add(ctx.tricnt[w], static_cast<count_t>(1));
              }
            }
            v_count += u_count;
            atomic_add(ctx.tricnt[u], u_count);
          }
          for (auto u : v0_outer_nbr_vec) {
            count_t u_count = 0;
            auto& v1_nbr_vec = ctx.complete_outer_neighbor[u];
            for (auto w : v1_nbr_vec) {
              if (v0_outer_nbr_set.Exist(w)) {
                ++u_count;
                atomic_add(ctx.tricnt[w], static_cast<count_t>(1));
              }
            }
            v_count += u_count;
            atomic_add(ctx.tricnt[u], u_count);
          }
          atomic_add(ctx.tricnt[v], v_count);
          for (auto u : v0_outer_nbr_vec) {
            v0_outer_nbr_set.Erase(u);
          }
          atomic_add(ctx.tricnt[v], v_count);
        }
      });
      ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
        if (ctx.tricnt[v] != 0) {
          messages.SyncStateOnOuterVertex<fragment_t, count_t>(
              frag, v, ctx.tricnt[v], tid);
        }
      });
      messages.ForceContinue();
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      messages.ParallelProcess<fragment_t, count_t>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, count_t deg) {
            atomic_add(ctx.tricnt[u], deg);
          });

      // output result to context data
      auto& global_degree = ctx.global_degree;
      auto& tricnt = ctx.tricnt;
      auto& ctx_data = ctx.data();

      ForEach(inner_vertices, [&](int tid, vertex_t v) {
        if (global_degree[v] == 0 || global_degree[v] == 1) {
          ctx_data[v] = 0;
        } else {
          double re = 2.0 * (static_cast<int64_t>(tricnt[v])) /
                      (static_cast<int64_t>(global_degree[v]) *
                       (static_cast<int64_t>(global_degree[v]) - 1));
          ctx_data[v] = re;
        }
      });
    }
  }
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_BETA_H_
