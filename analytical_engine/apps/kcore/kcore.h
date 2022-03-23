/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_APPS_KCORE_KCORE_H_
#define ANALYTICAL_ENGINE_APPS_KCORE_KCORE_H_

#include <functional>
#include <memory>
#include <vector>

#include "kcore/kcore_context.h"

namespace gs {
/**
 * @brief Compute a maximal connected subgraph of G in which all vertices have
 * degree at least k.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class KCore : public grape::ParallelAppBase<FRAG_T, KCoreContext<FRAG_T>>,
              public grape::ParallelEngine,
              public grape::Communicator {
 public:
  INSTALL_PARALLEL_WORKER(KCore<FRAG_T>, KCoreContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename FRAG_T::vid_t;

  void UpdateDegree(
      const fragment_t& frag,
      const grape::DenseVertexSet<typename FRAG_T::inner_vertices_t>& frontier,
      typename FRAG_T::template vertex_array_t<
          std::shared_ptr<std::atomic_int>>& degrees) {
    ForEach(frontier, [&degrees, &frag](int tid, vertex_t u) {
      for (auto& e : frag.GetOutgoingAdjList(u)) {
        auto v = e.get_neighbor();
        degrees[v]->operator--();
      }

      degrees[u]->store(0);
    });
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    // we put all computing logic in IncEval
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    int thrd_num = thread_num();
    auto outer_vertices = frag.OuterVertices();
    auto& remaining_vertices = ctx.remaining_vertices;
    auto& next_remaining_vertices = ctx.next_remaining_vertices;
    auto& to_remove_vertices = ctx.to_remove_vertices;
    auto& curr_k = ctx.curr_k;
    auto& degrees = ctx.degrees;

    messages.ParallelProcess<fragment_t, int32_t>(
        thrd_num, frag,
        [&degrees](int tid, vertex_t v, int32_t msg) { *degrees[v] += msg; });

    // remove vertices which degree less than curr_k
    ForEach(remaining_vertices,
            [&to_remove_vertices, &degrees, curr_k](int tid, vertex_t v) {
              if (degrees[v]->load() < curr_k) {
                to_remove_vertices.Insert(v);
              }
            });

    // keep vertices which degree greater or equal than currk
    ForEach(remaining_vertices,
            [&next_remaining_vertices, &degrees, curr_k](int tid, vertex_t v) {
              if (degrees[v]->load() >= curr_k) {
                next_remaining_vertices.Insert(v);
              }
            });

    UpdateDegree(frag, to_remove_vertices, degrees);

    ForEach(outer_vertices, [&frag, &degrees, &messages](int tid, vertex_t v) {
      int degree = degrees[v]->load();
      if (degree != 0) {
        messages.Channels()[tid].SyncStateOnOuterVertex<fragment_t, int32_t>(
            frag, v, degree);
        degrees[v]->store(0);
      }
    });

    size_t global_removed_count = 0;
    Sum(to_remove_vertices.Count(), global_removed_count);
    if (global_removed_count == 0) {
      curr_k++;
    }

    to_remove_vertices.Clear();
    remaining_vertices.Clear();
    remaining_vertices.Swap(next_remaining_vertices);

    if (curr_k > ctx.k) {
      auto inner_vertices = frag.InnerVertices();

      for (auto v : inner_vertices) {
        ctx.data()[v] = remaining_vertices.Exist(v) ? 1 : 0;
      }
      return;
    }
    messages.ForceContinue();
  }
};
};  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_KCORE_KCORE_H_
