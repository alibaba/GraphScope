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

#ifndef ANALYTICAL_ENGINE_BENCHMARKS_APPS_BFS_PROPERTY_BFS_H_
#define ANALYTICAL_ENGINE_BENCHMARKS_APPS_BFS_PROPERTY_BFS_H_

#include <limits>

#include "grape/grape.h"

#include "core/app/parallel_property_app_base.h"
#include "core/context/vertex_data_context.h"
#include "core/worker/parallel_property_worker.h"

namespace gs {

namespace benchmarks {

template <typename FRAG_T>
class PropertyBFSContext : public LabeledVertexDataContext<FRAG_T, int64_t> {
 public:
  using depth_type = int64_t;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit PropertyBFSContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, int64_t>(fragment, true),
        partial_result(this->data()[0]) {}

  void Init(ParallelPropertyMessageManager& messages, oid_t src_id) {
    auto& frag = this->fragment();
    source_id = src_id;

    auto vertices = frag.Vertices(0);
    partial_result.Init(vertices, std::numeric_limits<depth_type>::max());
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices(0);
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << partial_result[v] << std::endl;
    }
  }

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<depth_type>& partial_result;
  grape::DenseVertexSet<typename FRAG_T::inner_vertices_t> curr_inner_updated,
      next_inner_updated;

  depth_type current_depth = 0;
};

template <typename FRAG_T>
class PropertyBFS
    : public ParallelPropertyAppBase<FRAG_T, PropertyBFSContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_PROPERTY_WORKER(PropertyBFS<FRAG_T>,
                                   PropertyBFSContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    messages.InitChannels(thread_num(), 2 * 1023 * 64, 2 * 1024 * 64);

    ctx.current_depth = 1;

    vertex_t source;
    bool native_source = frag.GetInnerVertex(0, ctx.source_id, source);

    auto inner_vertices = frag.InnerVertices(0);

    // init double buffer which contains updated vertices using bitmap
    ctx.curr_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.next_inner_updated.Init(inner_vertices, GetThreadPool());

    auto& channel_0 = messages.Channels()[0];

    if (native_source) {
      ctx.partial_result[source] = 0;
      auto oes = frag.GetOutgoingAdjList(source, 0);
      for (auto& e : oes) {
        auto u = e.get_neighbor();
        if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
          ctx.partial_result[u] = 1;
          if (frag.IsOuterVertex(u)) {
            channel_0.SyncStateOnOuterVertex<fragment_t>(frag, u);
          } else {
            ctx.curr_inner_updated.Insert(u);
          }
        }
      }
    }

    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    auto& channels = messages.Channels();

    depth_type next_depth = ctx.current_depth + 1;
    int thrd_num = thread_num();
    ctx.next_inner_updated.ParallelClear(GetThreadPool());

    // process received messages and update depth
    messages.ParallelProcess<fragment_t, grape::EmptyType>(
        thrd_num, frag, [&ctx](int tid, vertex_t v, grape::EmptyType) {
          if (ctx.partial_result[v] == std::numeric_limits<depth_type>::max()) {
            ctx.partial_result[v] = ctx.current_depth;
            ctx.curr_inner_updated.Insert(v);
          }
        });

    // sync messages to other workers
    ForEach(ctx.curr_inner_updated, [next_depth, &frag, &ctx, &channels](
                                        int tid, vertex_t v) {
      auto oes = frag.GetOutgoingAdjList(v, 0);
      for (auto& e : oes) {
        auto u = e.get_neighbor();
        if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
          ctx.partial_result[u] = next_depth;
          if (frag.IsOuterVertex(u)) {
            channels[tid].SyncStateOnOuterVertex<fragment_t>(frag, u);
          } else {
            ctx.next_inner_updated.Insert(u);
          }
        }
      }
    });

    ctx.current_depth = next_depth;
    if (!ctx.next_inner_updated.Empty()) {
      messages.ForceContinue();
    }

    ctx.next_inner_updated.Swap(ctx.curr_inner_updated);
  }
};

}  // namespace benchmarks

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_BENCHMARKS_APPS_BFS_PROPERTY_BFS_H_
