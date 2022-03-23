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

#ifndef ANALYTICAL_ENGINE_BENCHMARKS_APPS_SSSP_SSSP_H_
#define ANALYTICAL_ENGINE_BENCHMARKS_APPS_SSSP_SSSP_H_

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>

#include "grape/grape.h"

namespace gs {

namespace benchmarks {

template <typename FRAG_T>
class SSSPContext : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit SSSPContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment, true),
        partial_result(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, oid_t source_id) {
    auto& frag = this->fragment();

    this->source_id = source_id;
    auto vertices = frag.Vertices();
    partial_result.Init(vertices, std::numeric_limits<double>::max());

    curr_modified.Init(frag.Vertices());
    next_modified.Init(frag.Vertices());
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    // If the distance is the max value for vertex_data_type
    // then the vertex is not connected to the source vertex.
    // According to specs, the output should be +inf
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      double d = partial_result[v];
      // os << frag.GetId(v) << "\t" << d << std::endl;

      if (d == std::numeric_limits<double>::max()) {
        os << frag.GetId(v) << " infinity" << std::endl;
      } else {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << d << std::endl;
      }
    }
  }

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<double>& partial_result;

  grape::DenseVertexSet<typename FRAG_T::vertices_t> curr_modified,
      next_modified;
};

template <typename FRAG_T>
class SSSP : public grape::ParallelAppBase<FRAG_T, SSSPContext<FRAG_T>>,
             public grape::ParallelEngine {
 public:
  // specialize the templated worker.
  INSTALL_PARALLEL_WORKER(SSSP<FRAG_T>, SSSPContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

    ctx.next_modified.ParallelClear(GetThreadPool());

    // Get the channel. Messages assigned to this channel will be sent by the
    // message manager in parallel with the evaluation process.
    auto& channel_0 = messages.Channels()[0];

    if (native_source) {
      ctx.partial_result[source] = 0;
      auto es = frag.GetOutgoingAdjList(source);
      for (auto& e : es) {
        vertex_t v = e.get_neighbor();
        ctx.partial_result[v] =
            std::min(ctx.partial_result[v], static_cast<double>(e.get_data()));
        if (frag.IsOuterVertex(v)) {
          // put the message to the channel.
          channel_0.SyncStateOnOuterVertex<fragment_t, double>(
              frag, v, ctx.partial_result[v]);
        } else {
          ctx.next_modified.Insert(v);
        }
      }
    }

    messages.ForceContinue();

    ctx.next_modified.Swap(ctx.curr_modified);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    auto& channels = messages.Channels();

    ctx.next_modified.ParallelClear(GetThreadPool());

    // parallel process and reduce the received messages
    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, double msg) {
          if (ctx.partial_result[u] > msg) {
            grape::atomic_min(ctx.partial_result[u], msg);
            ctx.curr_modified.Insert(u);
          }
        });

    // incremental evaluation.
    ForEach(ctx.curr_modified, inner_vertices,
            [&frag, &ctx](int tid, vertex_t v) {
              double distv = ctx.partial_result[v];
              auto es = frag.GetOutgoingAdjList(v);
              for (auto& e : es) {
                vertex_t u = e.get_neighbor();
                double ndistu = distv + e.get_data();
                if (ndistu < ctx.partial_result[u]) {
                  grape::atomic_min(ctx.partial_result[u], ndistu);
                  ctx.next_modified.Insert(u);
                }
              }
            });

    // put messages into channels corresponding to the destination fragments.
    auto outer_vertices = frag.OuterVertices();
    ForEach(ctx.next_modified, outer_vertices,
            [&channels, &frag, &ctx](int tid, vertex_t v) {
              channels[tid].SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.partial_result[v]);
            });

    if (!ctx.next_modified.PartialEmpty(0, frag.GetInnerVerticesNum())) {
      messages.ForceContinue();
    }

    ctx.next_modified.Swap(ctx.curr_modified);
  }
};

}  // namespace benchmarks

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_BENCHMARKS_APPS_SSSP_SSSP_H_
