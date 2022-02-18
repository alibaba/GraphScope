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

#ifndef ANALYTICAL_ENGINE_BENCHMARKS_APPS_SSSP_PROPERTY_SSSP_H_
#define ANALYTICAL_ENGINE_BENCHMARKS_APPS_SSSP_PROPERTY_SSSP_H_

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>

#include "grape/grape.h"

#include "core/app/parallel_property_app_base.h"
#include "core/context/vertex_data_context.h"
#include "core/worker/parallel_property_worker.h"

namespace gs {

namespace benchmarks {

#define PREFETCH_COLUMN

template <typename FRAG_T>
class PropertySSSPContext : public LabeledVertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit PropertySSSPContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, double>(fragment, true),
        partial_result(this->data()[0]) {}

  void Init(ParallelPropertyMessageManager& messages, oid_t source_id) {
    auto& frag = this->fragment();
    this->source_id = source_id;
    auto vertices = frag.Vertices(0);
    partial_result.Init(vertices, std::numeric_limits<double>::max());

    curr_modified.Init(frag.Vertices(0));
    next_modified.Init(frag.Vertices(0));
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    // If the distance is the max value for vertex_data_type
    // then the vertex is not connected to the source vertex.
    // According to specs, the output should be +inf
    auto inner_vertices = frag.InnerVertices(0);
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
class PropertySSSP
    : public ParallelPropertyAppBase<FRAG_T, PropertySSSPContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  // specialize the templated worker.
  INSTALL_PARALLEL_PROPERTY_WORKER(PropertySSSP<FRAG_T>,
                                   PropertySSSPContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());

    vertex_t source;
    bool native_source = frag.GetInnerVertex(0, ctx.source_id, source);

    ctx.next_modified.ParallelClear(GetThreadPool());

    // Get the channel. Messages assigned to this channel will be sent by the
    // message manager in parallel with the evaluation process.
    auto& channel_0 = messages.Channels()[0];
#ifdef PREFETCH_COLUMN
    auto dist_column = frag.template edge_data_column<int64_t>(0, 0);
#endif

    if (native_source) {
      ctx.partial_result[source] = 0;
#ifdef PREFETCH_COLUMN
      auto es = frag.GetOutgoingRawAdjList(source, 0);
#else
      auto es = frag.GetOutgoingAdjList(source, 0);
#endif
      for (auto& e : es) {
        vertex_t v = e.get_neighbor();
#ifdef PREFETCH_COLUMN
        ctx.partial_result[v] = std::min(ctx.partial_result[v],
                                         static_cast<double>(dist_column[e]));
#else
        ctx.partial_result[v] =
            std::min(ctx.partial_result[v],
                     static_cast<double>(e.template get_data<int64_t>(0)));
#endif
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
    auto inner_vertices = frag.InnerVertices(0);

    auto& channels = messages.Channels();
#ifdef PREFETCH_COLUMN
    auto dist_column = frag.template edge_data_column<int64_t>(0, 0);
#endif

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
#ifdef PREFETCH_COLUMN
            [&frag, &ctx, &dist_column](int tid, vertex_t v) {
#else
            [&frag, &ctx](int tid, vertex_t v) {
#endif
              double distv = ctx.partial_result[v];
#ifdef PREFETCH_COLUMN
              auto es = frag.GetOutgoingRawAdjList(v, 0);
#else
              auto es = frag.GetOutgoingAdjList(v, 0);
#endif
              for (auto& e : es) {
                vertex_t u = e.get_neighbor();
#ifdef PREFETCH_COLUMN
                double ndistu = distv + static_cast<double>(dist_column[e]);
#else
                double ndistu = distv + static_cast<double>(
                                            e.template get_data<int64_t>(0));
#endif
                if (ndistu < ctx.partial_result[u]) {
                  grape::atomic_min(ctx.partial_result[u], ndistu);
                  ctx.next_modified.Insert(u);
                }
              }
            });

    // put messages into channels corresponding to the destination fragments.

    auto outer_vertices = frag.OuterVertices(0);
    ForEach(ctx.next_modified, outer_vertices,
            [&channels, &frag, &ctx](int tid, vertex_t v) {
              channels[tid].SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.partial_result[v]);
            });

    if (!ctx.next_modified.PartialEmpty(0, frag.GetInnerVerticesNum(0))) {
      messages.ForceContinue();
    }

    ctx.next_modified.Swap(ctx.curr_modified);
  }
};

}  // namespace benchmarks

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_BENCHMARKS_APPS_SSSP_PROPERTY_SSSP_H_
