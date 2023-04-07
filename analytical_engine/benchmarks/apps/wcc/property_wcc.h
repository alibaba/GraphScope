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

#ifndef ANALYTICAL_ENGINE_BENCHMARKS_APPS_WCC_PROPERTY_WCC_H_
#define ANALYTICAL_ENGINE_BENCHMARKS_APPS_WCC_PROPERTY_WCC_H_

#include <limits>

#include "grape/grape.h"

#include "core/app/parallel_property_app_base.h"
#include "core/context/vertex_data_context.h"
#include "core/worker/parallel_property_worker.h"

namespace gs {

namespace benchmarks {

template <typename FRAG_T>
class PropertyWCCContext
    : public LabeledVertexDataContext<FRAG_T, typename FRAG_T::vid_t> {
 public:
  using vid_t = typename FRAG_T::vid_t;

  explicit PropertyWCCContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, typename FRAG_T::vid_t>(fragment,
                                                                 true),
        comp_id(this->data()[0]) {}

  void Init(ParallelPropertyMessageManager& messages) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices(0);
    comp_id.Init(vertices);
    curr_modified.Init(vertices);
    next_modified.Init(vertices);
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices(0);
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << comp_id[v] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<vid_t>& comp_id;

  grape::DenseVertexSet<typename FRAG_T::vertices_t> curr_modified,
      next_modified;
};

template <typename FRAG_T>
class PropertyWCC
    : public ParallelPropertyAppBase<FRAG_T, PropertyWCCContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_PROPERTY_WORKER(PropertyWCC<FRAG_T>,
                                   PropertyWCCContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  void PropagateLabelPush(const fragment_t& frag, context_t& ctx,
                          message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices(0);
    auto outer_vertices = frag.OuterVertices(0);

    // propagate label to incoming and outgoing neighbors
    ForEach(ctx.curr_modified, inner_vertices,
            [&frag, &ctx](int tid, vertex_t v) {
              auto cid = ctx.comp_id[v];
              auto es = frag.GetOutgoingAdjList(v, 0);
              for (auto& e : es) {
                auto u = e.get_neighbor();
                if (ctx.comp_id[u] > cid) {
                  grape::atomic_min(ctx.comp_id[u], cid);
                  ctx.next_modified.Insert(u);
                }
              }

              if (frag.directed()) {
                es = frag.GetIncomingAdjList(v, 0);
                for (auto& e : es) {
                  auto u = e.get_neighbor();
                  if (ctx.comp_id[u] > cid) {
                    grape::atomic_min(ctx.comp_id[u], cid);
                    ctx.next_modified.Insert(u);
                  }
                }
              }
            });

    ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      if (ctx.next_modified.Exist(v)) {
        messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                           ctx.comp_id[v], tid);
      }
    });
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices(0);
    auto outer_vertices = frag.OuterVertices(0);

    messages.InitChannels(thread_num());

    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.comp_id[v] = frag.GetInnerVertexGid(v);
    });
    ForEach(outer_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.comp_id[v] = frag.GetOuterVertexGid(v);
    });

    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      auto cid = ctx.comp_id[v];
      auto es = frag.GetOutgoingAdjList(v, 0);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        if (ctx.comp_id[u] > cid) {
          grape::atomic_min(ctx.comp_id[u], cid);
          ctx.next_modified.Insert(u);
        }
      }

      if (frag.directed()) {
        es = frag.GetIncomingAdjList(v, 0);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.comp_id[u] > cid) {
            grape::atomic_min(ctx.comp_id[u], cid);
            ctx.next_modified.Insert(u);
          }
        }
      }
    });

    ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      if (ctx.next_modified.Exist(v)) {
        messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                           ctx.comp_id[v], tid);
      }
    });

    if (!ctx.next_modified.PartialEmpty(0, frag.GetInnerVerticesNum(0))) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    ctx.next_modified.ParallelClear(GetThreadPool());
    // aggregate messages
    messages.ParallelProcess<fragment_t, vid_t>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, vid_t msg) {
          if (ctx.comp_id[u] > msg) {
            grape::atomic_min(ctx.comp_id[u], msg);
            ctx.curr_modified.Insert(u);
          }
        });

    PropagateLabelPush(frag, ctx, messages);

    if (!ctx.next_modified.PartialEmpty(0, frag.GetInnerVerticesNum(0))) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);
  }
};

}  // namespace benchmarks

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_BENCHMARKS_APPS_WCC_PROPERTY_WCC_H_
