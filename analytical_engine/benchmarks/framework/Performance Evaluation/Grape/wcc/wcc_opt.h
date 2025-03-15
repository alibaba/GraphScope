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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_OPT_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_OPT_H_

#include <grape/grape.h>

#include "wcc/wcc_opt_context.h"

namespace grape {

#define MIN_COMP_ID(a, b) ((a) < (b) ? (a) : (b))

/**
 * @brief WCC application, determines the weakly connected component each vertex
 * belongs to, which only works on both undirected graph.
 *
 * This version of WCC inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCOpt : public ParallelAppBase<FRAG_T, WCCOptContext<FRAG_T>,
                                      ParallelMessageManagerOpt>,
               public ParallelEngine {
  INSTALL_PARALLEL_OPT_WORKER(WCCOpt<FRAG_T>, WCCOptContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;

  static constexpr bool need_split_edges = true;

  void PropagateLabelPull(const fragment_t& frag, context_t& ctx,
                          message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    auto& channels = messages.Channels();

    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      auto parent = ctx.tree[v];
      auto old_cid = ctx.comp_id[parent];
      auto new_cid = old_cid;
      auto es = frag.GetOutgoingInnerVertexAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        new_cid = MIN_COMP_ID(ctx.comp_id[ctx.tree[u]], new_cid);
      }
      if (new_cid < old_cid) {
        atomic_min(ctx.comp_id[parent], new_cid);
        ctx.next_modified.Insert(parent);
      }
    });

    ForEach(outer_vertices, [&frag, &ctx, &channels](int tid, vertex_t v) {
      auto parent = ctx.tree[v];
      auto old_cid = ctx.comp_id[parent];
      auto new_cid = old_cid;
      auto es = frag.GetIncomingAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        new_cid = MIN_COMP_ID(ctx.comp_id[ctx.tree[u]], new_cid);
      }
      if (new_cid < old_cid) {
        atomic_min(ctx.comp_id[parent], new_cid);
        ctx.next_modified.Insert(parent);
      }
      if (new_cid < ctx.comp_id[v]) {
        channels[tid].SyncStateOnOuterVertex<fragment_t, oid_t>(frag, v,
                                                                new_cid);
        ctx.comp_id[v] = new_cid;
      }
    });
  }

  void PropagateLabelPush(const fragment_t& frag, context_t& ctx,
                          message_manager_t& messages) {
    if (ctx.curr_modified.Empty()) {
      return;
    }
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    // propagate label to incoming and outgoing neighbors
    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      auto parent = ctx.tree[v];
      if (!ctx.curr_modified.Exist(parent)) {
        return;
      }
      auto cid = ctx.comp_id[parent];
      auto es = frag.GetOutgoingAdjList(v);
      for (auto& e : es) {
        auto u_p = ctx.tree[e.get_neighbor()];
        if (ctx.comp_id[u_p] > cid) {
          atomic_min(ctx.comp_id[u_p], cid);
          ctx.next_modified.Insert(u_p);
        }
      }
    });

    ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      auto parent = ctx.tree[v];
      auto new_cid = ctx.comp_id[parent];
      if (new_cid < ctx.comp_id[v]) {
        messages.SyncStateOnOuterVertex<fragment_t, oid_t>(frag, v, new_cid,
                                                           tid);
        ctx.comp_id[v] = new_cid;
      }
    });
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num(), 98304, 98304);

    ctx.next_modified.ParallelClear(GetThreadPool());

    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      auto es = frag.GetOutgoingInnerVertexAdjList(v);
      vertex_t parent = v;
      for (auto& e : es) {
        parent = MIN_COMP_ID(parent, e.get_neighbor());
      }
      ctx.tree[v] = parent;
      ctx.comp_id[v] = std::numeric_limits<oid_t>::max();
    });
    ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t v) {
      auto cur = v;
      while (cur != ctx.tree[cur]) {
        cur = ctx.tree[cur];
      }
      ctx.tree[v] = cur;
      oid_t cid = frag.GetInnerVertexId(v);
      atomic_min(ctx.comp_id[cur], cid);
    });
    ForEach(outer_vertices, [&ctx, &frag](int tid, vertex_t v) {
      auto es = frag.GetIncomingAdjList(v);
      vertex_t parent = v;
      for (auto& e : es) {
        parent = MIN_COMP_ID(parent, ctx.tree[e.get_neighbor()]);
      }
      ctx.tree[v] = parent;

      oid_t cid = frag.GetOuterVertexId(v);
      atomic_min(ctx.comp_id[parent], cid);
      ctx.comp_id[v] = cid;
    });

    PropagateLabelPull(frag, ctx, messages);

    if (!ctx.next_modified.Empty()) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    ctx.next_modified.ParallelClear(GetThreadPool());

    // aggregate messages
    messages.ParallelProcess<fragment_t, oid_t>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, oid_t msg) {
          vertex_t root = ctx.tree[u];
          if (ctx.comp_id[root] > msg) {
            atomic_min(ctx.comp_id[root], msg);
            ctx.curr_modified.Insert(root);
          }
        });

    PropagateLabelPush(frag, ctx, messages);

    if (!ctx.next_modified.Empty()) {
      messages.ForceContinue();
    }

    ctx.curr_modified.Swap(ctx.next_modified);
  }

  void EstimateMessageSize(const fragment_t& frag, size_t& send_size,
                           size_t& recv_size) {
    LOG(INFO) << "EstimateMessageSize";
    send_size = frag.GetOuterVerticesNum();
    send_size *= (sizeof(vertex_t) + sizeof(oid_t));
    recv_size = frag.GetInnerVerticesNum();
    recv_size *= ((sizeof(vertex_t) + sizeof(oid_t)) * (frag.fnum() - 1));
  }
};

#undef MIN_COMP_ID

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_OPT_H_
