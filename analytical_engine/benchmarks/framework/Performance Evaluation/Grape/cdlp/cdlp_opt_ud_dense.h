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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_OPT_UD_DENSE_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_OPT_UD_DENSE_H_

#include <grape/grape.h>

#include "cdlp/cdlp_opt_context.h"
#include "cdlp/cdlp_utils.h"

namespace grape {

/**
 * @brief An implementation of CDLP(Community detection using label
 * propagation), the version in LDBC, which only works on the undirected graph.
 *
 * This version of CDLP inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T, typename LABEL_T>
class CDLPOptUDDense
    : public ParallelAppBase<FRAG_T, CDLPOptContext<FRAG_T, LABEL_T>,
                             ParallelMessageManagerOpt>,
      public ParallelEngine {
 public:
  using fragment_t = FRAG_T;
  using label_t = LABEL_T;
  using context_t = CDLPOptContext<fragment_t, label_t>;
  using message_manager_t = ParallelMessageManagerOpt;
  using worker_t = ParallelWorkerOpt<CDLPOptUDDense<fragment_t, label_t>>;
  using vid_t = typename fragment_t ::vid_t;
  using vertex_t = typename fragment_t::vertex_t;

  virtual ~CDLPOptUDDense() {}

  static std::shared_ptr<worker_t> CreateWorker(
      std::shared_ptr<CDLPOptUDDense<FRAG_T, LABEL_T>> app,
      std::shared_ptr<FRAG_T> frag) {
    return std::shared_ptr<worker_t>(new worker_t(app, frag));
  }

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;
  static constexpr bool need_split_edges = true;

 private:
  void PropagateLabel(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto& new_ilabels = ctx.new_ilabels;

    // touch neighbor and send messages in parallel
    ForEach(inner_vertices, [&frag, &ctx, &new_ilabels, &messages](int tid,
                                                                   vertex_t v) {
      auto es = frag.GetOutgoingAdjList(v);
      if (!es.Empty()) {
        label_t new_label = update_label_fast_dense<label_t>(es, ctx.labels);
        if (ctx.labels[v] != new_label) {
          new_ilabels[v] = new_label;
          ctx.changed.Insert(v);
          messages.SendMsgThroughOEdges<fragment_t, label_t>(frag, v, new_label,
                                                             tid);
        }
      }
    });

    ForEach(ctx.changed, [&ctx, &new_ilabels](int tid, vertex_t v) {
      ctx.labels[v] = new_ilabels[v];
    });
  }

  void PropagateLabelSparse(const fragment_t& frag, context_t& ctx,
                            message_manager_t& messages) {
    auto& new_ilabels = ctx.new_ilabels;

    ForEach(ctx.changed, [&frag, &ctx](int tid, vertex_t v) {
      auto es = frag.GetOutgoingInnerVertexAdjList(v);
      for (auto& e : es) {
        ctx.potential_change.Insert(e.get_neighbor());
      }
    });
    ctx.changed.ParallelClear(GetThreadPool());

    // touch neighbor and send messages in parallel
    ForEach(ctx.potential_change, [&frag, &ctx, &new_ilabels, &messages](
                                      int tid, vertex_t v) {
      auto es = frag.GetOutgoingAdjList(v);
      if (!es.Empty()) {
        label_t new_label = update_label_fast_dense<label_t>(es, ctx.labels);
        if (ctx.labels[v] != new_label) {
          new_ilabels[v] = new_label;
          ctx.changed.Insert(v);
          messages.SendMsgThroughOEdges<fragment_t, label_t>(frag, v, new_label,
                                                             tid);
        }
      }
    });
    ctx.potential_change.ParallelClear(GetThreadPool());

    ForEach(ctx.changed, [&ctx, &new_ilabels](int tid, vertex_t v) {
      ctx.labels[v] = new_ilabels[v];
    });
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num(),
                          8192 * (sizeof(vertex_t) + sizeof(label_t)),
                          8192 * (sizeof(vertex_t) + sizeof(label_t)));
    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    } else {
      messages.ForceContinue();
    }

    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.new_ilabels[v] = frag.GetInnerVertexId(v);
    });
    ForEach(outer_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.new_ilabels[v] = frag.GetOuterVertexId(v);
    });

    auto& channels = messages.Channels();

    ForEach(inner_vertices, [&frag, &ctx, &channels](int tid, vertex_t v) {
      auto es = frag.GetOutgoingAdjList(v);
      if (!es.Empty()) {
        label_t new_label = std::numeric_limits<label_t>::max();
        for (auto& e : es) {
          new_label =
              std::min<label_t>(new_label, ctx.new_ilabels[e.get_neighbor()]);
        }
        ctx.labels[v] = new_label;
        channels[tid].SendMsgThroughOEdges<fragment_t, label_t>(frag, v,
                                                                new_label);
      } else {
        ctx.labels[v] = ctx.new_ilabels[v];
      }
    });
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    ++ctx.step;

    if (ctx.step == 2) {
      messages.ParallelProcess<fragment_t, label_t>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, const label_t& msg) {
            ctx.labels[u] = msg;
          });

      if (ctx.step > ctx.max_round) {
        return;
      } else {
        messages.ForceContinue();
      }

      PropagateLabel(frag, ctx, messages);
    } else {
      // receive messages and set labels
      double rate =
          static_cast<double>(ctx.changed.ParallelCount(GetThreadPool())) /
          static_cast<double>(frag.GetInnerVerticesNum());

      if (rate > ctx.threshold) {
        messages.ParallelProcess<fragment_t, label_t>(
            thread_num(), frag,
            [&ctx](int tid, vertex_t u, const label_t& msg) {
              ctx.labels[u] = msg;
            });
        ctx.changed.ParallelClear(GetThreadPool());

        if (ctx.step > ctx.max_round) {
          return;
        } else {
          messages.ForceContinue();
        }

        PropagateLabel(frag, ctx, messages);
      } else {
        if (ctx.step > ctx.max_round) {
          messages.ParallelProcess<fragment_t, label_t>(
              thread_num(), frag,
              [&ctx](int tid, vertex_t u, const label_t& msg) {
                ctx.labels[u] = msg;
              });
          return;
        } else {
          messages.ParallelProcess<fragment_t, label_t>(
              thread_num(), frag,
              [&ctx, &frag](int tid, vertex_t u, const label_t& msg) {
                ctx.labels[u] = msg;
                auto ie = frag.GetIncomingAdjList(u);
                for (auto& e : ie) {
                  ctx.potential_change.Insert(e.neighbor);
                }
              });
          messages.ForceContinue();
        }

        PropagateLabelSparse(frag, ctx, messages);
      }
    }
  }

  void EstimateMessageSize(const fragment_t& frag, size_t& send_size,
                           size_t& recv_size) {
    send_size = frag.OEDestsSize() * (sizeof(vertex_t) + sizeof(label_t));
    recv_size = frag.GetOuterVerticesNum();
    recv_size *= (sizeof(vertex_t) + sizeof(label_t));
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_OPT_UD_DENSE_H_
