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

#ifndef EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_H_
#define EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_H_

#include "kclique/kclique_context.h"
#include "kclique/kclique_utils.h"

#define PEVAL_USE_RECURSIVE
// #define INCEVAL_USE_RECURSIVE

namespace grape {

/**
 * @brief An implementation of k-clique, which can work on undirected graph.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class KClique : public ParallelAppBase<FRAG_T, KCliqueContext<FRAG_T>,
                                       ParallelMessageManagerOpt>,
                public ParallelEngine {
  INSTALL_PARALLEL_OPT_WORKER(KClique<FRAG_T>, KCliqueContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  using msg_t = KCliqueMsg<vid_t>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    ctx.clique_num = 0;
    std::atomic<size_t> clique_num(0);
    if (frag.fnum() == 1) {
      auto inner_vertices = frag.InnerVertices();

#ifdef PEVAL_USE_RECURSIVE
      std::vector<std::vector<uint8_t>> tables(thread_num());
      for (auto& buf : tables) {
        buf.resize(frag.GetInnerVerticesNum(), 0);
      }

      std::vector<std::vector<std::vector<vertex_t>>> levels(thread_num());
      for (auto& level : levels) {
        level.resize(ctx.k);
      }

      ForEach(inner_vertices, [&frag, &clique_num, &tables, &levels, &ctx](
                                  int tid, vertex_t v) {
        clique_num += KCliqueUtils<FRAG_T>::UniFragCliqueNumRecursive(
            frag, v, tables[tid], ctx.k, levels[tid]);
      });
#else
      ForEach(inner_vertices, [&frag, &clique_num, &ctx](int tid, vertex_t v) {
        clique_num +=
            KCliqueUtils<FRAG_T>::UniFragCliqueNumIterative(frag, v, ctx.k);
      });
#endif
    } else {
      messages.InitChannels(thread_num());
      auto& channels = messages.Channels();
      GidComparer<vid_t> cmp(frag.fnum());
#ifdef PEVAL_USE_RECURSIVE
      std::vector<std::vector<uint8_t>> tables(thread_num());
      for (auto& buf : tables) {
        buf.resize(frag.GetVerticesNum(), 0);
      }
      std::vector<std::vector<std::vector<vid_t>>> levels(thread_num());
      for (auto& level : levels) {
        level.resize(ctx.k);
      }

      ForEach(inner_vertices, [&frag, &clique_num, &tables, &channels, &ctx,
                               cmp, &levels](int tid, vertex_t v) {
        clique_num += KCliqueUtils<FRAG_T>::MultiFragCliqueNumRecursive(
            frag, v, tables[tid], ctx.k, levels[tid], channels[tid], cmp);
      });
#else
      ForEach(inner_vertices,
              [&frag, &clique_num, &channels, &ctx, cmp](int tid, vertex_t v) {
                clique_num += KCliqueUtils<FRAG_T>::MultiFragCliqueNumIterative(
                    frag, v, ctx.k, channels[tid], cmp);
              });
#endif
    }
    ctx.clique_num += clique_num.load();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    std::atomic<size_t> clique_num(0);
    auto& channels = messages.Channels();
    GidComparer<vid_t> cmp(frag.fnum());
#ifdef INCEVAL_USE_RECURSIVE
    std::vector<std::vector<uint8_t>> tables(thread_num());
    for (auto& buf : tables) {
      buf.resize(frag.GetVerticesNum(), 0);
    }
    std::vector<std::vector<std::vector<vid_t>>> levels(thread_num());
    for (auto& level : levels) {
      level.resize(ctx.k - 1);
    }
    messages.ParallelProcess<fragment_t, msg_t>(
        thread_num(), frag,
        [&frag, &ctx, &clique_num, &channels, &tables, &levels, cmp](
            int tid, vertex_t v, const msg_t& msg_in) {
          clique_num += KCliqueUtils<FRAG_T>::MultiFragCliqueNumRecursiveStep(
              frag, v, tables[tid], ctx.k, msg_in, levels[tid], channels[tid],
              cmp);
        });
#else
    messages.ParallelProcess<fragment_t, msg_t>(
        thread_num(), frag,
        [&frag, &ctx, &clique_num, &channels, cmp](int tid, vertex_t v,
                                                   const msg_t& msg_in) {
          clique_num += KCliqueUtils<FRAG_T>::MultiFragCliqueNumIterativeStep(
              frag, v, ctx.k, msg_in, channels[tid], cmp);
        });
#endif
    ctx.clique_num += clique_num.load();
  }
};

}  // namespace grape

#undef PEVAL_USE_RECURSIVE
// #undef INCEVAL_USE_RECURSIVE

#endif  // EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_H_
