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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_

#include <utility>
#include <vector>

#include "simple_path/all_simple_paths_context.h"

#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {

template <typename FRAG_T>
class AllSimplePaths : public AppBase<FRAG_T, AllSimplePathsContext<FRAG_T>>,
                       public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(AllSimplePaths<FRAG_T>, AllSimplePathsContext<FRAG_T>,
                         FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vid_t p;
    // if source not exists.
    if (!frag.Oid2Gid(ctx.source_id, p))
      return;
    // if source in targets.
    if (std::find(ctx.targets.begin(), ctx.targets.end(), p) !=
        ctx.targets.end())
      return;
    if (ctx.cutoff < 1)
      return;
    ctx.depth = 0;
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);
    if (native_source) {
      vid_t gid = frag.Vertex2Gid(source);
      std::vector<vid_t> n_stack;
      n_stack.push_back(gid);
      vertexProcess(source, frag, ctx, messages, n_stack);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    std::vector<vid_t> msg;
    while (messages.GetMessage(msg)) {
      // VLOG(0)<<"frag id: "<<frag.fid()<<" msg back: "<<msg.back()<<std::endl;
      ctx.next_level_inner.push(msg);
    }
    ctx.curr_level_inner.swap(ctx.next_level_inner);
    // VLOG(0)<<"frag id: "<<frag.fid()<<" curr_level_inner size:
    // "<<ctx.curr_level_inner.size()<<std::endl;
    ctx.depth++;
    if (ctx.depth <= ctx.cutoff) {
      while (!ctx.curr_level_inner.empty()) {
        std::vector<vid_t> msg = ctx.curr_level_inner.front();
        ctx.curr_level_inner.pop();
        vertex_t v;
        vid_t gid = msg.back();
        frag.Gid2Vertex(gid, v);
        if (std::find(ctx.targets.begin(), ctx.targets.end(), gid) !=
            ctx.targets.end()) {
          ctx.result_queue.push(msg);
          // VLOG(0)<<"frag id: "<<frag.fid()<<" depth: "<<ctx.depth<<std::endl;
        }
        vertexProcess(v, frag, ctx, messages, msg);
      }
      messages.ForceContinue();
    }
  }

 private:
  void vertexProcess(vertex_t v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages, std::vector<vid_t> n_stack) {
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t u = e.get_neighbor();
      vid_t gid = frag.Vertex2Gid(u);
      if (std::find(n_stack.begin(), n_stack.end(), gid) == n_stack.end()) {
        n_stack.push_back(gid);
        // VLOG(0)<<"frag id: "<<frag.fid()<<" stack size:
        // "<<n_stack.size()<<std::endl;
        if (!frag.IsOuterVertex(u)) {
          ctx.next_level_inner.push(n_stack);
        } else {
          fid_t fid = frag.GetFragId(u);
          messages.SendToFragment(fid, n_stack);
        }
        n_stack.pop_back();
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_
