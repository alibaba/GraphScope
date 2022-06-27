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

#ifndef ANALYTICAL_ENGINE_APPS_SSSP_SSSP_PATH_H_
#define ANALYTICAL_ENGINE_APPS_SSSP_SSSP_PATH_H_

#include <limits>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"
#include "core/worker/default_worker.h"
#include "sssp/sssp_path_context.h"

namespace gs {

/**
 * @brief Compute shortest paths in the graph.
 * Return a tree, where the path to each vertices in the given graph is the
 * shortest. In result file, each line follows as [predecessor node, node,
 * sssp_length of node]
 * @param source: Starting node for path
 * @param weight: whether weight is edge attribute in efile.
 *  If @param weight is false, every edge has 1 weight. Otherwise, take edge
 * attribute as weight.
 * */
template <typename FRAG_T>
class SSSPPath : public AppBase<FRAG_T, SSSPPathContext<FRAG_T>>,
                 public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(SSSPPath<FRAG_T>, SSSPPathContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using edata_t = typename fragment_t::edata_t;
  using pair_msg_t = typename std::pair<vid_t, double>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    if (native_source) {
      ctx.path_distance[source] = 0.0;
      ctx.predecessor[source] = source;
      vertexProcess(source, frag, ctx, messages);
    }

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.post_time -= GetCurrentTime();
#endif

    messages.ForceContinue();

#ifdef PROFILING
    ctx.post_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
#ifdef PROFILING
    auto t1 = GetCurrentTime(), t2 = GetCurrentTime();
#endif
    auto inner_vertices = frag.InnerVertices();

    vertex_t v, u;
    pair_msg_t msg;
    while (messages.GetMessage<fragment_t, pair_msg_t>(frag, u, msg)) {
      frag.Gid2Vertex(msg.first, v);
      double new_distu = msg.second;
      if (ctx.path_distance[u] > new_distu) {
        ctx.path_distance[u] = new_distu;
        ctx.predecessor[u] = v;
        ctx.curr_updated.Insert(u);
      }
    }

    ctx.prev_updated.Swap(ctx.curr_updated);
    ctx.curr_updated.Clear();

    for (auto v : inner_vertices) {
      if (ctx.prev_updated.Exist(v)) {
        vertexProcess(v, frag, ctx, messages);
      }
    }

    if (!ctx.curr_updated.Empty()) {
      messages.ForceContinue();
    }

    {
      vertex_t source;
      bool native_source = frag.GetInnerVertex(ctx.source_id, source);
      size_t row_num = 0;

      for (auto v : inner_vertices) {
        if (!(native_source && v == source) &&
            ctx.path_distance[v] != std::numeric_limits<double>::max()) {
          row_num++;
        }
      }
      std::vector<oid_t> data;
      std::vector<size_t> shape{row_num, 2};
      for (auto v : inner_vertices) {
        if (!(native_source && v == source) &&
            ctx.path_distance[v] != std::numeric_limits<double>::max()) {
          data.push_back(frag.GetId(ctx.predecessor[v]));
          data.push_back(frag.GetId(v));
        }
      }
      ctx.assign(data, shape);
    }
#ifdef PROFILING
    t2 = GetCurrentTime();
    ctx.exec_time += t2 - t1;
#endif
  }

 private:
  void vertexProcess(vertex_t v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    auto oes = frag.GetOutgoingAdjList(v);
    vid_t v_vid = frag.Vertex2Gid(v);
    for (auto& e : oes) {
      auto u = e.get_neighbor();
      double new_distu;
      double edata = 1.0;
      vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
          [&](auto& e, auto& data) {
            data = static_cast<double>(e.get_data());
          })(e, edata);
      new_distu = ctx.path_distance[v] + edata;
      if (frag.IsOuterVertex(u)) {
        messages.SyncStateOnOuterVertex<fragment_t, pair_msg_t>(
            frag, u, std::make_pair(v_vid, new_distu));
      } else {
        if (ctx.path_distance[u] > new_distu) {
          ctx.path_distance[u] = new_distu;
          ctx.predecessor[u] = v;
          ctx.curr_updated.Insert(u);
        }
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SSSP_SSSP_PATH_H_
