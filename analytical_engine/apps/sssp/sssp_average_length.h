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

#ifndef ANALYTICAL_ENGINE_APPS_SSSP_SSSP_AVERAGE_LENGTH_H_
#define ANALYTICAL_ENGINE_APPS_SSSP_SSSP_AVERAGE_LENGTH_H_

#include <map>
#include <queue>
#include <tuple>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"
#include "core/worker/default_worker.h"
#include "sssp/sssp_average_length_context.h"

namespace gs {

/**
 * @brief Compute the average shortest path length in a *connected* graph.
 * Average shortest path length is average of all sssp length of (source = v,
 * target = u), where v, u is any vertex in graph. Note that this algorithm is
 * time consuming.
 * */
template <typename FRAG_T>
class SSSPAverageLength
    : public AppBase<FRAG_T, SSSPAverageLengthContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(SSSPAverageLength<FRAG_T>,
                         SSSPAverageLengthContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using edata_t = typename fragment_t::edata_t;
  // vertex msg: [source, v, sssp_length]
  // OR sum msg: [fid, fid, sssp_length_sum]
  using tuple_t = typename std::tuple<vid_t, vid_t, double>;
  // [true, vertex msg]
  // [false, sum msg]
  using pair_msg_t = typename std::pair<bool, tuple_t>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    bool update_sum = false;
    for (auto v : inner_vertices) {
      ctx.updated.Clear();

      vid_t src_vid = frag.Vertex2Gid(v);
      updateVertexState(v, src_vid, 0.0, ctx);
      while (!ctx.next_queue.empty()) {
        std::priority_queue<std::pair<double, vertex_t>> curr_queue;
        curr_queue.swap(ctx.next_queue);
        while (!curr_queue.empty()) {
          auto v = curr_queue.top().second;
          curr_queue.pop();
          vertexProcess(v, src_vid, frag, ctx, messages);
        }
        update_sum = true;
      }
      syncUpdated(src_vid, frag, ctx, messages);
    }
    if (update_sum)
      syncSum(frag, ctx, messages);

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
    std::map<vid_t, grape::DenseVertexSet<typename FRAG_T::inner_vertices_t>>
        updated_map;
    bool update_sum = false;

    pair_msg_t msg;
    int msg_cnt = 0;
    while (messages.GetMessage<pair_msg_t>(msg)) {
      bool is_vertex_msg = msg.first;
      if (is_vertex_msg) {
        vertex_t v;
        tuple_t vertex_msg = msg.second;
        vid_t src_vid = std::get<0>(vertex_msg);
        vid_t v_vid = std::get<1>(vertex_msg);
        double distv = std::get<2>(vertex_msg);
        frag.Gid2Vertex(v_vid, v);

        if (updateVertex(v, src_vid, distv, ctx)) {
          if (updated_map.find(src_vid) == updated_map.end())
            updated_map[src_vid].Init(frag.InnerVertices());
          updated_map[src_vid].Insert(v);
        }
      } else {  // sum msg
        fid_t fid = (fid_t)(std::get<0>(msg.second));
        ctx.all_sums[fid] = std::get<2>(msg.second);
      }
      msg_cnt++;
    }

    for (auto& it : updated_map) {
      vid_t src_vid = it.first;
      ctx.updated.Clear();
      ctx.updated.Swap(it.second);
      for (auto v : inner_vertices)
        if (ctx.updated.Exist(v))
          ctx.next_queue.push(
              std::make_pair(-ctx.path_distance[v][src_vid], v));

      while (!ctx.next_queue.empty()) {
        std::priority_queue<std::pair<double, vertex_t>> curr_queue;
        curr_queue.swap(ctx.next_queue);
        while (!curr_queue.empty()) {
          auto v = curr_queue.top().second;
          curr_queue.pop();
          vertexProcess(v, src_vid, frag, ctx, messages);
        }
        update_sum = true;
      }
      syncUpdated(src_vid, frag, ctx, messages);
    }
    if (update_sum) {
      syncSum(frag, ctx, messages);
    } else {
      // Write to tensor
      if (frag.fid() == 0) {
        auto n = frag.GetTotalVerticesNum();
        double sum = 0.0;
        for (auto it : ctx.all_sums) {
          sum += it.second;
        }
        double average_length = sum / static_cast<double>(n * (n - 1));

        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        ctx.assign(average_length);
      }
    }
#ifdef PROFILING
    t2 = GetCurrentTime();
    ctx.exec_time += t2 - t1;
#endif
  }

 private:
  inline void syncSum(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages) {
    int fid = frag.fid();
    if (fid == 0)
      ctx.all_sums[frag.fid()] = ctx.inner_sum;
    else
      messages.SendToFragment(
          0, pair_msg_t(false,
                        tuple_t((vid_t)(fid), (vid_t)(fid), ctx.inner_sum)));
  }

  inline void syncUpdated(vid_t src_vid, const fragment_t& frag, context_t& ctx,
                          message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (ctx.updated.Exist(v)) {
        auto oes = frag.GetOutgoingAdjList(v);
        for (auto& e : oes) {
          auto u = e.get_neighbor();
          if (frag.IsOuterVertex(u)) {
            double v_u = 1.0;
            vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
                [&](auto& e, auto& data) {
                  data = static_cast<double>(e.get_data());
                })(e, v_u);
            double dist = ctx.path_distance[v][src_vid] + v_u;
            vid_t u_vid = frag.Vertex2Gid(u);
            messages.SendToFragment(
                frag.GetFragId(u),
                pair_msg_t(true, tuple_t(src_vid, u_vid, dist)));
          }
        }
      }
    }
  }

  // Return true if vertex u is activated.
  bool updateVertex(vertex_t v, vid_t src_vid, double new_dist,
                    context_t& ctx) {
    auto& distance_v = ctx.path_distance[v];
    bool first_visit = (distance_v.find(src_vid) == distance_v.end());
    if (first_visit || distance_v[src_vid] > new_dist) {
      // vertex u is visited for the first time
      if (first_visit)
        ctx.inner_sum += new_dist;
      else
        ctx.inner_sum = ctx.inner_sum + new_dist - distance_v[src_vid];
      ctx.path_distance[v][src_vid] = new_dist;
      return true;
    }
    return false;
  }

  void updateVertexState(vertex_t v, vid_t src_vid, double new_dist,
                         context_t& ctx) {
    if (updateVertex(v, src_vid, new_dist, ctx)) {
      ctx.next_queue.push(std::make_pair(-new_dist, v));
      ctx.updated.Insert(v);
    }
  }

  void vertexProcess(vertex_t v, vid_t src_vid, const fragment_t& frag,
                     context_t& ctx, message_manager_t& messages) {
    double dist_v = ctx.path_distance[v][src_vid];

    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      auto u = e.get_neighbor();
      if (frag.IsInnerVertex(u)) {
        double v_u = 1.0;
        vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
            [&](auto& e, auto& data) {
              data = static_cast<double>(e.get_data());
            })(e, v_u);
        updateVertexState(u, src_vid, dist_v + v_u, ctx);
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SSSP_SSSP_AVERAGE_LENGTH_H_
