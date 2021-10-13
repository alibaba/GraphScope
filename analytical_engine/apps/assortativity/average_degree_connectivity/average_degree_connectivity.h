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

Author: Ning Xin
*/

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_

#include <mutex>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/assortativity/average_degree_connectivity/average_degree_connectivity_context.h"
#include "core/app/app_base.h"
#include "core/utils/app_utils.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Compute the average degree connectivity for graph.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class AverageDegreeConnectivity
    : public grape::ParallelAppBase<FRAG_T,
                                    AverageDegreeConnectivityContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(AverageDegreeConnectivity<FRAG_T>,
                          AverageDegreeConnectivityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;
  using edata_t = typename fragment_t::edata_t;
  // <vertex v, v's degree, edge's weight>
  using msg_t = typename std::tuple<vertex_t, int, double>;
  using degree_connectivity_t =
      typename std::unordered_map<int, std::pair<double, double>>;
  using vertex_array_t =
      typename FRAG_T::template vertex_array_t<std::tuple<int, double, double>>;
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    // process single node
    if (frag.GetTotalVerticesNum() == 1) {
      std::vector<size_t> shape{1, 2};
      std::vector<double> data = {0.0, 0.0};
      ctx.assign(data, shape);
      messages.ForceTerminate("single node");
    }
    // messages.InitChannels(1);
    messages.InitChannels(thread_num());
    // auto& channels = messages.Channels();
    auto inner_vertices = frag.InnerVertices();

    ForEach(inner_vertices, [&frag, &ctx, this](int tid, vertex_t v) {
      int source_degree =
          this->getDegreeByType(frag, v, ctx.source_degree_type_, ctx.directed);
      // s_i
      // computing the weighted degree of vertex v
      double weight_degree = this->getWeightedDegree(v, frag, ctx);
      auto& tuple = ctx.vertex_array[v];
      std::get<0>(tuple) = source_degree;
      std::get<2>(tuple) = weight_degree;
    });

    ForEach(
        inner_vertices, [&frag, &ctx, &messages, this](int tid, vertex_t v) {
          // w_{ij} * k_j
          // process incoming neighbours
          double dsum = 0.0;
          if (ctx.directed && ctx.source_degree_type_ == DegreeType::IN) {
            auto oes = frag.GetIncomingAdjList(v);
            dsum = this->computeEdgeDegreeSum(v, oes, frag, ctx, messages, tid);
          } else {  // process outgoing neighbours
            auto oes = frag.GetOutgoingAdjList(v);
            dsum = this->computeEdgeDegreeSum(v, oes, frag, ctx, messages, tid);
          }
          auto& tuple = ctx.vertex_array[v];
          std::get<1>(tuple) = dsum;
        });
    messages.ForceContinue();
  }

  // void UpdateTuple(std::tuple<int, double, double>& tuple, int source_degree,
  //                  double sum) {
  //   // std::lock_guard<std::mutex> lock(m);
  //   // update
  //   std::get<0>(tuple) = source_degree;
  //   std::get<1>(tuple) += sum;
  // }
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      msg_t msg;
      vertex_t vertex;
      messages.ParallelProcess<fragment_t, msg_t>(
          thread_num(), frag,
          [&frag, &ctx, this](int tid, vertex_t v, msg_t msg) {
            std::lock_guard<std::mutex> lock(m);
            // VLOG(0) << "update thread: " << tid << " start" << std::endl;
            vertex_t source = std::get<0>(msg);
            int source_degree = std::get<1>(msg);
            double weight = std::get<2>(msg);
            int target_degree = this->getDegreeByType(
                frag, v, ctx.target_degree_type_, ctx.directed);
            auto& tuple = ctx.vertex_array[source];
            std::get<0>(tuple) = source_degree;
            std::get<1>(tuple) += weight * target_degree;
            // VLOG(0) << "update thread: " << tid << " end" << std::endl;
            // this->UpdateTuple(ctx.vertex_array[source], source_degree,
            //                   weight * target_degree);
          });
      // partial aggregation
      auto vertices = frag.Vertices();
      for (auto& v : vertices) {
        auto tuple = ctx.vertex_array[v];
        auto degree = std::get<0>(tuple);
        auto dsum = std::get<1>(tuple);
        auto dnorm = std::get<2>(tuple);
        // merge
        if (ctx.degree_connectivity_map.count(degree) != 0) {
          ctx.degree_connectivity_map[degree].first += dsum;
          ctx.degree_connectivity_map[degree].second += dnorm;
        } else {
          ctx.degree_connectivity_map[degree].first = dsum;
          ctx.degree_connectivity_map[degree].second = dnorm;
        }
      }
      ctx.merge_stage = true;
      if (frag.fid() != 0) {
        grape::InArchive arc;
        arc << ctx.degree_connectivity_map;
        messages.SendRawMsgByFid(0, std::move(arc));
      }
      messages.ForceContinue();
    } else {
      // merge in worker 0
      if (frag.fid() == 0) {
        messages.ParallelProcess<
            std::unordered_map<int, std::pair<double, double>>>(
            thread_num(),
            [&ctx, this](
                int tid,
                std::unordered_map<int, std::pair<double, double>>& msg) {
              this->MergeMsg(msg, ctx, tid);
            });
        // write to ctx
        size_t row_num = ctx.degree_connectivity_map.size();
        std::vector<size_t> shape{row_num, 2};
        std::vector<double> data;
        for (auto& a : ctx.degree_connectivity_map) {
          double result = a.second.second == 0.0
                              ? a.second.first
                              : a.second.first / a.second.second;
          ctx.degree_connectivity_map[a.first].first = result;
          // degree
          data.push_back(static_cast<double>(a.first));
          // degree connectivity
          data.push_back(result);
          VLOG(0) << a.second.first << ": " << a.second.second << std::endl;
        }
        ctx.assign(data, shape);
      }
    }
  }

  void MergeMsg(std::unordered_map<int, std::pair<double, double>>& msg,
                context_t& ctx, int tid) {
    std::lock_guard<std::mutex> lock(m);
    VLOG(0) << "thread: " << tid << " start" << std::endl;
    for (auto& a : msg) {
      {
        // merge
        if (ctx.degree_connectivity_map.count(a.first) != 0) {
          ctx.degree_connectivity_map[a.first].first += a.second.first;
          ctx.degree_connectivity_map[a.first].second += a.second.second;
        } else {
          ctx.degree_connectivity_map[a.first].first = a.second.first;
          ctx.degree_connectivity_map[a.first].second = a.second.second;
        }
      }
    }
    VLOG(0) << "thread: " << tid << " end" << std::endl;
  }

 private:
  std::mutex m;

  template <typename T>
  double computeEdgeDegreeSum(vertex_t source, T adjList,
                              const fragment_t& frag, context_t& ctx,
                              message_manager_t& messages, int tid) {
    int source_degree = this->getDegreeByType(
        frag, source, ctx.source_degree_type_, ctx.directed);
    double res = 0.0;
    for (auto& e : adjList) {
      vertex_t neighbor = e.get_neighbor();
      // edge weight
      double data = 1.0;
      static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
          [&](auto& e, auto& edata) {
            edata = static_cast<double>(e.get_data());
          })(e, data);
      if (frag.IsOuterVertex(neighbor)) {
        // 单进程结果正确？？
        messages.SyncStateOnOuterVertex<fragment_t, msg_t>(
            frag, neighbor, std::make_tuple(source, source_degree, data), tid);
      } else {
        int target_degree = this->getDegreeByType(
            frag, neighbor, ctx.target_degree_type_, ctx.directed);
        res += data * target_degree;
      }
    }
    return res;
  }
  /**
   * get the weighted degree of vertex v
   */
  double getWeightedDegree(vertex_t v, const fragment_t& frag, context_t& ctx) {
    double res = 0.0;
    if (ctx.weighted) {
      // undirected graph or source degree type is OUT
      if (!ctx.directed || ctx.source_degree_type_ == DegreeType::OUT) {
        auto oes = frag.GetOutgoingAdjList(v);
        // compute the sum of weight
        res = computeEdgeSum(oes);
      } else if (ctx.source_degree_type_ == DegreeType::IN) {
        auto oes = frag.GetIncomingAdjList(v);
        res = computeEdgeSum(oes);
      } else {  // source degree type is INANDOUT, processing the incoming edges
                // and the outgoing edges
        auto oes = frag.GetIncomingAdjList(v);
        res = computeEdgeSum(oes);
        auto oes1 = frag.GetOutgoingAdjList(v);
        res += computeEdgeSum(oes1);
      }
    } else {
      res = static_cast<double>(
          getDegreeByType(frag, v, ctx.source_degree_type_, ctx.directed));
    }
    return res;
  }

  /**
   * @brief compute the sum of adjList's weight.
   *
   * @param adjList
   */
  template <typename T>
  double computeEdgeSum(T adjList) {
    double res = 0.0;
    for (auto& e : adjList) {
      double data = 0.0;
      static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
          [&](auto& e, auto& edata) {
            edata = static_cast<double>(e.get_data());
          })(e, data);
      res += data;
    }
    return res;
  }
  int getDegreeByType(const fragment_t& frag, const vertex_t& vertex,
                      DegreeType type, bool directed) {
    if (!directed) {
      return frag.GetLocalOutDegree(vertex);
    }
    if (type == DegreeType::IN) {
      return frag.GetLocalInDegree(vertex);
    } else if (type == DegreeType::OUT) {
      return frag.GetLocalOutDegree(vertex);
    }
    return frag.GetLocalInDegree(vertex) + frag.GetLocalOutDegree(vertex);
  }
};  // namespace gs
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_
