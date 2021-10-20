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

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_

#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/assortativity/average_degree_connectivity_context.h"
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
    : public AppBase<FRAG_T, AverageDegreeConnectivityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(AverageDegreeConnectivity<FRAG_T>,
                         AverageDegreeConnectivityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;
  using edata_t = typename fragment_t::edata_t;
  using pair_msg_t = typename std::pair<int, double>;
  using degree_connectivity_t =
      typename std::unordered_map<int, std::pair<double, double>>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    // process single node
    if (frag.GetTotalVerticesNum() == 1) {
      std::vector<size_t> shape{1, 2};
      std::vector<double> data = {0.0, 0.0};
      ctx.assign(data, shape);
      messages.ForceTerminate("single node");
    }
    auto inner_vertices = frag.InnerVertices();
    for (auto& v : inner_vertices) {
      processVertex(v, frag, ctx, messages);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      pair_msg_t msg;
      vertex_t vertex;
      while (messages.GetMessage<fragment_t, pair_msg_t>(frag, vertex, msg)) {
        int source_degree = msg.first;
        double weight = msg.second;
        int target_degree = getDegreeByType(
            frag, vertex, ctx.target_degree_type_, ctx.directed);
        if (ctx.degree_connectivity_map.count(source_degree) == 0) {
          ctx.degree_connectivity_map[source_degree].first =
              static_cast<double>(weight * target_degree);
        } else {
          ctx.degree_connectivity_map[source_degree].first +=
              static_cast<double>(weight * target_degree);
        }
      }
      ctx.merge_stage = true;
      if (frag.fid() != 0) {
        messages.SendToFragment<degree_connectivity_t>(
            0, ctx.degree_connectivity_map);
      }
      messages.ForceContinue();
    } else {
      // merge in worker 0
      if (frag.fid() == 0) {
        std::unordered_map<int, std::pair<double, double>> msg;
        while (messages.GetMessage(msg)) {
          for (auto& a : msg) {
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
        }
        ctx.assign(data, shape);
      }
    }
  }

 private:
  /**
   * @brief process vertex v ang traverse its neighbors
   *
   * @param v
   * @param frag
   * @param ctx
   * @param messages
   */
  void processVertex(vertex_t v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    int source_degree =
        getDegreeByType(frag, v, ctx.source_degree_type_, ctx.directed);
    // s_i
    double norm = getWeightedDegree(v, frag, ctx);
    if (ctx.degree_connectivity_map.count(source_degree) == 0) {
      ctx.degree_connectivity_map[source_degree].second = norm;
    } else {
      ctx.degree_connectivity_map[source_degree].second += norm;
    }
    // w_ij * k_j
    // process incoming neighbours
    if (ctx.directed && ctx.source_degree_type_ == DegreeType::IN) {
      auto oes = frag.GetIncomingAdjList(v);
      for (auto& e : oes) {
        edgeProcess(e, source_degree, frag, ctx, messages);
      }
    } else {  // process outgoing neighbours
      auto oes = frag.GetOutgoingAdjList(v);
      for (auto& e : oes) {
        edgeProcess(e, source_degree, frag, ctx, messages);
      }
    }
  }

  template <typename T>
  void edgeProcess(const T& e, int source_degree, const fragment_t& frag,
                   context_t& ctx, message_manager_t& messages) {
    vertex_t neighbor = e.get_neighbor();
    // edge weight
    double data = 1.0;
    static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
        [&](auto& e, auto& edata) {
          edata = static_cast<double>(e.get_data());
        })(e, data);
    if (frag.IsOuterVertex(neighbor)) {
      messages.SyncStateOnOuterVertex<fragment_t, pair_msg_t>(
          frag, neighbor, std::make_pair(source_degree, data));
    } else {
      int target_degree = getDegreeByType(
          frag, neighbor, ctx.target_degree_type_, ctx.directed);
      if (ctx.degree_connectivity_map.count(source_degree) == 0) {
        ctx.degree_connectivity_map[source_degree].first =
            static_cast<double>(data * target_degree);
      } else {
        ctx.degree_connectivity_map[source_degree].first +=
            static_cast<double>(data * target_degree);
      }
    }
  }

  double getWeightedDegree(vertex_t v, const fragment_t& frag, context_t& ctx) {
    double res = 0.0;
    if (ctx.weighted) {
      if (!ctx.directed || ctx.source_degree_type_ == DegreeType::OUT) {
        auto oes = frag.GetOutgoingAdjList(v);
        // compute the sum of weight
        res = computeWeightedDegree(oes);
      } else if (ctx.source_degree_type_ == DegreeType::IN) {
        auto oes = frag.GetIncomingAdjList(v);
        res = computeWeightedDegree(oes);
      } else {
        auto oes = frag.GetIncomingAdjList(v);
        res = computeWeightedDegree(oes);
        auto oes1 = frag.GetOutgoingAdjList(v);
        res += computeWeightedDegree(oes1);
      }
    } else {
      res = static_cast<double>(
          getDegreeByType(frag, v, ctx.source_degree_type_, ctx.directed));
    }
    return res;
  }

  /**
   * @brief compute the weighted degree by the adjacent list.
   *
   * @param adjList
   */
  template <typename T>
  double computeWeightedDegree(T adjList) {
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
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_
