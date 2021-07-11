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

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_

#include <tuple>
#include <unordered_map>
#include <utility>

#include "grape/grape.h"

#include "apps/assortativity/average_degree_connectivity/average_degree_connectivity_context.h"
#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Compute the degree assortativity for graph.
 *  Assortativity measures the similarity of connections in the graph with
 * respect to the node degree.
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
  using pair_msg_t = typename std::pair<int, edata_t>;
  using degree_connectivity_t =
      typename std::unordered_map<int, std::pair<double, edata_t>>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    for (auto& v : inner_vertices) {
      vertexProcess(v, frag, ctx, messages);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      // std::tuple<vid_t, int, edata_t> msg;
      pair_msg_t msg;
      vertex_t vertex;
      while (messages.GetMessage<fragment_t, pair_msg_t>(frag, vertex, msg)) {
        int source_degree = msg.first;
        edata_t weight = msg.second;
        int target_degree = GetDegreeByType(
            frag, vertex, ctx.target_degree_type_, ctx.directed);
        VLOG(0) << "vid: " << frag.Vertex2Gid(vertex)
                << ", target_degree: " << target_degree << std::endl;
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
      // merge in work 0
      if (frag.fid() == 0) {
        std::unordered_map<int, std::pair<double, edata_t>> msg;
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
        for (auto& a : ctx.degree_connectivity_map) {
          VLOG(0) << "0: " << a.first << ": " << a.second.first << ": "
                  << a.second.second << std::endl;
          ctx.degree_connectivity_map[a.first].first /=
              static_cast<double>(a.second.second);
          VLOG(0) << "degree: " << a.first
                  << "  connectivity:" << a.second.first << std::endl;
        }
      }
    }
  }
  void vertexProcess(vertex_t v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    int source_degree, target_degree;
    source_degree =
        GetDegreeByType(frag, v, ctx.source_degree_type_, ctx.directed);
    VLOG(0) << "fid: " << frag.fid() << ", vid: " << frag.Vertex2Gid(v)
            << std::endl;
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t neighbor = e.get_neighbor();
      // edge weight
      edata_t data = e.get_data();
      if (ctx.degree_connectivity_map.count(source_degree) == 0) {
        ctx.degree_connectivity_map[source_degree].second = data;
      } else {
        ctx.degree_connectivity_map[source_degree].second += data;
      }
      if (frag.IsOuterVertex(neighbor)) {
        messages.SyncStateOnOuterVertex<fragment_t, pair_msg_t>(
            frag, neighbor, std::make_pair(source_degree, data));
      } else {
        target_degree = GetDegreeByType(frag, neighbor, ctx.target_degree_type_,
                                        ctx.directed);
        // VLOG(0) << source_degree << " : " << data << " : " << target_degree
        // << std::endl;
        if (ctx.degree_connectivity_map.count(source_degree) == 0) {
          ctx.degree_connectivity_map[source_degree].first =
              static_cast<double>(data * target_degree);
        } else {
          ctx.degree_connectivity_map[source_degree].first +=
              static_cast<double>(data * target_degree);
        }
      }
    }
  }
  int GetDegreeByType(const fragment_t& frag, const vertex_t& vertex,
                      DegreeType type, bool directed) {
    if (type == DegreeType::IN) {
      return frag.GetLocalInDegree(vertex);
    } else if (type == DegreeType::OUT) {
      return frag.GetLocalOutDegree(vertex);
    }
    return directed
               ? frag.GetLocalInDegree(vertex) + frag.GetLocalOutDegree(vertex)
               : frag.GetLocalInDegree(vertex);
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_AVERAGE_DEGREE_CONNECTIVITY_H_
