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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_

#include "grape/grape.h"

#include "apps/centrality/degree/degree_centrality_context.h"
#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Compute the degree centrality for vertices. The degree centrality for
 * a vertex v is the fraction of vertices it is connected to.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class DegreeCentrality
    : public AppBase<FRAG_T, DegreeCentralityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(DegreeCentrality<FRAG_T>,
                         DegreeCentralityContext<FRAG_T>, FRAG_T)
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto max_degree = frag.GetTotalVerticesNum() - 1;
    auto& type = ctx.degree_centrality_type;
    auto& centrality = ctx.centrality;

    switch (type) {
    case DegreeCentralityType::IN: {
      for (auto& v : inner_vertices) {
        double degree = frag.GetLocalInDegree(v);
        centrality[v] = degree / max_degree;
      }
      break;
    }
    case DegreeCentralityType::OUT: {
      for (auto& v : inner_vertices) {
        double degree = frag.GetLocalOutDegree(v);
        centrality[v] = degree / max_degree;
      }
      break;
    }
    case DegreeCentralityType::BOTH: {
      for (auto& v : inner_vertices) {
        double degree = frag.GetLocalInDegree(v) + frag.GetLocalOutDegree(v);
        centrality[v] = degree / max_degree;
      }
      break;
    }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // Yes, there's no any code in IncEval.
    // Refer:
    // https://networkx.github.io/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.degree_centrality.html
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
