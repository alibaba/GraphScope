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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDRY_EDGE_BOUNDRY_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDRY_EDGE_BOUNDRY_H_

#include "grape/grape.h"

#include "apps/boundry/node_boundry_context.h"

namespace gs {
/**
 * @brief Compute the node boundry for given vertices.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class EdgeBoundry
    : public grape::ParallelAppBase<FRAG_T, EdgeBoundryContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(EdgeBoundry<FRAG_T>, EdgeBoundryContext<FRAG_T>,
                          FRAG_T)
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    folly::dynamic node_array_1 = folly::parseJson(ctx.nbunch1);
    for (auto& oid : node_array_1) {
      vertex_t u;
      if (frag.GetInnerVertex(oid, u)) {
        for (auto es : frag.GetOutgoingAdjList(u)) {
          vertex_t u = es.get_neighbor();
          if (node_array_1.find(frag.GetId(u)) == node_array_1.empty()) {
            ctx.boundary.insert(std::make_pair(
                frag.Vertex2Gid(u), frag.Vertex2Gid(es.get_neighbor())));
          }
        }
      }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // TODO: process boundry in worker-0
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDRY_EDGE_BOUNDRY_H_
