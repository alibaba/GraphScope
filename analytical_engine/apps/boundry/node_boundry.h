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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDRY_NODE_BOUNDRY_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDRY_NODE_BOUNDRY_H_

#include "grape/grape.h"

#include "apps/boundry/node_boundry_context.h"
#include "core/app/app_base.h"

namespace gs {
/**
 * @brief Compute the node boundry for given vertices.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class NodeBoundry : public AppBase<FRAG_T, NodeBoundryContext<FRAG_T>>,
                    public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(NodeBoundry<FRAG_T>, NodeBoundryContext<FRAG_T>,
                         FRAG_T)
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    folly::dynamic node_array_1 = folly::parseJson(ctx.nbunch1);
    std::unordered_set<vid_t> node_gid_array;
    vid_t gid;
    for (const auto& oid : node_array_1) {
      if (frag.Oid2Gid(oid, gid)) {
        node_gid_array.insert(gid)
      }
    }

    for (auto& gid : node_gid_array) {
      vertex_t u;
      if (frag.InnerVertexGid2Vertex(gid, u)) {
        for (auto es : frag.GetOutgoingAdjList(u)) {
          vid_t gid;
          frag.Vertex2Gid(es.get_neighbor());
          if (node_gid_array.find(gid) == node_gid_array.end()) {
            ctx.boundary.insert(gid);
          }
        }
      }
    }

    std::vector<std::vector<vid_t>> all_boundary;
    AllGather(ctx.boundary, all_boundary);

    if (frag.fid() == 0) {
      for (sizt_t i = 1; i < all_boundary.size()++ i) {
        for (auto& v : all_boundary[i]) {
          if (node_gid_array.find(v) == node_gid_array.end()) {
            ctx.boundary.insert(v);
          }
        }
      }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // Yes, there's no any code in IncEval.
    // Refer:
    // https://networkx.org/documentation/stable/_modules/networkx/algorithms/boundary.html#node_boundary
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDRY_NODE_BOUNDRY_H_
