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

#include <set>
#include <vector>

#include "folly/json.h"
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
    std::set<vid_t> node_gid_set, node_gid_set_2;
    vid_t gid;
    vertex_t v;
    for (const auto& oid : node_array_1) {
      if (frag.Oid2Gid(oid, gid)) {
        node_gid_set.insert(gid);
      }
    }
    if (!ctx.nbunch2.empty()) {
      auto node_array_2 = folly::parseJson(ctx.nbunch2);
      for (const auto& oid : node_array_2) {
        if (frag.Oid2Gid(oid, gid)) {
          node_gid_set_2.insert(gid);
        }
      }
    }

    for (auto& gid : node_gid_set) {
      if (frag.InnerVertexGid2Vertex(gid, v)) {
        for (auto e : frag.GetOutgoingAdjList(v)) {
          vid_t gid = frag.Vertex2Gid(e.get_neighbor());
          if (node_gid_set.find(gid) == node_gid_set.end() &&
              (node_gid_set_2.empty() ||
               node_gid_set_2.find(gid) != node_gid_set_2.end())) {
            ctx.boundry.insert(gid);
          }
        }
      }
    }

    std::vector<std::set<vid_t>> all_boundry;
    AllGather(ctx.boundry, all_boundry);

    if (frag.fid() == 0) {
      for (size_t i = 1; i < all_boundry.size(); ++i) {
        for (auto& v : all_boundry[i]) {
          ctx.boundry.insert(v);
        }
      }
      for (auto& v : ctx.boundry) {
        LOG(INFO) << frag.Gid2Oid(v) << "\n";
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
