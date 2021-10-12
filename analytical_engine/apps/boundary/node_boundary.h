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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_H_

#include <set>
#include <vector>

#include "folly/json.h"
#include "grape/grape.h"

#include "apps/boundary/node_boundary_context.h"
#include "apps/boundary/utils.h"
#include "core/app/app_base.h"

namespace gs {
/**
 * @brief Compute the node boundary for given vertices.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class NodeBoundary : public AppBase<FRAG_T, NodeBoundaryContext<FRAG_T>>,
                     public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(NodeBoundary<FRAG_T>, NodeBoundaryContext<FRAG_T>,
                         FRAG_T)
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    // parse input node array from json
    folly::dynamic node_array_1 = folly::parseJson(ctx.nbunch1);
    std::vector<oid_t> oid_array_1;
    ExtractOidArrayFromDynamic(node_array_1, oid_array_1);
    std::set<vid_t> node_gid_set, node_gid_set_2;
    vid_t gid;
    vertex_t v;
    for (const auto& oid : oid_array_1) {
      if (frag.Oid2Gid(oid, gid)) {
        node_gid_set.insert(gid);
      }
    }
    if (!ctx.nbunch2.empty()) {
      auto node_array_2 = folly::parseJson(ctx.nbunch2);
      std::vector<oid_t> oid_array_2;
      ExtractOidArrayFromDynamic(node_array_2, oid_array_2);
      for (const auto& oid : oid_array_2) {
        if (frag.Oid2Gid(oid, gid)) {
          node_gid_set_2.insert(gid);
        }
      }
    }

    // get the boundary
    for (auto& gid : node_gid_set) {
      if (frag.InnerVertexGid2Vertex(gid, v)) {
        for (auto e : frag.GetOutgoingAdjList(v)) {
          vid_t gid = frag.Vertex2Gid(e.get_neighbor());
          if (node_gid_set.find(gid) == node_gid_set.end() &&
              (node_gid_set_2.empty() ||
               node_gid_set_2.find(gid) != node_gid_set_2.end())) {
            ctx.boundary.insert(gid);
          }
        }
      }
    }

    // gather and process boundary on worker-0
    std::vector<std::set<vid_t>> all_boundary;
    AllGather(ctx.boundary, all_boundary);

    if (frag.fid() == 0) {
      for (size_t i = 1; i < all_boundary.size(); ++i) {
        for (auto& v : all_boundary[i]) {
          ctx.boundary.insert(v);
        }
      }
      writeToCtx(frag, ctx);
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // Yes, there's no any code in IncEval.
    // Refer:
    // https://networkx.org/documentation/stable/_modules/networkx/algorithms/boundary.html#node_boundary
  }

 private:
  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    std::vector<typename fragment_t::oid_t> data;
    for (auto& v : ctx.boundary) {
      data.push_back(frag.Gid2Oid(v));
    }
    std::vector<size_t> shape{data.size()};
    ctx.assign(data, shape);
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_H_
