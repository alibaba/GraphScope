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
    dynamic::Value source_array;
    dynamic::Parse(ctx.nbunch1, source_array);
    std::set<vid_t> source_gid_set, target_gid_set;
    vid_t gid;
    vertex_t v;
    bool no_target_set = true;
    for (auto& node : source_array) {
      if (frag.Oid2Gid(dynamic_to_oid<oid_t>(node), gid)) {
        source_gid_set.insert(gid);
      }
    }
    if (!ctx.nbunch2.empty()) {
      dynamic::Value target_array;
      dynamic::Parse(ctx.nbunch2, target_array);
      for (auto& node : target_array.GetArray()) {
        if (frag.Oid2Gid(dynamic_to_oid<oid_t>(node), gid)) {
          target_gid_set.insert(gid);
        }
      }
      no_target_set = false;
    }

    // get the boundary
    for (auto& gid : source_gid_set) {
      if (frag.Gid2Vertex(gid, v) && frag.IsInnerVertex(v)) {
        for (auto& e : frag.GetOutgoingAdjList(v)) {
          vid_t v_gid = frag.Vertex2Gid(e.get_neighbor());
          if (source_gid_set.find(v_gid) == source_gid_set.end() &&
              (no_target_set ||
               target_gid_set.find(v_gid) != target_gid_set.end())) {
            ctx.boundary.insert(v_gid);
          }
        }
      }
    }

    writeToCtx(frag, ctx);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // Yes, there's no any code in IncEval.
    // Refer:
    // https://networkx.org/documentation/stable/_modules/networkx/algorithms/boundary.html#node_boundary
  }

 private:
  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    // reduce and process boundary on worker-0
    std::set<vid_t> all_boundary;
    AllReduce(ctx.boundary, all_boundary,
              [](std::set<vid_t>& out, const std::set<vid_t>& in) {
                for (auto& v : in) {
                  out.insert(v);
                }
              });

    if (frag.fid() == 0) {
      std::vector<typename fragment_t::oid_t> data;
      for (auto& v : all_boundary) {
        data.push_back(frag.Gid2Oid(v));
      }
      std::vector<size_t> shape{data.size()};
      ctx.assign(data, shape);
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_H_
