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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDARY_EDGE_BOUNDARY_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDARY_EDGE_BOUNDARY_H_

#include <set>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/boundary/edge_boundary_context.h"
#include "apps/boundary/utils.h"
#include "core/app/app_base.h"

namespace gs {
/**
 * @brief Compute the edge boundary for given vertices.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class EdgeBoundary : public AppBase<FRAG_T, EdgeBoundaryContext<FRAG_T>>,
                     public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(EdgeBoundary<FRAG_T>, EdgeBoundaryContext<FRAG_T>,
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
    vertex_t u;
    bool no_target_set = true;
    for (auto& node : source_array) {
      if (frag.Oid2Gid(dynamic_to_oid<oid_t>(node), gid)) {
        source_gid_set.insert(gid);
      }
    }
    if (!ctx.nbunch2.empty()) {
      dynamic::Value target_array;
      dynamic::Parse(ctx.nbunch2, target_array);
      for (auto& node : target_array) {
        if (frag.Oid2Gid(dynamic_to_oid<oid_t>(node), gid)) {
          target_gid_set.insert(gid);
        }
      }
      no_target_set = false;
    }

    // get the boundary
    for (auto gid : source_gid_set) {
      if (frag.Gid2Vertex(gid, u) && frag.IsInnerVertex(u)) {
        for (auto& e : frag.GetOutgoingAdjList(u)) {
          vid_t v_gid = frag.Vertex2Gid(e.get_neighbor());
          if (no_target_set) {
            if (source_gid_set.find(v_gid) == source_gid_set.end()) {
              ctx.boundary.insert(std::make_pair(gid, v_gid));
            }
          } else {
            if (target_gid_set.find(v_gid) != target_gid_set.end()) {
              ctx.boundary.insert(std::make_pair(gid, v_gid));
            }
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
    // https://networkx.org/documentation/stable/_modules/networkx/algorithms/boundary.html#edge_boundary
  }

 private:
  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    std::set<std::pair<vid_t, vid_t>> all_boundary;
    AllReduce(ctx.boundary, all_boundary,
              [](std::set<std::pair<vid_t, vid_t>>& out,
                 const std::set<std::pair<vid_t, vid_t>>& in) {
                for (auto& e : in) {
                  out.insert(e);
                }
              });
    if (frag.fid() == 0) {
      std::vector<typename fragment_t::oid_t> data;
      for (auto& e : all_boundary) {
        data.push_back(frag.Gid2Oid(e.first));
        data.push_back(frag.Gid2Oid(e.second));
      }
      std::vector<size_t> shape{data.size() / 2, 2};
      ctx.assign(data, shape);
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDARY_EDGE_BOUNDARY_H_
