/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_APPS_SAMPLING_PATH_SAMPLING_PATH_H_
#define ANALYTICAL_ENGINE_APPS_SAMPLING_PATH_SAMPLING_PATH_H_

#include <queue>
#include <utility>
#include <vector>

#include "apps/sampling_path/sampling_path_context.h"

namespace gs {
/**
 * @brief Sampling paths obey source label-edge label-destination label pattern.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SamplingPath
    : public PropertyAppBase<FRAG_T, SamplingPathContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_PROPERTY_WORKER(SamplingPath<FRAG_T>,
                                  SamplingPathContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using path_t = typename context_t::path_t;
  using layer_t = int;

  void BFS(const fragment_t& frag, context_t& ctx, message_manager_t& messages,
           std::queue<std::pair<layer_t, path_t>>& paths) {
    auto& path_pattern = ctx.path_pattern;
    auto& path_result = ctx.path_result;

    while (!paths.empty()) {
      auto& pair = paths.front();
      auto level = pair.first;
      auto& path = pair.second;

      if ((size_t)(level + 2) < ctx.path_pattern.size()) {
        vertex_t u;
        auto curr_e_label = ctx.path_pattern[level + 1];
        auto curr_v_label = ctx.path_pattern[level + 2];

        CHECK_GT(path.size(), 0);
        CHECK(frag.Gid2Vertex(path[path.size() - 1], u));
        auto oes = frag.GetOutgoingAdjList(u, curr_e_label);

        for (auto& e : oes) {
          auto v = e.neighbor();

          if (frag.vertex_label(v) == curr_v_label) {
            std::vector<vid_t> new_path(path);
            new_path.push_back(frag.Vertex2Gid(v));

            // |pattern| = k, the result should have "k / 2 + 1" vertices
            // e.g. pattern = "v0-e0-v1-e1-v2", path = "v0 v1 v2"
            if (new_path.size() == path_pattern.size() / 2 + 1) {
              path_result.push_back(new_path);
            } else {
              auto new_pair = std::make_pair(level + 2, new_path);
              if (frag.IsInnerVertex(v)) {
                paths.push(new_pair);
              } else {
                messages.SendToFragment(frag.GetFragId(v), new_pair);
              }
            }
          }
        }
      }
      paths.pop();
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto curr_u_label = ctx.path_pattern[0];
    auto inner_vertices = frag.InnerVertices(curr_u_label);

    std::queue<std::pair<layer_t, path_t>> paths;

    for (auto u : inner_vertices) {
      std::pair<layer_t, path_t> pair(0, {frag.Vertex2Gid(u)});
      paths.push(pair);
    }

    BFS(frag, ctx, messages, paths);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    std::queue<std::pair<layer_t, path_t>> paths;

    {
      std::pair<layer_t, path_t> msg;

      while (messages.GetMessage(msg)) {
        paths.push(msg);
      }
    }

    // A rough implementation to limit path count
    uint32_t total_path_count;
    Sum((uint32_t) ctx.path_result.size(), total_path_count);

    if (total_path_count >= ctx.total_path_limit) {
      auto& path_result = ctx.path_result;
      std::vector<size_t> shape{path_result.size(),
                                ctx.path_pattern.size() / 2 + 1};

      ctx.set_shape(shape);

      auto* data = ctx.tensor().data();
      size_t idx = 0;

      for (auto& path : path_result) {
        for (auto gid : path) {
          data[idx++] = frag.Gid2Oid(gid);
        }
      }
      return;
    }

    BFS(frag, ctx, messages, paths);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SAMPLING_PATH_SAMPLING_PATH_H_
