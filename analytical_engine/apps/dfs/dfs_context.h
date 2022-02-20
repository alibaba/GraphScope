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

#ifndef ANALYTICAL_ENGINE_APPS_DFS_DFS_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_DFS_DFS_CONTEXT_H_

#include <limits>
#include <string>
#include <vector>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class DFSContext : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit DFSContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, oid_t source_id,
            std::string dfs_format) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    auto vertices = frag.Vertices();

    parent.Init(inner_vertices, 0);
    rank.Init(inner_vertices, -1);
    is_visited.Init(vertices, false);
    this->is_in_frag = false;
    this->output_stage = false;
    this->total_num = 0;
    vertex_t source;
    bool native_source = frag.GetInnerVertex(source_id, source);
    if (native_source) {
      this->is_in_frag = true;
      this->current_vertex = source;
      rank[source] = 0;
      is_visited[source] = true;
      max_rank = 0;
    }
    if (frag.fid() == 0) {
      results.resize(frag.GetTotalVerticesNum());
      this->output_format = dfs_format;
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    if (output_format == "edges" || output_format == "successors") {
      if (frag.fid() == 0) {
        for (int i = 0; i < total_num - 1; i++) {
          os << results[i] << "\t" << results[i + 1] << "\n";
        }
      }
    } else if (output_format == "predecessors") {
      if (frag.fid() == 0) {
        for (int i = 1; i < total_num; i++) {
          os << results[i] << "\t" << results[i - 1] << "\n";
        }
      }
    } else {
      auto inner_vertices = frag.InnerVertices();
      for (auto& u : inner_vertices) {
        os << frag.GetId(u) << "\t" << rank[u] << "\n";
      }
    }
  }

  typename FRAG_T::template inner_vertex_array_t<vid_t> parent;
  typename FRAG_T::template inner_vertex_array_t<int> rank;
  typename FRAG_T::template vertex_array_t<bool> is_visited;
  vertex_t current_vertex;
  std::vector<oid_t> results;
  bool is_in_frag;
  bool output_stage;
  vid_t source_gid;
  int max_rank;
  int total_num;
  std::string output_format;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_DFS_DFS_CONTEXT_H_
