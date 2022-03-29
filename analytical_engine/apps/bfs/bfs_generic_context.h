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

#ifndef ANALYTICAL_ENGINE_APPS_BFS_BFS_GENERIC_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_BFS_BFS_GENERIC_CONTEXT_H_

#include <limits>
#include <queue>
#include <string>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class BFSGenericContext : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using depth_type = int64_t;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit BFSGenericContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, oid_t src_id, int limit,
            const std::string& format) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    source_id = src_id;
    if (limit == -1) {
      depth_limit = frag.GetTotalVerticesNum();
    } else {
      depth_limit = limit;
    }
    output_format = format;
    if (output_format != "edges" && output_format != "predecessors" &&
        output_format != "successors") {
      LOG(ERROR) << "Output format error. edges/predecessors/successors"
                 << std::endl;
    }

    visited.Init(vertices, false);
    predecessor.Init(vertices);

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    if (output_format == "edges")
      outputEdges(frag, os);
    else if (output_format == "predecessors")
      outputPredecessors(frag, os);
    else if (output_format == "successors")
      outputSuccessors(frag, os);

#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<vid_t> predecessor;
  typename FRAG_T::template vertex_array_t<bool> visited;
  std::queue<vertex_t> curr_level_inner, next_level_inner;

  int depth_limit;
  std::string output_format;
  int depth;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif

 private:
  void outputEdges(const FRAG_T& frag, std::ostream& os) {
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices)
      if (visited[v] && frag.GetId(v) != source_id)
        os << frag.Gid2Oid(predecessor[v]) << " " << frag.GetId(v) << std::endl;
  }

  void outputPredecessors(const FRAG_T& frag, std::ostream& os) {
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices)
      if (visited[v] && frag.GetId(v) != source_id)
        os << frag.GetId(v) << ": " << frag.Gid2Oid(predecessor[v])
           << std::endl;
  }

  void outputSuccessors(const FRAG_T& frag, std::ostream& os) {
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices)
      if (visited[v]) {
        vid_t v_vid = frag.Vertex2Gid(v);
        auto oes = frag.GetOutgoingAdjList(v);
        for (auto& e : oes) {
          if (predecessor[e.get_neighbor()] == v_vid)
            os << frag.GetId(v) << ": " << frag.GetId(e.get_neighbor())
               << std::endl;
        }
      }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BFS_BFS_GENERIC_CONTEXT_H_
