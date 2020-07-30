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

#ifndef ANALYTICAL_ENGINE_APPS_CLUSTERING_TRIANGLES_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CLUSTERING_TRIANGLES_CONTEXT_H_

#include <iomanip>
#include <limits>
#include <vector>

#include "grape/grape.h"

namespace gs {
/**
 * @brief Context for triangles.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class TrianglesContext : public grape::VertexDataContext<FRAG_T, int> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit TrianglesContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, int>(fragment, true),
        tricnt(this->data()) {}

  void Init(grape::ParallelMessageManager& messages) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    global_degree.Init(vertices);
    complete_neighbor.Init(vertices);
    tricnt.SetValue(0);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << tricnt[v] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<int> global_degree;
  typename FRAG_T::template vertex_array_t<std::vector<vertex_t>>
      complete_neighbor;
  typename FRAG_T::template vertex_array_t<int>& tricnt;

  int stage = 0;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CLUSTERING_TRIANGLES_CONTEXT_H_
