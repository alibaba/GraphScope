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

#ifndef ANALYTICAL_ENGINE_APPS_KCORE_KCORE_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_KCORE_KCORE_CONTEXT_H_

#include <limits>
#include <map>
#include <memory>
#include <queue>
#include <unordered_set>
#include <utility>

#include "grape/grape.h"

namespace gs {

template <typename FRAG_T>
class KCoreContext : public grape::VertexDataContext<FRAG_T, int> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit KCoreContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, int>(fragment) {}

  typename FRAG_T::template vertex_array_t<std::shared_ptr<std::atomic_int>>
      degrees;
  grape::DenseVertexSet<typename FRAG_T::inner_vertices_t> to_remove_vertices,
      remaining_vertices, next_remaining_vertices;
  int k;
  int curr_k;

  void Init(grape::ParallelMessageManager& messages, int k) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();

    degrees.Init(vertices);
    to_remove_vertices.Init(inner_vertices);
    remaining_vertices.Init(inner_vertices);
    next_remaining_vertices.Init(inner_vertices);
    this->k = k;
    curr_k = 0;

    for (auto& v : vertices) {
      degrees[v] = std::make_shared<std::atomic_int>(0);
      if (frag.IsInnerVertex(v)) {
        remaining_vertices.Insert(v);
        degrees[v]->store(frag.GetLocalOutDegree(v));
      } else {
        degrees[v]->store(0);
      }
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& v : inner_vertices) {
      if (remaining_vertices.Exist(v)) {
        os << frag.GetId(v) << '\n';
      }
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_KCORE_KCORE_CONTEXT_H_
