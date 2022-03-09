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

#ifndef ANALYTICAL_ENGINE_APPS_APSP_ALL_PAIRS_SHORTEST_PATH_LENGTH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_APSP_ALL_PAIRS_SHORTEST_PATH_LENGTH_CONTEXT_H_

#ifdef NETWORKX

#include <limits>
#include <map>
#include <queue>
#include <utility>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class AllPairsShortestPathLengthContext
    : public grape::VertexDataContext<FRAG_T, dynamic::Value> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit AllPairsShortestPathLengthContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, dynamic::Value>(fragment) {}

  void Init(grape::ParallelMessageManager& messages) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    length.Init(inner_vertices);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    auto vertices = frag.Vertices();

    for (auto src : inner_vertices) {
      for (auto v : vertices) {
        os << frag.GetId(src) << " " << frag.GetId(v) << " " << length[src][v]
           << std::endl;
      }
    }
  }

  const dynamic::Value& GetVertexResult(const vertex_t& v) override {
    auto& frag = this->fragment();
    CHECK(frag.IsInnerVertex(v));
    if (this->data()[v].IsNull()) {
      this->data()[v] = dynamic::Value(rapidjson::kArrayType);
      for (auto& t : frag.Vertices()) {
        if (length[v][t] < std::numeric_limits<double>::max()) {
          oid_t oid = frag.GetId(t);  // avoid to be moved.
          this->data()[v].PushBack(dynamic::Value(rapidjson::kArrayType)
                                       .PushBack(oid)
                                       .PushBack(length[v][t]));
        }
      }
    }
    return this->data()[v];
  }

  // length[src][v] is the path length from src to v
  typename FRAG_T::template vertex_array_t<
      typename FRAG_T::template vertex_array_t<double>>
      length;
};
}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_APPS_APSP_ALL_PAIRS_SHORTEST_PATH_LENGTH_CONTEXT_H_
