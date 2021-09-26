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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_GENERIC_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_GENERIC_CONTEXT_H_

#include <limits>
#include <map>
#include <queue>
#include <utility>

#include "grape/grape.h"

namespace gs {

template <typename FRAG_T>
class BetweennessCentralityGenericContext
    : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit BetweennessCentralityGenericContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment),
        centrality(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, bool normalized = true,
            bool endpoints = false) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    pair_dependency.Init(inner_vertices);
    this->factor = frag.GetTotalVerticesNum() - 1;
    this->endpoints = endpoints;
    this->norm = frag.directed() ? 1.0 : 0.5;
    if (normalized) {
      if (endpoints) {
        if (frag.GetVerticesNum() < 2)
          this->norm = 1.0;
        else
          this->norm = 1.0 / (this->factor) / (this->factor + 1);
      } else {
        if (frag.GetVerticesNum() <= 2)
          this->norm = 1.0;
        else
          this->norm = 1.0 / (this->factor) / (this->factor - 1);
      }
    }
    centrality.SetValue(0.0);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << centrality[u] << std::endl;
    }
  }

  bool endpoints;
  double norm;
  double factor;
  typename FRAG_T::template vertex_array_t<double>& centrality;
  typename FRAG_T::template vertex_array_t<
      typename FRAG_T::template vertex_array_t<double>>
      pair_dependency;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_GENERIC_CONTEXT_H_
