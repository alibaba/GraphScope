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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_CLOSENESS_CLOSENESS_CENTRALITY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_CLOSENESS_CLOSENESS_CENTRALITY_CONTEXT_H_

#include <limits>
#include <map>
#include <queue>
#include <utility>
#include <vector>

#include "grape/grape.h"

namespace gs {

template <typename FRAG_T>
class ClosenessCentralityContext
    : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit ClosenessCentralityContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment),
        centrality(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, bool wf) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    wf_improve = wf;
    centrality.SetValue(0.0);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << centrality[u] << std::endl;
    }
  }

  bool wf_improve;  // use Wasserman-Faust improved formula.
  std::vector<typename FRAG_T::template vertex_array_t<double>> length;
  typename FRAG_T::template vertex_array_t<double>& centrality;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_CLOSENESS_CLOSENESS_CENTRALITY_CONTEXT_H_
