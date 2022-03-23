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

#ifndef ANALYTICAL_ENGINE_APPS_PAGERANK_PAGERANK_NETWORKX_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_PAGERANK_PAGERANK_NETWORKX_CONTEXT_H_

#include <iomanip>

#include "grape/grape.h"

namespace gs {
/**
 * @brief Context for the Networkx version of PageRank.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRankNetworkXContext
    : public grape::VertexDataContext<FRAG_T, double> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

 public:
  explicit PageRankNetworkXContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment, true),
        result(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, double alpha,
            int max_round, double tolerance) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    auto vertices = frag.Vertices();

    this->alpha = alpha;
    this->max_round = max_round;
    this->tolerance = tolerance;
    degree.Init(inner_vertices, 0);
    result.SetValue(0.0);
    pre_result.Init(vertices, 0.0);
    step = 0;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
         << result[v] << std::endl;
    }
  }

  typename FRAG_T::template inner_vertex_array_t<double> degree;
  typename FRAG_T::template vertex_array_t<double>& result;
  typename FRAG_T::template vertex_array_t<double> pre_result;

  vid_t dangling_vnum = 0;
  int step = 0;
  int max_round = 0;
  double alpha = 0;
  double tolerance;

  double dangling_sum = 0.0;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PAGERANK_PAGERANK_NETWORKX_CONTEXT_H_
