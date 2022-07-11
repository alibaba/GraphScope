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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_CONTEXT_H_

#include <limits>

#include "grape/grape.h"

#include "core/app/app_base.h"

namespace gs {
template <typename FRAG_T>
class KatzCentralityContext : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using vid_t = typename FRAG_T::vid_t;

  explicit KatzCentralityContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment, true),
        x(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, double alpha, double beta,
            double tolerance, int max_round, bool normalized,
            int degree_threshold = std::numeric_limits<int>::max()) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    x.SetValue(0);
    x_last.Init(vertices, 0);

    this->alpha = alpha;
    this->beta = beta;
    this->tolerance = tolerance;
    this->max_round = max_round;
    this->normalized = normalized;
    this->degree_threshold = degree_threshold;
    curr_round = 0;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << x[u] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<double>& x;
  typename FRAG_T::template vertex_array_t<double> x_last;

  double alpha;
  double beta;
  double tolerance;
  double global_sum;
  int max_round;
  bool normalized;
  int degree_threshold;
  int curr_round;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_CONTEXT_H_
