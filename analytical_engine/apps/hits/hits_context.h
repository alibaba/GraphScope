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

#ifndef ANALYTICAL_ENGINE_APPS_HITS_HITS_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_HITS_HITS_CONTEXT_H_

#include <limits>
#include <utility>

#include "grape/grape.h"

#include "core/context/vertex_property_context.h"

namespace gs {
enum { AuthIteration = 0, HubIteration = 1, Normalize = 2 };

template <typename FRAG_T>
class HitsContext : public VertexPropertyContext<FRAG_T> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit HitsContext(const FRAG_T& fragment)
      : VertexPropertyContext<FRAG_T>(fragment) {}

  void Init(grape::ParallelMessageManager& messages, double tolerance,
            int max_round, bool normalized) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    hub.Init(vertices, 1.0 / frag.GetTotalVerticesNum());
    auth.Init(vertices);
    hub_last.Init(vertices);

    step = 0;
    sum_a = 0;
    sum_h = 0;
    this->tolerance = tolerance;
    this->max_round = max_round;
    this->normalized = normalized;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << hub[u] << "\t" << auth[u] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<double> auth;
  typename FRAG_T::template vertex_array_t<double> hub;
  typename FRAG_T::template vertex_array_t<double> hub_last;
  double tolerance;
  int max_round;
  bool normalized;
  int stage;
  int step;
  double sum_a;
  double sum_h;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_HITS_HITS_CONTEXT_H_
