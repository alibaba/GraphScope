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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_CONTEXT_H_

#include <limits>
#include <string>

#include "grape/grape.h"

#include "core/app/app_base.h"

namespace gs {
template <typename FRAG_T>
class BetweennessCentralityContext
    : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit BetweennessCentralityContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment, true),
        centrality(this->data()) {}

  void Init(grape::DefaultMessageManager& messages, bool normalized = true,
            bool endpoints = false) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    centrality.SetValue(0.0);
    depth.Init(vertices, -100);
    pair_dependency.Init(vertices, 0.0);
    number_of_path.Init(vertices, 0);
    this->curr_depth = -1;
    this->epoch_tasks = 0;
    this->phase = "Forward";
    this->PEval = true;
    this->round = 0;
    this->max_round = frag.GetTotalVerticesNum() - 1;
    this->source = vid_t(0);
    this->remain_source = frag.GetInnerVerticesNum();
    this->endpoints = endpoints;
    this->norm = frag.directed() ? 1.0 : 0.5;
    if (normalized) {
      if (endpoints) {
        if (frag.GetVerticesNum() < 2)
          this->norm = 1.0;
        else
          this->norm = 1.0 / (this->max_round) / (this->max_round + 1);
      } else {
        if (frag.GetVerticesNum() <= 2)
          this->norm = 1.0;
        else
          this->norm = 1.0 / (this->max_round) / (this->max_round - 1);
      }
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << centrality[u] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<double>& centrality;
  typename FRAG_T::template vertex_array_t<int> depth;
  typename FRAG_T::template vertex_array_t<double> pair_dependency;
  typename FRAG_T::template vertex_array_t<int> number_of_path;

  double norm;
  int curr_depth;
  vid_t source;
  int remain_source;
  int epoch_tasks;
  int round, max_round;
  bool PEval;
  bool endpoints;
  std::string phase;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_CONTEXT_H_
