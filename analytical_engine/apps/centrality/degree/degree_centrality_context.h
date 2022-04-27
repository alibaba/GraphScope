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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_CONTEXT_H_

#include <limits>
#include <string>

#include "grape/grape.h"

#include "core/app/app_base.h"

namespace gs {
enum class DegreeCentralityType { IN, OUT, BOTH };
template <typename FRAG_T>
class DegreeCentralityContext
    : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using vid_t = typename FRAG_T::vid_t;

  explicit DegreeCentralityContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment),
        centrality(this->data()) {}

  void Init(grape::ParallelMessageManager& messages,
            const std::string& centrality_type) {
    if (centrality_type == "in") {
      degree_centrality_type = DegreeCentralityType::IN;
    } else if (centrality_type == "out") {
      degree_centrality_type = DegreeCentralityType::OUT;
    } else if (centrality_type == "both") {
      degree_centrality_type = DegreeCentralityType::BOTH;
    } else {
      LOG(FATAL) << "Invalid parameter: " << centrality_type;
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << centrality[u] << std::endl;
    }
  }

  DegreeCentralityType degree_centrality_type;
  typename FRAG_T::template vertex_array_t<double>& centrality;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_CONTEXT_H_
