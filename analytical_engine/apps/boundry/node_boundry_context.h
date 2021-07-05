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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDRY_NODE_BOUNDRY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDRY_NODE_BOUNDRY_CONTEXT_H_

#include <limits>
#include <string>

#include "grape/grape.h"

namespace gs {
template <typename FRAG_T>
class NodeBoundryContext
    : public TensorContext<FRAG_T, std::string> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit NodeBoundryContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, std::string>(fragment) {}

  void Init(grape::ParallelMessageManager& messages,
            const std::string& nbunch1, const std::string& nbunch2) {
    this->nbunch1 = nbunch1;
    this->nbunch2 = nbunch2;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto& u : inner_vertices) {
      os << frag.GetId(u) << "\t" << centrality[u] << std::endl;
    }
  }

  std::string nbunch1, nbunch2;
  std::unordered_set<vid_t> boundary;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDRY_NODE_BOUNDRY_CONTEXT_H_
