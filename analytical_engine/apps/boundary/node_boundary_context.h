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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_CONTEXT_H_

#include <limits>
#include <set>
#include <string>

#include "grape/grape.h"

namespace gs {
template <typename FRAG_T>
class NodeBoundaryContext
    : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit NodeBoundaryContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, const std::string& nbunch1,
            const std::string& nbunch2) {
    this->nbunch1 = nbunch1;
    this->nbunch2 = nbunch2;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    if (frag.fid() == 0) {
      for (auto& v : boundary) {
        os << frag.Gid2Oid(v) << "\n";
      }
    }
  }

  std::string nbunch1, nbunch2;
  std::set<vid_t> boundary;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDARY_NODE_BOUNDARY_CONTEXT_H_
