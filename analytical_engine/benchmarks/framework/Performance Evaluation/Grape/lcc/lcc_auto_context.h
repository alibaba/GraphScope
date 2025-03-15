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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_AUTO_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_AUTO_CONTEXT_H_

#include <iomanip>
#include <limits>

#include <grape/grape.h>

namespace grape {
/**
 * @brief Context for the auto-parallel version of LCC.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class LCCAutoContext : public VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit LCCAutoContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment) {}

  void Init(AutoParallelMessageManager<FRAG_T>& messages) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    global_degree.Init(vertices, 0, [](int* lhs, int rhs) {
      *lhs = rhs;
      return true;
    });
    complete_neighbor.Init(
        vertices, std::vector<vid_t>(),
        [](std::vector<vid_t>* lhs, std::vector<vid_t>&& rhs) {
          *lhs = std::move(rhs);
          return true;
        });
    tricnt.Init(vertices, 0, [](int* lhs, int rhs) {
      *lhs = *lhs + rhs;
      return true;
    });

    messages.RegisterSyncBuffer(
        frag, &global_degree, MessageStrategy::kAlongOutgoingEdgeToOuterVertex);
    messages.RegisterSyncBuffer(
        frag, &complete_neighbor,
        MessageStrategy::kAlongOutgoingEdgeToOuterVertex);
    messages.RegisterSyncBuffer(frag, &tricnt,
                                MessageStrategy::kSyncOnOuterVertex);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (global_degree[v] == 0 || global_degree[v] == 1) {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << 0.0 << std::endl;
      } else {
        double re = 2.0 * (tricnt[v]) /
                    (static_cast<int64_t>(global_degree[v]) *
                     (static_cast<int64_t>(global_degree[v]) - 1));
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << re << std::endl;
      }
    }
  }

  int stage = 0;

  SyncBuffer<typename FRAG_T::vertices_t, int> global_degree;
  SyncBuffer<typename FRAG_T::vertices_t, std::vector<vid_t>> complete_neighbor;
  SyncBuffer<typename FRAG_T::vertices_t, int> tricnt;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_AUTO_CONTEXT_H_
