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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_CONTEXT_H_

#include <iomanip>
#include <limits>
#include <vector>

#include <grape/grape.h>

namespace grape {
/**
 * @brief Context for the parallel version of LCC.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class LCCContext : public VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit LCCContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment) {}

  void Init(ParallelMessageManager& messages,
            int degree_threshold = std::numeric_limits<int>::max()) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    global_degree.Init(vertices);
    complete_neighbor.Init(vertices);
    tricnt.Init(vertices, 0);
    this->degree_threshold = degree_threshold;
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

#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  typename FRAG_T::template vertex_array_t<int> global_degree;
  typename FRAG_T::template vertex_array_t<std::vector<vertex_t>>
      complete_neighbor;
  typename FRAG_T::template vertex_array_t<int> tricnt;
  int degree_threshold = 0;
  int stage = 0;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_CONTEXT_H_
