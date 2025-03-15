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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_DIRECTED_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_DIRECTED_CONTEXT_H_

#include <grape/grape.h>

#include <iomanip>
#include <limits>
#include <vector>

#include "lcc_opt_context.h"

namespace grape {
/**
 * @brief Context for the parallel version of LCC.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T, typename COUNT_T>
class LCCDirectedContext : public VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using count_t = COUNT_T;

  explicit LCCDirectedContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment) {
    global_degree.Init(fragment.Vertices());
    deduped_degree.Init(fragment.InnerVertices());
    complete_neighbor.Init(fragment.Vertices());
    neighbor_weight.Init(fragment.Vertices());
    tricnt.Init(fragment.Vertices());
  }

  void Init(ParallelMessageManagerOpt& messages) { tricnt.SetValue(0); }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (deduped_degree[v] == 0 || deduped_degree[v] == 1) {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << 0.0 << std::endl;
      } else {
        double re = static_cast<double>(tricnt[v]) /
                    (static_cast<int64_t>(deduped_degree[v]) *
                     (static_cast<int64_t>(deduped_degree[v]) - 1));
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << re << std::endl;
      }
    }
  }

  typename FRAG_T::template vertex_array_t<int> global_degree;
  typename FRAG_T::template inner_vertex_array_t<int> deduped_degree;
  std::vector<lcc_opt_impl::memory_pool<vertex_t>> neighbor_pools;
  typename FRAG_T::template vertex_array_t<lcc_opt_impl::ref_vector<vertex_t>>
      complete_neighbor;
  std::vector<lcc_opt_impl::memory_pool<uint8_t>> weight_pools;
  typename FRAG_T::template vertex_array_t<lcc_opt_impl::ref_vector<uint8_t>>
      neighbor_weight;
  typename FRAG_T::template vertex_array_t<count_t> tricnt;
  int stage = 0;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_DIRECTED_CONTEXT_H_
