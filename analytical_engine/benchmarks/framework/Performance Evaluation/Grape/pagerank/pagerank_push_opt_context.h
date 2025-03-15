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

#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_OPT_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_OPT_CONTEXT_H_

#include <grape/grape.h>

#include <iomanip>

namespace grape {
/**
 * @brief Context for the batch version of PageRank.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRankPushOptContext : public VertexDataContext<FRAG_T, double> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

 public:
  explicit PageRankPushOptContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment), result(this->data()) {
    next_result.Init(fragment.InnerVertices());
  }

  void Init(ParallelMessageManagerOpt& messages, double delta, int max_round) {
    this->delta = delta;
    this->max_round = max_round;
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

  typename FRAG_T::template inner_vertex_array_t<double>& result;
  typename FRAG_T::template inner_vertex_array_t<double> next_result;

  vid_t total_dangling_vnum = 0;
  vid_t graph_vnum;
  int step = 0;
  int max_round = 0;
  double delta = 0;

  double dangling_sum = 0.0;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_OPT_CONTEXT_H_
