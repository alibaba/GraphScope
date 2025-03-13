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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_OPT_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_OPT_CONTEXT_H_

#include <grape/grape.h>

#include <vector>

namespace grape {
/**
 * @brief Context for the parallel version of CDLP.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T, typename LABEL_T>
class CDLPOptContext : public VertexDataContext<FRAG_T, LABEL_T> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using label_t = LABEL_T;

  explicit CDLPOptContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, label_t>(fragment, true),
        labels(this->data()) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    changed.Init(inner_vertices);
    potential_change.Init(inner_vertices);
    new_ilabels.Init(frag.Vertices());
  }

  void Init(ParallelMessageManagerOpt& messages, int max_round,
            double threshold = 0.002) {
    this->max_round = max_round;
    this->threshold = threshold;
    step = 0;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << labels[v] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<label_t>& labels;
  typename FRAG_T::template vertex_array_t<label_t> new_ilabels;
  DenseVertexSet<typename FRAG_T::inner_vertices_t> potential_change, changed;

  int step = 0;
  int max_round = 0;
  double threshold = 0;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_OPT_CONTEXT_H_
