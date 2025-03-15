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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_CONTEXT_H_

#include <vector>

#include <grape/grape.h>

namespace grape {
/**
 * @brief Context for the parallel version of CDLP.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
#ifdef GID_AS_LABEL
class CDLPContext : public VertexDataContext<FRAG_T, typename FRAG_T::vid_t> {
#else
class CDLPContext : public VertexDataContext<FRAG_T, typename FRAG_T::oid_t> {
#endif

 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

#ifdef GID_AS_LABEL
  using label_t = vid_t;
#else
  using label_t = oid_t;
#endif
  explicit CDLPContext(const FRAG_T& fragment)
#ifdef GID_AS_LABEL
      : VertexDataContext<FRAG_T, typename FRAG_T::vid_t>(fragment, true),
#else
      : VertexDataContext<FRAG_T, typename FRAG_T::oid_t>(fragment, true),
#endif
        labels(this->data()) {
  }

  void Init(ParallelMessageManager& messages, int max_round) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    this->max_round = max_round;
    changed.Init(inner_vertices);

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
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
  typename FRAG_T::template inner_vertex_array_t<bool> changed;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif

  int step = 0;
  int max_round = 0;

#ifdef RANDOM_LABEL
  std::vector<std::mt19937> random_engines;
#endif
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_CONTEXT_H_
