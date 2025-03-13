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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_AUTO_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_AUTO_CONTEXT_H_

#include <grape/grape.h>

namespace grape {
/**
 * @brief Context for the auto-parallel version of CDLP.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class CDLPAutoContext
    : public VertexDataContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
#ifdef GID_AS_LABEL
  using label_t = vid_t;
#else
  using label_t = oid_t;
#endif

  explicit CDLPAutoContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, typename FRAG_T::oid_t>(fragment, true),
        labels(this->data()) {}

  void Init(AutoParallelMessageManager<FRAG_T>& messages, int max_round) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();

    this->max_round = max_round;
    labels.Init(vertices, 0, [](label_t* lhs, label_t rhs) {
      *lhs = rhs;
      return true;
    });
    changed.Init(inner_vertices);

    messages.RegisterSyncBuffer(frag, &labels,
                                MessageStrategy::kAlongEdgeToOuterVertex);

#ifdef GID_AS_LABEL
    auto outer_vertices = frag.OuterVertices();
    for (auto& v : inner_vertices) {
      labels[v] = frag.GetInnerVertexGid(v);
    }
    for (auto& v : outer_vertices) {
      labels[v] = frag.GetOuterVertexGid(v);
    }
#else
    for (auto& v : vertices) {
      labels[v] = frag.GetId(v);
    }
#endif

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

  SyncBuffer<typename FRAG_T::vertices_t, label_t> labels;
  typename FRAG_T::template inner_vertex_array_t<bool> changed;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif

  int step = 0;
  int max_round = 0;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_AUTO_CONTEXT_H_
