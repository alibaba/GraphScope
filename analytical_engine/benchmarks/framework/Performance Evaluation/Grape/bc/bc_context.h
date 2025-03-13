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

#ifndef EXAMPLES_ANALYTICAL_APPS_BC_BC_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_BC_BC_CONTEXT_H_

#include <grape/grape.h>

#include <limits>

namespace grape {
/**
 * @brief Context for the BC(betweeness centrality).
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BCContext : public VertexDataContext<FRAG_T, float> {
 public:
  using depth_type = int64_t;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit BCContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, float>(fragment, true),
        centrality_value(this->data()) {}

  void Init(ParallelMessageManagerOpt& messages, oid_t src_id) {
    source_id = src_id;
    centrality_value.SetValue(0);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << centrality_value[v] << " " << path_num[v]
         << " " << partial_result[v] << std::endl;
    }
    typename FRAG_T::vertex_t s;
    if (frag.GetInnerVertex(0, s)) {
      LOG(INFO) << "[frag-" << frag.fid()
                << "] BC(0) = " << centrality_value[s];
    }
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<depth_type> partial_result;
  DenseVertexSet<typename FRAG_T::inner_vertices_t> curr_inner_updated,
      next_inner_updated;

  DenseVertexSet<typename FRAG_T::outer_vertices_t> outer_updated;

  typename FRAG_T::template vertex_array_t<double> path_num;
  typename FRAG_T::template vertex_array_t<float>& centrality_value;

  depth_type current_depth = 0;

  int stage = 0;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_BC_BC_CONTEXT_H_
