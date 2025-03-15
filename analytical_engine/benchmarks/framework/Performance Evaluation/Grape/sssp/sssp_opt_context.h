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

#ifndef EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_OPT_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_OPT_CONTEXT_H_

#include <grape/grape.h>

#include <iomanip>
#include <iostream>
#include <limits>

namespace grape {

/**
 * @brief Context for the parallel version of SSSP.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSPOptContext : public VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  explicit SSSPOptContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment, true),
        partial_result(this->data()) {
    curr_modified.Init(fragment.Vertices());
    next_modified.Init(fragment.Vertices());
  }

  void Init(ParallelMessageManagerOpt& messages, oid_t source_id) {
    this->source_id = source_id;
    partial_result.SetValue(std::numeric_limits<double>::max());
  }

  void Output(std::ostream& os) override {
    // If the distance is the max value for vertex_data_type
    // then the vertex is not connected to the source vertex.
    // According to specs, the output should be +inf
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      double d = partial_result[v];
      if (d == std::numeric_limits<double>::max()) {
        os << frag.GetId(v) << " infinity" << std::endl;
      } else {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << d << std::endl;
      }
    }
  }

  oid_t source_id;
  typename FRAG_T::template vertex_array_t<double>& partial_result;

  DenseVertexSet<typename FRAG_T::vertices_t> curr_modified, next_modified;
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_OPT_CONTEXT_H_
