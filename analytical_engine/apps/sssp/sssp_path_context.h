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

#ifndef ANALYTICAL_ENGINE_APPS_SSSP_SSSP_PATH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SSSP_SSSP_PATH_CONTEXT_H_

#include <limits>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class SSSPPathContext : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit SSSPPathContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, oid_t source) {
    auto& frag = this->fragment();

    source_id = source;
    predecessor.Init(frag.InnerVertices());
    path_distance.Init(frag.InnerVertices(),
                       std::numeric_limits<double>::max());

    curr_updated.Init(frag.InnerVertices());
    prev_updated.Init(frag.InnerVertices());

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    vertex_t source;
    bool native_source = frag.GetInnerVertex(source_id, source);

    for (auto v : inner_vertices) {
      if (!(native_source && v == source) &&
          path_distance[v] != std::numeric_limits<double>::max()) {
        os << frag.GetId(predecessor[v]) << " " << frag.GetId(v) << std::endl;
      }
    }
  }

  oid_t source_id;

  typename FRAG_T::template inner_vertex_array_t<vertex_t> predecessor;
  typename FRAG_T::template inner_vertex_array_t<double> path_distance;
  grape::DenseVertexSet<typename FRAG_T::inner_vertices_t> curr_updated,
      prev_updated;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SSSP_SSSP_PATH_CONTEXT_H_
