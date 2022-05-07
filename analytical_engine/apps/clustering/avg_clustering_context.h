/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_APPS_CLUSTERING_AVG_CLUSTERING_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_CLUSTERING_AVG_CLUSTERING_CONTEXT_H_

#include <iomanip>
#include <limits>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {
/**
 * @brief Context for average clustering.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class AvgClusteringContext : public TensorContext<FRAG_T, float> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit AvgClusteringContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, float>(fragment) {}

  void Init(grape::ParallelMessageManager& messages,
            int degree_threshold = std::numeric_limits<int>::max()) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();

    global_degree.Init(vertices, 0);
    rec_degree.Init(inner_vertices, 0);
    complete_neighbor.Init(vertices);
    tricnt.Init(vertices, 0);
    this->degree_threshold = degree_threshold;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    if (frag.fid() == 0) {
      os << std::setiosflags(std::ios::fixed) << std::setprecision(4)
         << total_clustering / frag.GetTotalVerticesNum() << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<int> global_degree;
  typename FRAG_T::template inner_vertex_array_t<int> rec_degree;
  typename FRAG_T::template vertex_array_t<
      std::vector<std::pair<vertex_t, uint32_t>>>
      complete_neighbor;
  typename FRAG_T::template vertex_array_t<int> tricnt;
  int degree_threshold = 0;
  float total_clustering = 0.0;
  int stage = 0;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CLUSTERING_AVG_CLUSTERING_CONTEXT_H_
