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

#ifndef ANALYTICAL_ENGINE_APPS_SSSP_SSSP_AVERAGE_LENGTH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SSSP_SSSP_AVERAGE_LENGTH_CONTEXT_H_

#include <limits>
#include <map>
#include <queue>
#include <utility>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class SSSPAverageLengthContext : public TensorContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit SSSPAverageLengthContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, double>(fragment) {}

  void Init(grape::DefaultMessageManager& messages) {
    auto& frag = this->fragment();

    inner_sum = 0.0;
    path_distance.Init(frag.InnerVertices());
    updated.Init(frag.InnerVertices());

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    if (frag.fid() == 0) {
      size_t n = frag.GetTotalVerticesNum();
      double sum = 0.0;
      for (auto it : all_sums) {
        sum += it.second;
      }
      double average_length = sum / static_cast<double>((n * (n - 1)));
      os << average_length << std::endl;
    }

#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  // length sum of each fragment, only maintained by frag 0
  std::map<fid_t, double> all_sums;

  // path_distance[v][src] is path length from src to v
  typename FRAG_T::template vertex_array_t<std::map<vid_t, double>>
      path_distance;

  // length sum of inner vertex
  double inner_sum;

  std::priority_queue<std::pair<double, vertex_t>> next_queue;
  grape::DenseVertexSet<typename FRAG_T::inner_vertices_t> updated;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SSSP_SSSP_AVERAGE_LENGTH_CONTEXT_H_
