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

#ifndef ANALYTICAL_ENGINE_APPS_SSSP_SSSP_HAS_PATH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SSSP_SSSP_HAS_PATH_CONTEXT_H_

#include <limits>

#include "grape/grape.h"

#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class SSSPHasPathContext : public TensorContext<FRAG_T, bool> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit SSSPHasPathContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, bool>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, oid_t src_id,
            oid_t tgt_id) {
    auto& frag = this->fragment();

    source_id = src_id;
    target_id = tgt_id;
    has_path = false;
    visited.Init(frag.Vertices(), false);

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    if (frag.GetInnerVertex(target_id, target))
      os << has_path << "\n";

#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  oid_t source_id, target_id;
  vertex_t target;
  bool has_target;

  typename FRAG_T::template vertex_array_t<bool> visited;
  bool has_path;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SSSP_SSSP_HAS_PATH_CONTEXT_H_
