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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_CONTEXT_H_

#include <grape/grape.h>

namespace grape {

#ifdef WCC_USE_GID
template <typename FRAG_T>
using WCCContextType = VertexDataContext<FRAG_T, typename FRAG_T::vid_t>;
#else
template <typename FRAG_T>
using WCCContextType = VertexDataContext<FRAG_T, typename FRAG_T::oid_t>;
#endif

/**
 * @brief Context for the parallel version of WCC.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCContext : public WCCContextType<FRAG_T> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using cid_t = typename WCCContextType<FRAG_T>::data_t;

  explicit WCCContext(const FRAG_T& fragment)
      : WCCContextType<FRAG_T>(fragment, true), comp_id(this->data()) {}

  void Init(ParallelMessageManager& messages) {
    auto& frag = this->fragment();

    curr_modified.Init(frag.Vertices());
    next_modified.Init(frag.Vertices());
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << comp_id[v] << std::endl;
    }
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "eval_time: " << eval_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  typename FRAG_T::template vertex_array_t<cid_t>& comp_id;

  DenseVertexSet<typename FRAG_T::vertices_t> curr_modified, next_modified;

#ifdef PROFILING
  double preprocess_time = 0;
  double eval_time = 0;
  double postprocess_time = 0;
#endif
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_CONTEXT_H_
