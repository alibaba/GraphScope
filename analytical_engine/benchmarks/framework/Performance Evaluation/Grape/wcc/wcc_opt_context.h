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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_OPT_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_OPT_CONTEXT_H_

#include <grape/grape.h>

namespace grape {

template <typename FRAG_T>
using WCCOptContextType = VertexDataContext<FRAG_T, typename FRAG_T::oid_t>;

/**
 * @brief Context for the parallel version of WCC.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCOptContext : public WCCOptContextType<FRAG_T> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using cid_t = typename WCCOptContextType<FRAG_T>::data_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit WCCOptContext(const FRAG_T& fragment)
      : WCCOptContextType<FRAG_T>(fragment, true), comp_id(this->data()) {
    tree.Init(fragment.Vertices());
    curr_modified.Init(fragment.InnerVertices());
    next_modified.Init(fragment.InnerVertices());
  }

  void Init(ParallelMessageManagerOpt& messages) {}

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << comp_id[tree[v]] << std::endl;
    }
  }

  typename FRAG_T::template inner_vertex_array_t<cid_t>& comp_id;
  typename FRAG_T::template vertex_array_t<vertex_t> tree;

  DenseVertexSet<typename FRAG_T::inner_vertices_t> curr_modified;
  DenseVertexSet<typename FRAG_T::inner_vertices_t> next_modified;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_OPT_CONTEXT_H_
