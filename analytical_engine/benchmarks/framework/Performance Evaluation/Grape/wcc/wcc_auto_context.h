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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_AUTO_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_AUTO_CONTEXT_H_

#include <limits>
#include <vector>

#include <grape/grape.h>

namespace grape {

#ifdef WCC_USE_GID
template <typename FRAG_T>
using WCCAutoContextType = VertexDataContext<FRAG_T, typename FRAG_T::vid_t>;
#else
template <typename FRAG_T>
using WCCAutoContextType = VertexDataContext<FRAG_T, typename FRAG_T::oid_t>;
#endif

/**
 * @brief Context for the auto-parallel version of WCCAuto.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCAutoContext : public WCCAutoContextType<FRAG_T> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using cid_t = typename WCCAutoContextType<FRAG_T>::data_t;

 public:
  explicit WCCAutoContext(const FRAG_T& fragment)
      : WCCAutoContextType<FRAG_T>(fragment, true),
        global_cluster_id(this->data()) {}

  void Init(AutoParallelMessageManager<FRAG_T>& messages) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();

    local_comp_id.Init(inner_vertices, std::numeric_limits<vid_t>::max());
    global_cluster_id.Init(vertices, std::numeric_limits<cid_t>::max(),
                           [](cid_t* lhs, cid_t rhs) {
                             if (*lhs > rhs) {
                               *lhs = rhs;
                               return true;
                             } else {
                               return false;
                             }
                           });
    messages.RegisterSyncBuffer(frag, &global_cluster_id,
                                MessageStrategy::kSyncOnOuterVertex);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << global_cluster_id.GetValue(v) << std::endl;
    }
  }

  std::vector<std::vector<vertex_t>> outer_vertices;
  typename FRAG_T::template vertex_array_t<vid_t> local_comp_id;
  std::vector<cid_t> global_comp_id;
  SyncBuffer<typename FRAG_T::vertices_t, cid_t> global_cluster_id;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_AUTO_CONTEXT_H_
