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

#ifndef ANALYTICAL_ENGINE_APPS_SAMPLING_PATH_SAMPLING_PATH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SAMPLING_PATH_SAMPLING_PATH_CONTEXT_H_

#include <string>
#include <vector>

#include "grape/grape.h"

#include "core/app/property_app_base.h"
#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class SamplingPathContext
    : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using path_t = std::vector<vid_t>;
  using label_t = typename FRAG_T::label_id_t;

  explicit SamplingPathContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  std::vector<label_t> path_pattern;
  std::vector<path_t> path_result;
  uint32_t total_path_limit;
  /**
   *
   * @param frag
   * @param messages
   * @param path_pattern represents the specific path:
   * src_label_id->edge_label_id->dst_label_id->...
   */
  void Init(grape::DefaultMessageManager& messages,
            std::vector<label_t>& path_pattern, uint32_t total_path_limit) {
    auto& frag = this->fragment();
    auto v_label_num = frag.vertex_label_num();
    auto e_label_num = frag.edge_label_num();

    this->path_pattern = path_pattern;
    this->total_path_limit = total_path_limit;

    // make sure the path pattern is valid
    CHECK_GE(path_pattern.size(), 3);
    CHECK_EQ(path_pattern.size() % 2, 1);

    for (uint32_t u_label_idx = 0; u_label_idx + 1 < path_pattern.size();
         u_label_idx += 2) {
      auto u_label = path_pattern[u_label_idx];
      auto e_label = path_pattern[u_label_idx + 1];

      CHECK_LT(u_label, v_label_num);
      CHECK_LT(e_label, e_label_num);
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    for (auto& path : path_result) {
      std::string buf;

      for (auto gid : path) {
        buf += std::to_string(frag.Gid2Oid(gid)) + " ";
      }
      if (!buf.empty()) {
        buf[buf.size() - 1] = '\n';
        os << buf;
      }
    }
  }
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_APPS_SAMPLING_PATH_SAMPLING_PATH_CONTEXT_H_
