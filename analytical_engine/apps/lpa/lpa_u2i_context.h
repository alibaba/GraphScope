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

#ifndef ANALYTICAL_ENGINE_APPS_LPA_LPA_U2I_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_LPA_LPA_U2I_CONTEXT_H_

#include <vector>

#include "grape/grape.h"

#include "core/app/property_app_base.h"
#include "core/context/labeled_vertex_property_context.h"

namespace gs {
template <typename FRAG_T>
class LPAU2IContext : public LabeledVertexPropertyContext<FRAG_T> {
 public:
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;
  using label_t = std::vector<double>;
  using edata_t = double;

  explicit LPAU2IContext(const FRAG_T& fragment)
      : LabeledVertexPropertyContext<FRAG_T>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, int32_t max_round = 20) {
    auto& frag = this->fragment();
    auto v_label_num = frag.vertex_label_num();
    auto e_label_num = frag.edge_label_num();

    CHECK_EQ(v_label_num, 2);
    CHECK_EQ(e_label_num, 1);
    step = 0;
    this->max_round = max_round;

    label.resize(v_label_num);
    in_degree.resize(v_label_num);
    out_degree.resize(v_label_num);
    out_nbr_in_degree_sum.resize(v_label_num);

    for (auto v_label = 0; v_label != v_label_num; ++v_label) {
      auto inner_vertices = frag.InnerVertices(v_label);

      label[v_label].Init(frag.Vertices(v_label));
      in_degree[v_label].Init(inner_vertices, 0);
      out_degree[v_label].Init(inner_vertices, 0);
      out_nbr_in_degree_sum[v_label].Init(inner_vertices, 0);
    }

    // Only holds user result
    for (auto prop_id = 0u; prop_id < prop_num; prop_id++) {
      label_column_indices.push_back(this->add_column(
          0, "label_" + std::to_string(prop_id), ContextDataType::kDouble));
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto iv = frag.InnerVertices(0);

    for (auto v : iv) {
      os << frag.GetId(v) << "\t";
      for (auto val : label[0][v]) {
        os << val << "\t";
      }
      os << std::endl;
    }
  }

  uint32_t step;
  uint32_t max_round;
  std::vector<grape::VertexArray<typename FRAG_T::vertices_t, label_t>> label;
  std::vector<grape::VertexArray<typename FRAG_T::inner_vertices_t, vid_t>>
      in_degree;
  std::vector<grape::VertexArray<typename FRAG_T::inner_vertices_t, vid_t>>
      out_degree;
  std::vector<grape::VertexArray<typename FRAG_T::inner_vertices_t, vid_t>>
      out_nbr_in_degree_sum;
  std::vector<int64_t> label_column_indices;
  static constexpr uint32_t prop_num = 2;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_APPS_LPA_LPA_U2I_CONTEXT_H_
