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

#ifndef ANALYTICAL_ENGINE_APPS_LPA_LPA_U2I_H_
#define ANALYTICAL_ENGINE_APPS_LPA_LPA_U2I_H_

#include <utility>

#include "apps/lpa/lpa_u2i_context.h"

namespace gs {
/**
 * @brief Label propagation algorithm. U stands for the user label. V stands for
 * the item label.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class LPAU2I : public PropertyAppBase<FRAG_T, LPAU2IContext<FRAG_T>> {
 public:
  INSTALL_DEFAULT_PROPERTY_WORKER(LPAU2I<FRAG_T>, LPAU2IContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using label_id_t = typename FRAG_T::label_id_t;
  using label_t = typename context_t::label_t;
  using edata_t = typename context_t::edata_t;
  static constexpr uint32_t prop_num = context_t::prop_num;
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto v_label_num = frag.vertex_label_num();

    for (auto v_label = 0; v_label != v_label_num; ++v_label) {
      auto inner_vertices = frag.InnerVertices(v_label);
      auto outer_vertices = frag.OuterVertices(v_label);
      auto& label = ctx.label[v_label];

      for (auto u : inner_vertices) {
        if (v_label == 0) {
          label_t vdata;
          for (auto prop_id = 0u; prop_id < prop_num; prop_id++) {
            vdata.push_back(frag.template GetData<double>(u, prop_id));
          }
          label[u] = vdata;
        } else {
          label[u].resize(prop_num, 0);
        }
      }

      for (auto u : outer_vertices) {
        label[u].resize(prop_num, 0);
      }
    }

    // init degree for u
    for (auto v_label = 0; v_label != v_label_num; ++v_label) {
      auto inner_vertices = frag.InnerVertices(v_label);
      auto& in_degree = ctx.in_degree[v_label];
      auto& out_degree = ctx.out_degree[v_label];

      for (auto u : inner_vertices) {
        in_degree[u] = frag.GetLocalInDegree(u, 0);
        out_degree[u] = frag.GetLocalOutDegree(u, 0);
      }
    }

    for (auto v_label = 0; v_label != v_label_num; ++v_label) {
      auto inner_vertices = frag.InnerVertices(v_label);
      auto& out_nbr_in_degree_sum = ctx.out_nbr_in_degree_sum[v_label];

      for (auto u : inner_vertices) {
        auto ies = frag.GetIncomingAdjList(u, 0);
        auto oes = frag.GetOutgoingAdjList(u, 0);

        for (auto& e : oes) {
          auto v = e.neighbor();
          // pull in-degree from outgoing neighbors
          if (frag.IsInnerVertex(v)) {
            out_nbr_in_degree_sum[u] += ctx.in_degree[frag.vertex_label(v)][v];
          }
        }

        for (auto& e : ies) {
          auto v = e.neighbor();

          if (frag.IsOuterVertex(v)) {
            messages.SyncStateOnOuterVertex(frag, v, ctx.in_degree[v_label][u]);
          }
        }
      }
    }

    messages.ForceContinue();
  }

  void SyncLabelOnInnerVertex(const fragment_t& frag, context_t& ctx,
                              message_manager_t& messages, uint32_t v_label) {
    auto inner_vertices = frag.InnerVertices(v_label);
    auto& label = ctx.label[v_label];

    for (auto u : inner_vertices) {
      messages.SendMsgThroughEdges(frag, u, 0, label[u]);
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto& label = ctx.label;
    auto& step = ctx.step;
    step++;

    if (step > ctx.max_round) {
      // write out user result
      auto iv = frag.InnerVertices(0);

      for (auto prop_id = 0u; prop_id < prop_num; prop_id++) {
        auto column = ctx.template get_typed_column<double>(
            0, ctx.label_column_indices[prop_id]);
        for (auto v : iv) {
          column->at(v) = label[0][v][prop_id];
        }
      }
      return;
    }

    if (step == 1) {
      // accumulate in-degree synced in PEval
      auto& out_nbr_in_degree_sum = ctx.out_nbr_in_degree_sum;
      vertex_t u(0);
      vid_t msg;

      while (messages.GetMessage(frag, u, msg)) {
        auto v_label = frag.vertex_label(u);

        out_nbr_in_degree_sum[v_label][u] += msg;
      }
      // distribute u label to mirrors
      SyncLabelOnInnerVertex(frag, ctx, messages, 0);
    } else {
      // get outer vertex's label
      std::pair<vid_t, label_t> msg;

      while (messages.GetMessage(msg)) {
        vertex_t u(0);
        CHECK(frag.Gid2Vertex(msg.first, u));
        auto v_label = frag.vertex_label(u);

        label[v_label][u] = msg.second;
      }

      auto v_label = step % 2 == 0 ? 1 : 0;
      auto inner_vertices = frag.InnerVertices(v_label);
      // u2i stage
      if (step % 2 == 0) {
        // pull i label from u label along incoming edges
        for (auto u : inner_vertices) {
          auto ies = frag.GetIncomingAdjList(u, 0);

          label[v_label][u].clear();
          for (auto& e : ies) {
            auto v = e.neighbor();
            auto edata = e.template get_data<edata_t>(0);

            for (auto prop_id = 0u; prop_id < prop_num; prop_id++) {
              label[v_label][u][prop_id] +=
                  label[frag.vertex_label(v)][v][prop_id] * edata;
            }
          }
        }
        SyncLabelOnInnerVertex(frag, ctx, messages, v_label);

      } else {
        // i2u stage
        grape::VertexArray<typename FRAG_T::inner_vertices_t, label_t>
            inner_tmp_label;
        grape::VertexArray<typename FRAG_T::inner_vertices_t, label_t>
            inner_new_label;

        inner_tmp_label.Init(inner_vertices);
        inner_new_label.Init(inner_vertices);

        // u_label part1
        for (auto u : inner_vertices) {
          auto oes = frag.GetOutgoingAdjList(u, 0);

          inner_tmp_label[u].resize(prop_num, 0);
          for (auto& e : oes) {
            auto v = e.neighbor();
            auto edata = e.template get_data<edata_t>(0);

            for (auto prop_id = 0u; prop_id < prop_num; prop_id++) {
              inner_tmp_label[u][prop_id] +=
                  label[frag.vertex_label(v)][v][prop_id] * edata;
            }
          }
        }

        // u_label part2
        auto& out_degree = ctx.out_degree[v_label];
        auto& out_nbr_in_degree_sum = ctx.out_nbr_in_degree_sum[v_label];

        for (auto u : inner_vertices) {
          inner_new_label[u].resize(prop_num);

          for (auto prop_id = 0u; prop_id < prop_num; prop_id++) {
            if (label[v_label][u][prop_id] == 0 ||
                label[v_label][u][prop_id] == 1) {
              inner_new_label[u][prop_id] = label[v_label][u][prop_id];
            } else {
              if (out_nbr_in_degree_sum[u] != out_degree[u]) {
                inner_new_label[u][prop_id] =
                    (inner_tmp_label[u][prop_id] -
                     out_degree[u] * label[v_label][u][prop_id]) /
                    (out_nbr_in_degree_sum[u] - out_degree[u]);
              } else {
                inner_new_label[u][prop_id] = label[v_label][u][prop_id];
              }
            }
          }
        }

        for (auto u : inner_vertices) {
          label[v_label][u] = inner_new_label[u];
        }

        SyncLabelOnInnerVertex(frag, ctx, messages, v_label);
      }
    }
    if (frag.fnum() == 1) {
      messages.ForceContinue();
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_LPA_LPA_U2I_H_
