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

#ifndef ANALYTICAL_ENGINE_APPS_PROPERTY_AUTO_SSSP_PROPERTY_H_
#define ANALYTICAL_ENGINE_APPS_PROPERTY_AUTO_SSSP_PROPERTY_H_

#include <limits>
#include <vector>

#include "grape/grape.h"

#include "core/app/property_auto_app_base.h"
#include "core/context/vertex_data_context.h"

namespace gs {
/**
 * @brief An SSSP implementation for labeled graph without explicit message API
 * calling.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class AutoSSSPPropertyContext
    : public LabeledVertexDataContext<FRAG_T, double> {
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;
  using label_id_t = typename FRAG_T::label_id_t;

 public:
  explicit AutoSSSPPropertyContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, double>(fragment),
        data_(this->data()) {}

  void Init(PropertyAutoMessageManager<FRAG_T>& messages, oid_t source_id_) {
    auto& frag = this->fragment();
    auto v_label_num = frag.vertex_label_num();

    source_id = source_id_;

    for (label_id_t v_label = 0; v_label != v_label_num; ++v_label) {
      partial_result.emplace_back(data_[v_label]);
    }

    for (label_id_t v_label = 0; v_label != v_label_num; ++v_label) {
      auto vertices = frag.Vertices(v_label);
      partial_result[v_label].Init(vertices, std::numeric_limits<double>::max(),
                                   [](double* lhs, double rhs) {
                                     if (*lhs > rhs) {
                                       *lhs = rhs;
                                       return true;
                                     } else {
                                       return false;
                                     }
                                   });
      messages.RegisterSyncBuffer(frag, v_label, &partial_result[v_label],
                                  grape::MessageStrategy::kSyncOnOuterVertex);
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    for (label_id_t i = 0; i < frag.vertex_label_num(); ++i) {
      auto iv = frag.InnerVertices(i);
      for (auto v : iv) {
        double d = partial_result[i][v];

        os << frag.GetId(v) << "\t" << d << std::endl;
      }
    }
  }

  oid_t source_id;
  std::vector<grape::VertexArray<typename FRAG_T::vertices_t, double>>& data_;
  std::vector<grape::SyncBuffer<typename FRAG_T::vertices_t, double>>
      partial_result;
};

template <typename FRAG_T>
class AutoSSSPProperty
    : public PropertyAutoAppBase<FRAG_T, AutoSSSPPropertyContext<FRAG_T>> {
 public:
  INSTALL_AUTO_PROPERTY_WORKER(AutoSSSPProperty<FRAG_T>,
                               AutoSSSPPropertyContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;

  using vertex_t = typename FRAG_T::vertex_t;
  using label_id_t = typename FRAG_T::label_id_t;

  void PEval(const fragment_t& frag, context_t& ctx) {
    vertex_t source;
    bool native_source = false;

    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();

    for (label_id_t i = 0; i < v_label_num; ++i) {
      native_source = frag.GetInnerVertex(i, ctx.source_id, source);
      if (native_source) {
        break;
      }
    }

    if (native_source) {
      label_id_t label = frag.vertex_label(source);
      ctx.partial_result[label].SetValue(source, 0);
    } else {
      return;
    }

    for (label_id_t j = 0; j < e_label_num; ++j) {
      auto es = frag.GetOutgoingAdjList(source, j);
      for (auto& e : es) {
        auto u = e.neighbor();
        auto u_dist = static_cast<double>(e.template get_data<int64_t>(0));
        label_id_t u_label = frag.vertex_label(u);
        if (ctx.partial_result[u_label][u] > u_dist) {
          ctx.partial_result[u_label].SetValue(u, u_dist);
        }
      }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);

      for (auto v : iv) {
        auto v_dist = ctx.partial_result[i][v];

        for (label_id_t j = 0; j < e_label_num; ++j) {
          auto es = frag.GetOutgoingAdjList(v, j);
          for (auto& e : es) {
            auto u = e.neighbor();
            auto u_dist =
                v_dist + static_cast<double>(e.template get_data<int64_t>(0));
            label_id_t u_label = frag.vertex_label(u);
            if (ctx.partial_result[u_label][u] > u_dist) {
              ctx.partial_result[u_label].SetValue(u, u_dist);
            }
          }
        }
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PROPERTY_AUTO_SSSP_PROPERTY_H_
