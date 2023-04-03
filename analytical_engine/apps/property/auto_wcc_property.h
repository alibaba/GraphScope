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

#ifndef ANALYTICAL_ENGINE_APPS_PROPERTY_AUTO_WCC_PROPERTY_H_
#define ANALYTICAL_ENGINE_APPS_PROPERTY_AUTO_WCC_PROPERTY_H_

#include <limits>
#include <vector>

#include "grape/grape.h"

#include "core/app/property_auto_app_base.h"
#include "core/context/vertex_data_context.h"

namespace gs {
/**
 * @brief A connected component algorithm for labeled graph without explicit API
 * calling
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class AutoWCCPropertyContext
    : public LabeledVertexDataContext<FRAG_T, typename FRAG_T::vid_t> {
  using vid_t = typename FRAG_T::vid_t;
  using label_id_t = typename FRAG_T::label_id_t;

 public:
  explicit AutoWCCPropertyContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, typename FRAG_T::vid_t>(fragment) {}

  void Init(PropertyAutoMessageManager<FRAG_T>& messages) {
    auto& frag = this->fragment();
    auto v_label_num = frag.vertex_label_num();
    partial_result.resize(v_label_num);

    for (label_id_t v_label = 0; v_label != v_label_num; ++v_label) {
      auto vertices = frag.Vertices(v_label);
      grape::SyncBuffer<typename FRAG_T::vertices_t, vid_t> tmp_buffer(
          this->data()[v_label]);
      partial_result[v_label].Swap(tmp_buffer);
      partial_result[v_label].Init(vertices, std::numeric_limits<vid_t>::max(),
                                   [](vid_t* lhs, vid_t rhs) {
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
        os << frag.GetId(v) << " " << partial_result[i][v] << std::endl;
      }
    }
  }

  std::vector<grape::SyncBuffer<typename FRAG_T::vertices_t, vid_t>>
      partial_result;
};

template <typename FRAG_T>
class AutoWCCProperty
    : public PropertyAutoAppBase<FRAG_T, AutoWCCPropertyContext<FRAG_T>> {
 public:
  INSTALL_AUTO_PROPERTY_WORKER(AutoWCCProperty<FRAG_T>,
                               AutoWCCPropertyContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;

  using vertex_t = typename FRAG_T::vertex_t;
  using label_id_t = typename FRAG_T::label_id_t;

  void PEval(const fragment_t& frag, context_t& ctx) {
    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto& cur_comp = ctx.partial_result[i];
      auto iv = frag.InnerVertices(i);
      for (auto v : iv) {
        cur_comp.SetValue(v, frag.GetInnerVertexGid(v));
        cur_comp.Reset(v);
      }
      auto ov = frag.OuterVertices(i);
      for (auto v : ov) {
        cur_comp.SetValue(v, frag.GetOuterVertexGid(v));
        cur_comp.Reset(v);
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);

      for (auto v : iv) {
        auto cid = ctx.partial_result[i][v];

        for (label_id_t j = 0; j < e_label_num; ++j) {
          auto es = frag.GetOutgoingAdjList(v, j);
          for (auto& e : es) {
            auto u = e.neighbor();
            label_id_t u_label = frag.vertex_label(u);
            if (ctx.partial_result[u_label][u] > cid) {
              ctx.partial_result[u_label].SetValue(u, cid);
            }
          }

          if (frag.directed()) {
            es = frag.GetIncomingAdjList(v, j);
            for (auto& e : es) {
              auto u = e.neighbor();
              label_id_t u_label = frag.vertex_label(u);
              if (ctx.partial_result[u_label][u] > cid) {
                ctx.partial_result[u_label].SetValue(u, cid);
              }
            }
          }
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
        auto cid = ctx.partial_result[i][v];

        for (label_id_t j = 0; j < e_label_num; ++j) {
          auto es = frag.GetOutgoingAdjList(v, j);
          for (auto& e : es) {
            auto u = e.neighbor();
            label_id_t u_label = frag.vertex_label(u);
            if (ctx.partial_result[u_label][u] > cid) {
              ctx.partial_result[u_label].SetValue(u, cid);
            }
          }

          if (frag.directed()) {
            es = frag.GetIncomingAdjList(v, j);
            for (auto& e : es) {
              auto u = e.neighbor();
              label_id_t u_label = frag.vertex_label(u);
              if (ctx.partial_result[u_label][u] > cid) {
                ctx.partial_result[u_label].SetValue(u, cid);
              }
            }
          }
        }
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PROPERTY_AUTO_WCC_PROPERTY_H_
