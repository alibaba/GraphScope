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

#ifndef ANALYTICAL_ENGINE_APPS_PROPERTY_WCC_PROPERTY_H_
#define ANALYTICAL_ENGINE_APPS_PROPERTY_WCC_PROPERTY_H_

#include <vector>

#include "grape/grape.h"

#include "core/app/property_app_base.h"
#include "core/context/vertex_data_context.h"

namespace gs {
/**
 * @brief A connected component algorithm for labeled graph in parallel
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCPropertyContext
    : public LabeledVertexDataContext<FRAG_T, typename FRAG_T::vid_t> {
  using vid_t = typename FRAG_T::vid_t;
  using label_id_t = typename FRAG_T::label_id_t;

 public:
  explicit WCCPropertyContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, typename FRAG_T::vid_t>(fragment,
                                                                 true),
        comp_id(this->data()) {}

  void Init(grape::DefaultMessageManager& messages) {
    auto& frag = this->fragment();
    auto v_label_num = frag.vertex_label_num();

    curr_modified.resize(v_label_num);
    next_modified.resize(v_label_num);

    for (label_id_t v_label = 0; v_label != v_label_num; ++v_label) {
      auto vertices = frag.Vertices(v_label);
      curr_modified[v_label].Init(vertices, false);
      next_modified[v_label].Init(vertices, false);
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    for (label_id_t i = 0; i < frag.vertex_label_num(); ++i) {
      auto iv = frag.InnerVertices(i);
      for (auto v : iv) {
        os << frag.GetId(v) << " " << comp_id[i][v] << std::endl;
      }
    }
  }

  std::vector<typename FRAG_T::template vertex_array_t<vid_t>>& comp_id;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> curr_modified;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> next_modified;
};

template <typename FRAG_T>
class WCCProperty : public PropertyAppBase<FRAG_T, WCCPropertyContext<FRAG_T>> {
 public:
  INSTALL_DEFAULT_PROPERTY_WORKER(WCCProperty<FRAG_T>,
                                  WCCPropertyContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;

  using vertex_t = typename FRAG_T::vertex_t;
  using label_id_t = typename FRAG_T::label_id_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto& cur_comp = ctx.comp_id[i];
      auto iv = frag.InnerVertices(i);
      for (auto v : iv) {
        cur_comp[v] = frag.GetInnerVertexGid(v);
      }
      auto ov = frag.OuterVertices(i);
      for (auto v : ov) {
        cur_comp[v] = frag.GetOuterVertexGid(v);
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);

      for (auto v : iv) {
        auto cid = ctx.comp_id[i][v];

        for (label_id_t j = 0; j < e_label_num; ++j) {
          auto es = frag.GetOutgoingAdjList(v, j);
          for (auto& e : es) {
            auto u = e.neighbor();
            label_id_t u_label = frag.vertex_label(u);
            if (ctx.comp_id[u_label][u] > cid) {
              ctx.comp_id[u_label][u] = cid;
              ctx.next_modified[u_label][u] = true;
            }
          }

          if (frag.directed()) {
            es = frag.GetIncomingAdjList(v, j);
            for (auto& e : es) {
              auto u = e.neighbor();
              label_id_t u_label = frag.vertex_label(u);
              if (ctx.comp_id[u_label][u] > cid) {
                ctx.comp_id[u_label][u] = cid;
                ctx.next_modified[u_label][u] = true;
              }
            }
          }
        }
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto ov = frag.OuterVertices(i);
      for (auto v : ov) {
        if (ctx.next_modified[i][v]) {
          messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                             ctx.comp_id[i][v]);
          ctx.next_modified[i][v] = false;
        }
      }
    }

    ctx.curr_modified.swap(ctx.next_modified);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    {
      vertex_t v(0);
      vid_t val;
      while (messages.GetMessage<fragment_t, vid_t>(frag, v, val)) {
        label_id_t v_label = frag.vertex_label(v);
        if (ctx.comp_id[v_label][v] > val) {
          ctx.comp_id[v_label][v] = val;
          ctx.curr_modified[v_label][v] = true;
        }
      }
    }

    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();
    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);

      for (auto v : iv) {
        if (!ctx.curr_modified[i][v]) {
          continue;
        }
        ctx.curr_modified[i][v] = false;
        auto cid = ctx.comp_id[i][v];

        for (label_id_t j = 0; j < e_label_num; ++j) {
          auto es = frag.GetOutgoingAdjList(v, j);
          for (auto& e : es) {
            auto u = e.neighbor();
            label_id_t u_label = frag.vertex_label(u);
            if (ctx.comp_id[u_label][u] > cid) {
              ctx.comp_id[u_label][u] = cid;
              ctx.next_modified[u_label][u] = true;
            }
          }

          if (frag.directed()) {
            es = frag.GetIncomingAdjList(v, j);
            for (auto& e : es) {
              auto u = e.neighbor();
              label_id_t u_label = frag.vertex_label(u);
              if (ctx.comp_id[u_label][u] > cid) {
                ctx.comp_id[u_label][u] = cid;
                ctx.next_modified[u_label][u] = true;
              }
            }
          }
        }
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto ov = frag.OuterVertices(i);
      for (auto v : ov) {
        if (ctx.next_modified[i][v]) {
          messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                             ctx.comp_id[i][v]);
          ctx.next_modified[i][v] = false;
        }
      }
    }
    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);
      bool ok = false;
      for (auto v : iv) {
        if (ctx.next_modified[i][v]) {
          messages.ForceContinue();
          ok = true;
          break;
        }
      }
      if (ok) {
        break;
      }
    }
    ctx.curr_modified.swap(ctx.next_modified);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PROPERTY_WCC_PROPERTY_H_
