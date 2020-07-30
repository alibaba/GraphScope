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

#ifndef ANALYTICAL_ENGINE_APPS_PROPERTY_SSSP_PROPERTY_APPEND_H_
#define ANALYTICAL_ENGINE_APPS_PROPERTY_SSSP_PROPERTY_APPEND_H_
#include <iomanip>
#include <limits>

#include <vector>

#include "grape/grape.h"

#include "core/app/property_app_base.h"
#include "core/context/vertex_data_context.h"

namespace gs {

/**
 * An SSSP implementation for labeled appendable graph
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSPPropertyAppendContext
    : public LabeledVertexDataContext<FRAG_T, double> {
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;

 public:
  explicit SSSPPropertyAppendContext(const FRAG_T& fragment)
      : LabeledVertexDataContext<FRAG_T, double>(fragment, true),
        comp_id(this->data()) {}

  void Init(grape::DefaultMessageManager& messages, oid_t source_id_) {
    auto& frag = this->fragment();
    auto v_label_num = frag.vertex_label_num();

    source_id = source_id_;
    curr_modified.resize(v_label_num);
    next_modified.resize(v_label_num);

    for (auto v_label = 0; v_label != v_label_num; ++v_label) {
      auto vertices = frag.Vertices(v_label);
      comp_id[v_label].SetValue(std::numeric_limits<double>::max());
      curr_modified[v_label].Init(vertices, false);
      next_modified[v_label].Init(vertices, false);
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    for (auto i = 0; i < frag.vertex_label_num(); ++i) {
      auto iv = frag.InnerVertices(i);
      for (auto v : iv) {
        double d = comp_id[i][v];
        if (d == std::numeric_limits<double>::max()) {
          os << frag.GetId(v) << " infinity" << std::endl;
        } else {
          os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
             << d << std::endl;
        }
      }
    }
  }

  oid_t source_id;
  std::vector<typename FRAG_T::template vertex_array_t<double>>& comp_id;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> curr_modified;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> next_modified;
};

template <typename FRAG_T>
class SSSPPropertyAppend
    : public PropertyAppBase<FRAG_T, SSSPPropertyAppendContext<FRAG_T>> {
 public:
  INSTALL_DEFAULT_PROPERTY_WORKER(SSSPPropertyAppend<FRAG_T>,
                                  SSSPPropertyAppendContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;

  using vertex_t = typename FRAG_T::vertex_t;
  using label_id_t = typename FRAG_T::label_id_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
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
      ctx.comp_id[label][source] = 0;
    } else {
      return;
    }

    for (label_id_t j = 0; j < e_label_num; ++j) {
      auto es = frag.GetOutgoingAdjList(source, j);
      for (auto& e : es) {
        auto u = e.neighbor();
        auto u_dist = static_cast<double>(e.template get_data<int64_t>(0));
        label_id_t u_label = frag.vertex_label(u);

        if (ctx.comp_id[u_label][u] > u_dist) {
          ctx.comp_id[u_label][u] = u_dist;
          ctx.next_modified[u_label][u] = true;
        }
      }

      auto extra_es = frag.GetExtraOutgoingAdjList(source, j);
      for (auto& e : extra_es) {
        auto u = e.neighbor();
        auto u_dist = static_cast<double>(e.template get_data<int64_t>(0));
        label_id_t u_label = frag.vertex_label(u);

        if (ctx.comp_id[u_label][u] > u_dist) {
          ctx.comp_id[u_label][u] = u_dist;
          ctx.next_modified[u_label][u] = true;
        }
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto ov = frag.OuterVertices(i);
      for (auto v : ov) {
        if (ctx.next_modified[i][v]) {
          messages.SyncStateOnOuterVertex<fragment_t, double>(
              frag, v, ctx.comp_id[i][v]);
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

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    {
      vertex_t v(0);
      double val;
      while (messages.GetMessage<fragment_t, double>(frag, v, val)) {
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
        auto v_dist = ctx.comp_id[i][v];

        for (label_id_t j = 0; j < e_label_num; ++j) {
          auto es = frag.GetOutgoingAdjList(v, j);
          for (auto& e : es) {
            auto u = e.neighbor();
            auto u_dist =
                v_dist + static_cast<double>(e.template get_data<int64_t>(0));
            label_id_t u_label = frag.vertex_label(u);
            if (ctx.comp_id[u_label][u] > u_dist) {
              ctx.comp_id[u_label][u] = u_dist;
              ctx.next_modified[u_label][u] = true;
            }
          }

          auto extra_es = frag.GetExtraOutgoingAdjList(v, j);
          for (auto& e : extra_es) {
            auto u = e.neighbor();
            auto u_dist =
                v_dist + static_cast<double>(e.template get_data<int64_t>(0));
            label_id_t u_label = frag.vertex_label(u);

            if (ctx.comp_id[u_label][u] > u_dist) {
              ctx.comp_id[u_label][u] = u_dist;
              ctx.next_modified[u_label][u] = true;
            }
          }
        }
      }
    }

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto ov = frag.OuterVertices(i);
      for (auto v : ov) {
        if (ctx.next_modified[i][v]) {
          messages.SyncStateOnOuterVertex<fragment_t, double>(
              frag, v, ctx.comp_id[i][v]);
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

#endif  // ANALYTICAL_ENGINE_APPS_PROPERTY_SSSP_PROPERTY_APPEND_H_
