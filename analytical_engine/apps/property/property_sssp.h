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

#ifndef ANALYTICAL_ENGINE_APPS_PROPERTY_PROPERTY_SSSP_H_
#define ANALYTICAL_ENGINE_APPS_PROPERTY_PROPERTY_SSSP_H_

#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>

#include "grape/grape.h"

#include "core/app/property_app_base.h"
#include "core/context/labeled_vertex_property_context.h"
#include "core/context/vertex_property_context.h"
#include "core/parallel/property_message_manager.h"

namespace gs {

/**
 * A sssp implementation for labeled graph
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PropertySSSPContext : public LabeledVertexPropertyContext<FRAG_T> {
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;

 public:
  explicit PropertySSSPContext(const FRAG_T& fragment)
      : LabeledVertexPropertyContext<FRAG_T>(fragment) {}

  void Init(PropertyMessageManager& messages, oid_t source_id_) {
    auto& frag = this->fragment();
    source_id = source_id_;

    typename FRAG_T::label_id_t v_label_num = frag.vertex_label_num();
    comp_id.resize(v_label_num);
    curr_modified.resize(v_label_num);
    next_modified.resize(v_label_num);
    dist_column_indices.resize(v_label_num);

    for (typename FRAG_T::label_id_t v_label = 0; v_label != v_label_num;
         ++v_label) {
      auto vertices = frag.Vertices(v_label);
      comp_id[v_label].Init(vertices, std::numeric_limits<double>::max());
      curr_modified[v_label].Init(vertices, false);
      next_modified[v_label].Init(vertices, false);

      dist_column_indices[v_label] = this->add_column(
          v_label, "dist_" + std::to_string(v_label), ContextDataType::kDouble);
    }
  }

  std::vector<typename FRAG_T::template vertex_array_t<double>> comp_id;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> curr_modified;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> next_modified;

  std::vector<int64_t> dist_column_indices;
  oid_t source_id;
};

template <typename FRAG_T>
class PropertySSSP
    : public PropertyAppBase<FRAG_T, PropertySSSPContext<FRAG_T>> {
 public:
  INSTALL_DEFAULT_PROPERTY_WORKER(PropertySSSP<FRAG_T>,
                                  PropertySSSPContext<FRAG_T>, FRAG_T)

  static constexpr bool need_split_edges = false;
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();
    bool native_source = false;

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

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);
      auto column =
          ctx.template get_typed_column<double>(i, ctx.dist_column_indices[i]);
      for (auto v : iv) {
        column->at(v) = ctx.comp_id[i][v];
      }
    }
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

    for (label_id_t i = 0; i < v_label_num; ++i) {
      auto iv = frag.InnerVertices(i);
      auto column =
          ctx.template get_typed_column<double>(i, ctx.dist_column_indices[i]);
      for (auto v : iv) {
        column->at(v) = ctx.comp_id[i][v];
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PROPERTY_PROPERTY_SSSP_H_
