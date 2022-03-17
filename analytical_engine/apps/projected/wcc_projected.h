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

#ifndef ANALYTICAL_ENGINE_APPS_PROJECTED_WCC_PROJECTED_H_
#define ANALYTICAL_ENGINE_APPS_PROJECTED_WCC_PROJECTED_H_

#include <queue>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/app/app_base.h"

namespace gs {

template <typename FRAG_T>
class WCCProjectedContext
    : public grape::VertexDataContext<FRAG_T, typename FRAG_T::vid_t> {
  using vid_t = typename FRAG_T::vid_t;

 public:
  explicit WCCProjectedContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, typename FRAG_T::vid_t>(fragment,
                                                                 true),
        comp_id(this->data()) {}

  void Init(grape::DefaultMessageManager& messages) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();

    curr_modified.Init(vertices, false);
    next_modified.Init(vertices, false);
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto iv = frag.InnerVertices();

    for (auto v : iv) {
      os << frag.GetId(v) << " " << comp_id[v] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<vid_t>& comp_id;
  typename FRAG_T::template vertex_array_t<bool> curr_modified;
  typename FRAG_T::template vertex_array_t<bool> next_modified;
};

template <typename FRAG_T>
class WCCProjected : public AppBase<FRAG_T, WCCProjectedContext<FRAG_T>> {
 public:
  INSTALL_DEFAULT_WORKER(WCCProjected<FRAG_T>, WCCProjectedContext<FRAG_T>,
                         FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();
    auto vertices = frag.Vertices();

    for (auto v : inner_vertices) {
      ctx.comp_id[v] = frag.GetInnerVertexGid(v);
    }
    for (auto v : outer_vertices) {
      ctx.comp_id[v] = frag.GetOuterVertexGid(v);
    }

    for (auto v : inner_vertices) {
      auto cid = ctx.comp_id[v];

      auto es = frag.GetOutgoingAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        if (ctx.comp_id[u] > cid) {
          ctx.comp_id[u] = cid;
          ctx.next_modified[u] = true;
        }
      }

      if (frag.directed()) {
        es = frag.GetIncomingAdjList(v);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.comp_id[u] > cid) {
            ctx.comp_id[u] = cid;
            ctx.next_modified[u] = true;
          }
        }
      }
    }

    for (auto v : outer_vertices) {
      if (ctx.next_modified[v]) {
        messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                           ctx.comp_id[v]);
        ctx.next_modified[v] = false;
      }
    }
    for (auto v : inner_vertices) {
      if (ctx.next_modified[v]) {
        messages.ForceContinue();
        break;
      }
    }
    ctx.next_modified.Swap(ctx.curr_modified);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               grape::DefaultMessageManager& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();
    auto vertices = frag.Vertices();

    {
      vertex_t v(0);
      vid_t val;
      while (messages.GetMessage<fragment_t, vid_t>(frag, v, val)) {
        if (ctx.comp_id[v] > val) {
          ctx.comp_id[v] = val;
          ctx.curr_modified[v] = true;
        }
      }
    }

    for (auto v : inner_vertices) {
      if (!ctx.curr_modified[v]) {
        continue;
      }
      ctx.curr_modified[v] = false;
      auto cid = ctx.comp_id[v];

      auto es = frag.GetOutgoingAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        if (ctx.comp_id[u] > cid) {
          ctx.comp_id[u] = cid;
          ctx.next_modified[u] = true;
        }
      }

      if (frag.directed()) {
        es = frag.GetIncomingAdjList(v);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.comp_id[u] > cid) {
            ctx.comp_id[u] = cid;
            ctx.next_modified[u] = true;
          }
        }
      }
    }

    for (auto v : outer_vertices) {
      if (ctx.next_modified[v]) {
        messages.SyncStateOnOuterVertex<fragment_t, vid_t>(frag, v,
                                                           ctx.comp_id[v]);
        ctx.next_modified[v] = false;
      }
    }
    for (auto v : inner_vertices) {
      if (ctx.next_modified[v]) {
        messages.ForceContinue();
        break;
      }
    }
    ctx.curr_modified.Swap(ctx.next_modified);
  }
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_APPS_PROJECTED_WCC_PROJECTED_H_
