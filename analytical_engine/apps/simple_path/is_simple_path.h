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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_H_

#include <utility>
#include <vector>

#include "simple_path/is_simple_path_context.h"

#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {

template <typename FRAG_T>
class IsSimplePath : public AppBase<FRAG_T, IsSimplePathContext<FRAG_T>>,
                     public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(IsSimplePath<FRAG_T>, IsSimplePathContext<FRAG_T>,
                         FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    vid_t first_v, second_v;
    int true_counter = 0;
    // First deal with the case where there is only one point in the list.
    if (ctx.counter == 1) {
      if (ctx.is_simple_path == false)
        true_counter = 0;
      else
        true_counter = 1;
    } else if (ctx.is_simple_path == false) {
      true_counter = 1;
    } else {
      for (auto pl : ctx.pair_list) {
        first_v = pl.first;
        second_v = pl.second;
        bool has_pair = false;
        if (frag.InnerVertexGid2Vertex(first_v, source)) {
          auto oes = frag.GetOutgoingAdjList(source);
          for (auto& e : oes) {
            vertex_t u = e.get_neighbor();
            vid_t compare_node = frag.Vertex2Gid(u);
            if (compare_node == second_v) {
              has_pair = true;
              break;
            }
          }
        }
        if (!has_pair) {
          true_counter = 1;
          break;
        }
      }
    }
    Sum(true_counter, ctx.true_counter);

    if (ctx.counter == 1 && ctx.true_counter == 1) {
      ctx.is_simple_path = true;
    } else if (ctx.counter > 1 && ctx.true_counter == 0) {
      ctx.is_simple_path = true;
    } else {
      ctx.is_simple_path = false;
    }
    {
      if (frag.fid() == 0) {
        std::vector<size_t> shape{1};

        ctx.set_shape(shape);
        ctx.assign(ctx.is_simple_path);
      }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {}
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_H_
