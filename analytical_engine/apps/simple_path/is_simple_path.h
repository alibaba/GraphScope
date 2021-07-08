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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PTAH_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PTAH_H_

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
  INSTALL_DEFAULT_WORKER(IsSimplePath<FRAG_T>, IsSimplePathContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    oid_t first_v,second_v;
    int true_counter = 0;
    if(ctx.is_simple_path == false)
      true_counter = 1;
    else{
      for(auto pl :ctx.pair_list){                             
        first_v = pl.first;
        second_v = pl.second;
        if(frag.GetInnerVertex(first_v, source)){                                     
          auto oes = frag.GetOutgoingAdjList(source);
          bool has_pair = false;
          for (auto& e : oes){                                 
            vertex_t u = e.get_neighbor();
            oid_t compare_node=GetId(u);
            if(compare_node == second_v){             
              has_pair = true;
              break;
            }
          }
          if(!has_pair){                                       
            true_counter = 1;
            break ;
          }
        }
      }
    }
    Sum(true_counter, ctx.true_counter);
    if(ctx.true_counter == 0)
      ctx.is_simple_path = true;
    else 
      ctx.is_simple_path = false;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
   
  }
 
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PTAH_H_
