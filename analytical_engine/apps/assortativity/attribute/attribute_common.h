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

Author: Ning Xin
*/

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ATTRIBUTE_COMMON_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ATTRIBUTE_COMMON_H_

#include <unordered_map>

namespace gs {

/**
 * @brief count the attribute-attribute pairs
 *
 * @param source_data the data of source node
 * @param target_data the data of target node
 * @param ctx
 */
template <typename vdata_t, typename context_t>
inline void AttributeMixingCount(vdata_t source_data, vdata_t target_data,
                                 context_t& ctx) {
  if (ctx.attribute_mixing_map.count(source_data) == 0 ||
      ctx.attribute_mixing_map[source_data].count(target_data) == 0) {
    ctx.attribute_mixing_map[source_data][target_data] = 1;
  } else {
    ctx.attribute_mixing_map[source_data][target_data] += 1;
  }
}

/**
 * @brief traverse the outgoing neighbors of vertex v and update the
 * attribute-attribute pairs.
 *
 * @param v
 * @param frag
 * @param ctx
 * @param messages
 */
template <typename vdata_t, typename vertex_t, typename fragment_t,
          typename context_t, typename message_manager_t>
void ProcessVertex(const vertex_t& v, const fragment_t& frag, context_t& ctx,
                   message_manager_t& messages) {
  vdata_t source_data = frag.GetData(v);
  // get all neighbors of vertex v
  auto oes = frag.GetOutgoingAdjList(v);
  for (auto& e : oes) {
    vertex_t neighbor = e.get_neighbor();
    if (frag.IsOuterVertex(neighbor)) {
      messages.SyncStateOnOuterVertex(frag, neighbor, source_data);
    } else {
      vdata_t target_data = frag.GetData(neighbor);
      AttributeMixingCount(source_data, target_data, ctx);
    }
  }
}
/**
 * @brief update the attribute-attribute pairs from the outer vertex.
 *
 * @param frag
 * @param ctx
 * @param messages
 */
template <typename vertex_t, typename vdata_t, typename fragment_t,
          typename context_t, typename message_manager_t>
void UpdateAttributeMixingMap(const fragment_t& frag, context_t& ctx,
                              message_manager_t& messages) {
  vdata_t source_data;
  vertex_t u;
  // update attribute mixing map
  while (messages.GetMessage(frag, u, source_data)) {
    vdata_t target_data = frag.GetData(u);
    AttributeMixingCount(source_data, target_data, ctx);
  }
  ctx.merge_stage = true;
  // send message to worker 0
  if (frag.fid() != 0) {
    messages.SendToFragment(0, ctx.attribute_mixing_map);
  }
  messages.ForceContinue();
}

/**
 * @brief merge attribute mixing map of all workers in worker 0 and the result
 * is saved in the contxt of worker 0.
 *
 * @param ctx
 * @param messages
 *
 */
template <typename vdata_t, typename context_t, typename message_manager_t>
void MergeAttributeMixingMap(context_t& ctx, message_manager_t& messages) {
  std::unordered_map<vdata_t, std::unordered_map<vdata_t, int>> msg;
  while (messages.GetMessage(msg)) {
    for (auto& pair1 : msg) {
      for (auto& pair2 : pair1.second) {
        // merge
        if (ctx.attribute_mixing_map.count(pair1.first) == 0 ||
            ctx.attribute_mixing_map[pair1.first].count(pair2.first) == 0) {
          ctx.attribute_mixing_map[pair1.first][pair2.first] = pair2.second;
        } else {
          ctx.attribute_mixing_map[pair1.first][pair2.first] += pair2.second;
        }
      }
    }
  }
}
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ATTRIBUTE_COMMON_H_
