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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_PROPERTY_MESSAGE_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_PROPERTY_MESSAGE_MANAGER_H_

#include "grape/graph/adj_list.h"
#include "grape/parallel/default_message_manager.h"

namespace gs {

/**
 * @brief Property message manager.
 *
 * The send and recv methods are not thread-safe.
 */
class PropertyMessageManager : public grape::DefaultMessageManager {
 public:
  /**
   * @brief Communication via a crossing edge a<-c. It sends message
   * from a to c.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughIEdges(const GRAPH_T& frag,
                                   const typename GRAPH_T::vertex_t& v,
                                   const typename GRAPH_T::label_id_t e_label,
                                   const MESSAGE_T& msg) {
    grape::DestList dsts = frag.IEDests(v, e_label);
    auto* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = frag.GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      auto fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }

  /**
   * @brief Communication via a crossing edge a->b. It sends message
   * from a to b.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughOEdges(const GRAPH_T& frag,
                                   const typename GRAPH_T::vertex_t& v,
                                   const typename GRAPH_T::label_id_t e_label,
                                   const MESSAGE_T& msg) {
    auto dsts = frag.OEDests(v, e_label);
    auto* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = frag.GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      auto fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }

  /**
   * @brief Communication via crossing edges a->b and a<-c. It sends message
   * from a to b and c.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughEdges(const GRAPH_T& frag,
                                  const typename GRAPH_T::vertex_t& v,
                                  const typename GRAPH_T::label_id_t e_label,
                                  const MESSAGE_T& msg) {
    auto dsts = frag.IOEDests(v, e_label);
    auto* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = frag.GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      auto fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_PROPERTY_MESSAGE_MANAGER_H_
