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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_THREAD_LOCAL_PROPERTY_MESSAGE_BUFFER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_THREAD_LOCAL_PROPERTY_MESSAGE_BUFFER_H_

#include <memory>
#include <utility>
#include <vector>

#include "grape/graph/adj_list.h"
#include "grape/serialization/in_archive.h"

namespace gs {

/**
 * @brief ThreadLocalPropertyMessageBuffer provides buffers for label fragment
 * for a thread. Every thead should use individual
 * ThreadLocalPropertyMessageBuffer as the class name indicated.
 * @tparam MM_T
 */
template <typename MM_T>
class ThreadLocalPropertyMessageBuffer {
 public:
  /**
   * @brief Initialize thread local message buffer.
   *
   * @param fnum Number of fragments.
   * @param mm MessageManager pointer.
   * @param block_size Size of thread local message buffer.
   * @param block_cap Capacity of thread local message buffer.
   */
  void Init(grape::fid_t fnum, MM_T* mm, size_t block_size, size_t block_cap) {
    fnum_ = fnum;
    mm_ = mm;

    to_send_.clear();
    to_send_.resize(fnum_);

    block_size_ = block_size;
    block_cap_ = block_cap;

    for (auto& arc : to_send_) {
      arc.Reserve(block_cap_);
    }

    sent_size_ = 0;
  }

  /**
   * @brief Communication by synchronizing the status on outer vertices, for
   * edge-cut fragments.
   *
   * Assume a fragment F_1, a crossing edge a->b' in F_1 and a is an inner
   * vertex in F_1. This function invoked on F_1 send status on b' to b on
   * F_2, where b is an inner vertex.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type,
   * @param frag Source fragment.
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SyncStateOnOuterVertex(const GRAPH_T& frag,
                                     const typename GRAPH_T::vertex_t& v,
                                     const MESSAGE_T& msg) {
    grape::fid_t fid = frag.GetFragId(v);
    to_send_[fid] << frag.GetOuterVertexGid(v) << msg;
    if (to_send_[fid].GetSize() > block_size_) {
      flushLocalBuffer(fid);
    }
  }

  template <typename GRAPH_T>
  inline void SyncStateOnOuterVertex(const GRAPH_T& frag,
                                     const typename GRAPH_T::vertex_t& v) {
    grape::fid_t fid = frag.GetFragId(v);
    to_send_[fid] << frag.GetOuterVertexGid(v);
    if (to_send_[fid].GetSize() > block_size_) {
      flushLocalBuffer(fid);
    }
  }

  /**
   * @brief Communication via a crossing edge a<-c. It sends message
   * from a to c.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type,
   * @param frag Source fragment.
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughIEdges(const GRAPH_T& frag,
                                   const typename GRAPH_T::vertex_t& v,
                                   typename GRAPH_T::label_id_t label,
                                   const MESSAGE_T& msg) {
    grape::DestList dsts = frag.IEDests(v, label);
    const grape::fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = frag.GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      grape::fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
      if (to_send_[fid].GetSize() > block_size_) {
        flushLocalBuffer(fid);
      }
    }
  }

  /**
   * @brief Communication via a crossing edge a->b. It sends message
   * from a to b.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughOEdges(const GRAPH_T& frag,
                                   const typename GRAPH_T::vertex_t& v,
                                   typename GRAPH_T::label_id_t label,
                                   const MESSAGE_T& msg) {
    grape::DestList dsts = frag.OEDests(v, label);
    const grape::fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = frag.GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      grape::fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
      if (to_send_[fid].GetSize() > block_size_) {
        flushLocalBuffer(fid);
      }
    }
  }

  /**
   * @brief Communication via crossing edges a->b and a<-c. It sends message
   * from a to b and c.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v: a
   * @param msg
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughEdges(const GRAPH_T& frag,
                                  const typename GRAPH_T::vertex_t& v,
                                  typename GRAPH_T::label_id_t label,
                                  const MESSAGE_T& msg) {
    grape::DestList dsts = frag.IOEDests(v, label);
    const grape::fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = frag.GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      grape::fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
      if (to_send_[fid].GetSize() > block_size_) {
        flushLocalBuffer(fid);
      }
    }
  }

  /**
   * @brief Send message to a fragment.
   *
   * @tparam MESSAGE_T Message type.
   * @param dst_fid Destination fragment id.
   * @param msg
   */
  template <typename MESSAGE_T>
  inline void SendToFragment(grape::fid_t dst_fid, const MESSAGE_T& msg) {
    to_send_[dst_fid] << msg;
    if (to_send_[dst_fid].GetSize() > block_size_) {
      flushLocalBuffer(dst_fid);
    }
  }

  /**
   * @brief Flush messages to message manager.
   */
  inline void FlushMessages() {
    for (grape::fid_t fid = 0; fid < fnum_; ++fid) {
      if (to_send_[fid].GetSize() > 0) {
        sent_size_ += to_send_[fid].GetSize();
        flushLocalBuffer(fid);
      }
    }
  }

  size_t SentMsgSize() const { return sent_size_; }

  inline void Reset() { sent_size_ = 0; }

 private:
  inline void flushLocalBuffer(grape::fid_t fid) {
    mm_->SendRawMsgByFid(fid, std::move(to_send_[fid]));
    to_send_[fid].Reserve(block_cap_);
  }

  std::vector<grape::InArchive> to_send_;
  MM_T* mm_;
  grape::fid_t fnum_;

  size_t block_size_;
  size_t block_cap_;

  size_t sent_size_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_THREAD_LOCAL_PROPERTY_MESSAGE_BUFFER_H_
