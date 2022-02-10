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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_PARALLEL_PROPERTY_MESSAGE_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_PARALLEL_PROPERTY_MESSAGE_MANAGER_H_

#include <mpi.h>

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "grape/communication/sync_comm.h"
#include "grape/parallel/message_in_buffer.h"
#include "grape/parallel/message_manager_base.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"
#include "grape/worker/comm_spec.h"

#include "core/parallel/thread_local_property_message_buffer.h"

namespace gs {
/**
 * @brief A kind of parallel message manager.
 *
 * ParallelPropertyMessageManager support multi-threads to send messages
 * concurrently with channels. Each channel contains a thread local message
 * buffer.
 *
 * For each thread local message buffer, when accumulated a given amount of
 * messages, the buffer will be sent through MPI.
 *
 * After a round of evaluation, there is a global barrier to determine whether
 * the fixed point is reached.
 *
 */
class ParallelPropertyMessageManager : public grape::MessageManagerBase {
  static constexpr size_t default_msg_send_block_size = 2 * 1023 * 1024;
  static constexpr size_t default_msg_send_block_capacity = 2 * 1023 * 1024;

 public:
  ParallelPropertyMessageManager() : comm_(NULL_COMM) {}
  ~ParallelPropertyMessageManager() override {
    if (ValidComm(comm_)) {
      MPI_Comm_free(&comm_);
    }
  }

  /**
   * @brief Inherit
   */
  void Init(MPI_Comm comm) override {
    MPI_Comm_dup(comm, &comm_);
    comm_spec_.Init(comm_);
    fid_ = comm_spec_.fid();
    fnum_ = comm_spec_.fnum();

    force_terminate_ = false;
    terminate_info_.Init(fnum_);

    recv_queues_[0].SetProducerNum(fnum_);
    recv_queues_[1].SetProducerNum(fnum_);

    round_ = 0;

    sent_size_ = 0;
  }

  /**
   * @brief Inherit
   */
  void Start() override { startRecvThread(); }

  /**
   * @brief Inherit
   */
  void StartARound() override {
    if (round_ != 0) {
      waitSend();
      auto& rq = recv_queues_[round_ % 2];
      if (!to_self_.empty()) {
        for (auto& iarc : to_self_) {
          grape::OutArchive oarc(std::move(iarc));
          rq.Put(std::move(oarc));
        }
        to_self_.clear();
      }
      rq.DecProducerNum();
    }
    sent_size_ = 0;
    startSendThread();
  }

  /**
   * @brief Inherit
   */
  void FinishARound() override {
    sent_size_ = finishMsgFilling();
    resetRecvQueue();
    round_++;
  }

  /**
   * @brief Inherit
   */
  bool ToTerminate() override {
    int flag[2];
    flag[0] = 1;
    if (sent_size_ == 0 && !force_continue_) {
      flag[0] = 0;
    }
    flag[1] = force_terminate_ ? 1 : 0;
    int ret[2];
    MPI_Allreduce(&flag[0], &ret[0], 2, MPI_INT, MPI_SUM, comm_);
    if (ret[1] > 0) {
      terminate_info_.success = false;
      grape::sync_comm::AllGather(terminate_info_.info, comm_);
      return true;
    }
    return (ret[0] == 0);
  }

  /**
   * @brief Inherit
   */
  void Finalize() override {
    waitSend();
    MPI_Barrier(comm_);
    stopRecvThread();

    MPI_Comm_free(&comm_);
    comm_ = NULL_COMM;
  }

  /**
   * @brief Inherit
   */
  void ForceContinue() override { force_continue_ = true; }

  /**
   * @brief Inherit
   */
  void ForceTerminate(const std::string& terminate_info) override {
    force_terminate_ = true;
    terminate_info_.info[comm_spec_.fid()] = terminate_info;
  }

  /**
   * @brief Inherit
   */
  const grape::TerminateInfo& GetTerminateInfo() const override {
    return terminate_info_;
  }

  /**
   * @brief Inherit
   */
  size_t GetMsgSize() const override { return sent_size_; }

  /**
   * @brief Init a set of channels, each channel is a thread local message
   * buffer.
   *
   * @param channel_num Number of channels.
   * @param block_size Size of each channel.
   * @param block_cap Capacity of each channel.
   */
  void InitChannels(int channel_num = 1,
                    size_t block_size = default_msg_send_block_size,
                    size_t block_cap = default_msg_send_block_capacity) {
    channels_.resize(channel_num);
    for (auto& channel : channels_) {
      channel.Init(fnum_, this, block_size, block_cap);
    }
  }

  std::vector<ThreadLocalPropertyMessageBuffer<ParallelPropertyMessageManager>>&
  Channels() {
    return channels_;
  }

  /**
   * @brief Send a buffer to a fragment.
   *
   * @param fid Destination fragment id.
   * @param arc Message buffer.
   */
  inline void SendRawMsgByFid(grape::fid_t fid, grape::InArchive&& arc) {
    std::pair<grape::fid_t, grape::InArchive> item;
    item.first = fid;
    item.second = std::move(arc);
    sending_queue_.Put(std::move(item));
  }

  /**
   * @brief SyncStateOnOuterVertex on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SyncStateOnOuterVertex(const GRAPH_T& frag,
                                     const typename GRAPH_T::vertex_t& v,
                                     const MESSAGE_T& msg, int channel_id = 0) {
    channels_[channel_id].SyncStateOnOuterVertex<GRAPH_T, MESSAGE_T>(frag, v,
                                                                     msg);
  }

  template <typename GRAPH_T>
  inline void SyncStateOnOuterVertex(const GRAPH_T& frag,
                                     const typename GRAPH_T::vertex_t& v,
                                     int channel_id = 0) {
    channels_[channel_id].SyncStateOnOuterVertex<GRAPH_T>(frag, v);
  }

  /**
   * @brief SendMsgThroughIEdges on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughIEdges(const GRAPH_T& frag,
                                   const typename GRAPH_T::vertex_t& v,
                                   typename GRAPH_T::label_id_t label,
                                   const MESSAGE_T& msg, int channel_id = 0) {
    channels_[channel_id].SendMsgThroughIEdges<GRAPH_T, MESSAGE_T>(frag, v,
                                                                   label, msg);
  }

  /**
   * @brief SendMsgThroughOEdges on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughOEdges(const GRAPH_T& frag,
                                   const typename GRAPH_T::vertex_t& v,
                                   typename GRAPH_T::label_id_t label,
                                   const MESSAGE_T& msg, int channel_id = 0) {
    channels_[channel_id].SendMsgThroughOEdges<GRAPH_T, MESSAGE_T>(frag, v,
                                                                   label, msg);
  }

  /**
   * @brief SendMsgThroughEdges on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename GRAPH_T, typename MESSAGE_T>
  inline void SendMsgThroughEdges(const GRAPH_T& frag,
                                  const typename GRAPH_T::vertex_t& v,
                                  typename GRAPH_T::label_id_t label,
                                  const MESSAGE_T& msg, int channel_id = 0) {
    channels_[channel_id].SendMsgThroughEdges<GRAPH_T, MESSAGE_T>(frag, v,
                                                                  label, msg);
  }

  /**
   * @brief Get a bunch of messages, stored in a MessageInBuffer.
   *
   * @param buf Message buffer which holds a grape::OutArchive.
   */
  inline bool GetMessages(grape::MessageInBuffer& buf) {
    grape::OutArchive arc;
    auto& que = recv_queues_[round_ % 2];
    if (que.Get(arc)) {
      buf.Init(std::move(arc));
      return true;
    } else {
      return false;
    }
  }

  /**
   * @brief Parallel process all incoming messages with given function of last
   * round.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @tparam FUNC_T Function type.
   * @param thread_num Number of threads.
   * @param frag
   * @param func
   */
  template <typename GRAPH_T, typename MESSAGE_T, typename FUNC_T>
  inline void ParallelProcess(int thread_num, const GRAPH_T& frag,
                              const FUNC_T& func) {
    std::vector<std::thread> threads(thread_num);

    for (int i = 0; i < thread_num; ++i) {
      threads[i] = std::thread(
          [&](int tid) {
            typename GRAPH_T::vid_t id;
            typename GRAPH_T::vertex_t vertex(0);
            MESSAGE_T msg;
            auto& que = recv_queues_[round_ % 2];
            grape::OutArchive arc;
            while (que.Get(arc)) {
              while (!arc.Empty()) {
                arc >> id >> msg;
                frag.Gid2Vertex(id, vertex);
                func(tid, vertex, msg);
              }
            }
          },
          i);
    }

    for (auto& thrd : threads) {
      thrd.join();
    }
  }

  /**
   * @brief Parallel process all incoming messages with given function of last
   * round.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @tparam FUNC_T Function type.
   * @param thread_num Number of threads.
   * @param frag
   * @param func
   */
  template <typename MESSAGE_T, typename FUNC_T>
  inline void ParallelProcess(int thread_num, const FUNC_T& func) {
    std::vector<std::thread> threads(thread_num);

    for (int i = 0; i < thread_num; ++i) {
      threads[i] = std::thread(
          [&](int tid) {
            MESSAGE_T msg;
            auto& que = recv_queues_[round_ % 2];
            grape::OutArchive arc;
            while (que.Get(arc)) {
              while (!arc.Empty()) {
                arc >> msg;
                func(tid, msg);
              }
            }
          },
          i);
    }

    for (auto& thrd : threads) {
      thrd.join();
    }
  }

 private:
  void startSendThread() {
    force_continue_ = false;
    int round = round_;

    CHECK_EQ(sending_queue_.Size(), 0);
    sending_queue_.SetProducerNum(1);
    send_thread_ = std::thread(
        [this](int msg_round) {
          std::vector<MPI_Request> reqs;
          std::pair<grape::fid_t, grape::InArchive> item;
          while (sending_queue_.Get(item)) {
            if (item.second.GetSize() == 0) {
              continue;
            }
            if (item.first == fid_) {
              to_self_.emplace_back(std::move(item.second));
            } else {
              MPI_Request req;
              MPI_Isend(item.second.GetBuffer(), item.second.GetSize(),
                        MPI_CHAR, comm_spec_.FragToWorker(item.first),
                        msg_round, comm_, &req);
              reqs.push_back(req);
              to_others_.emplace_back(std::move(item.second));
            }
          }
          for (grape::fid_t i = 0; i < fnum_; ++i) {
            if (i == fid_) {
              continue;
            }
            MPI_Request req;
            MPI_Isend(NULL, 0, MPI_CHAR, comm_spec_.FragToWorker(i), msg_round,
                      comm_, &req);
            reqs.push_back(req);
          }
          MPI_Waitall(reqs.size(), &reqs[0], MPI_STATUSES_IGNORE);
          to_others_.clear();
        },
        round + 1);
  }

  void probeAllIncomingMessages() {
    MPI_Status status;
    while (true) {
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm_, &status);
      if (status.MPI_SOURCE == comm_spec_.worker_id()) {
        MPI_Recv(NULL, 0, MPI_CHAR, status.MPI_SOURCE, 0, comm_,
                 MPI_STATUS_IGNORE);
        return;
      }
      int tag = status.MPI_TAG;
      int count;
      MPI_Get_count(&status, MPI_CHAR, &count);
      if (count == 0) {
        MPI_Recv(NULL, 0, MPI_CHAR, status.MPI_SOURCE, tag, comm_,
                 MPI_STATUS_IGNORE);
        recv_queues_[tag % 2].DecProducerNum();
      } else {
        grape::OutArchive arc(count);
        MPI_Recv(arc.GetBuffer(), count, MPI_CHAR, status.MPI_SOURCE, tag,
                 comm_, MPI_STATUS_IGNORE);
        recv_queues_[tag % 2].Put(std::move(arc));
      }
    }
  }

  void startRecvThread() {
    recv_thread_ = std::thread([this]() { probeAllIncomingMessages(); });
  }

  void stopRecvThread() {
    MPI_Send(NULL, 0, MPI_CHAR, comm_spec_.worker_id(), 0, comm_);
    recv_thread_.join();
  }

  inline size_t finishMsgFilling() {
    size_t ret = 0;
    for (auto& channel : channels_) {
      channel.FlushMessages();
      ret += channel.SentMsgSize();
      channel.Reset();
    }
    sending_queue_.DecProducerNum();
    return ret;
  }

  void resetRecvQueue() {
    auto& curr_recv_queue = recv_queues_[round_ % 2];
    if (round_) {
      grape::OutArchive arc;
      while (curr_recv_queue.Get(arc)) {}
    }
    curr_recv_queue.SetProducerNum(fnum_);
  }

  void waitSend() { send_thread_.join(); }

  grape::fid_t fid_;
  grape::fid_t fnum_;
  grape::CommSpec comm_spec_;

  MPI_Comm comm_;

  std::vector<grape::InArchive> to_self_;
  std::vector<grape::InArchive> to_others_;

  std::vector<ThreadLocalPropertyMessageBuffer<ParallelPropertyMessageManager>>
      channels_;
  int round_;

  grape::BlockingQueue<std::pair<grape::fid_t, grape::InArchive>>
      sending_queue_;
  std::thread send_thread_;

  std::array<grape::BlockingQueue<grape::OutArchive>, 2> recv_queues_;
  std::thread recv_thread_;

  bool force_continue_;
  size_t sent_size_;

  bool force_terminate_;
  grape::TerminateInfo terminate_info_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_PARALLEL_PROPERTY_MESSAGE_MANAGER_H_
