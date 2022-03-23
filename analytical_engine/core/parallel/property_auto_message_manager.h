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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_PROPERTY_AUTO_MESSAGE_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_PROPERTY_AUTO_MESSAGE_MANAGER_H_

#include <algorithm>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <typeinfo>
#include <utility>
#include <vector>

#include "grape/communication/sync_comm.h"
#include "grape/fragment/edgecut_fragment_base.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/parallel/message_manager_base.h"
#include "grape/parallel/sync_buffer.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/types.h"
#include "grape/worker/comm_spec.h"

#include "core/config.h"

namespace gs {

/**
 * @brief A kind of message manager supporting auto parallelism.
 *
 * After registering the vertex array and message strategy as a sync buffer,
 * message generation and ingestion can be applied by message manager
 * automatically.
 */
template <typename FRAG_T>
class PropertyAutoMessageManager : public grape::DefaultMessageManager {
  using Base = grape::DefaultMessageManager;
  using vid_t = typename FRAG_T::vid_t;
  using label_id_t = typename FRAG_T::label_id_t;

  struct ap_event {
    ap_event(const FRAG_T& f, label_id_t l, grape::ISyncBuffer* b,
             grape::MessageStrategy m, int e)
        : fragment(f), label(l), buffer(b), message_strategy(m), event_id(e) {}

    const FRAG_T& fragment;
    label_id_t label;
    grape::ISyncBuffer* buffer;
    grape::MessageStrategy message_strategy;
    int event_id;
  };

 public:
  PropertyAutoMessageManager() {}
  ~PropertyAutoMessageManager() override {}

  using Base::Init;

  using Base::Start;

  /**
   * @brief Inherit
   */
  void StartARound() override {
    Base::StartARound();
    aggregateAutoMessages();
  }

  /**
   * @brief Inherit
   */
  void FinishARound() override {
    generateAutoMessages();
    Base::FinishARound();
  }

  using Base::ToTerminate;

  using Base::Finalize;

  using Base::GetMsgSize;

  using Base::ForceContinue;

  /**
   * @brief Register a buffer to be sync automatically between rounds.
   *
   * @param frag
   * @param buffer
   * @param strategy
   */
  inline void RegisterSyncBuffer(const FRAG_T& frag, label_id_t label,
                                 grape::ISyncBuffer* buffer,
                                 grape::MessageStrategy strategy) {
    int event_id = auto_parallel_events_.size();
    auto_parallel_events_.emplace_back(frag, label, buffer, strategy, event_id);
  }

 private:
  void aggregateAutoMessages() {
    std::map<int, ap_event*> event_map;
    for (auto& event : auto_parallel_events_) {
      event_map.emplace(event.event_id, &event);
    }

    int event_id;
    while (Base::GetMessage<int>(event_id)) {
      ap_event* event = event_map.at(event_id);

      auto& i_ec_frag = event->fragment;
      if (event->message_strategy ==
              grape::MessageStrategy::kSyncOnOuterVertex ||
          event->message_strategy ==
              grape::MessageStrategy::kAlongEdgeToOuterVertex ||
          event->message_strategy ==
              grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex ||
          event->message_strategy ==
              grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex) {
        if (event->buffer->GetTypeId() == typeid(double)) {
          syncOnVertexRecv<double>(i_ec_frag, event->buffer);
        } else if (event->buffer->GetTypeId() == typeid(uint32_t)) {
          syncOnVertexRecv<uint32_t>(i_ec_frag, event->buffer);
        } else if (event->buffer->GetTypeId() == typeid(int32_t)) {
          syncOnVertexRecv<int32_t>(i_ec_frag, event->buffer);
        } else if (event->buffer->GetTypeId() == typeid(int64_t)) {
          syncOnVertexRecv<int64_t>(i_ec_frag, event->buffer);
        } else if (event->buffer->GetTypeId() == typeid(uint64_t)) {
          syncOnVertexRecv<uint64_t>(i_ec_frag, event->buffer);
        } else if (event->buffer->GetTypeId() ==
                   typeid(std::vector<uint32_t>)) {
          syncOnVertexRecv<std::vector<uint32_t>>(i_ec_frag, event->buffer);
        } else {
          LOG(FATAL) << "Unexpected data type "
                     << event->buffer->GetTypeId().name();
        }
      } else {
        LOG(FATAL) << "Unexpected message stratety "
                   << underlying_value(event->message_strategy);
      }
    }
  }

  void generateAutoMessages() {
    for (auto& event_ref : auto_parallel_events_) {
      ap_event* event = &event_ref;
      auto& i_ec_frag = event->fragment;
      auto inner_size = i_ec_frag.InnerVertices(event->label).size();
      if (event->buffer->updated(0, inner_size)) {
        ForceContinue();
        break;
      }
    }

    for (auto& event_ref : auto_parallel_events_) {
      ap_event* event = &event_ref;

      auto& i_ec_frag = event->fragment;
      if (event->message_strategy ==
          grape::MessageStrategy::kSyncOnOuterVertex) {
        if (event->buffer->GetTypeId() == typeid(double)) {
          syncOnOuterVertexSend<double>(i_ec_frag, event->label, event->buffer,
                                        event->event_id);
        } else if (event->buffer->GetTypeId() == typeid(uint32_t)) {
          syncOnOuterVertexSend<uint32_t>(i_ec_frag, event->label,
                                          event->buffer, event->event_id);
        } else if (event->buffer->GetTypeId() == typeid(int32_t)) {
          syncOnOuterVertexSend<int32_t>(i_ec_frag, event->label, event->buffer,
                                         event->event_id);
        } else if (event->buffer->GetTypeId() == typeid(int64_t)) {
          syncOnOuterVertexSend<int64_t>(i_ec_frag, event->label, event->buffer,
                                         event->event_id);
        } else if (event->buffer->GetTypeId() == typeid(uint64_t)) {
          syncOnOuterVertexSend<uint64_t>(i_ec_frag, event->label,
                                          event->buffer, event->event_id);
        } else {
          LOG(FATAL) << "Unexpected data type for auto parallelization: "
                     << event->buffer->GetTypeId().name();
        }
        /*
      } else if (event->message_strategy ==
                     grape::MessageStrategy::kAlongEdgeToOuterVertex ||
                 event->message_strategy ==
                     grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex ||
                 event->message_strategy ==
                     grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
        if (event->buffer->GetTypeId() == typeid(double)) {
          syncOnInnerVertexSend<double>(i_ec_frag, event->label, event->buffer,
                                        event->event_id,
                                        event->message_strategy);
        } else if (event->buffer->GetTypeId() == typeid(uint32_t)) {
          syncOnInnerVertexSend<uint32_t>(i_ec_frag, event->label,
      event->buffer, event->event_id, event->message_strategy); } else if
      (event->buffer->GetTypeId() == typeid(int32_t)) {
          syncOnInnerVertexSend<int32_t>(i_ec_frag, event->label, event->buffer,
                                         event->event_id,
                                         event->message_strategy);
        } else if (event->buffer->GetTypeId() == typeid(int64_t)) {
          syncOnInnerVertexSend<int64_t>(i_ec_frag, event->label, event->buffer,
                                         event->event_id,
                                         event->message_strategy);
        } else if (event->buffer->GetTypeId() ==
                   typeid(std::vector<uint32_t>)) {
          syncOnInnerVertexSend<std::vector<uint32_t>>(i_ec_frag, event->label,
      event->buffer, event->event_id, event->message_strategy); } else {
          LOG(FATAL) << "Unexpected data type for auto parallelization: "
                     << event->buffer->GetTypeId().name();
        }
        */
      } else {
        LOG(FATAL) << "Unexpected message stratety "
                   << underlying_value(event->message_strategy);
      }
    }
  }

  /*
  template <typename T>
  inline void syncOnInnerVertexSend(const FRAG_T& frag, label_id_t label,
  grape::ISyncBuffer* buffer, int event_id, grape::MessageStrategy
  message_strategy) { auto* bptr = dynamic_cast<grape::SyncBuffer<T,
  vid_t>*>(buffer); auto inner_vertices = frag.InnerVertices(label);
  std::vector<size_t> message_num(Base::fnum(), 0);

    if (message_strategy == grape::MessageStrategy::kAlongEdgeToOuterVertex) {
      for (auto v : inner_vertices) {
        if (bptr->IsUpdated(v)) {
          grape::DestList dsts = frag.IOEDests(v);
          fid_t* ptr = dsts.begin;
          while (ptr != dsts.end) {
            ++message_num[*(ptr++)];
          }
        }
      }
    } else if (message_strategy ==
               grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex) {
      for (auto v : inner_vertices) {
        if (bptr->IsUpdated(v)) {
          grape::DestList dsts = frag.IEDests(v);
          fid_t* ptr = dsts.begin;
          while (ptr != dsts.end) {
            ++message_num[*(ptr++)];
          }
        }
      }
    } else if (message_strategy ==
               grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
      for (auto v : inner_vertices) {
        if (bptr->IsUpdated(v)) {
          grape::DestList dsts = frag.OEDests(v);
          fid_t* ptr = dsts.begin;
          while (ptr != dsts.end) {
            ++message_num[*(ptr++)];
          }
        }
      }
    }

    for (fid_t i = 0; i < Base::fnum(); i++) {
      if (message_num[i] > 0) {
        Base::SendToFragment<int>(i, event_id);
        Base::SendToFragment<size_t>(i, message_num[i]);
      }
    }

    if (message_strategy == grape::MessageStrategy::kAlongEdgeToOuterVertex) {
      for (auto v : inner_vertices) {
        if (bptr->IsUpdated(v)) {
          Base::SendMsgThroughEdges(frag, v, bptr->GetValue(v));
          bptr->Reset(v);
        }
      }
    } else if (message_strategy ==
               grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex) {
      for (auto v : inner_vertices) {
        if (bptr->IsUpdated(v)) {
          Base::SendMsgThroughIEdges(frag, v, bptr->GetValue(v));
          bptr->Reset(v);
        }
      }
    } else if (message_strategy ==
               grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
      for (auto v : inner_vertices) {
        if (bptr->IsUpdated(v)) {
          Base::SendMsgThroughOEdges(frag, v, bptr->GetValue(v));
          bptr->Reset(v);
        }
      }
    }
  }
  */

  template <typename T>
  inline void syncOnOuterVertexSend(const FRAG_T& frag, label_id_t label,
                                    grape::ISyncBuffer* buffer, int event_id) {
    auto* bptr =
        dynamic_cast<grape::SyncBuffer<typename FRAG_T::vertices_t, T>*>(
            buffer);
    auto inner_vertices = frag.InnerVertices(label);
    auto outer_vertices = frag.OuterVertices(label);
    std::vector<size_t> message_num(Base::fnum(), 0);

    for (auto v : inner_vertices) {
      bptr->Reset(v);
    }

    for (auto v : outer_vertices) {
      if (bptr->IsUpdated(v)) {
        fid_t fid = frag.GetFragId(v);
        ++message_num[fid];
      }
    }

    for (fid_t i = 0; i < Base::fnum(); i++) {
      if (message_num[i] > 0) {
        Base::SendToFragment<int>(i, event_id);
        Base::SendToFragment<size_t>(i, message_num[i]);
      }
    }

    for (auto v : outer_vertices) {
      if (bptr->IsUpdated(v)) {
        Base::SyncStateOnOuterVertex(frag, v, bptr->GetValue(v));
        bptr->Reset(v);
      }
    }
  }

  template <typename T>
  inline void syncOnVertexRecv(const FRAG_T& frag, grape::ISyncBuffer* buffer) {
    auto* bptr =
        dynamic_cast<grape::SyncBuffer<typename FRAG_T::vertices_t, T>*>(
            buffer);

    size_t message_num = 0;
    T rhs;
    grape::Vertex<vid_t> v(0);
    Base::GetMessage<size_t>(message_num);
    while (message_num--) {
      GetMessage(frag, v, rhs);
      bptr->Aggregate(v, std::move(rhs));
    }
  }

  std::vector<ap_event> auto_parallel_events_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_PROPERTY_AUTO_MESSAGE_MANAGER_H_
