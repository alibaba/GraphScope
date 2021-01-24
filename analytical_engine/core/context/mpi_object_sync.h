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
#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_MPI_OBJECT_SYNC_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_MPI_OBJECT_SYNC_H_

#include <utility>
#include <vector>

#include "grape/worker/comm_spec.h"
#include "vineyard/client/client.h"

namespace gs {

/**
 * @brief MPIObjectSync is the base class to represent a global data
 * structure.
 */
class MPIObjectSync {
 protected:
  void SyncGlobalObjectID(grape::CommSpec const& comm_spec,
                          vineyard::ObjectID& object_id) {
    if (comm_spec.worker_id() == 0) {
      grape::BcastSend(object_id, comm_spec.comm());
    } else {
      grape::BcastRecv(object_id, comm_spec.comm(), 0);
    }
  }

  void GatherWorkerObjectID(vineyard::Client& client,
                            grape::CommSpec const& comm_spec,
                            vineyard::ObjectID object_id,
                            std::vector<vineyard::ObjectID>& assembled_ids) {
    // gather chunk id per worker, and add to the target chunkmap
    if (comm_spec.worker_id() == 0) {
      assembled_ids.push_back(object_id);
      for (int src_worker_id = 1; src_worker_id < comm_spec.worker_num();
           ++src_worker_id) {
        vineyard::ObjectID recv_object_id;
        grape::recv_buffer(&recv_object_id, 1, src_worker_id, comm_spec.comm(),
                           0x10);
        assembled_ids.push_back(recv_object_id);
      }
    } else {
      grape::send_buffer(&object_id, 1, 0, comm_spec.comm(), 0x10);
    }
  }

  void GatherWorkerObjectIDs(vineyard::Client& client,
                             grape::CommSpec const& comm_spec,
                             std::vector<vineyard::ObjectID> const& object_ids,
                             std::vector<vineyard::ObjectID>& assembled_ids) {
    // gather chunk id vector per worker, and add to the target chunkmap
    if (comm_spec.worker_id() == 0) {
      assembled_ids.insert(assembled_ids.end(), object_ids.begin(),
                           object_ids.end());
      for (int src_worker_id = 1; src_worker_id < comm_spec.worker_num();
           ++src_worker_id) {
        std::vector<vineyard::ObjectID> recv_object_ids;
        grape::RecvVector(recv_object_ids, src_worker_id, comm_spec.comm(),
                          0x12);
        assembled_ids.insert(assembled_ids.end(), recv_object_ids.begin(),
                             recv_object_ids.end());
      }
    } else {
      grape::SendVector(object_ids, 0, comm_spec.comm(), 0x12);
    }
  }

  template <typename T>
  void SyncObjectMeta(grape::CommSpec const& comm_spec, T& destination) {
    if (comm_spec.worker_id() == 0) {
      grape::BcastSend(destination, comm_spec.comm());
    } else {
      grape::BcastRecv(destination, comm_spec.comm(), 0);
    }
  }
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_MPI_OBJECT_SYNC_H_
