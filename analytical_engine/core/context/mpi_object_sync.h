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

#include <vector>

#include "grape/communication/sync_comm.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/common/util/uuid.h"

namespace vineyard {
class Client;
}

namespace gs {

/**
 * @brief MPIObjectSync is the base class to represent a global data
 * structure.
 */
class MPIObjectSync {
 protected:
  void SyncGlobalObjectID(grape::CommSpec const& comm_spec,
                          vineyard::ObjectID& object_id) {
    grape::sync_comm::Bcast(object_id, 0, comm_spec.comm());
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
        grape::sync_comm::Recv(recv_object_id, src_worker_id, 0x10,
                               comm_spec.comm());
        assembled_ids.push_back(recv_object_id);
      }
    } else {
      grape::sync_comm::Send(object_id, 0, 0x10, comm_spec.comm());
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
        grape::sync_comm::Recv(recv_object_ids, src_worker_id, 0x12,
                               comm_spec.comm());
        assembled_ids.insert(assembled_ids.end(), recv_object_ids.begin(),
                             recv_object_ids.end());
      }
    } else {
      grape::sync_comm::Send(object_ids, 0, 0x12, comm_spec.comm());
    }
  }

  template <typename T>
  void SyncObjectMeta(grape::CommSpec const& comm_spec, T& destination) {
    grape::sync_comm::Bcast(destination, 0, comm_spec.comm());
  }
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_MPI_OBJECT_SYNC_H_
