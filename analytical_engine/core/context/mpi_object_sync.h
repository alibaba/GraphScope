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
#include "vineyard/basic/ds/object_set.h"
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
                            vineyard::ObjectID const object_id,
                            vineyard::ObjectSetBuilder& target_chunk_map) {
    // gather chunk id per worker, and add to the target chunkmap
    if (comm_spec.worker_id() == 0) {
      target_chunk_map.AddObject(client.instance_id(), object_id);
      for (int src_worker_id = 1; src_worker_id < comm_spec.worker_num();
           ++src_worker_id) {
        std::pair<vineyard::InstanceID, vineyard::ObjectID> chunk;
        grape::recv_buffer(&chunk, 1, src_worker_id, comm_spec.comm(), 0x10);
        target_chunk_map.AddObject(chunk.first, chunk.second);
      }
    } else {
      auto chunk = std::make_pair(client.instance_id(), object_id);
      grape::send_buffer(&chunk, 1, 0, comm_spec.comm(), 0x10);
    }
  }

  void GatherWorkerObjectIDs(vineyard::Client& client,
                             grape::CommSpec const& comm_spec,
                             std::vector<vineyard::ObjectID> const& object_ids,
                             vineyard::ObjectSetBuilder& target_chunk_map) {
    // gather chunk id per worker, and add to the target chunkmap
    if (comm_spec.worker_id() == 0) {
      target_chunk_map.AddObjects(client.instance_id(), object_ids);
      for (int src_worker_id = 1; src_worker_id < comm_spec.worker_num();
           ++src_worker_id) {
        vineyard::InstanceID instance_id = vineyard::UnspecifiedInstanceID();
        std::vector<vineyard::ObjectID> recv_object_ids;
        grape::recv_buffer(&instance_id, 1, src_worker_id, comm_spec.comm(),
                           0x11);
        grape::RecvVector(recv_object_ids, src_worker_id, comm_spec.comm(),
                          0x12);
        target_chunk_map.AddObjects(instance_id, recv_object_ids);
      }
    } else {
      auto instance_id = client.instance_id();
      grape::send_buffer(&instance_id, 1, 0, comm_spec.comm(), 0x11);
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
