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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_TENSOR_DATAFRAME_BUILDER_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_TENSOR_DATAFRAME_BUILDER_H_

#include <memory>
#include <utility>
#include <vector>

#include "grape/communication/sync_comm.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/dataframe.h"
#include "vineyard/basic/ds/tensor.h"

#include "core/context/mpi_object_sync.h"
namespace gs {

/**
 * @brief MPIGlobalTensorBuilder is designed for creating global tensors
 * w.r.t. message passing interface
 *
 */
class MPIGlobalTensorBuilder : public vineyard::GlobalTensorBuilder,
                               public MPIObjectSync {
 public:
  explicit MPIGlobalTensorBuilder(vineyard::Client& client,
                                  grape::CommSpec const& comm_spec)
      : GlobalTensorBuilder(client), comm_spec_(comm_spec) {}

  void AddChunk(vineyard::ObjectID const chunk_id) {
    this->local_chunk_ids_.emplace_back(chunk_id);
  }

  void AddChunks(std::vector<vineyard::ObjectID> const& chunk_ids) {
    for (auto& chunk_id : chunk_ids) {
      this->local_chunk_ids_.emplace_back(chunk_id);
    }
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    std::shared_ptr<vineyard::GlobalTensor> tensor;
    vineyard::ObjectID id = vineyard::InvalidObjectID();
    if (comm_spec_.worker_id() == 0) {
      tensor = std::dynamic_pointer_cast<vineyard::GlobalTensor>(
          vineyard::GlobalTensorBuilder::_Seal(client));
      id = tensor->id();
    } else {
      VINEYARD_CHECK_OK(this->Build(client));
    }
    SyncGlobalObjectID(comm_spec_, id);  // this sync can be seen as a barrier
    if (comm_spec_.worker_id() != 0) {
      // FIXME: the aim of `Construct` is to fillup the ObjectSet, needs better
      // design.
      tensor = std::make_shared<vineyard::GlobalTensor>();
      vineyard::ObjectMeta meta;
      VINEYARD_CHECK_OK(client.GetMetaData(id, meta, true));
      tensor->Construct(meta);
    }
    return tensor;
  }

  vineyard::Status Build(vineyard::Client& client) override {
    std::vector<vineyard::ObjectID> all_ids;
    GatherWorkerObjectIDs(client, comm_spec_, local_chunk_ids_, all_ids);
    this->AddPartitions(all_ids);
    MPI_Barrier(comm_spec_.comm());
    return vineyard::Status::OK();
  }

 private:
  grape::CommSpec const& comm_spec_;
  std::vector<vineyard::ObjectID> local_chunk_ids_;
};

/**
 * @brief MPIGlobalDataFrameBuilder is designed for generating global dataframes
 * w.r.t. message passing interface
 *
 */
class MPIGlobalDataFrameBuilder : public vineyard::GlobalDataFrameBuilder,
                                  private MPIObjectSync {
 public:
  explicit MPIGlobalDataFrameBuilder(vineyard::Client& client,
                                     grape::CommSpec const& comm_spec)
      : GlobalDataFrameBuilder(client), comm_spec_(comm_spec) {}

  ~MPIGlobalDataFrameBuilder() = default;

  void AddChunk(vineyard::ObjectID const chunk_id) {
    this->local_chunk_ids_.emplace_back(chunk_id);
  }

  void AddChunks(std::vector<vineyard::ObjectID> const& chunk_ids) {
    for (auto& chunk_id : chunk_ids) {
      this->local_chunk_ids_.emplace_back(chunk_id);
    }
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    std::shared_ptr<vineyard::GlobalDataFrame> df;
    vineyard::ObjectID id = vineyard::InvalidObjectID();
    if (comm_spec_.worker_id() == 0) {
      df = std::dynamic_pointer_cast<vineyard::GlobalDataFrame>(
          vineyard::GlobalDataFrameBuilder::_Seal(client));
      id = df->id();
    } else {
      VINEYARD_CHECK_OK(this->Build(client));
    }
    SyncGlobalObjectID(comm_spec_, id);  // this sync can be seen as a barrier
    if (comm_spec_.worker_id() != 0) {
      df = std::make_shared<vineyard::GlobalDataFrame>();
      vineyard::ObjectMeta meta;
      VINEYARD_CHECK_OK(client.GetMetaData(id, meta, true));
      df->Construct(meta);
    }
    return df;
  }

  vineyard::Status Build(vineyard::Client& client) override {
    std::vector<vineyard::ObjectID> all_ids;
    GatherWorkerObjectIDs(client, comm_spec_, local_chunk_ids_, all_ids);
    this->AddPartitions(all_ids);
    MPI_Barrier(comm_spec_.comm());
    return vineyard::Status::OK();
  }

 private:
  grape::CommSpec const& comm_spec_;
  std::vector<vineyard::ObjectID> local_chunk_ids_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_TENSOR_DATAFRAME_BUILDER_H_
