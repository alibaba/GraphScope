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

#include "core/server/dispatcher.h"

#include <glog/logging.h>
#include <mpi.h>

#include <exception>
#include <ostream>
#include <thread>
#include <utility>

#include "boost/leaf/capture.hpp"
#include "boost/leaf/handle_errors.hpp"
#include "grape/communication/sync_comm.h"
#include "grape/serialization/out_archive.h"
#include "grape/worker/comm_spec.h"

#include "vineyard/common/util/blocking_queue.h"
#include "vineyard/graph/utils/error.h"
#include "vineyard/graph/utils/mpi_utils.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/server/command_detail.h"
#include "proto/graphscope/proto/attr_value.pb.h"
#include "proto/graphscope/proto/graph_def.pb.h"

namespace bl = boost::leaf;

namespace gs {

Dispatcher::Dispatcher(const grape::CommSpec& comm_spec)
    : running_(false), comm_spec_(comm_spec) {
  // a naive implementation using MPI
  auto publisher = comm_spec_.worker_id() == grape::kCoordinatorRank;
  // we use blocking queue as synchronizer
  if (publisher) {
    cmd_queue_.SetLimit(1);
    result_queue_.SetLimit(1);
  }
}

void Dispatcher::Start() {
  if (running_) {
    LOG(FATAL) << "Dispatcher already started";
  }
  running_ = true;
  auto publisher = comm_spec_.worker_id() == grape::kCoordinatorRank;

  if (publisher) {
    auto publisher_th = std::thread([this]() { publisherLoop(); });
    publisher_th.join();
  } else {
    auto subscriber_th = std::thread([this]() { subscriberLoop(); });
    subscriber_th.join();
  }
}

void Dispatcher::Stop() { running_ = false; }

std::vector<DispatchResult> Dispatcher::Dispatch(
    std::shared_ptr<CommandDetail> cmd) {
  cmd_queue_.Push(cmd);
  return result_queue_.Pop();
}

void Dispatcher::Subscribe(std::shared_ptr<Subscriber> subscriber) {
  subscriber_ = std::move(subscriber);
}

void Dispatcher::SetCommand(std::shared_ptr<CommandDetail> cmd) {
  processCmd(cmd);
  MPI_Barrier(comm_spec_.comm());
}

std::shared_ptr<DispatchResult> Dispatcher::processCmd(
    std::shared_ptr<CommandDetail> cmd) {
  // handle all errors and get error message
  auto r = bl::try_handle_all(
      [&, this]() -> bl::result<std::shared_ptr<DispatchResult>> {
        try {
          return subscriber_->OnReceive(cmd);
        } catch (const std::exception& e) {
          RETURN_GS_ERROR(
              vineyard::ErrorCode::kCommandError,
              "Unmatched std::exception detected: " + std::string(e.what()));
        }
      },
      [&](const vineyard::GSError& e) {
        auto r = std::make_shared<DispatchResult>(comm_spec_.worker_id());

        r->set_error(
            ErrorCodeToProto(e.error_code),
            e.error_msg + (e.backtrace.empty() ? "" : "\n" + e.backtrace));
        return r;
      },
      [&](const bl::error_info& unmatched) {
        LOG(FATAL) << "BUG: Unmatched error, some function may return a new "
                      "type of error";
        return nullptr;
      });

  if (!r->message().empty()) {
    LOG(ERROR) << "Worker " + std::to_string(r->worker_id()) + ": " +
                      r->message();
  }

  return r;
}

void Dispatcher::publisherPreprocessCmd(std::shared_ptr<CommandDetail> cmd) {
  if (cmd->type == rpc::CREATE_GRAPH || cmd->type == rpc::ADD_LABELS) {
    // Distribute raw bytes if there are some data from pandas
    auto params_vec = DistributeGraph(cmd->large_attr, comm_spec_.worker_num());
    CHECK_EQ(static_cast<int>(params_vec.size()), comm_spec_.worker_num());
    for (int i = 1; i < comm_spec_.worker_num(); ++i) {
      grape::InArchive ia;
      cmd->large_attr = params_vec[i];
      ia << *(cmd.get());
      grape::sync_comm::Send(ia, i, 0, MPI_COMM_WORLD);
    }
    cmd->large_attr = params_vec[0];
  } else {
    grape::sync_comm::Bcast(*(cmd.get()), grape::kCoordinatorRank,
                            MPI_COMM_WORLD);
  }
}

void Dispatcher::subscriberPreprocessCmd(rpc::OperationType type,
                                         std::shared_ptr<CommandDetail>& cmd) {
  if (type == rpc::CREATE_GRAPH || type == rpc::ADD_LABELS) {
    grape::OutArchive oa;
    grape::sync_comm::Recv(oa, grape::kCoordinatorRank, 0, MPI_COMM_WORLD);
    oa >> *(cmd.get());
  } else {
    grape::sync_comm::Bcast(*(cmd.get()), grape::kCoordinatorRank,
                            MPI_COMM_WORLD);
  }
}

void Dispatcher::publisherLoop() {
  CHECK_EQ(comm_spec_.worker_id(), grape::kCoordinatorRank);
  while (running_) {
    auto cmd = cmd_queue_.Pop();
    grape::sync_comm::Bcast(cmd->type, grape::kCoordinatorRank, MPI_COMM_WORLD);
    publisherPreprocessCmd(cmd);
    // process local event
    auto r = processCmd(cmd);
    std::vector<DispatchResult> results(comm_spec_.worker_num());

    results[0] = std::move(*r);
    vineyard::_GatherR(results, comm_spec_.comm());

    result_queue_.Push(std::move(results));
  }
}

void Dispatcher::subscriberLoop() {
  CHECK_NE(comm_spec_.worker_id(), grape::kCoordinatorRank);
  while (running_) {
    rpc::OperationType type;
    grape::sync_comm::Bcast(type, grape::kCoordinatorRank, MPI_COMM_WORLD);
    std::shared_ptr<CommandDetail> cmd = std::make_shared<CommandDetail>();
    subscriberPreprocessCmd(type, cmd);
    auto r = processCmd(cmd);

    vineyard::_GatherL(*r, grape::kCoordinatorRank, comm_spec_.comm());
  }
}

grape::InArchive& operator<<(grape::InArchive& archive,
                             const DispatchResult& result) {
  archive << result.worker_id_;
  archive << result.error_code_;
  archive << result.message_;
  archive << result.has_large_data_;
  archive << result.data_;
  archive << result.aggregate_policy_;
  archive << result.graph_def_.SerializeAsString();
  return archive;
}

grape::OutArchive& operator>>(grape::OutArchive& archive,
                              DispatchResult& result) {
  archive >> result.worker_id_;
  archive >> result.error_code_;
  archive >> result.message_;
  archive >> result.has_large_data_;
  archive >> result.data_;
  archive >> result.aggregate_policy_;
  std::string buf;
  archive >> buf;
  CHECK(result.graph_def_.ParseFromString(buf));
  return archive;
}

}  // namespace gs
