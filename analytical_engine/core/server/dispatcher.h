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
#ifndef ANALYTICAL_ENGINE_CORE_SERVER_DISPATCHER_H_
#define ANALYTICAL_ENGINE_CORE_SERVER_DISPATCHER_H_

#include <memory>
#include <string>
#include <vector>

#include "boost/leaf/result.hpp"
#include "grape/config.h"
#include "grape/serialization/in_archive.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/common/util/blocking_queue.h"

#include "proto/error_codes.pb.h"
#include "proto/graph_def.pb.h"
#include "proto/types.pb.h"

namespace bl = boost::leaf;

namespace grape {
class OutArchive;
}  // namespace grape

namespace gs {
struct CommandDetail;

/**
 * @brief DispatchResult wraps computation result, metadata and error message
 * for python client.
 */
class DispatchResult {
 public:
  // We use the enum type to make serialization convenient.
  enum AggregatePolicy : int {
    kPickFirst,
    kPickFirstNonEmpty,
    kRequireConsistent,
    kConcat,
    kPickFirstNonEmptyGraphDef,
    kMergeGraphDef,
  };

  DispatchResult() = default;

  explicit DispatchResult(int worker_id, rpc::Code error_code = rpc::Code::OK)
      : worker_id_(worker_id), error_code_(error_code) {}

  int worker_id() const { return worker_id_; }

  void set_error(rpc::Code error_code, const std::string& message) {
    error_code_ = error_code;
    message_ = message;
  }

  rpc::Code error_code() const { return error_code_; }

  std::string message() { return message_; }

  bool has_large_data() const { return has_large_data_; }

  /**
   * Set the graph metadata. The meta should be kept consistent among all
   * workers.
   * @param graph_def
   */
  void set_graph_def(const rpc::graph::GraphDefPb& graph_def,
                     AggregatePolicy policy = AggregatePolicy::kMergeGraphDef) {
    graph_def_ = graph_def;
    aggregate_policy_ = policy;
  }

  const rpc::graph::GraphDefPb& graph_def() const { return graph_def_; }

  rpc::graph::GraphDefPb& graph_def() { return graph_def_; }

  void set_data(const std::string& data,
                AggregatePolicy policy = AggregatePolicy::kRequireConsistent,
                bool large_data = false) {
    if (policy != AggregatePolicy::kPickFirst ||
        worker_id_ == grape::kCoordinatorRank) {
      data_ = data;
    }
    has_large_data_ = large_data;
    aggregate_policy_ = policy;
  }

  void set_data(const grape::InArchive& arc,
                AggregatePolicy policy = AggregatePolicy::kRequireConsistent,
                bool large_data = false) {
    if (policy != AggregatePolicy::kPickFirst ||
        worker_id_ == grape::kCoordinatorRank) {
      data_.assign(arc.GetBuffer(), arc.GetSize());
    }
    has_large_data_ = large_data;
    aggregate_policy_ = policy;
  }

  const std::string& data() const { return data_; }

  AggregatePolicy aggregate_policy() const { return aggregate_policy_; }

 private:
  int worker_id_{};
  rpc::Code error_code_{};
  std::string message_;
  bool has_large_data_{};
  std::string data_;
  AggregatePolicy aggregate_policy_{};

  rpc::graph::GraphDefPb graph_def_;

  friend grape::InArchive& operator<<(grape::InArchive& archive,
                                      const DispatchResult& result);
  friend grape::OutArchive& operator>>(grape::OutArchive& archive,
                                       DispatchResult& result);
};

/**
 * @brief The class that intent to monitor the commands from the coordinator
 * should inherit Subscriber.
 */
class Subscriber {
 public:
  Subscriber() = default;
  virtual ~Subscriber() = default;

  virtual bl::result<std::shared_ptr<DispatchResult>> OnReceive(
      std::shared_ptr<CommandDetail> cmd) = 0;
};

/**
 * @brief The dispatcher broadcast commands to every worker using MPI.
 */
class Dispatcher {
 public:
  explicit Dispatcher(const grape::CommSpec& comm_spec);

  void Start();

  void Stop();

  std::vector<DispatchResult> Dispatch(std::shared_ptr<CommandDetail> cmd);

  void Subscribe(std::shared_ptr<Subscriber> subscriber);

  void SetCommand(std::shared_ptr<CommandDetail> cmd);

 private:
  std::shared_ptr<DispatchResult> processCmd(
      std::shared_ptr<CommandDetail> cmd);

  void publisherLoop();

  void subscriberLoop();

  void publisherPreprocessCmd(std::shared_ptr<CommandDetail> cmd);

  void subscriberPreprocessCmd(rpc::OperationType type,
                               std::shared_ptr<CommandDetail>& cmd);

 private:
  bool running_;
  grape::CommSpec comm_spec_;
  std::shared_ptr<Subscriber> subscriber_;
  vineyard::PCBlockingQueue<std::shared_ptr<CommandDetail>> cmd_queue_;
  vineyard::PCBlockingQueue<std::vector<DispatchResult>> result_queue_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_SERVER_DISPATCHER_H_
