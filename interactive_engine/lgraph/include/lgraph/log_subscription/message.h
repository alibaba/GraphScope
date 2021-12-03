/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cppkafka/consumer.h>
#include "lgraph/log_subscription/operation.h"

namespace LGRAPH_NAMESPACE {
namespace log_subscription {

class MessageParser {
public:
  MessageParser(const void* data, size_t size);
  ~MessageParser() = default;

  MessageParser(const MessageParser &) = default;
  MessageParser &operator=(const MessageParser &) = default;
  MessageParser(MessageParser &&) noexcept = default;
  MessageParser &operator=(MessageParser &&) noexcept = default;

  SnapshotId GetSnapshotId() const {
    return snapshot_id_;
  }

  size_t GetOperationCount() const {
    return op_batch_proto_->operations_size();
  }

  Operation GetOperation(size_t index);

  std::vector<Operation> GetOperations();

private:
  SnapshotId snapshot_id_;
  std::shared_ptr<OperationBatchPb> op_batch_proto_;
};

class LogMessage {
public:
  explicit LogMessage(cppkafka::Message &&msg) : kafka_msg_(std::move(msg)) {}
  ~LogMessage() = default;

  LogMessage(const LogMessage &) = delete;
  LogMessage &operator=(const LogMessage &) = delete;
  LogMessage(LogMessage &&rhs) noexcept = default;
  LogMessage &operator=(LogMessage &&rhs) noexcept = default;

  explicit operator bool() const {
    return (kafka_msg_ && true);
  }

  bool IsError() const {
    return (kafka_msg_.get_error() && true);
  }

  std::string GetErrorMsg() const {
    return kafka_msg_.get_error().to_string();
  }

  bool IsEof() const {
    return kafka_msg_.is_eof();
  }

  int64_t GetOffset() const {
    return kafka_msg_.get_offset();
  }

  const unsigned char *Data() const {
    return kafka_msg_.get_payload().get_data();
  }

  size_t Size() const {
    return kafka_msg_.get_payload().get_size();
  }

  MessageParser GetParser() const {
    return MessageParser{Data(), Size()};
  }

private:
  cppkafka::Message kafka_msg_;
};

inline MessageParser::MessageParser(const void* data, size_t size) : snapshot_id_(0), op_batch_proto_() {
  LogEntryPb log_entry_proto;
  Check(log_entry_proto.ParsePartialFromArray(data, static_cast<int>(size)), "Parse LogEntryPb Failed!");
  snapshot_id_ = static_cast<SnapshotId>(log_entry_proto.snapshotid());
  op_batch_proto_.reset(log_entry_proto.release_operations());
}

inline Operation MessageParser::GetOperation(size_t index) {
  return {op_batch_proto_->mutable_operations(static_cast<int>(index)), op_batch_proto_};
}

inline std::vector<Operation> MessageParser::GetOperations() {
  std::vector<Operation> operations;
  operations.reserve(op_batch_proto_->operations_size());
  for (auto &op : *op_batch_proto_->mutable_operations()) {
    operations.emplace_back(&op, op_batch_proto_);
  }
  return operations;
}

}
}
