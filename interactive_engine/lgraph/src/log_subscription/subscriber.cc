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

#include "lgraph/log_subscription/subscriber.h"

namespace LGRAPH_NAMESPACE {
namespace log_subscription {

Subscriber::Subscriber(const std::string &kafka_servers, const std::string &topic,
                       int32_t partition_id, int64_t start_offset)
    : consumer_(cppkafka::Configuration{
      {"metadata.broker.list", kafka_servers},
      {"broker.address.family", "v4"},
      {"group.id","lgraph-log-subscribers"},
      {"enable.auto.commit", false}}) {
  consumer_.assign({cppkafka::TopicPartition{topic, partition_id, start_offset}});
}

Subscriber::~Subscriber() {
  consumer_.unassign();
}

LogMessage Subscriber::Poll(size_t timeout_ms) {
  auto kafka_msg = consumer_.poll(std::chrono::milliseconds(timeout_ms));
  return LogMessage{std::move(kafka_msg)};
}

std::vector<LogMessage> Subscriber::PollBatch(size_t max_batch_size, size_t timeout_ms) {
  auto kafka_msg_batch = consumer_.poll_batch(max_batch_size, std::chrono::milliseconds(timeout_ms));
  std::vector<LogMessage> msg_batch;
  msg_batch.reserve(kafka_msg_batch.size());
  for (cppkafka::Message &kafka_msg : kafka_msg_batch) {
    msg_batch.emplace_back(std::move(kafka_msg));
  }
  return msg_batch;
}

}
}
