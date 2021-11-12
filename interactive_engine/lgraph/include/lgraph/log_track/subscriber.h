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
#include "lgraph/log_track/message.h"

namespace LGRAPH_NAMESPACE {
namespace log_track {

class LogSubscriber {
public:
  LogSubscriber(const std::string &kafka_servers, const std::string &topic, int32_t partition_id, int64_t start_offset);
  ~LogSubscriber();

  LogSubscriber(const LogSubscriber &) = delete;
  LogSubscriber &operator=(const LogSubscriber &) = delete;
  LogSubscriber(LogSubscriber &&) = delete;
  LogSubscriber &operator=(LogSubscriber &&) = delete;

  LogMessage Poll(size_t timeout_ms);
  std::vector<LogMessage> PollBatch(size_t max_batch_size, size_t timeout_ms);

  int64_t GetCurrentOffset() const {
    return current_offset_;
  }

private:
  cppkafka::Consumer consumer_;
  int64_t current_offset_;
};

}
}

