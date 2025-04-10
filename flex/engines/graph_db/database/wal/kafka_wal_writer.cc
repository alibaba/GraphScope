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

#include "flex/engines/graph_db/database/wal/kafka_wal_writer.h"
#include "flex/engines/graph_db/database/wal/kafka_wal_utils.h"
#include "flex/engines/graph_db/database/wal/wal.h"

#include <chrono>
#include <filesystem>

namespace gs {

std::unique_ptr<IWalWriter> KafkaWalWriter::Make() {
  return std::unique_ptr<IWalWriter>(new KafkaWalWriter());
}

void KafkaWalWriter::open(const std::string& uri, int thread_id) {
  auto res = parse_uri(uri);
  if (!res) {
    LOG(FATAL) << "Failed to parse uri: " << uri;
  }
  gs::Decoder decoder(res.value().data(), res.value().size());
  while (!decoder.empty()) {
    auto key = decoder.get_string();
    auto value = decoder.get_string();
    if (key == "metadata.broker.list") {
      kafka_brokers_ = value;
    } else if (key == "topic_name") {
      kafka_topic_ = value;
    }
  }

  if (thread_id_ != -1 || producer_) {
    LOG(FATAL) << "KafkaWalWriter has been opened";
  }
  thread_id_ = thread_id;
  if (!kafka_brokers_.empty()) {
    if (kafka_topic_.empty()) {
      LOG(FATAL) << "Kafka topic is empty";
    }
    cppkafka::Configuration config = {{"metadata.broker.list", kafka_brokers_}};
    producer_ =
        std::make_shared<cppkafka::BufferedProducer<std::string>>(config);
    producer_->set_max_number_retries(DEFAULT_MAX_RETRIES);
    builder_.topic(kafka_topic_).partition(thread_id_);
  } else {
    LOG(FATAL) << "Kafka brokers is empty";
  }
}

void KafkaWalWriter::close() {
  if (producer_) {
    producer_->flush();
    producer_.reset();
    thread_id_ = -1;
    kafka_topic_.clear();
  }
}

bool KafkaWalWriter::append(const char* data, size_t length) {
  try {
    producer_->sync_produce(builder_.payload({data, length}));
    producer_->flush(true);
  } catch (const cppkafka::HandleException& e) {
    LOG(ERROR) << "Failed to send to kafka: " << e.what();
    return false;
  }
  VLOG(10) << "Finished sending to kafka with message size: " << length
           << ", partition: " << thread_id_;
  return true;
}

const bool KafkaWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "kafka", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                 &KafkaWalWriter::Make));

}  // namespace gs