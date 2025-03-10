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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_WRITER_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_WRITER_H_

#include <memory>
#include <unordered_map>
#include "flex/engines/graph_db/database/wal/wal.h"

#include "cppkafka/cppkafka.h"

namespace gs {

/**
 * KafkaWalWriter is a concrete implementation of IWalWriter that writes WAL
 * entries to a Kafka topic.
 * It uses cppkafka to produce messages to Kafka.
 * The Kafka brokers are specified by the environment variable
 * KAFKA_BROKER_LIST. The Kafka topic is specified by the open() method, the
 * thread_id is used as the partition number.
 *
 * After restarting the GraphDB service, the KafkaWalWriter will continue to
 * write to the same Kafka topic and partition. Consumers should be able to
 * select the WAL entries by the timestamp.
 */
class KafkaWalWriter : public IWalWriter {
 public:
  static constexpr const int DEFAULT_MAX_RETRIES = 10;

  static std::unique_ptr<IWalWriter> Make();

  KafkaWalWriter(const std::string& kafka_brokers)
      : thread_id_(-1),
        kafka_brokers_(kafka_brokers),
        kafka_topic_(""),
        producer_(nullptr),
        builder_("") {}  // brokers could be a list of brokers

  ~KafkaWalWriter() { close(); }

  void open(const std::string& kafka_topic, int thread_id) override;
  void close() override;
  bool append(const char* data, size_t length) override;
  std::string type() const override { return "kafka"; }

  //////Kafka specific methods

  inline std::tuple<int64_t, int64_t> getCurrentOffset() {
    return producer_->get_producer().query_offsets(
        cppkafka::TopicPartition(kafka_topic_, thread_id_));
  }

 private:
  int32_t thread_id_;
  std::string kafka_brokers_;
  std::string kafka_topic_;
  std::shared_ptr<cppkafka::BufferedProducer<std::string>> producer_;
  cppkafka::MessageBuilder builder_;
  int64_t cur_offset_;

  static const bool registered_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_WRITER_H_