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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_PARSER_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_PARSER_H_

#include <vector>
#include "cppkafka/cppkafka.h"
#include "flex/engines/graph_db/database/wal/wal.h"

namespace gs {

/*
 * Get all partitions of the given topic.
 */
std::vector<cppkafka::TopicPartition> get_all_topic_partitions(
    const cppkafka::Configuration& config, const std::string& topic_name,
    bool from_beginning = true);

class KafkaWalParser : public IWalParser {
 public:
  static constexpr const std::chrono::milliseconds POLL_TIMEOUT =
      std::chrono::milliseconds(100);
  static constexpr const size_t MAX_BATCH_SIZE = 1000;

  static std::unique_ptr<IWalParser> Make(const std::string&);

  // always track all partitions and from begining
  KafkaWalParser(const cppkafka::Configuration& config);
  ~KafkaWalParser() { close(); }

  void open(const std::string& topic_name) override;
  void close() override;

  uint32_t last_ts() const override;
  const WalContentUnit& get_insert_wal(uint32_t ts) const override;
  const std::vector<UpdateWalUnit>& get_update_wals() const override;

  //////Kafka specific methods
  void open(const std::vector<cppkafka::TopicPartition>& partitions);

 private:
  std::unique_ptr<cppkafka::Consumer> consumer_;
  std::vector<WalContentUnit> insert_wal_list_;
  uint32_t last_ts_;

  std::vector<UpdateWalUnit> update_wal_list_;
  std::vector<std::string> message_vector_;  // used to hold the polled messages
  cppkafka::Configuration config_;

  static const bool registered_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_PARSER_H_