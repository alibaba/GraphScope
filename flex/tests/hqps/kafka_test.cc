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

#include <iostream>

#include <flex/engines/graph_db/database/wal/kafka_wal_parser.h>
#include <flex/engines/graph_db/database/wal/kafka_wal_writer.h>
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include <glog/logging.h>

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "usage: kafka_test <kafka_brokers> <kafka_topic> "
              << std::endl;
    return -1;
  }

  std::string kafka_brokers = argv[1];
  std::string kafka_topic = argv[2];
  LOG(INFO) << "Kafka brokers: " << kafka_brokers;

  // Write messages to the specified kafka topic, and read them back.
  gs::KafkaWalWriter writer(kafka_brokers);
  cppkafka::Configuration config = {{"metadata.broker.list", kafka_brokers},
                                    {"group.id", "test"},
                                    {"enable.auto.commit", false}};
  gs::KafkaWalParser parser(config);
  writer.open(kafka_topic, 0);

  // Let user enter number of messages to write
  LOG(INFO) << "Enter number of messages to write: ";
  int num_messages = 3;
  for (int i = 0; i < num_messages; ++i) {
    std::string message = "message " + std::to_string(i);
    grape::InArchive in_archive;
    in_archive.Resize(sizeof(gs::WalHeader));
    in_archive << message;
    auto header = reinterpret_cast<gs::WalHeader*>(in_archive.GetBuffer());
    header->timestamp = i + 1;
    header->type = 0;
    header->length = in_archive.GetSize() - sizeof(gs::WalHeader);
    LOG(INFO) << "Writing wal: " << header->timestamp << ", " << header->length;
    writer.append(in_archive.GetBuffer(), in_archive.GetSize());
  }
  auto offset = writer.getCurrentOffset();
  LOG(INFO) << "Current offset: " << std::get<0>(offset) << ", "
            << std::get<1>(offset);
  writer.close();
  LOG(INFO) << "Messages have been written to Kafka topic: " << argv[2]
            << std::endl;

  // Digest the messages
  std::vector<cppkafka::TopicPartition> topic_partitions = {
      {kafka_topic, 0, 0}};

  // parser.open(topic_partitions);
  parser.open(kafka_topic);
  auto last_ts = parser.last_ts();
  for (int i = 0; i < num_messages; ++i) {
    const gs::WalContentUnit& wal = parser.get_insert_wal(last_ts + i);
    if (wal.ptr) {
      grape::OutArchive out_archive;
      out_archive.SetSlice(wal.ptr, wal.size);
      std::string message;
      out_archive >> message;
      LOG(INFO) << "Read message: " << message;
    } else {
      LOG(ERROR) << "No message for timestamp " << last_ts + i;
    }
  }

  return 0;
}