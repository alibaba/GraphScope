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
#include "flex/engines/graph_db/database/wal/kafka_wal_utils.h"
#include "glog/logging.h"

namespace gs {
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER
std::vector<cppkafka::TopicPartition> get_all_topic_partitions(
    const cppkafka::Configuration& config, const std::string& topic_name,
    bool from_beginning) {
  std::vector<cppkafka::TopicPartition> partitions;
  cppkafka::Consumer consumer(config);  // tmp consumer
  LOG(INFO) << config.get("metadata.broker.list");
  LOG(INFO) << config.get("group.id");

  LOG(INFO) << "Get metadata for topic " << topic_name;
  auto meta_vector = consumer.get_metadata().get_topics({topic_name});
  if (meta_vector.empty()) {
    LOG(WARNING) << "Failed to get metadata for topic " << topic_name
                 << ", maybe the topic does not exist";
    return {};
  }
  auto metadata = meta_vector.front().get_partitions();
  for (const auto& partition : metadata) {
    if (from_beginning) {
      partitions.push_back(
          cppkafka::TopicPartition(topic_name, partition.get_id(), 0));
    } else {
      partitions.push_back(cppkafka::TopicPartition(
          topic_name, partition.get_id()));  // from the beginning
    }
  }
  return partitions;
}

std::optional<std::vector<char>> parse_uri(const std::string& wal_uri) {
  std::vector<char> buf;
  gs::Encoder encoder(buf);

  const std::string prefix = "kafka://";
  if (wal_uri.find(prefix) != 0) {
    LOG(ERROR) << "Invalid uri: " << wal_uri;
    return std::nullopt;
  }

  std::string hosts_part = wal_uri.substr(prefix.length());
  size_t query_pos = hosts_part.find('/');
  std::string hosts;
  std::string query;
  hosts = hosts_part.substr(0, query_pos);
  query = hosts_part.substr(query_pos + 1);

  std::string kafka_brokers = hosts;
  encoder.put_string(std::string("metadata.broker.list"));
  encoder.put_string(kafka_brokers);
  size_t top_pos = query.find('?');
  std::string topic;
  if (top_pos != std::string::npos) {
    topic = query.substr(0, top_pos);
    query = query.substr(top_pos + 1);
  } else {
    topic = query;
    query = "";
  }
  encoder.put_string(std::string("topic_name"));
  encoder.put_string(topic);
  std::istringstream query_stream(query);
  std::string pair;
  while (std::getline(query_stream, pair, '&')) {
    size_t eq_pos = pair.find('=');
    if (eq_pos != std::string::npos) {
      std::string key = pair.substr(0, eq_pos);
      std::string value = pair.substr(eq_pos + 1);
      encoder.put_string(key);
      encoder.put_string(value);
    }
  }
  encoder.put_string(std::string("enable.auto.commit"));
  encoder.put_string(std::string("false"));

  return buf;
}
#endif

}  // namespace gs