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

#include "flex/engines/graph_db/database/wal/kafka_wal_parser.h"
#include "flex/engines/graph_db/database/wal/wal.h"

namespace gs {

std::vector<cppkafka::TopicPartition> get_all_topic_partitions(
    const cppkafka::Configuration& config, const std::string& topic_name) {
  std::vector<cppkafka::TopicPartition> partitions;
  cppkafka::Consumer consumer(config);  // tmp consumer
  auto meta_vector = consumer.get_metadata().get_topics({topic_name});
  if (meta_vector.empty()) {
    LOG(WARNING) << "Failed to get metadata for topic " << topic_name
                 << ", maybe the topic does not exist";
    return {};
  }
  auto metadata = meta_vector.front().get_partitions();
  for (const auto& partition : metadata) {
    partitions.push_back(cppkafka::TopicPartition(
        topic_name, partition.get_id(), 0));  // from the beginning
  }
  return partitions;
}

std::unique_ptr<IWalParser> KafkaWalParser::Make(const std::string&) {
  const char* broker_list = std::getenv("KAFKA_BROKER_LIST");
  if (broker_list == nullptr) {
    LOG(FATAL) << "KAFKA_BROKER_LIST is not set";
  }
  const char* group_id = std::getenv("KAFKA_GROUP_ID");
  std::string group_id_str = group_id ? group_id : "interactive_consumer";
  cppkafka::Configuration config = {{"metadata.broker.list", broker_list},
                                    {"group.id", group_id_str}};
  return std::unique_ptr<IWalParser>(new KafkaWalParser(config));
}

KafkaWalParser::KafkaWalParser(const cppkafka::Configuration& config)
    : consumer_(nullptr), last_ts_(0), config_(config) {
  consumer_ = std::make_unique<cppkafka::Consumer>(config);
}

void KafkaWalParser::open(const std::string& topic_name) {
  auto topic_partitions = get_all_topic_partitions(config_, topic_name);
  open(topic_partitions);
}

void KafkaWalParser::open(
    const std::vector<cppkafka::TopicPartition>& topic_partitions) {
  consumer_->assign(topic_partitions);
  insert_wal_list_.resize(4096);

  while (true) {
    auto msgs = consumer_->poll_batch(MAX_BATCH_SIZE);
    if (msgs.empty() || msgs.empty()) {
      LOG(INFO) << "No message are polled, the topic has been all consumed.";
      break;
    }
    for (auto& msg : msgs) {
      if (msg) {
        if (msg.get_error()) {
          if (!msg.is_eof()) {
            LOG(INFO) << "[+] Received error notification: " << msg.get_error();
          }
        } else {
          message_vector_.emplace_back(msg.get_payload());
        }
      }
    }
  }

  for (auto& wal : message_vector_) {
    VLOG(1) << "Got wal:" << wal.size();
    const char* payload = wal.data();
    const WalHeader* header = reinterpret_cast<const WalHeader*>(payload);
    uint32_t cur_ts = header->timestamp;
    if (cur_ts == 0) {
      LOG(WARNING) << "Invalid timestamp 0, skip";
      continue;
    }
    int length = header->length;
    if (header->type) {
      UpdateWalUnit unit;
      unit.timestamp = cur_ts;
      unit.ptr = const_cast<char*>(payload + sizeof(WalHeader));
      unit.size = length;
      update_wal_list_.push_back(unit);
    } else {
      if (cur_ts >= insert_wal_list_.size()) {
        insert_wal_list_.resize(cur_ts + 1);
      }
      if (insert_wal_list_[cur_ts].ptr) {
        LOG(WARNING) << "Duplicated timestamp " << cur_ts << ", skip";
      }
      insert_wal_list_[cur_ts].ptr =
          const_cast<char*>(payload + sizeof(WalHeader));
      insert_wal_list_[cur_ts].size = length;
    }
    last_ts_ = std::max(cur_ts, last_ts_);
  }

  LOG(INFO) << "last_ts: " << last_ts_;
  if (!update_wal_list_.empty()) {
    std::sort(update_wal_list_.begin(), update_wal_list_.end(),
              [](const UpdateWalUnit& lhs, const UpdateWalUnit& rhs) {
                return lhs.timestamp < rhs.timestamp;
              });
  }
}

void KafkaWalParser::close() {
  if (consumer_) {
    consumer_.reset();
  }
  insert_wal_list_.clear();
}

uint32_t KafkaWalParser::last_ts() const { return last_ts_; }

const WalContentUnit& KafkaWalParser::get_insert_wal(uint32_t ts) const {
  if (insert_wal_list_[ts].ptr == NULL) {
    LOG(WARNING) << "No WAL for timestamp " << ts;
  }
  return insert_wal_list_[ts];
}
const std::vector<UpdateWalUnit>& KafkaWalParser::get_update_wals() const {
  return update_wal_list_;
}

const bool KafkaWalParser::registered_ = WalParserFactory::RegisterWalParser(
    "kafaka", static_cast<WalParserFactory::wal_parser_initializer_t>(
                  &KafkaWalParser::Make));

}  // namespace gs