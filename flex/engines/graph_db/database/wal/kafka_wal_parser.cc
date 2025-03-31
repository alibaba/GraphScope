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
#include "flex/engines/graph_db/database/wal/kafka_wal_utils.h"
#include "flex/engines/graph_db/database/wal/wal.h"

namespace gs {

std::unique_ptr<IWalParser> KafkaWalParser::Make(const std::string& uri) {
  // uri should be like
  // "kafka://localhost:9092,localhost:9093/my_topic?group.id=my_consumer_group"
  auto res = parse_uri(uri);
  if (!res) {
    LOG(FATAL) << "Failed to parse uri: " << uri;
    return nullptr;
  }
  gs::Decoder decoder(res.value().data(), res.value().size());
  std::string topic_name;
  cppkafka::Configuration config;
  while (!decoder.empty()) {
    auto key = decoder.get_string();
    auto value = decoder.get_string();
    if (key == "topic_name") {
      topic_name = value;
    } else {
      config.set(std::string(key), std::string(value));
    }
  }
  auto parser = std::unique_ptr<IWalParser>(new KafkaWalParser(config));
  parser->open(topic_name);
  return parser;
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
  uint32_t cnt = 0;
  while (true) {
    auto msgs = consumer_->poll_batch(MAX_BATCH_SIZE);
    if (msgs.empty() && cnt == last_ts_) {
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
          const std::string& wal = message_vector_.back();
          auto header = reinterpret_cast<const WalHeader*>(wal.data());
          if (header->timestamp == 0) {
            LOG(WARNING) << "Invalid timestamp 0, skip";
            continue;
          }
          if (header->type) {
            UpdateWalUnit unit;
            unit.timestamp = header->timestamp;
            unit.ptr = const_cast<char*>(wal.data() + sizeof(WalHeader));
            unit.size = header->length;
            update_wal_list_.push_back(unit);
            cnt++;
          } else {
            if (header->timestamp >= insert_wal_list_.size()) {
              insert_wal_list_.resize(header->timestamp + 1);
            }
            if (insert_wal_list_[header->timestamp].ptr) {
              LOG(WARNING) << "Duplicated timestamp " << header->timestamp
                           << ", skip";
              continue;
            }
            cnt++;
            insert_wal_list_[header->timestamp].ptr =
                const_cast<char*>(wal.data() + sizeof(WalHeader));
            insert_wal_list_[header->timestamp].size = header->length;
          }
          last_ts_ = std::max(header->timestamp, last_ts_);
        }
      }
    }
    consumer_->commit();
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
    "kafka", static_cast<WalParserFactory::wal_parser_initializer_t>(
                 &KafkaWalParser::Make));

}  // namespace gs