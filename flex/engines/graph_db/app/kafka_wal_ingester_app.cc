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

#include "flex/engines/graph_db/app/kafka_wal_ingester_app.h"
#include "cppkafka/cppkafka.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/wal/kafka_wal_utils.h"

namespace gs {
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER

struct WalIngester {
  constexpr static size_t BUFFSIZ = 4096;
  GraphDBSession& session_;
  timestamp_t begin_;
  timestamp_t end_;
  timestamp_t ingested_plus_one_;
  std::vector<std::string> data_;
  // 0: not exist, 1: exist, 2: ingested
  std::vector<uint8_t> states_;

  void resize() {
    size_t origin_len = data_.size();
    std::vector<std::string> new_data(origin_len + BUFFSIZ);
    std::vector<uint8_t> new_states(origin_len + BUFFSIZ, 0);
    size_t idx = (ingested_plus_one_ - begin_) % origin_len;
    for (size_t i = 0; i < origin_len; ++i) {
      new_data[i] = data_[idx];
      new_states[i] = states_[idx];
      if (states_[idx]) {
        end_ = ingested_plus_one_ + i + 1;
      }
      ++idx;
      idx %= origin_len;
    }
    data_ = std::move(new_data);
    states_ = std::move(new_states);
    begin_ = ingested_plus_one_;
  }

  timestamp_t last_ingested() const { return ingested_plus_one_ - 1; }
  WalIngester(GraphDBSession& session, timestamp_t cur)
      : session_(session),
        begin_(cur),
        end_(cur),
        ingested_plus_one_(cur),
        data_(BUFFSIZ),
        states_(BUFFSIZ, 0) {}

  bool empty() const { return ingested_plus_one_ == end_; }

  void ingest_impl(const std::string& data) {
    auto header = reinterpret_cast<const WalHeader*>(data.data());
    if (header->type == 0) {
      InsertTransaction::IngestWal(
          session_.graph(), header->timestamp,
          const_cast<char*>(data.data()) + sizeof(WalHeader), header->length,
          session_.allocator());
    } else {
      auto txn = session_.GetUpdateTransaction();
      auto header = reinterpret_cast<const WalHeader*>(data.data());
      txn.IngestWal(session_.graph(), session_.db().work_dir(),
                    header->timestamp,
                    const_cast<char*>(data.data()) + sizeof(WalHeader),
                    header->length, session_.allocator());
      txn.Commit();
    }
  }

  void ingest() {
    size_t len = data_.size();
    size_t idx = (ingested_plus_one_ - begin_) % len;
    bool flag = false;
    while (states_[idx] == 2 || states_[idx] == 1) {
      if (states_[idx] == 1) {
        ingest_impl(data_[idx]);
      }
      states_[idx] = 0;
      ++ingested_plus_one_;
      ++idx;
      idx %= len;
      flag = true;
    }
    if (flag) {
      session_.commit(ingested_plus_one_);
    }
  }
  void push(const std::string& data) {
    auto header = reinterpret_cast<const WalHeader*>(data.data());
    if (header->timestamp < begin_) {
      LOG(ERROR) << "Invalid timestamp: " << header->timestamp;
      return;
    }
    size_t index;
    size_t n = data_.size();
    if (header->timestamp < end_) {
      index = (header->timestamp - begin_) % n;
    } else if (header->timestamp - begin_ < n) {
      index = header->timestamp - begin_;
      end_ = header->timestamp + 1;
    } else {
      ingest();
      while (header->timestamp - ingested_plus_one_ + 1 > states_.size()) {
        resize();
      }
      index = (header->timestamp - begin_) % states_.size();
      end_ = header->timestamp + 1;
    }
    if (header->length == 0) {
      states_[index] = 2;
    } else if (header->type == 0) {
      ingest_impl(data);
      states_[index] = 2;
    } else {
      states_[index] = 1;
      data_[index] = data;
    }
  }
};

class KafkaWalConsumer {
 public:
  static constexpr const std::chrono::milliseconds POLL_TIMEOUT =
      std::chrono::milliseconds(100);

  // always track all partitions and from begining
  KafkaWalConsumer(WalIngester& ingester, cppkafka::Configuration config,
                   const std::string& topic_name, int32_t thread_num)
      : ingester_(ingester) {
    auto topic_partitions = get_all_topic_partitions(config, topic_name, false);
    consumers_.reserve(topic_partitions.size());
    for (size_t i = 0; i < topic_partitions.size(); ++i) {
      consumers_.emplace_back(std::make_unique<cppkafka::Consumer>(config));
      consumers_.back()->assign({topic_partitions[i]});
    }
  }

  void poll() {
    for (auto& consumer : consumers_) {
      auto msgs = consumer->poll_batch(1024);
      for (const auto& msg : msgs) {
        if (msg) {
          if (msg.get_error()) {
            if (!msg.is_eof()) {
              LOG(INFO) << "[+] Received error notification: "
                        << msg.get_error();
            }
          } else {
            std::string payload = msg.get_payload();
            ingester_.push(payload);
            consumer->commit(msg);
          }
        }
      }
    }
  }

 private:
  std::vector<std::unique_ptr<cppkafka::Consumer>> consumers_;
  WalIngester& ingester_;
};

void Ingest(const std::string& data, GraphDBSession& session) {}

bool KafkaWalIngesterApp::Query(GraphDBSession& graph, Decoder& input,
                                Encoder& output) {
  cppkafka::Configuration config;
  std::string topic_name;
  while (!input.empty()) {
    auto key = input.get_string();
    auto value = input.get_string();
    if (key == "topic_name") {
      topic_name = value;
    } else {
      LOG(INFO) << "Kafka config: " << key << " = " << value;
      config.set(std::string(key), std::string(value));
    }
  }
  LOG(INFO) << "Kafka brokers: " << config.get("metadata.broker.list");
  timestamp_t cur_ts = graph.db().get_last_ingested_wal_ts() + 1;
  gs::WalIngester ingester(graph, cur_ts);
  gs::KafkaWalConsumer consumer(ingester, config, topic_name, 1);
  while (!force_stop_.load()) {
    consumer.poll();
    ingester.ingest();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  while (!ingester.empty()) {
    consumer.poll();
    ingester.ingest();
  }
  int64_t ts = ingester.last_ingested();
  output.put_long(ts);
  return true;
}

bool KafkaWalIngesterApp::terminal() {
  force_stop_.store(true);
  return true;
}

AppWrapper KafkaWalIngesterAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new KafkaWalIngesterApp(), NULL);
}
#endif
}  // namespace gs
