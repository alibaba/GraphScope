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
#include "flex/engines/graph_db/database/wal/kafka_wal_parser.h"

namespace gs {
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER

class KafkaWalConsumer {
 public:
  struct CustomComparator {
    inline bool operator()(const std::string& lhs, const std::string& rhs) {
      const WalHeader* header1 = reinterpret_cast<const WalHeader*>(lhs.data());
      const WalHeader* header2 = reinterpret_cast<const WalHeader*>(rhs.data());
      return header1->timestamp > header2->timestamp;
    }
  };
  static constexpr const std::chrono::milliseconds POLL_TIMEOUT =
      std::chrono::milliseconds(100);

  // always track all partitions and from begining
  KafkaWalConsumer(cppkafka::Configuration config,
                   const std::string& topic_name, int32_t thread_num) {
    auto topic_partitions = get_all_topic_partitions(config, topic_name);
    consumers_.reserve(topic_partitions.size());
    for (size_t i = 0; i < topic_partitions.size(); ++i) {
      consumers_.emplace_back(std::make_unique<cppkafka::Consumer>(config));
      consumers_.back()->assign({topic_partitions[i]});
    }
  }

  std::string poll() {
    for (auto& consumer : consumers_) {
      auto msg = consumer->poll();
      if (msg) {
        if (msg.get_error()) {
          if (!msg.is_eof()) {
            LOG(INFO) << "[+] Received error notification: " << msg.get_error();
          }
        } else {
          std::string payload = msg.get_payload();
          message_queue_.push(payload);
          consumer->commit(msg);
        }
      }
    }
    if (message_queue_.empty()) {
      return "";
    }
    std::string payload = message_queue_.top();
    message_queue_.pop();
    return payload;
  }

 private:
  std::vector<std::unique_ptr<cppkafka::Consumer>> consumers_;
  std::priority_queue<std::string, std::vector<std::string>, CustomComparator>
      message_queue_;
};

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
      config.set(std::string(key), std::string(value));
    }
  }
  LOG(INFO) << "Kafka brokers: " << config.get("metadata.broker.list");

  gs::KafkaWalConsumer consumer(config, topic_name, 1);
  // TODO: how to make it stop
  while (graph.db().kafka_wal_ingester_state()) {
    auto res = consumer.poll();
    if (res.empty()) {
      std::this_thread::sleep_for(gs::KafkaWalConsumer::POLL_TIMEOUT);
      continue;
    }

    auto header = reinterpret_cast<const WalHeader*>(res.data());
    if (header->type == 0) {
      auto txn = graph.GetInsertTransaction();
      txn.IngestWal(graph.graph(), txn.timestamp(),
                    const_cast<char*>(res.data()) + sizeof(WalHeader),
                    header->length, txn.allocator());
      txn.Commit();
    } else if (header->type == 1) {
      auto txn = graph.GetUpdateTransaction();
      txn.IngestWal(graph.graph(), graph.db().work_dir(), txn.timestamp(),
                    const_cast<char*>(res.data()) + sizeof(WalHeader),
                    header->length, txn.allocator());
      txn.Commit();
    } else {
      LOG(ERROR) << "Unknown wal type: " << header->type;
    }
  }
  return true;
}
AppWrapper KafkaWalIngesterAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new KafkaWalIngesterApp(), NULL);
}
#endif
}  // namespace gs
