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

#ifdef BUILD_KAFKA_WAL_WRITER

#include <csignal>
#include <filesystem>
#include <iostream>
#include <queue>

#include <glog/logging.h>

#include <boost/program_options.hpp>
#include "cppkafka/cppkafka.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/third_party/httplib.h"

namespace gs {

// Give a WAL(in string format), forward to the Interactive Engine, which should
// be on the same machine, and the engine will write the WAL to the disk.
class WalSender {
 public:
  static constexpr int32_t CONNECTION_TIMEOUT = 10;
  static constexpr int32_t READ_TIMEOUT = 60;
  static constexpr int32_t WRITE_TIMEOUT = 60;
  WalSender(const std::string& endpoint, const std::string& dst_url)
      : host_(endpoint.substr(0, endpoint.find(':'))),
        port_(std::stoi(endpoint.substr(endpoint.find(':') + 1))),
        client_(host_, port_),
        req_url_(dst_url) {
    client_.set_connection_timeout(CONNECTION_TIMEOUT);
    client_.set_read_timeout(READ_TIMEOUT);
    client_.set_write_timeout(WRITE_TIMEOUT);
  }

  void send(const std::string& payload) {
    httplib::Headers headers = {{"Content-Type", "application/octet-stream"}};
    auto res =
        client_.Post(req_url_, headers, payload, "application/octet-stream");
    if (res) {
      LOG(INFO) << "Send to engine: " << res->status << ", " << res->body;
    } else {
      LOG(ERROR) << "Send to engine failed: " << res.error();
    }
  }

 private:
  std::string host_;
  int port_;
  httplib::Client client_;
  std::string req_url_;
};  // namespace gs

}  // namespace gs

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  std::string brokers;
  std::string topic_name;
  std::string group_id;
  std::string engine_endpoint;
  std::string req_url;

  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "kafka-brokers,b", bpo::value<std::string>(&brokers)->required(),
      "Kafka brokers list")(
      "url,u",
      bpo::value<std::string>(&req_url)->default_value("/v1/graph/1/wal"),
      "req_url")(
      "group-id,g",
      bpo::value<std::string>(&group_id)->default_value("interactive_group"),
      "Kafka group id")(
      "engine-endpoint,e",
      bpo::value<std::string>(&engine_endpoint)->default_value(""),
      "Interactive engine endpoint")(
      "topic,t", bpo::value<std::string>(&topic_name)->required(),
      "Kafka topic name");

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  try {
    bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
    bpo::notify(vm);
  } catch (std::exception& e) {
    std::cerr << "Error parsing command line: " << e.what() << std::endl;
    std::cerr << e.what() << std::endl;
    std::cout << desc << std::endl;
    return -1;
  }

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  auto kafka_brokers = vm["kafka-brokers"].as<std::string>();
  LOG(INFO) << "Kafka brokers: " << kafka_brokers;
  LOG(INFO) << "engine endpoint: " << engine_endpoint;

  // Construct the configuration
  cppkafka::Configuration config = {{"metadata.broker.list", kafka_brokers},
                                    {"group.id", group_id},
                                    // Disable auto commit
                                    {"enable.auto.commit", false}};

  gs::KafkaWalConsumer consumer(config, topic_name, 1);
  std::unique_ptr<gs::WalSender> sender;
  if (engine_endpoint.empty()) {
    LOG(INFO) << "Not forwarding to engine";
  } else {
    sender = std::make_unique<gs::WalSender>(engine_endpoint, req_url);
  }

  // Create the consumer

  while (true) {
    auto msg = consumer.poll();
    if (msg.empty()) {
      LOG(INFO) << "No message polled, exit";
      break;
    }
    LOG(INFO) << "Received message:" << msg.size();
    if (sender) {
      sender->send(msg);
    }
  }

  LOG(INFO) << "Consuming messages from topic " << topic_name;
  return 0;
}

#endif