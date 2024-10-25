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
  WalSender(const std::string& host, int port, const std::string& graph_id)
      : host_(host), port_(port), client_(host_, port_), graph_id_(graph_id) {
    client_.set_connection_timeout(CONNECTION_TIMEOUT);
    client_.set_read_timeout(READ_TIMEOUT);
    client_.set_write_timeout(WRITE_TIMEOUT);
    req_url_ = "/v1/graph/" + graph_id_ + "/wal";
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
  std::string graph_id_;
  std::string req_url_;
};

}  // namespace gs

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  std::string brokers;
  std::string topic_name;
  std::string graph_id;
  std::string group_id;
  std::string engine_url;
  int32_t engine_port;

  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "kafka-brokers,b", bpo::value<std::string>(&brokers)->required(),
      "Kafka brokers list")(
      "graph-id,i", bpo::value<std::string>(&graph_id)->required(), "graph_id")(
      "group-id,g",
      bpo::value<std::string>(&group_id)->default_value("interactive_group"),
      "Kafka group id")("engine-url,u",
                        bpo::value<std::string>(&engine_url)->required(),
                        "Engine URL")(
      "engine-port,p", bpo::value<int32_t>(&engine_port)->required(),
      "Engine port");

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

  auto kafka_brokers = vm["kafka-brokers"].as<std::string>();
  LOG(INFO) << "Kafka brokers: " << kafka_brokers;
  LOG(INFO) << "engine endpoint: " << vm["engine-url"].as<std::string>() << ":"
            << vm["engine-port"].as<int32_t>();

  // Construct the configuration
  cppkafka::Configuration config = {{"metadata.broker.list", kafka_brokers},
                                    {"group.id", group_id},
                                    // Disable auto commit
                                    {"enable.auto.commit", false}};

  topic_name = "graph_" + graph_id + "_wal";
  // Create the consumer
  gs::WalSender sender(vm["engine-url"].as<std::string>(),
                       vm["engine-port"].as<int32_t>(), graph_id);
  gs::WalConsumer consumer(config, topic_name, 1);

  // signal(SIGINT, [](int) { consumer.terminate(); });

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  while (true) {
    auto msg = consumer.poll();
    if (msg.first == std::numeric_limits<uint32_t>::max()) {
      continue;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(INFO) << "Received message: <" << msg.first << " -> " << msg.second
              << ">";
    sender.send(msg.second);
  }

  LOG(INFO) << "Consuming messages from topic " << topic_name;

  return 0;
}