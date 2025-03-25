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

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/graph_db_service.h"
#include "flex/engines/http_server/options.h"
#include "flex/utils/service_utils.h"
#include "grape/util.h"

#include <boost/program_options.hpp>
#include <seastar/core/alien.hh>

#include <glog/logging.h>

using namespace server;
namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "http-port,p", bpo::value<uint16_t>()->default_value(10000),
      "http port of query handler")("data-path,d", bpo::value<std::string>(),
                                    "data directory path")(
      "warmup,w", bpo::value<bool>()->default_value(false),
      "warmup graph data")("memory-level,m",
                           bpo::value<int>()->default_value(1))(
      "compiler-path,c", bpo::value<std::string>()->default_value(""))(
      "sharding-mode", bpo::value<std::string>()->default_value("cooperative"))(
      "wal-uri",
      bpo::value<std::string>()->default_value("file://{GRAPH_DATA_DIR}/wal"));
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }

  bool enable_dpdk = false;
  bool warmup = vm["warmup"].as<bool>();
  int memory_level = vm["memory-level"].as<int>();
  uint32_t shard_num = vm["shard-num"].as<uint32_t>();
  uint16_t http_port = vm["http-port"].as<uint16_t>();

  std::string data_path = "";

  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  std::string compiler_path = vm["compiler-path"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  gs::blockSignal(SIGINT);
  gs::blockSignal(SIGTERM);

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();
  std::string graph_schema_path = data_path + "/graph.yaml";
  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema.ok()) {
    LOG(FATAL) << "Failed to load schema: " << schema.status().error_message();
  }
  gs::GraphDBConfig config(schema.value(), data_path, compiler_path, shard_num);
  config.memory_level = memory_level;
  config.wal_uri = vm["wal-uri"].as<std::string>();
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  db.Open(config);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  // start service
  LOG(INFO) << "GraphScope http server start to listen on port " << http_port;

  server::ServiceConfig service_config;
  service_config.shard_num = shard_num;
  service_config.dpdk_mode = enable_dpdk;
  service_config.query_port = http_port;
  service_config.start_admin_service = false;
  service_config.start_compiler = false;
  service_config.set_sharding_mode(vm["sharding-mode"].as<std::string>());
  server::GraphDBService::get().init(service_config);

  server::GraphDBService::get().run_and_wait_for_exit();

  return 0;
}
