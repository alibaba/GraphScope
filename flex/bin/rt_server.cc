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

#include "grape/util.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/server/options.h"
#include "flex/engines/graph_db/server/service.h"

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
      "http port of query handler")("graph-config,g", bpo::value<std::string>(),
                                    "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "bulk-load,l", bpo::value<std::string>(), "bulk-load config file");
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
  uint32_t shard_num = vm["shard-num"].as<uint32_t>();
  uint16_t http_port = vm["http-port"].as<uint16_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string bulk_load_config_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (vm.count("bulk-load")) {
    bulk_load_config_path = vm["bulk-load"].as<std::string>();
  }

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto ret = gs::Schema::LoadFromYaml(graph_schema_path, bulk_load_config_path);
  db.Init(std::get<0>(ret), std::get<1>(ret), std::get<2>(ret),
          std::get<3>(ret), data_path, shard_num);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  // start service
  LOG(INFO) << "GraphScope http server start to listen on port " << http_port;
  service::get().init(shard_num, http_port, enable_dpdk);
  service::get().run_and_wait_for_exit();

  return 0;
}
