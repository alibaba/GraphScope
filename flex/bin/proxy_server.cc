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

#include <filesystem>
#include <iostream>
#include "stdlib.h"

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/http_server/service/proxy_service.h"
#include "flex/utils/service_utils.h"

#include <boost/program_options.hpp>

#include <glog/logging.h>

namespace bpo = boost::program_options;

namespace gs {
// Function to parse endpoints from a string
bool parse_endpoints(const std::string& input_string,
                     std::vector<std::pair<std::string, uint16_t>>& endpoints) {
  std::istringstream iss(input_string);
  std::string endpoint;

  while (std::getline(iss, endpoint, ',')) {
    // Split the endpoint into host and port using ':'
    size_t delimiter_pos = endpoint.find(':');
    if (delimiter_pos == std::string::npos) {
      std::cerr << "Invalid endpoint: " << endpoint << ", missing delimiter ':'"
                << std::endl;
      continue;
    }

    std::string host = endpoint.substr(0, delimiter_pos);
    std::string port_str = endpoint.substr(delimiter_pos + 1);
    uint16_t port;
    try {
      port = std::stoull(port_str);
    } catch (const std::invalid_argument& e) {
      LOG(ERROR) << "Invalid port: " << port_str << ", must be a number"
                 << std::endl;
      return false;
    }

    // Check for valid port range
    if (port < 1 || port > 65535) {
      LOG(ERROR) << "Invalid port: " << port << ", must be between 1 and 65535"
                 << std::endl;
      return false;
    }
    endpoints.push_back({host, port});
  }
  return true;
}
}  // namespace gs

/**
 * The main entrance for ProxyServer.
 * The ProxyServer will block if one request is not executed by the server.
 */
int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help,h", "Display help messages")(
      "endpoints,e", bpo::value<std::string>()->required(),
      "The endpoints of the proxy server, e.g., {ip}:{port},{ip}:{port},...")(
      "heartbeat-interval,i", bpo::value<int>()->default_value(1),
      "The interval of heartbeat check in seconds")(
      "enable-heartbeat-check", bpo::value<bool>()->default_value(false),
      "Enable heartbeat check or not")(
      "port,p", bpo::value<uint16_t>()->default_value(9999),
      "The port of the proxy server")(
      "hang-until-success", bpo::value<bool>()->default_value(true),
      "Hang until the request is successfully forwarded")(
      "parallelism", bpo::value<int>()->default_value(1),
      "The number of threads to handle requests");

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  if (!vm.count("endpoints")) {
    LOG(FATAL) << "endpoints is not specified";
    return 0;
  }
  std::vector<std::pair<std::string, uint16_t>> endpoints;
  if (!gs::parse_endpoints(vm["endpoints"].as<std::string>(), endpoints)) {
    LOG(FATAL) << "Failed to parse endpoints";
    return 0;
  }

  LOG(INFO) << "got endpoints of size: " << endpoints.size()
            << ", :" << gs::to_string(endpoints);

  uint32_t shard_num = 1;
  uint16_t http_port = 9999;
  if (vm.count("port")) {
    http_port = vm["port"].as<uint16_t>();
  }
  if (vm.count("parallelism")) {
    shard_num = vm["parallelism"].as<int>();
  }

  if (!server::ProxyService::get()
           .init(shard_num, http_port, endpoints,
                 vm["enable-heartbeat-check"].as<bool>(),
                 vm["heartbeat-interval"].as<int32_t>(),
                 vm["hang-until-success"].as<bool>())
           .ok()) {
    LOG(FATAL) << "Failed to init ProxyService";
    return 0;
  }
  server::ProxyService::get().run_and_wait_for_exit();

  return 0;
}
