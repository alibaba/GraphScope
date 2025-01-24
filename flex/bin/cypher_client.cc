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

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include "flex/third_party/httplib.h"

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")("version,v",
                                                     "Display version")(
      "uri,u", bpo::value<std::string>()->default_value("127.0.0.1"),
      "uri of the db")("port,p", bpo::value<int>()->default_value(10000),
                       "port number");
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

  std::string uri = vm["uri"].as<std::string>();
  int port = vm["port"].as<int>();
  httplib::Client cli(uri, port);
  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  while (true) {
    std::cout << ">>> ";
    std::string query;
    getline(std::cin, query);
    if (query == "exit") {
      break;
    }
    if (query == "") {
      continue;
    }
    query.append(1, '\xF6');
    query.append(1, 4);
    auto res = cli.Post("/v1/graph/current/query", query, "text/plain");
    std::string ret = res->body;
    std::cout << ret << std::endl;
  }
  return 0;
}