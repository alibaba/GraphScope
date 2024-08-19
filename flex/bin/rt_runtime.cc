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
#include <chrono>
#include <fstream>
#include <iostream>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

std::string read_pb(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);

  if (!file.is_open()) {
    LOG(FATAL) << "open pb file: " << filename << " failed...";
    return "";
  }

  file.seekg(0, std::ios::end);
  size_t size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::string buffer;
  buffer.resize(size);

  file.read(&buffer[0], size);

  file.close();

  return buffer;
}

void load_params(const std::string& filename,
                 std::vector<std::map<std::string, std::string>>& map) {
  std::ifstream in(filename);
  if (!in.is_open()) {
    LOG(FATAL) << "open params file: " << filename << " failed...";
    return;
  }
  std::string line;
  std::vector<std::string> keys;
  std::getline(in, line);
  std::stringstream ss(line);
  std::string key;
  while (std::getline(ss, key, '|')) {
    keys.push_back(key);
    LOG(INFO) << key;
  }
  while (std::getline(in, line)) {
    std::map<std::string, std::string> m;
    std::stringstream ss(line);
    std::string value;
    for (auto& key : keys) {
      std::getline(ss, value, '|');
      m[key] = value;
    }
    map.push_back(m);
  }
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "query-file,q", bpo::value<std::string>(), "query file")(
      "params_file,p", bpo::value<std::string>(), "params file");

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

  uint32_t shard_num = vm["shard-num"].as<uint32_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";

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

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema.ok()) {
    LOG(ERROR) << "Failed to load graph schema from " << graph_schema_path;
    return -1;
  }
  db.Open(schema.value(), data_path, shard_num);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
  std::string req_file = vm["query-file"].as<std::string>();
  std::string query = read_pb(req_file);
  auto txn = db.GetReadTransaction();
  std::vector<std::map<std::string, std::string>> map;
  load_params(vm["params_file"].as<std::string>(), map);
  physical::PhysicalPlan pb;
  pb.ParseFromString(query);
  size_t count{0};
  std::vector<char> buffer;
  gs::Encoder output(buffer);
  int num = 0;
  for (auto& m : map) {
    auto start = std::chrono::high_resolution_clock::now();
    auto ctx = gs::runtime::runtime_eval(pb, txn, m);
    output.clear();
    gs::runtime::eval_sink(ctx, txn, output);
    auto end = std::chrono::high_resolution_clock::now();
    count += std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                 .count();
    num++;
  }
  LOG(INFO) << "Avg time: " << count / num / 1000 << " millseconds";

  // std::cout << timer.get_time() / 1us << " microseconds\n";
}