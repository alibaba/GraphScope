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
#include "flex/engines/http_server/options.h"

#include <boost/program_options.hpp>
#include <fstream>

#include <glog/logging.h>

using namespace server;
namespace bpo = boost::program_options;

std::vector<std::vector<char>> parse_query_file(const std::string& fname) {
  std::vector<std::vector<char>> ret;
  std::ifstream fin(fname);
  std::string line;

  std::vector<std::string> types;

  while (std::getline(fin, line)) {
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string token;
    while (std::getline(ss, token, '|')) {
      tokens.push_back(token);
    }
    if (types.empty()) {
      types = tokens;
    } else {
      CHECK_EQ(tokens.size(), types.size());
      std::vector<char> buf;
      gs::Encoder encoder(buf);
      size_t n = tokens.size();
      for (size_t k = 0; k < n; ++k) {
        if (types[k] == "INT") {
          int val = std::stoi(tokens[k]);
          encoder.put_int(val);
        } else if (types[k] == "LONG") {
          int64_t val = std::stoll(tokens[k]);
          encoder.put_long(val);
        } else if (types[k] == "STRING") {
          encoder.put_string(tokens[k]);
        } else {
          LOG(FATAL) << "unrecognize type: " << types[k];
        }
      }
      ret.emplace_back(std::move(buf));
    }
  }
  return ret;
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")("version,v",
                                                     "Display version")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "memory-level,m", bpo::value<int>()->default_value(1))(
      "stored-procedure-lib,l", bpo::value<std::string>(),
      "stored procedure library path")(
      "query-file,q", bpo::value<std::string>(), "query parameters file")(
      "query-num,n", bpo::value<int>()->default_value(0))(
      "output-file,o", bpo::value<std::string>(), "output file");
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

  int memory_level = vm["memory-level"].as<int>();

  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string plugin_path = "";
  std::string query_file_path = "";
  std::string output_path = "";
  int query_num = vm["query-num"].as<int>();

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
  if (!vm.count("stored-procedure-lib")) {
    LOG(ERROR) << "stored-procedure-lib is required";
    return -1;
  }
  plugin_path = vm["stored-procedure-lib"].as<std::string>();
  if (!vm.count("query-file")) {
    LOG(ERROR) << "query-file is required";
    return -1;
  }
  query_file_path = vm["query-file"].as<std::string>();
  if (vm.count("output-file")) {
    output_path = vm["output-file"].as<std::string>();
  }

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema.ok()) {
    LOG(FATAL) << "Failed to load schema: " << schema.status().error_message();
  }
  gs::GraphDBConfig config(schema.value(), data_path, 1);
  config.memory_level = memory_level;
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  db.Open(config);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  auto app_factory = std::make_shared<gs::SharedLibraryAppFactory>(plugin_path);
  auto app_wrapper = app_factory->CreateApp(db);
  auto app = app_wrapper.app();

  auto parameters = parse_query_file(query_file_path);

  auto& session = db.GetSession(0);
  if (query_num == 0) {
    query_num = parameters.size();
  }
  std::vector<std::vector<char>> outputs(query_num);

  double t1 = -grape::GetCurrentTime();
  for (int i = 0; i < query_num; ++i) {
    auto& parameter = parameters[i % parameters.size()];
    gs::Decoder input(parameter.data(), parameter.size());
    gs::Encoder output(outputs[i]);
    app->run(session, input, output);
  }
  t1 += grape::GetCurrentTime();

  double t2 = -grape::GetCurrentTime();
  for (int i = 0; i < query_num; ++i) {
    auto& parameter = parameters[i % parameters.size()];
    gs::Decoder input(parameter.data(), parameter.size());
    outputs[i].clear();
    gs::Encoder output(outputs[i]);
    app->run(session, input, output);
  }
  t2 += grape::GetCurrentTime();

  double t3 = -grape::GetCurrentTime();
  for (int i = 0; i < query_num; ++i) {
    auto& parameter = parameters[i % parameters.size()];
    gs::Decoder input(parameter.data(), parameter.size());
    outputs[i].clear();
    gs::Encoder output(outputs[i]);
    app->run(session, input, output);
  }
  t3 += grape::GetCurrentTime();

  LOG(INFO) << "Finished run " << query_num << " queries, elapsed " << t1
            << " s, avg " << t1 / static_cast<double>(query_num) * 1000000
            << " us";
  LOG(INFO) << "Finished run " << query_num << " queries, elapsed " << t2
            << " s, avg " << t2 / static_cast<double>(query_num) * 1000000
            << " us";
  LOG(INFO) << "Finished run " << query_num << " queries, elapsed " << t3
            << " s, avg " << t3 / static_cast<double>(query_num) * 1000000
            << " us";

  if (!output_path.empty()) {
    FILE* fout = fopen(output_path.c_str(), "a");
    for (auto& output : outputs) {
      fwrite(output.data(), sizeof(char), output.size(), fout);
    }
    fflush(fout);
    fclose(fout);
  }

  return 0;
}