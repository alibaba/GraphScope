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
#include "flex/engines/graph_db/app/cypher_app_utils.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
#include "flex/engines/graph_db/runtime/utils/opr_timer.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace bpo = boost::program_options;

std::string read_query(const std::string& filename) {
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

gs::runtime::Context eval_plan(
    const physical::PhysicalPlan& plan, gs::ReadTransaction& txn,
    const std::map<std::string, std::string>& params) {
  gs::runtime::GraphReadInterface gri(txn);
  gs::runtime::OprTimer timer;

  gs::runtime::Context ctx;
  {
    ctx = bl::try_handle_all(
        [&plan, &params, &gri, &timer]() {
          return gs::runtime::PlanParser::get()
              .parse_read_pipeline(gri.schema(), gs::runtime::ContextMeta(),
                                   plan)
              .value()
              .Execute(gri, gs::runtime::Context(), params, timer);
        },
        [&ctx](const gs::Status& err) {
          LOG(ERROR) << "Error in execution: " << err.error_message();
          return ctx;
        },
        [&](const bl::error_info& err) {
          LOG(ERROR) << "Error: " << err.error().value() << ", "
                     << err.exception()->what();
          return ctx;
        },
        [&]() {
          LOG(ERROR) << "Unknown error in execution";
          return ctx;
        });
  }
  return ctx;
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("data-path,d", bpo::value<std::string>(),
                                      "data directory path")(
      "query-file,q", bpo::value<std::string>(), "query file")(
      "compiler-path,c", bpo::value<std::string>()->default_value(""),
      "compiler path");

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

  std::string graph_schema_path = "";
  std::string data_path = "";

  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  graph_schema_path = data_path + "/graph.yaml";
  std::string compiler_path = vm["compiler-path"].as<std::string>();
  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema.ok()) {
    LOG(ERROR) << "Failed to load graph schema from " << graph_schema_path;
    return -1;
  }
  db.Open(schema.value(), data_path);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
  std::string req_file = vm["query-file"].as<std::string>();
  std::string query = read_query(req_file);
  std::unordered_map<std::string, physical::PhysicalPlan> plan_cache;
  if (!gs::generate_plan(query, data_path + "/statistics.json", compiler_path,
                         graph_schema_path, "/tmp", plan_cache)) {
    LOG(ERROR) << "Failed to generate plan";
    return -1;
  }
  auto txn = db.GetReadTransaction();

  auto pb = plan_cache[query];
  std::vector<char> outputs;

  auto ctx = eval_plan(pb, txn, {});
  gs::Encoder output(outputs);
  gs::runtime::Sink::sink_beta(ctx, txn, output);
  return 0;
}