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

#include <sys/wait.h>  // for waitpid()
#include <unistd.h>    // for fork() and execvp()
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
#include "flex/engines/graph_db/runtime/utils/opr_timer.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace bpo = boost::program_options;

bool generate_plan(const std::string& query, const std::string& compiler_yaml,
                   physical::PhysicalPlan& plan) {
  // dump query to file
  const char* graphscope_dir = getenv("GRAPHSCOPE_DIR");
  if (graphscope_dir == nullptr) {
    std::cerr << "GRAPHSCOPE_DIR is not set!" << std::endl;
    graphscope_dir = "../../../GraphScope/";
  }

  auto id = std::this_thread::get_id();

  std::stringstream ss;
  ss << id;
  std::string thread_id = ss.str();

  const std::string compiler_config_path =
      "/data/0110/GraphScope//flex/tests/hqps/interactive_config_test.yaml";
  const std::string query_file = "/tmp/temp" + thread_id + ".cypher";
  const std::string output_file = "/tmp/temp" + thread_id + ".pb";
  const std::string jar_path = std::string(graphscope_dir) +
                               "/interactive_engine/compiler/target/"
                               "compiler-0.0.1-SNAPSHOT.jar:" +
                               std::string(graphscope_dir) +
                               "/interactive_engine/compiler/target/libs/*";
  const std::string djna_path =
      std::string("-Djna.library.path=") + std::string(graphscope_dir) +
      "/interactive_engine/executor/ir/target/release/";
  const std::string schema_path = "-Dgraph.schema=" + compiler_yaml;
  auto raw_query = query;
  {
    std::ofstream out(query_file);
    out << query;
    out.close();
  }

  // call compiler to generate plan
  {
    pid_t pid = fork();

    if (pid == -1) {
      std::cerr << "Fork failed!" << std::endl;
      return false;
    } else if (pid == 0) {
      const char* const args[] = {
          "java",
          "-cp",
          jar_path.c_str(),
          schema_path.c_str(),
          djna_path.c_str(),
          "com.alibaba.graphscope.common.ir.tools.GraphPlanner",
          compiler_config_path.c_str(),
          query_file.c_str(),
          output_file.c_str(),
          "/tmp/temp.cypher.yaml",
          nullptr  // execvp expects a null-terminated array
      };
      execvp(args[0], const_cast<char* const*>(args));

      std::cerr << "Exec failed!" << std::endl;
      return false;
    } else {
      int status;
      waitpid(pid, &status, 0);
      if (WIFEXITED(status)) {
        std::cout << "Child exited with status " << WEXITSTATUS(status)
                  << std::endl;
      }

      {
        std::ifstream file(output_file, std::ios::binary);

        if (!file.is_open()) {
          return false;
        }

        file.seekg(0, std::ios::end);
        size_t size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::string buffer;
        buffer.resize(size);

        file.read(&buffer[0], size);

        file.close();
        if (!plan.ParseFromString(std::string(buffer))) {
          return false;
        }
      }
      // clean up temp files
      {
        unlink(output_file.c_str());
        unlink(query_file.c_str());
        // unlink(compiler_config_path.c_str());
        //  unlink("/tmp/temp.cypher.yaml");
        //  unlink("/tmp/temp.cypher.yaml_extra_config.yaml");
      }
    }
  }

  return true;
}

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

gs::runtime::Context eval_plan(
    const physical::PhysicalPlan& plan, gs::ReadTransaction& txn,
    const std::map<std::string, std::string>& params) {
  gs::runtime::GraphReadInterface gri(txn);
  gs::runtime::OprTimer timer;
  return gs::runtime::PlanParser::get()
      .parse_read_pipeline(gri.schema(), gs::runtime::ContextMeta(), plan)
      .Execute(gri, gs::runtime::Context(), params, timer);
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

  uint32_t shard_num = vm["shard-num"].as<uint32_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string output_path = "";

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
  if (vm.count("output-file")) {
    output_path = vm["output-file"].as<std::string>();
  }

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
  std::string q_str = read_pb(req_file);
  physical::PhysicalPlan pb;

  generate_plan(q_str, graph_schema_path, pb);
  LOG(INFO) << pb.DebugString();
  auto txn = db.GetReadTransaction();
  std::vector<std::map<std::string, std::string>> map;

  std::vector<char> outputs;

  std::map<std::string, std::string> mp;
  auto ctx = eval_plan(pb, txn, mp);
  gs::Encoder output(outputs);
  gs::runtime::Sink::sink_beta(ctx, txn, output);

  return 0;
}
