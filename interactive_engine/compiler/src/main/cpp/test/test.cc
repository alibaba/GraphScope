/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include "graph_planner.h"

#include <sys/stat.h>

std::string get_dir_name() {
  // Get the directory of this source file
  std::string current_dir = __FILE__;
  size_t pos = current_dir.find_last_of("/");
  current_dir = current_dir.substr(0, pos);
  return current_dir;
}

void check_path_exits(const std::string& path) {
    // split path by ':'
    std::vector<std::string> paths;
    std::string::size_type start = 0;
    std::string::size_type end = path.find(':');
    while (end != std::string::npos) {
        auto sub_path = path.substr(start, end - start);
        paths.push_back(sub_path);
        start = end + 1;
        end = path.find(':', start);
    }
    auto sub_path = path.substr(start);
    paths.push_back(sub_path);

    for (const auto& p : paths) {
        struct stat buffer;
        if (stat(p.c_str(), &buffer) != 0) {
            std::cerr << "Path not exists: " << p << std::endl;
            exit(1);
        }
    }
     std::cout<< "Path exists: " << path << std::endl;
}

int main(int argc, char **argv) {
  auto current_dir = get_dir_name();
  std::string java_class_path =
      current_dir + "/../../../../target/compiler-0.0.1-SNAPSHOT.jar";
      java_class_path+= ":" + current_dir + "/../../../../target/libs/";
  std::string jna_class_path =
      current_dir + "/../../../../../executor/ir/target/release/";
  std::string graph_schema_yaml =
      current_dir +
      "/../../../../../../flex/interactive/examples/modern_graph/graph.yaml";
  std::string graph_statistic_json =
      current_dir +
      "/../../../test/resources/statistics/modern_statistics.json";

  // check director or file exists
  check_path_exits(java_class_path);
  check_path_exits(jna_class_path);
  check_path_exits(graph_schema_yaml);
  check_path_exits(graph_statistic_json);

  gs::GraphPlannerWrapper graph_planner_wrapper(
      java_class_path, jna_class_path, graph_schema_yaml, graph_statistic_json);

  std::string cypher_query_string = "MATCH (a:person) RETURN a.name";
  std::string config_path =
      current_dir +
      "/../../../../../../flex/tests/hqps/interactive_config_test.yaml";
  auto plan =
      graph_planner_wrapper.CompilePlan(config_path, cypher_query_string);
  std::cout << "Plan: " << plan.physical_plan.DebugString() << std::endl;
  std::cout << "schema: " << plan.result_schema << std::endl;
  return 0;
}