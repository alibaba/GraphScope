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

#include "flex/planner/graph_planner.h"
#include <glog/logging.h>
#include <filesystem>
#include <string>

int main(int argc, char** argv) {
  if (argc != 6) {
    LOG(ERROR) << "Usage: " << argv[0]
               << " <java_path> <jna_path> <graph_schema_path> "
                  "<compiler_config> <cypher_query>";
    LOG(ERROR) << "But got " << argc << " arguments.";
    return 1;
  }
  std::string java_path = argv[1];
  std::string jna_path = argv[2];
  std::string graph_schema_path = argv[3];
  std::string compiler_config_path = argv[4];
  std::string cypher_query = argv[5];
  if (java_path.empty() || jna_path.empty() || graph_schema_path.empty()) {
    LOG(ERROR) << "Invalid input.";
    return 1;
  }
  if (!std::filesystem::exists(compiler_config_path)) {
    LOG(ERROR) << "Invalid compiler config path.";
    return 1;
  }
  gs::jni::GraphPlannerWrapper planner(java_path, jna_path, graph_schema_path);
  if (!planner.is_valid()) {
    LOG(ERROR) << "Invalid GraphPlannerWrapper.";
    return 1;
  }
  auto plan = planner.CompilePlan(compiler_config_path, cypher_query);
  CHECK(plan.plan_size() == 3) << "Invalid plan size: " << plan.plan_size();
  return 0;
}
