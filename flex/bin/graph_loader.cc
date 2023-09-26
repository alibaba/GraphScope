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

#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"

#include <yaml-cpp/yaml.h>

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cout << "Usage: " << argv[0]
              << "<schema_file> <bulk load file> <data dir> [thread_num]"
              << std::endl;
    return 0;
  }

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  std::string data_path, schema_file, bulk_load_config_path;
  int32_t thread_num = 1;
  schema_file = argv[1];
  bulk_load_config_path = argv[2];
  data_path = argv[3];
  if (argc > 4) {
    thread_num = atoi(argv[4]);
  }

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(schema_file);
  auto bulk_load_config =
      gs::LoadingConfig::ParseFromYaml(schema, bulk_load_config_path);
  db.Init(schema, bulk_load_config, data_path, thread_num);

  t0 += grape::GetCurrentTime();
  auto& graph = db.graph();
  LOG(INFO) << "graph num vertex labels: " << graph.schema().vertex_label_num();
  LOG(INFO) << "graph num edge labels: " << graph.schema().edge_label_num();
  for (auto i = 0; i < graph.schema().vertex_label_num(); ++i) {
    LOG(INFO) << "vertex label " << i
              << " name: " << graph.schema().get_vertex_label_name(i)
              << ", num vertices: " << graph.vertex_num(i);
  }

  for (auto i = 0; i < graph.schema().edge_label_num(); ++i) {
    LOG(INFO) << "edge label " << i
              << " name: " << graph.schema().get_edge_label_name(i);
  }

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  return 0;
}
