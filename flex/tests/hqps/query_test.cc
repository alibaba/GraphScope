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
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/tests/hqps/sample_query.h"

int main(int argc, char** argv) {
  // <oid> <label_name>
  if (argc != 4) {
    LOG(ERROR) << "Usage: ./query_test <graph_schema> "
                  "<bulk_load_yaml> <data_dir>";
    return 1;
  }
  auto graph_schema = std::string(argv[1]);
  auto bulk_load_yaml = std::string(argv[2]);
  auto data_dir = std::string(argv[3]);

  auto& db = gs::GraphDB::get();
  auto schema = gs::Schema::LoadFromYaml(graph_schema);
  auto bulk_load_config =
      gs::LoadingConfig::ParseFromYaml(schema, bulk_load_yaml);
  db.Init(schema, bulk_load_config, data_dir, 1);
  auto& sess = gs::GraphDB::get().GetSession(0);

  gs::SampleQuery query;
  std::vector<char> encoder_array;
  gs::Encoder input_encoder(encoder_array);
  input_encoder.put_long(19791209300143);
  input_encoder.put_long(1354060800000);
  std::vector<char> output_array;
  gs::Encoder output(output_array);
  gs::Decoder input(encoder_array.data(), encoder_array.size());

  gs::MutableCSRInterface graph(sess);
  query.Query(graph, input);

  LOG(INFO) << "Finish context test.";
}