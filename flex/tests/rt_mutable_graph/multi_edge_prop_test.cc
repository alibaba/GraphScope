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

#include <glog/logging.h>

namespace gs {
class TestStringEdgeProperty {
 public:
  TestStringEdgeProperty(GraphDB& db)
      : db_(db),
        src_label_(db.graph().schema().get_vertex_label_id("person")),
        dst_label_(db.graph().schema().get_vertex_label_id("software")),
        edge_label_(db.graph().schema().get_edge_label_id("created")) {}

  void test() {
    auto person_v_num = db_.graph().vertex_num(src_label_);
    for (auto i = 0; i < person_v_num; ++i) {
      auto edges = db_.graph().get_outgoing_edges(src_label_, i, dst_label_,
                                                  edge_label_);
      while (edges->is_valid()) {
        LOG(INFO) << edges->get_neighbor() << ", "
                  << edges->get_data().to_string();
        edges->next();
      }
    }
    auto txn = db_.GetReadTransaction();
    auto gw = txn.GetIncomingGraphView<char_array<12>>(dst_label_, src_label_,
                                                       edge_label_);
    const auto& types = db_.graph().schema().get_edge_properties(
        src_label_, dst_label_, edge_label_);
    for (auto i = 0; i < person_v_num; ++i) {
      auto ie = gw.get_edges(i);
      for (auto e : ie) {
        auto fc = e.get_data();
        auto cur = static_cast<const char*>(fc.data);
        LOG(INFO) << "weight: " << *reinterpret_cast<const double*>(fc.data)
                  << " year: "
                  << *reinterpret_cast<const int*>(cur + sizeof(double));
      }
    }
  }

 private:
  GraphDB& db_;
  label_t src_label_;
  label_t dst_label_;
  label_t edge_label_;
};

}  // namespace gs
// ./string_edge_property_test graph.yaml data_dir
int main(int argc, char** argv) {
  if (argc < 3) {
    LOG(ERROR) << "Usage: ./string_edge_property_test graph.yaml data_dir";
    return 1;
  }
  bool warmup = false;
  uint32_t shard_num = 1;

  std::string graph_schema_path = "";
  std::string data_path = "";

  graph_schema_path = argv[1];
  data_path = argv[2];

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  db.Open(schema, data_path, shard_num, warmup, true);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
  gs::TestStringEdgeProperty(db).test();
  return 0;
}
