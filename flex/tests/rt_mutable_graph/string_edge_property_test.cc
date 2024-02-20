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
    int64_t src;
    std::string dst{};
    src = 1;
    test_get_edge(src);
    src = 1;
    dst = "5";
    test_get_graph_view(src, dst);
    src = 3;
    dst = "5";
    test_add_edge(src, dst);
  }

  void test_get_edge(int64_t oid) {
    vid_t src_lid;
    CHECK(db_.graph().get_lid(src_label_, oid, src_lid));
    auto oe = db_.graph().get_outgoing_edges(src_label_, src_lid, dst_label_,
                                             edge_label_);
    CHECK(oe != nullptr) << "Got nullptr oe\n";
    CHECK(oe->get_data().type == PropertyType::kString)
        << "Inconsistent type, Except: string, Got " << oe->get_data().type;
    std::cout << oe->get_data().AsStringView() << "\n";
    LOG(INFO) << "Finish test get edge\n";
  }

  void test_get_graph_view(int64_t src, const std::string& dst) {
    auto txn = db_.GetReadTransaction();
    vid_t src_lid, dst_lid;

    CHECK(db_.graph().get_lid(src_label_, src, src_lid));
    CHECK(db_.graph().get_lid(dst_label_, dst, dst_lid));

    {
      auto graph_view = txn.GetOutgoingSingleGraphView<std::string_view>(
          src_label_, dst_label_, edge_label_);
      auto oe = graph_view.get_edge(src_lid);
      LOG(INFO) << oe.get_data() << "\n";
      CHECK(oe.get_data() == "0.4")
          << "Inconsistent value, Excepted : 0.4, Got " << oe.get_data()
          << "\n";
    }
    {
      auto graph_view = txn.GetIncomingGraphView<std::string_view>(
          dst_label_, src_label_, edge_label_);
      auto ie = graph_view.get_edges(dst_lid);
      for (auto& e : ie) {
        LOG(INFO) << e.get_data() << "\n";
      }
    }
    LOG(INFO) << "Finish test get GraphView\n";
  }

  void test_add_edge(int64_t src, const std::string& dst) {
    {
      auto txn = db_.GetSingleVertexInsertTransaction();
      std::string name = "test-3";
      int age = 34;
      CHECK(txn.AddVertex(src_label_, src, {Any::From(name), Any::From(age)}))
          << "Add vertex failed";
      txn.Commit();
    }
    vid_t src_lid, dst_lid;
    CHECK(db_.graph().get_lid(src_label_, src, src_lid));
    CHECK(db_.graph().get_lid(dst_label_, dst, dst_lid));

    {
      auto txn = db_.GetSingleEdgeInsertTransaction();
      std::string str = "test";
      CHECK(txn.AddEdge(src_label_, src, dst_label_, dst, edge_label_,
                        Any::From(str)))
          << "Add edge failed\n";
      txn.Commit();
    }
    {
      auto txn = db_.GetReadTransaction();
      auto graph_view = txn.GetOutgoingSingleGraphView<std::string_view>(
          src_label_, dst_label_, edge_label_);
      auto oe = graph_view.get_edge(src_lid);
      CHECK(oe.get_data() == "test")
          << "Inconsistent value, Excepted: test, Got " << oe.get_data()
          << "\n";
    }
    LOG(INFO) << "Finish test add edge\n";
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
  db.Close();
  std::filesystem::remove_all(data_path + "/wal/");
  {
    double t0 = -grape::GetCurrentTime();
    db.Open(schema, data_path, shard_num, warmup, false);

    t0 += grape::GetCurrentTime();

    LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
    gs::TestStringEdgeProperty(db).test();
  }
  return 0;
}
