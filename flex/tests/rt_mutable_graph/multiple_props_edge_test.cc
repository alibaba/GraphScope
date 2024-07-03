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
class TestMultiplePropertiesEdge {
 public:
  TestMultiplePropertiesEdge(GraphDB& db)
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
    CHECK(oe->get_data().type == PropertyType::kRecordView)
        << "Inconsistent type, Except: string, Got " << oe->get_data().type;
    auto data = oe->get_data().AsRecordView();
    CHECK(data.size() == 2)
        << "Inconsistent size, Except: 2, Got " << data.size();
    CHECK(data[0].type == PropertyType::kStringView)
        << "Inconsistent type, Except: string, Got " << data[0].type;
    CHECK(data[1].type == PropertyType::kInt32)
        << "Inconsistent type, Except: int, Got " << data[1].type;
    std::cout << data[0].AsStringView() << " " << data[1].AsInt32() << "\n";
    LOG(INFO) << "Finish test get edge\n";
  }

  void test_get_graph_view(int64_t src, const std::string_view& dst) {
    auto txn = db_.GetReadTransaction();
    vid_t src_lid, dst_lid;

    CHECK(db_.graph().get_lid(src_label_, src, src_lid));
    CHECK(db_.graph().get_lid(dst_label_, dst, dst_lid));

    {
      auto graph_view = txn.GetOutgoingGraphView<RecordView>(
          src_label_, dst_label_, edge_label_);
      auto oes = graph_view.get_edges(src_lid);
      for (auto& oe : oes) {
        auto data = oe.get_data();
        CHECK(data.size() == 2)
            << "Inconsistent size, Excepted: 2, Got " << data.size() << "\n";
        CHECK(data[0].AsStringView() == "0.4")
            << "Inconsistent value, Excepted: 0.4, Got "
            << data[0].AsStringView() << "\n";
      }
    }
    {
      auto graph_view = txn.GetIncomingGraphView<RecordView>(
          dst_label_, src_label_, edge_label_);
      auto ie = graph_view.get_edges(dst_lid);

      for (auto& e : ie) {
        auto record = e.get_data();
        LOG(INFO) << record[0].AsStringView() << "|" << record[1].AsInt32()
                  << "\n";
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
      std::string_view str = "test";
      CHECK(txn.AddEdge(src_label_, src, dst_label_, dst, edge_label_,
                        {Any::From(str), Any::From(2012)}))
          << "Add edge failed\n";
      txn.Commit();
    }
    {
      auto txn = db_.GetReadTransaction();
      auto graph_view = txn.GetOutgoingGraphView<RecordView>(
          src_label_, dst_label_, edge_label_);
      auto oes = graph_view.get_edges(src_lid);
      for (auto& oe : oes) {
        auto data = oe.get_data();
        CHECK(data.size() == 2)
            << "Inconsistent size, Excepted: 2, Got " << data.size() << "\n";
        CHECK(data.get_field<std::string_view>(0) == "test")
            << "Inconsistent value, Excepted: test, Got "
            << data.get_field<std::string_view>(0) << "\n";
        CHECK(data.get_field<int32_t>(1) == 2012)
            << "Inconsistent value, Excepted: 2012, Got "
            << data.get_field<int32_t>(1) << "\n";
      }
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

  auto schema_res = gs::Schema::LoadFromYaml(graph_schema_path);
  if (!schema_res.ok()) {
    LOG(ERROR) << "Fail to load graph schema file: "
               << schema_res.status().error_message();
    return -1;
  }
  db.Open(schema_res.value(), data_path, shard_num, warmup, true);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
  gs::TestMultiplePropertiesEdge(db).test();
  db.Close();
  std::filesystem::remove_all(data_path + "/wal/");
  {
    double t0 = -grape::GetCurrentTime();
    db.Open(schema_res.value(), data_path, shard_num, warmup, false);

    t0 += grape::GetCurrentTime();

    LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
    gs::TestMultiplePropertiesEdge(db).test();
  }
  return 0;
}