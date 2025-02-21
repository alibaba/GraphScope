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
class TestImmutableCsr {
 public:
  TestImmutableCsr(GraphDB& db)
      : db_(db),
        src_label_(db.graph().schema().get_vertex_label_id("person")),
        dst_label_(db.graph().schema().get_vertex_label_id("software")),
        edge_label_(db.graph().schema().get_edge_label_id("created")),
        know_label_(db.graph().schema().get_edge_label_id("knows")) {}

  void test() {
    int64_t src;
    std::string dst{};
    src = 1;
    test_get_edge(src);
    src = 1;
    dst = "3";
    test_get_graph_view(src, dst);
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
    std::cout << oe->get_neighbor() << " " << data[0].AsStringView() << " "
              << data[1].AsInt32() << "\n";
    LOG(INFO) << "Finish test get edge\n";
  }

  void test_get_graph_view(int64_t src, const std::string_view& dst) {
    auto txn = db_.GetReadTransaction();
    vid_t src_lid, dst_lid;

    CHECK(db_.graph().get_lid(src_label_, src, src_lid));
    CHECK(db_.graph().get_lid(dst_label_, dst, dst_lid));

    {
      auto graph_view = txn.GetOutgoingImmutableGraphView<RecordView>(
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
      auto graph_view = txn.GetIncomingImmutableGraphView<RecordView>(
          dst_label_, src_label_, edge_label_);
      auto ies = graph_view.get_edges(dst_lid);
      LOG(INFO) << ies.estimated_degree() << "\n";
      for (auto& ie : ies) {
        auto record = ie.get_data();
        LOG(INFO) << record[0].AsStringView() << "|" << record[1].AsInt32()
                  << "\n";
      }
    }

    {
      auto graph_view = txn.GetOutgoingImmutableGraphView<std::string_view>(
          src_label_, src_label_, know_label_);
      auto oes = graph_view.get_edges(src_lid);
      for (auto& oe : oes) {
        auto data = oe.get_data();
        LOG(INFO) << data << "\n";
      }
    }
    LOG(INFO) << "Finish test get GraphView\n";
  }

 private:
  GraphDB& db_;
  label_t src_label_;
  label_t dst_label_;
  label_t edge_label_;
  label_t know_label_;
};

}  // namespace gs
// ./immutable_csr_test data_dir
int main(int argc, char** argv) {
  bool warmup = false;
  uint32_t shard_num = 1;

  std::string graph_schema_path = "";
  std::string data_path = "";

  data_path = argv[1];

  graph_schema_path = data_path + "/graph.yaml";

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
  gs::TestImmutableCsr(db).test();
  LOG(INFO) << "Finished test immutable csr\n";
  db.Close();
  std::filesystem::remove_all(data_path + "/wal/");
  {
    double t0 = -grape::GetCurrentTime();
    db.Open(schema_res.value(), data_path, shard_num, warmup, false);

    t0 += grape::GetCurrentTime();

    LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
    gs::TestImmutableCsr(db).test();
  }
  return 0;
}