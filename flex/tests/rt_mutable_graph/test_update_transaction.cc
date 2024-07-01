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
class TestUpdateTransaction {
 public:
  TestUpdateTransaction(GraphDB& db)
      : db_(db),
        src_label_(db.graph().schema().get_vertex_label_id("person")),
        dst_label_(db.graph().schema().get_vertex_label_id("software")),
        edge_label_(db.graph().schema().get_edge_label_id("created")) {}

  void test() {
    const std::string name = "unknown";
    const int age = 32;
    const std::string data = "0.35";
    test_set_vertex_field(name, age);
    test_set_edge_data(data);
  }

  void test_set_vertex_field(const std::string& name, int age) {
    std::string_view original_name;
    int original_age;
    {
      auto txn = db_.GetUpdateTransaction();
      auto it = txn.GetVertexIterator(src_label_);
      while (it.GetId().AsInt64() != 1) {
        it.Next();
      }
      original_name = it.GetField(0).AsStringView();
      original_age = it.GetField(1).AsInt32();

      it.SetField(0, name);
      it.SetField(1, age);
      txn.Abort();
    }
    {
      auto txn = db_.GetReadTransaction();
      int64_t oid = 1;
      auto it = txn.FindVertex(src_label_, oid);
      CHECK(it.GetField(0).AsStringView() == original_name);
      CHECK(it.GetField(1).AsInt32() == original_age);
    }

    {
      auto txn = db_.GetUpdateTransaction();
      auto it = txn.GetVertexIterator(src_label_);
      while (it.GetId().AsInt64() != 1) {
        it.Next();
      }
      original_name = it.GetField(0).AsStringView();
      original_age = it.GetField(1).AsInt32();

      it.SetField(0, name);
      it.SetField(1, age);
      txn.Commit();
    }

    {
      auto txn = db_.GetReadTransaction();
      int64_t oid = 1;
      auto it = txn.FindVertex(src_label_, oid);
      CHECK(it.GetField(0).AsStringView() == name);
      CHECK(it.GetField(1).AsInt32() == age);
    }
    LOG(INFO) << "Finish test set vertex field\n";
  }

  void test_set_edge_data(const std::string& data) {
    std::string_view original_data;
    vid_t neighbor;
    {
      auto txn = db_.GetUpdateTransaction();

      auto it = txn.GetOutEdgeIterator(src_label_, 0, dst_label_, edge_label_);
      neighbor = it.GetNeighbor();
      original_data = it.GetData().AsStringView();
      it.SetData(data);
      txn.Abort();
    }
    {
      auto txn = db_.GetReadTransaction();
      auto es = txn.GetOutgoingEdges<std::string_view>(src_label_, 0,
                                                       dst_label_, edge_label_);
      for (auto& e : es) {
        if (e.get_neighbor() == neighbor) {
          CHECK(e.get_data() == original_data);
        }
      }
    }

    {
      auto txn = db_.GetUpdateTransaction();

      auto it = txn.GetOutEdgeIterator(src_label_, 0, dst_label_, edge_label_);
      neighbor = it.GetNeighbor();
      original_data = it.GetData().AsStringView();
      it.SetData(data);
      txn.Commit();
    }

    {
      auto txn = db_.GetReadTransaction();
      auto es = txn.GetOutgoingEdges<std::string_view>(src_label_, 0,
                                                       dst_label_, edge_label_);
      for (auto& e : es) {
        if (e.get_neighbor() == neighbor) {
          CHECK(e.get_data() == data);
        }
      }
    }
    LOG(INFO) << "Finish test set edge data\n";
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
  gs::TestUpdateTransaction(db).test();
  db.Close();
  return 0;
}
