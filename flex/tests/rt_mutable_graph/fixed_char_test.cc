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
class TestFixedChar {
 public:
  TestFixedChar(GraphDB& db)
      : db_(db),
        src_label_(db.graph().schema().get_vertex_label_id("person")),
        dst_label_(db.graph().schema().get_vertex_label_id("software")) {}
  int get_vertex_property_id(label_t label, const std::string& name) {
    auto props_name = db_.graph().schema().get_vertex_property_names(label);
    for (size_t idx = 0; idx < props_name.size(); ++idx) {
      const auto& prop_name = props_name[idx];
      if (prop_name == name) {
        return idx;
      }
    }
    return -1;
  }
  void test() {
    auto txn = db_.GetReadTransaction();
    auto person_iter = txn.GetVertexIterator(src_label_);
    auto props_name =
        db_.graph().schema().get_vertex_property_names(src_label_);
    int name_id = get_vertex_property_id(src_label_, "name");
    CHECK(person_iter.GetField(name_id).AsStringView() == "mark");
    while (person_iter.IsValid()) {
      LOG(INFO) << person_iter.GetField(name_id).AsStringView();
      CHECK(person_iter.GetField(name_id).AsStringView().size() == 4);
      person_iter.Next();
    }
    auto software_iter = txn.GetVertexIterator(dst_label_);
    auto props_name2 =
        db_.graph().schema().get_vertex_property_names(dst_label_);
    int name_id2 = get_vertex_property_id(dst_label_, "name");
    CHECK(software_iter.GetField(name_id2).AsStringView() == "lop ");
    software_iter.Next();
    CHECK(software_iter.GetField(name_id2).AsStringView() == "ripp");
  }
  GraphDB& db_;
  label_t src_label_;
  label_t dst_label_;
  label_t edge_label_;
};
}  // namespace gs

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
  gs::TestFixedChar(db).test();
  db.Close();
  std::filesystem::remove_all(data_path + "/wal/");
  {
    double t0 = -grape::GetCurrentTime();
    db.Open(schema_res.value(), data_path, shard_num, warmup, false);

    t0 += grape::GetCurrentTime();

    LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
    gs::TestFixedChar(db).test();
  }
  return 0;
}
