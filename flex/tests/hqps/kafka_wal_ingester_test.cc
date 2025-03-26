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
#include <iostream>
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER
#include <flex/engines/graph_db/database/wal/kafka_wal_parser.h>
#include <flex/engines/graph_db/database/wal/kafka_wal_writer.h>
#endif
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/schema.h"

int main(int argc, char** argv) {
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER
  gs::Schema schema;
  schema.add_vertex_label(
      "PERSON",
      {
          gs::PropertyType::kInt64,  // version
      },
      {"weight"},
      {std::tuple<gs::PropertyType, std::string, size_t>(
          gs::PropertyType::kInt64, "id", 0)},
      {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem}, 4096);
  gs::GraphDB db;
  std::string work_dir = argv[1];
  std::string kafka_brokers = argv[2];
  std::string kafka_topic = argv[3];
  gs::GraphDBConfig config(schema, work_dir, "", 1);
  std::string uri =
      "kafka://" + kafka_brokers + "/" + kafka_topic + "?group.id=test";
  config.wal_uri = uri;
  db.Open(config);
  {
    for (int i = 0; i < 100; ++i) {
      auto txn = db.GetInsertTransaction(0);
      gs::label_t label = db.schema().get_vertex_label_id("PERSON");
      int64_t id = i;
      int64_t weight = i * 2 + 1;
      txn.AddVertex(label, id, {gs::Any(weight)});
      txn.Commit();
    }
  }

  gs::GraphDB db2;
  db2.Open(config);

  CHECK(db2.GetReadTransaction(0).GetVertexNum(0) == 100)
      << "Vertex num: " << db2.GetReadTransaction(0).GetVertexNum(0);
  cppkafka::Configuration config1 = {{"metadata.broker.list", kafka_brokers},
                                     {"group.id", "test"},
                                     {"enable.auto.commit", false},
                                     {"auto.offset.reset", "earliest"}};
  db2.start_kafka_wal_ingester(config1, kafka_topic);

  {
    for (int i = 100; i < 200; ++i) {
      auto txn = db.GetInsertTransaction(0);
      gs::label_t label = db.schema().get_vertex_label_id("PERSON");
      int64_t id = i;
      int64_t weight = 200;
      txn.AddVertex(label, id, {gs::Any(weight)});
      txn.Commit();
    }
  }
  LOG(INFO) << db.GetReadTransaction(0).GetVertexNum(0);

  std::this_thread::sleep_for(std::chrono::seconds(3));
  {
    auto txn = db2.GetReadTransaction(0);
    CHECK(txn.GetVertexNum(0) == 200) << "Vertex num: " << txn.GetVertexNum(0);
    gs::vid_t lid;
    CHECK(txn.GetVertexIndex(0, 90L, lid));
    LOG(INFO) << "Vertex id: " << lid;
    CHECK(txn.GetVertexIndex(0, 188L, lid));
    LOG(INFO) << "Vertex id: " << lid;
    db2.stop_kafka_wal_ingester();
  }
#endif
  return 0;
}