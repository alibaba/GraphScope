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
  db.Open(schema, work_dir, 1);
  std::string uri = "kafka://" + kafka_brokers + "/" + kafka_topic;
  gs::KafkaWalWriter writer;
  writer.open(uri, 0);
  grape::InArchive in_archive;
  in_archive.Resize(sizeof(gs::WalHeader));
  gs::label_t label = db.schema().get_vertex_label_id("PERSON");
  in_archive << static_cast<uint8_t>(0) << label;
  int64_t id = 998244353;
  in_archive << id;
  int64_t weight = 100;
  in_archive << weight;
  auto header = reinterpret_cast<gs::WalHeader*>(in_archive.GetBuffer());
  header->timestamp = 1;
  header->type = 0;
  header->length = in_archive.GetSize() - sizeof(gs::WalHeader);
  writer.append(in_archive.GetBuffer(), in_archive.GetSize());
  writer.close();
  cppkafka::Configuration config = {{"metadata.broker.list", kafka_brokers},
                                    {"group.id", "test"},
                                    {"enable.auto.commit", false}};
  db.start_kafka_wal_ingester(config, kafka_topic);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  db.stop_kafka_wal_ingester();
  auto txn = db.GetReadTransaction(0);
  gs::vid_t lid;
  CHECK(txn.GetVertexNum(label) == 1);
  CHECK(txn.GetVertexIndex(label, id, lid));
  auto iter = txn.GetVertexIterator(label);
  CHECK(iter.GetField(0).AsInt64() == 100);
  std::cout << "Vertex id: " << lid << std::endl;
  db.Close();
#endif
  return 0;
}