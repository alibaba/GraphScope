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

#include <glog/logging.h>

#include <fstream>
#include <random>
#include <string>
#include <thread>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/property/types.h"

#define SLEEP_TIME_MILLI_SEC 1

namespace gs {
void dump_schema_to_file(const std::string& work_dir,
                         const gs::Schema& schema) {
  std::string yaml_content = "schema:\n";
  yaml_content += "  vertex_types:\n";
  auto type_2_str = [](PropertyType type) -> std::string {
    if (type == PropertyType::kBool) {
      return "primitive_type: DT_BOOL";
    } else if (type == PropertyType::kInt32) {
      return "primitive_type: DT_SIGNED_INT32";
    } else if (type == PropertyType::kUInt32) {
      return "primitive_type: DT_UNSIGNED_INT32";
    } else if (type == PropertyType::kDate) {
      return "primitive_type: DT_SIGNED_INT64";
    } else if (type == PropertyType::kInt64) {
      return "primitive_type: DT_SIGNED_INT64";
    } else if (type == PropertyType::kUInt64) {
      return "primitive_type: DT_UNSIGNED_INT64";
    } else if (type == PropertyType::kDouble) {
      return "primitive_type: DT_DOUBLE";
    } else if (type == PropertyType::kFloat) {
      return "primitive_type: DT_FLOAT";
    } else if (type == PropertyType::kStringView) {
      return "string:\n              long_text:";
    } else if (type == PropertyType::kDay) {
      return "temporal:\n              timestamp:";
    } else {
      return "unknown";
    }
  };
  size_t vertex_label_num = schema.vertex_label_num();
  for (size_t idx = 0; idx < vertex_label_num; ++idx) {
    yaml_content += "    - type_id: " + std::to_string(idx) + "\n";
    yaml_content +=
        "      type_name: " + schema.get_vertex_label_name(idx) + "\n";
    yaml_content += "      properties:\n";
    const auto& pk = schema.get_vertex_primary_key(idx);
    for (const auto& key : pk) {
      yaml_content +=
          "        - property_id: " + std::to_string(std::get<2>(key)) + "\n";
      yaml_content += "          property_name: " + (std::get<1>(key)) + "\n";
      yaml_content += "          property_type: \n            " +
                      type_2_str(std::get<0>(key)) + "\n";
    }
    size_t offset = pk.size();
    auto& prop_names = schema.get_vertex_property_names(idx);
    auto& prop_types = schema.get_vertex_properties(idx);
    for (size_t i = 0; i < prop_names.size(); ++i) {
      yaml_content +=
          "        - property_id: " + std::to_string(i + offset) + "\n";
      yaml_content += "          property_name: " + prop_names[i] + "\n";
      yaml_content += "          property_type: \n            " +
                      type_2_str(prop_types[i]) + "\n";
    }
  }

  yaml_content += "  edge_types:\n";
  size_t edge_label_num = schema.edge_label_num();
  for (size_t edge_label = 0; edge_label < edge_label_num; ++edge_label) {
    yaml_content += "    - type_id: " + std::to_string(edge_label) + "\n";
    const auto& edge_label_name = schema.get_edge_label_name(edge_label);
    yaml_content += "      type_name: " + edge_label_name + "\n";
    yaml_content += "      vertex_type_pair_relations:\n";
    bool first = true;
    std::string props_content{};
    for (size_t src_label = 0; src_label < vertex_label_num; ++src_label) {
      const auto& src_label_name = schema.get_vertex_label_name(src_label);
      for (size_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
        const auto& dst_label_name = schema.get_vertex_label_name(dst_label);
        if (schema.exist(src_label_name, dst_label_name, edge_label_name)) {
          yaml_content += "        - source_vertex: " + src_label_name + "\n";
          yaml_content +=
              "          destination_vertex: " + dst_label_name + "\n";
          const auto& props = schema.get_edge_properties(
              src_label_name, dst_label_name, edge_label_name);

          const auto& prop_names = schema.get_edge_property_names(
              src_label_name, dst_label_name, edge_label_name);
          if (first && (!props.empty())) {
            props_content += "      properties:\n";
            for (size_t i = 0; i < props.size(); ++i) {
              props_content +=
                  "        - property_id: " + std::to_string(i) + "\n";
              props_content +=
                  "          property_name: " + prop_names[i] + "\n";
              props_content += "          property_type: \n            " +
                               type_2_str(props[i]) + "\n";
            }
          }

          first = false;
        }
      }
    }
    yaml_content += props_content;
  }

  {
    std::string yaml_filename = work_dir + "/graph.yaml";
    std::ofstream out(yaml_filename);
    out << yaml_content;
    out.close();
  }
}

class DBInitializer {
 public:
  void open(GraphDB& db, const std::string& work_dir, const Schema& schema) {
    gs::GraphDBConfig config(schema, work_dir, compiler_path, thread_num);
    db.Open(config);
    dump_schema_to_file(work_dir, schema);
  }
  DBInitializer(const DBInitializer&) = delete;
  static DBInitializer& get() {
    static DBInitializer instance;
    return instance;
  }
  void set_compiler_path(const std::string& path) { compiler_path = path; }
  void set_thread_num(int num) { thread_num = num; }

 private:
  DBInitializer() {}
  int thread_num;
  std::string compiler_path;
};

}  // namespace gs

using gs::Any;
using gs::EdgeStrategy;
using gs::GraphDB;
using gs::GraphDBSession;
using gs::StorageStrategy;
using oid_t = int64_t;
using gs::PropertyType;
using gs::Schema;
using gs::vid_t;

oid_t generate_id() {
  static std::atomic<oid_t> current_id(0);
  return current_id.fetch_add(1);
}

void append_string_to_field(gs::UpdateTransaction::vertex_iterator& vit,
                            int col_id, const std::string& str) {
  std::string cur_str = std::string(vit.GetField(col_id).AsStringView());
  if (cur_str.empty()) {
    cur_str = str;
  } else {
    cur_str += ";";
    cur_str += str;
  }
  vit.SetField(col_id, Any::From(cur_str));
}

template <typename FUNC_T>
void parallel_transaction(GraphDB& db, const FUNC_T& func, int txn_num) {
  std::vector<int> txn_ids;
  for (int i = 0; i < txn_num; ++i) {
    txn_ids.push_back(i);
  }
  std::shuffle(txn_ids.begin(), txn_ids.end(),
               std::mt19937(std::random_device()()));

  int thread_num = db.SessionNum();
  std::vector<std::thread> threads;
  std::atomic<int> txn_counter(0);
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          GraphDBSession& session = db.GetSession(tid);
          while (true) {
            int txn_id = txn_counter.fetch_add(1);
            if (txn_id >= txn_num)
              break;

            func(session, txn_ids[txn_id]);
          }
        },
        i);
  }
  for (auto& t : threads) {
    t.join();
  }
}

template <typename FUNC_T>
void parallel_client(GraphDB& db, const FUNC_T& func) {
  int thread_num = db.SessionNum();
  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back(
        [&](int tid) {
          GraphDBSession& session = db.GetSession(tid);
          func(session, tid);
        },
        i);
  }
  for (auto& t : threads) {
    t.join();
  }
}

template <typename TXN_T>
typename TXN_T::vertex_iterator get_random_vertex(TXN_T& txn,
                                                  gs::label_t label_id) {
  auto v0 = txn.GetVertexIterator(label_id);
  int num = 0;
  for (; v0.IsValid(); v0.Next()) {
    ++num;
  }
  if (num == 0) {
    return v0;
  }
  std::random_device rand_dev;
  std::mt19937 gen(rand_dev());
  std::uniform_int_distribution<int> dist(0, num - 1);
  int picked = dist(gen);
  auto v1 = txn.GetVertexIterator(label_id);
  for (int i = 0; i != picked; ++i) {
    v1.Next();
  }
  return v1;
}

// Atomicity

void AtomicityInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;
  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::Varchar(256),  // name
                              PropertyType::Varchar(256),  // emails
                          },
                          {"name", "emails"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "id", 0)},
                          {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem,
                           gs::StorageStrategy::kMem},
                          4096);
  schema.add_edge_label("PERSON", "PERSON", "KNOWS",
                        {
                            PropertyType::kInt64,  // since
                        },
                        {"since"}, gs::EdgeStrategy::kMultiple,
                        gs::EdgeStrategy::kMultiple);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();

  int64_t id1 = 1;
  std::string name1 = "Alice";
  std::string email1 = "alice@aol.com";
  int64_t id2 = 2;
  std::string name2 = "Bob";
  std::string email2 = "bob@hotmail.com;bobby@yahoo.com";
  txn.run(
      "With $person_id as person_id, $name as name, $email as email\n"
      "CREATE(person : PERSON{id : person_id, name : name, emails : email}) ",
      {{"person_id", std::to_string(id1)}, {"name", name1}, {"email", email1}});
  // CHECK(txn.AddVertex(person_label_id, generate_id(),
  //                   {Any::From(id1), Any::From(name1), Any::From(email1)}));
  txn.run(
      "With $person_id as person_id, $name as name, $email as email\n"
      "CREATE(person : PERSON{id : person_id, name : name, emails : email}) ",
      {{"person_id", std::to_string(id2)}, {"name", name2}, {"email", email2}});
  txn.Commit();
}

bool AtomicityC(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
                const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  std::string empty_name = "";
  std::string empty_email = "";
  txn.run(
      "With $person2_id as person2_id, $name as name, $emails as emails\n"
      "CREATE (person : PERSON {id : person2_id, name : name, emails: "
      "emails})",
      {{"person2_id", std::to_string(person2_id)},
       {"name", empty_name},
       {"emails", empty_email}});
  txn.run(
      "With $person1_id as person1_id, $person2_id as person2_id, $since as "
      "since\n"
      "CREATE (person1:PERSON {id: person1_id})-[:KNOWS{since: "
      "since}]->(person2: PERSON {id: person2_id})",
      {{"person1_id", std::to_string(person1_id)},
       {"person2_id", std::to_string(person2_id)},
       {"since", std::to_string(since)}});
  txn.run(
      "MATCH (p:PERSON {id: $person_id})  SET p.emails = "
      "gs.function.concat(p.emails, $new_email)",
      {{"person_id", std::to_string(person1_id)}, {"new_email", new_email}});

  txn.Commit();
  return true;
}

bool AtomicityRB(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
                 const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  txn.run(
      "MATCH (p1: PERSON {id: $person1_id}) SET p1.emails = "
      "gs.function.concat(p1.emails ,$new_email)",
      {{"person1_id", std::to_string(person1_id)}, {"new_email", new_email}});
  auto res = txn.run("MATCH (p2: PERSON {id: $person2_id}) RETURN p2",
                     {{"person2_id", std::to_string(person2_id)}});
  if (!res.empty()) {
    txn.Abort();
  } else {
    std::string empty_name = "";
    std::string empty_email = "";
    txn.run(
        "With $person2_id as person2_id, $name as name, $emails as emails\n"
        "CREATE (person : PERSON {id : person2_id, name : name, emails: "
        "emails})",
        {{"person2_id", std::to_string(person2_id)},
         {"name", empty_name},
         {"emails", empty_email}});
  }
  return true;
}

int64_t count_email_num(const std::string_view& sv) {
  if (sv.empty()) {
    return 0;
  }
  int64_t ret = 1;
  for (auto c : sv) {
    if (c == ';') {
      ++ret;
    }
  }
  return ret;
}

std::map<std::string, int32_t> AtomicityCheck(GraphDB& db) {
  auto txn = db.GetReadTransaction();
  std::string result = txn.run(
      "MATCH(p: PERSON) With p.id as id, p.name as name, p.emails as emails \n"
      "  With id, CASE WHEN name <> \"\" THEN 1 ELSE 0 END as name_count, "
      "gs.function.listSize(emails) as email_count\n"
      "RETURN count(id) as numPersons, sum(name_count) as numNames, "
      "sum(email_count) as numEmails",
      {});
  gs::Decoder decoder(result.data(), result.size());
  std::map<std::string, int32_t> ret;
  ret["numPersons"] = decoder.get_long();
  ret["numNames"] = decoder.get_int();
  ret["numEmails"] = decoder.get_int();

  return ret;
}

void AtomicityCTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  AtomicityInit(db, work_dir, thread_num);

  AtomicityC(db.GetSession(0), 1L, 3L, "alice@otherdomain.net", 2020);

  auto result = AtomicityCheck(db);
  if (result["numPersons"] == 3 && result["numNames"] == 2 &&
      result["numEmails"] == 4) {
    LOG(INFO) << "AtomicityCTest passed";
  } else {
    LOG(ERROR) << result["numPersons"] << " " << result["numNames"] << " "
               << result["numEmails"];
    LOG(FATAL) << "AtomicityCTest failed";
  }
}

void AtomicityRBTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  AtomicityInit(db, work_dir, thread_num);

  AtomicityRB(db.GetSession(0), 1L, 2L, "alice@otherdomain.net", 2020);

  auto result = AtomicityCheck(db);
  if (result["numPersons"] == 2 && result["numNames"] == 2 &&
      result["numEmails"] == 3) {
    LOG(INFO) << "AtomicityRBTest passed";
  } else {
    LOG(FATAL) << "AtomicityRBTest failed";
  }
}

// Dirty Writes

void G0Init(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;
  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::Varchar(256),  // version history
                          },
                          {
                              "versionHistory",
                          },
                          {
                              std::tuple<PropertyType, std::string, size_t>(
                                  PropertyType::kInt64, "id", 0),
                          },
                          {
                              gs::StorageStrategy::kMem,
                              gs::StorageStrategy::kMem,
                          },
                          4096);
  schema.add_edge_label("PERSON", "PERSON", "KNOWS",
                        {
                            PropertyType::Varchar(256),  // version history
                        },
                        {"versionHistory"}, EdgeStrategy::kMultiple,
                        EdgeStrategy::kMultiple);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id, $version_history as version_history\n"
      "CREATE (person : PERSON {id : person_id, versionHistory : "
      "version_history})",
      {{"person_id", "1"}, {"version_history", "0"}});
  txn.run(
      "With $person_id as person_id, $version_history as version_history\n"
      "CREATE (person : PERSON {id : person_id, versionHistory : "
      "version_history})",
      {{"person_id", "2"}, {"version_history", "0"}});
  txn.run(
      "With $person1_id as person1_id, $person2_id as person2_id, "
      "$version_history as version_history\n"
      "CREATE (person1:PERSON {id: person1_id})-[:KNOWS{versionHistory: "
      "version_history}]->(person2: PERSON {id: person2_id})",
      {{"person1_id", "1"}, {"person2_id", "2"}, {"version_history", "0"}});
  txn.Commit();
}

void G0(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
        int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  std::map<std::string, std::string> parameters = {
      {"person1Id", std::to_string(person1_id)},
      {"person2Id", std::to_string(person2_id)},
      {"transactionId", std::to_string(txn_id)}};
  txn.run(
      "MATCH (p1:PERSON {id: $person1Id})-[k:KNOWS]->(p2:PERSON {id: "
      "$person2Id})\n"
      "SET p1.versionHistory = gs.function.concat(p1.versionHistory, "
      "$transactionId), p2.versionHistory = "
      "gs.function.concat(p2.versionHistory , "
      "$transactionId), k.versionHistory  = "
      "gs.function.concat(k.versionHistory, "
      "$transactionId)",
      parameters);

  txn.Commit();
}

std::tuple<std::string, std::string, std::string> G0Check(GraphDB& db,
                                                          int64_t person1_id,
                                                          int64_t person2_id) {
  auto txn = db.GetReadTransaction();
  std::map<std::string, std::string> parameters = {
      {"person1Id", std::to_string(person1_id)},
      {"person2Id", std::to_string(person2_id)}};
  auto res = txn.run(
      "MATCH (p1:PERSON {id: $person1Id})-[k:KNOWS]->(p2:PERSON {id: "
      "$person2Id})\nRETURN\n  p1.versionHistory AS p1VersionHistory,\n  "
      "k.versionHistory  AS kVersionHistory,\n  p2.versionHistory AS "
      "p2VersionHistory",
      parameters);
  gs::Decoder decoder(res.data(), res.size());
  std::string p1_version_history = std::string(decoder.get_string());
  std::string k_version_history = std::string(decoder.get_string());
  std::string p2_version_history = std::string(decoder.get_string());
  return std::make_tuple(p1_version_history, p2_version_history,
                         k_version_history);
}

void G0Test(const std::string& work_dir, int thread_num) {
  GraphDB db;
  G0Init(db, work_dir, thread_num);

  parallel_transaction(
      db, [&](GraphDBSession& db, int txn_id) { G0(db, 1, 2, txn_id + 1); },
      200);
  std::string p1_version_history, p2_version_history, k_version_history;
  std::tie(p1_version_history, p2_version_history, k_version_history) =
      G0Check(db, 1, 2);
  LOG(INFO) << p1_version_history;
  LOG(INFO) << p2_version_history;
  LOG(INFO) << k_version_history;

  if (p1_version_history == p2_version_history &&
      p2_version_history == k_version_history) {
    LOG(INFO) << "G0Test passed";
  } else {
    LOG(FATAL) << "G0Test failed";
  }
}

// Aborted Reads

void G1AInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;

  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,  // version
                          },
                          {"version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id, $version as version\n"
      "CREATE (person : PERSON {id : person_id, version : version})",
      {{"person_id", "1"}, {"version", "1"}});
  txn.Commit();
}

void G1A1(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  // select a random person
  auto vit = get_random_vertex(txn, person_label_id);
  std::map<std::string, std::string> parameters = {
      {"personId", std::to_string(person_id)}};
  auto res =
      txn.run("MATCH (p:PERSON {id: $personId})\n RETURN p.id", parameters);
  if (res.empty()) {
    LOG(FATAL) << "G1a1 Result empty";
  }
  gs::Decoder decoder(res.data(), res.size());
  int64_t id = decoder.get_long();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  // attempt to set version = 2
  std::map<std::string, std::string> parameters2 = {
      {"personId", std::to_string(id)}};
  txn.run("MATCH (p:PERSON {id: $personId})\n SET p.version = 2", parameters2);
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  txn.Abort();
}

int64_t G1A2(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetReadTransaction();
  std::map<std::string, std::string> parameters = {
      {"personId", std::to_string(person_id)}};
  auto res =
      txn.run("MATCH (p:PERSON {id: $personId}) RETURN p.version AS pVersion",
              parameters);
  if (res.empty()) {
    LOG(FATAL) << "G1a2 Result empty";
  }
  gs::Decoder decoder(res.data(), res.size());
  return decoder.get_long();
}

void G1ATest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  G1AInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& db, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        auto p_version = G1A2(db, 1L);
        if (p_version != 1) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        G1A1(db, 1L);
      }
    }
  });

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "G1ATest passed";
  } else {
    LOG(FATAL) << "G1ATest failed";
  }
}

// Intermediate Reads

void G1BInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;

  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,  // version
                          },
                          {"version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id, $version as version\n"
      "CREATE (person : PERSON {id : person_id, version : version})",
      {{"person_id", "1"}, {"version", "99"}});

  txn.Commit();
}

void G1B1(GraphDBSession& db, int64_t person_id, int64_t even, int64_t odd) {
  auto txn = db.GetUpdateTransaction();
  std::map<std::string, std::string> parameters1 = {
      {"personId", std::to_string(person_id)}, {"even", std::to_string(even)}};
  txn.run("MATCH (p:PERSON {id: $personId}) SET p.version = $even",
          parameters1);
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  std::map<std::string, std::string> parameters2 = {
      {"personId", std::to_string(person_id)}, {"odd", std::to_string(odd)}};
  txn.run("MATCH (p:PERSON {id: $personId}) SET p.version = $odd", parameters2);
  txn.Commit();
}

int64_t G1B2(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetReadTransaction();
  std::map<std::string, std::string> parameters = {
      {"person_id", std::to_string(person_id)}};
  auto res =
      txn.run("MATCH (p:PERSON {id: $person_id}) RETURN p.version AS pVersion",
              parameters);
  if (res.empty()) {
    LOG(FATAL) << "G1b2 Result empty";
  }
  gs::Decoder decoder(res.data(), res.size());
  return decoder.get_long();
}

void G1BTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  G1BInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        auto p_version = G1B2(session, 1);
        if (p_version % 2 != 1) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        G1B1(session, 1, 0, 1);
      }
    }
  });

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "G1BTest passed";
  } else {
    LOG(FATAL) << "G1BTest failed";
  }
}

// Circular Information Flow

void G1CInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;
  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,  // version
                          },
                          {"version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id, $version as version\n"
      "CREATE (person : PERSON {id : person_id, version : version})",
      {{"person_id", "1"}, {"version", "0"}});
  txn.run(
      "With $person_id as person_id, $version as version\n"
      "CREATE (person : PERSON {id : person_id, version : version})",
      {{"person_id", "2"}, {"version", "0"}});

  txn.Commit();
}

int64_t G1C(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
            int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  txn.run(
      "MATCH (p1:PERSON {id: $person1_id})\n"
      "SET p1.version = $txn_id",
      {{"person1_id", std::to_string(person1_id)},
       {"txn_id", std::to_string(txn_id)}});
  auto res = txn.run(
      "MATCH (p2:PERSON {id: $person2_id})\n"
      "RETURN p2.version AS p2Version",
      {{"person2_id", std::to_string(person2_id)}});
  if (res.empty()) {
    LOG(FATAL) << "G1c Result empty";
  }
  gs::Decoder decoder(res.data(), res.size());
  int64_t ret = decoder.get_long();
  txn.Commit();

  return ret;
}

void G1CTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  G1CInit(db, work_dir, thread_num);

  int64_t c = 1000;
  std::vector<int64_t> results(c);

  parallel_transaction(
      db,
      [&](GraphDBSession& session, int txn_id) {
        std::random_device rand_dev;
        std::mt19937 gen(rand_dev());
        std::uniform_int_distribution<int> dist(0, 1);
        // select two different persons randomly
        auto order = dist(gen);
        int64_t person1_id = order + 1;
        int64_t person2_id = 2 - order;
        results[txn_id] = G1C(session, person1_id, person2_id, txn_id + 1);
      },
      c);

  int64_t num_incorrect_checks = 0;
  for (int64_t i = 1; i <= c; i++) {
    auto v1 = results[i - 1];
    if (v1 == 0)
      continue;
    auto v2 = results[v1 - 1];
    if (v2 == -1 || i == v2) {
      num_incorrect_checks++;
    }
  }

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "G1CTest passed";
  } else {
    LOG(FATAL) << "G1CTest failed";
  }
}

// Item-Many-Preceders

void IMPInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;

  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,  // version
                          },
                          {"version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id, $version as version\n"
      "CREATE (person : PERSON {id : person_id, version : version})",
      {{"person_id", "1"}, {"version", "1"}});
  txn.Commit();
}

void IMP1(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  txn.run(
      "MATCH (p:PERSON {id: $personId}) SET p.version = p.version + 1 RETURN p",
      {{"personId", std::to_string(person_id)}});
  txn.Commit();
}

std::tuple<int64_t, int64_t> IMP2(GraphDBSession& db, int64_t person1_id) {
  auto txn = db.GetReadTransaction();
  auto res =
      txn.run("MATCH (p:PERSON {id: $personId}) RETURN p.version AS firstRead",
              {{"personId", std::to_string(person1_id)}});
  if (res.empty()) {
    LOG(FATAL) << "IMP2 Result empty";
  }
  gs::Decoder decoder(res.data(), res.size());
  int64_t v1 = decoder.get_long();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  auto res2 =
      txn.run("MATCH (p:PERSON {id: $personId}) RETURN p.version AS secondRead",
              {{"personId", std::to_string(person1_id)}});
  if (res2.empty()) {
    LOG(FATAL) << "IMP2 Result empty";
  }
  gs::Decoder decoder2(res2.data(), res2.size());
  int64_t v2 = decoder2.get_long();

  return std::make_tuple(v1, v2);
}

void IMPTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  IMPInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        const auto& [v1, v2] = IMP2(session, 1L);
        if (v1 != v2) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        IMP1(session, 1L);
      }
    }
  });

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "IMPTest passed";
  } else {
    LOG(FATAL) << "IMPTest failed";
  }
}

// Predicate-Many-Preceders

void PMPInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;
  schema.add_vertex_label("PERSON", {}, {},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "id", 0)},
                          {gs::StorageStrategy::kMem}, 4096);
  schema.add_vertex_label("POST", {}, {},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "id", 0)},
                          {gs::StorageStrategy::kMem}, 4096);
  schema.add_edge_label("PERSON", "POST", "LIKES", {}, {},
                        gs::EdgeStrategy::kMultiple,
                        gs::EdgeStrategy::kMultiple);
  gs::DBInitializer::get().open(db, work_dir, schema);
  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id\n"
      "CREATE (person : PERSON {id : person_id})",
      {{"person_id", "1"}});
  txn.run(
      "With $post_id as post_id\n"
      "CREATE (post : POST {id : post_id})",
      {{"post_id", "1"}});
  txn.Commit();
}

bool PMP1(GraphDBSession& db, int64_t person_id, int64_t post_id) {
  auto txn = db.GetUpdateTransaction();
  txn.run(
      "With $personId as personId, $postId as postId\n"
      "CREATE (p: PERSON {id: personId})-[:LIKES]->(post:POST {id: postId})",
      {{"personId", std::to_string(person_id)},
       {"postId", std::to_string(post_id)}});
  txn.Commit();
  return true;
}

std::tuple<int64_t, int64_t> PMP2(GraphDBSession& db, int64_t post_id) {
  auto txn = db.GetReadTransaction();
  auto res1 = txn.run(
      "MATCH  (po1: POST {id: $postId}) with po1\n"
      "OPTIONAL MATCH (po1)<-[:LIKES]-(pe1:PERSON) RETURN "
      "count(pe1) "
      "AS firstRead",
      {{"postId", std::to_string(post_id)}});
  if (res1.empty()) {
    LOG(FATAL) << "PMP2 Result empty";
  }
  gs::Decoder decoder(res1.data(), res1.size());
  int64_t c1 = decoder.get_long();
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  auto res2 = txn.run(
      "MATCH  (po1: POST {id: $postId}) with po1\n"
      "OPTIONAL MATCH (po1)<-[:LIKES]-(pe1:PERSON) RETURN "
      "count(pe1) "
      "AS firstRead",
      {{"postId", std::to_string(post_id)}});
  if (res2.empty()) {
    LOG(FATAL) << "PMP2 Result empty";
  }
  gs::Decoder decoder2(res2.data(), res2.size());
  int64_t c2 = decoder2.get_long();
  return std::make_tuple(c1, c2);
}

void PMPTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  PMPInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  std::atomic<int64_t> num_aborted_txns(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        int64_t v1, v2;
        std::tie(v1, v2) = PMP2(session, 1L);
        if (v1 != v2) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        int person_id = 1L;
        int post_id = 1L;
        if (!PMP1(session, person_id, post_id)) {
          num_aborted_txns.fetch_add(1);
        }
      }
    }
  });

  LOG(INFO) << "Number of aborted txns: " << num_aborted_txns;

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "PMPTest passed";
  } else {
    LOG(FATAL) << "PMPTest failed";
  }
}

// Observed Transaction Vanishes

void OTVInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;
  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,  // version
                          },
                          {"version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "id", 0)},
                          {
                              gs::StorageStrategy::kMem,
                              gs::StorageStrategy::kMem,
                              gs::StorageStrategy::kMem,
                          },
                          4096);
  schema.add_edge_label("PERSON", "PERSON", "KNOWS", {}, {},
                        gs::EdgeStrategy::kMultiple,
                        gs::EdgeStrategy::kMultiple);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  for (int i = 1; i <= 4; ++i) {
    txn.run(
        "With $person1_id as person1_id, $version as version\n"
        "CREATE (p1:PERSON {id: person1_id, version: version})",
        {{"person1_id", std::to_string(i)}, {"version", "0"}});
  }
  for (int i = 1; i <= 3; ++i) {
    txn.run(
        "With $person1_id as person1_id, $person2_id as person2_id\n"
        "CREATE (p1:PERSON {id: person1_id})-[:KNOWS]->(p2:PERSON {id: "
        "person2_id})",
        {{"person1_id", std::to_string(i)},
         {"person2_id", std::to_string(i + 1)}});
  }
  txn.run(
      "With $person4_id as person4_id, $person1_id as person1_id\n"
      "CREATE (p1:PERSON {id: person4_id})-[:KNOWS]->(p2:PERSON {id: "
      "person1_id})",
      {{"person4_id", "4"}, {"person1_id", "1"}});
  txn.Commit();
}

void OTV1(GraphDBSession& db, int cycle_size) {
  std::random_device rand_dev;
  std::mt19937 gen(rand_dev());
  std::uniform_int_distribution<int> dist(1, cycle_size);
  for (int i = 0; i < 100; i++) {
    long person_id = dist(gen);
    auto txn = db.GetUpdateTransaction();
    txn.run(
        "MATCH (p1:PERSON {id: "
        "$personId})-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4)"
        " SET p1.version = p1.version + 1,"
        " p2.version = p2.version + 1,"
        " p3.version = p3.version + 1,"
        " p4.version = p4.version + 1\n",
        {{"personId", std::to_string(person_id)}});
    txn.Commit();
  }
}

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
OTV2(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetReadTransaction();

  std::map<std::string, std::string> parameters = {
      {"personId", std::to_string(person_id)}};

  auto res1 = txn.run(
      "MATCH (p1:PERSON {id: "
      "$personId})-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4), "
      "(p4)-[:KNOWS]->(p1) "
      "RETURN p1.version, p2.version, p3.version, p4.version",
      parameters);
  if (res1.empty()) {
    LOG(FATAL) << "OTV2 Result empty";
  }
  gs::Decoder decoder1(res1.data(), res1.size());
  int64_t v1 = decoder1.get_long();
  int64_t v2 = decoder1.get_long();
  int64_t v3 = decoder1.get_long();
  int64_t v4 = decoder1.get_long();
  auto tup1 = std::make_tuple(v1, v2, v3, v4);

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  auto res2 = txn.run(
      "MATCH (p1:PERSON {id: "
      "$personId})-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4), "
      "(p4)-[:KNOWS]->(p1) "
      "RETURN p1.version, p2.version, p3.version, p4.version",
      parameters);
  if (res2.empty()) {
    LOG(FATAL) << "OTV2 Result empty";
  }
  gs::Decoder decoder2(res2.data(), res2.size());
  int64_t v5 = decoder2.get_long();
  int64_t v6 = decoder2.get_long();
  int64_t v7 = decoder2.get_long();
  int64_t v8 = decoder2.get_long();
  auto tup2 = std::make_tuple(v5, v6, v7, v8);
  return std::make_tuple(tup1, tup2);
}

void OTVTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  OTVInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  // OTV1(db.GetSession(0), 4);
  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 4);
    if (client_id) {
      std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
      for (int i = 0; i < 100; ++i) {
        std::tie(tup1, tup2) = OTV2(session, dist(gen));
        int64_t v1_max =
            std::max(std::max(std::get<0>(tup1), std::get<1>(tup1)),
                     std::max(std::get<2>(tup1), std::get<3>(tup1)));
        int64_t v2_min =
            std::min(std::min(std::get<0>(tup2), std::get<1>(tup2)),
                     std::min(std::get<2>(tup2), std::get<3>(tup2)));
        if (v1_max > v2_min) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      OTV1(session, 4);
    }
  });

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "OTVTest passed";
  } else {
    LOG(FATAL) << "OTVTest failed";
  }
}

// Fractured Reads

void FRInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  OTVInit(db, work_dir, thread_num);
}

void FR1(GraphDBSession& db, int64_t person_id) { OTV1(db, person_id); }

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
FR2(GraphDBSession& db, int64_t person_id) {
  return OTV2(db, person_id);
}

void FRTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  FRInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (client_id) {
      for (int i = 0; i < 1000; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = FR2(session, 1L);
        if (tup1 != tup2) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      FR1(session, 1L);
    }
  });

  if (num_incorrect_checks == 0) {
    LOG(INFO) << "FRTest passed";
  } else {
    LOG(FATAL) << "FRTest failed";
  }
}

// Lost Updates

void LUInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;
  schema.add_vertex_label(
      "PERSON",
      {
          PropertyType::kInt64,  // num friends
      },
      {
          "numFriends",
      },
      {std::tuple<gs::PropertyType, std::string, size_t>(
          gs::PropertyType::kInt64, "id", 0)},
      {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem}, 4096);
  schema.add_edge_label("PERSON", "PERSON", "KNOWS", {}, {},
                        gs::EdgeStrategy::kMultiple,
                        gs::EdgeStrategy::kMultiple);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  txn.run(
      "With $person_id as person_id, $numFriends as numFriends\n"
      "CREATE (:PERSON {id: person_id, numFriends: numFriends})",
      {{"person_id", "1"}, {"numFriends", "0"}});
  txn.Commit();
}

bool LU1(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  txn.run(
      "With $person1_id as person1_id, $person2_id as person2_id, $numFriends "
      "as numFriends\n"
      " CREATE (p2 :PERSON {id: person2_id, numFriends: numFriends})\n"
      " CREATE (p1 :PERSON {id: person1_id})-[:KNOWS]->(p2 :PERSON {id: "
      "person2_id})",
      {{"person1_id", std::to_string(1L)},
       {"person2_id", std::to_string(person_id)},
       {"numFriends", std::to_string(1)}});
  txn.run(
      "MATCH (p1:PERSON {id: 1L})\n"
      "SET p1.numFriends = p1.numFriends + 1",
      {});

  txn.Commit();
  return true;
}

std::pair<int64_t, int64_t> LU2(GraphDBSession& db, int person_id) {
  auto txn = db.GetReadTransaction();
  std::map<std::string, std::string> parameters = {
      {"personId", std::to_string(person_id)}};
  auto res = txn.run(
      "MATCH (p:PERSON {id: $personId})\n"
      "OPTIONAL MATCH (p)-[k:KNOWS]->(w)\n"
      "WITH p, count(k) AS numKnowsEdges\n"
      "RETURN numKnowsEdges,\n"
      "       p.numFriends AS numFriendsProp\n",
      parameters);
  if (res.empty()) {
    LOG(FATAL) << "LU2 Result empty";
  }
  gs::Decoder decoder(res.data(), res.size());
  int64_t numKnowsEdges = decoder.get_long();
  int64_t numFriendsProp = decoder.get_long();

  return {numKnowsEdges, numFriendsProp};
}

void LUTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  LUInit(db, work_dir, thread_num);
  std::atomic<int64_t> num_aborted_txns(0);
  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (LU1(session, client_id + 2)) {
    } else {
      num_aborted_txns.fetch_add(1);
    }
  });
  LOG(INFO) << "Number of aborted txns: " << num_aborted_txns;

  auto [numKnowEdges, numFriendProp] = LU2(db.GetSession(0), 1L);

  if (numKnowEdges == numFriendProp) {
    LOG(INFO) << "LUTest passed";
  } else {
    LOG(FATAL) << "LUTest failed";
  }
}

// Write Skews

void WSInit(GraphDB& db, const std::string& work_dir, int thread_num) {
  Schema schema;

  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,  // version
                          },
                          {"value"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  schema.add_edge_label("PERSON", "PERSON", "KNOWS", {}, {},
                        EdgeStrategy::kMultiple, EdgeStrategy::kMultiple);
  gs::DBInitializer::get().open(db, work_dir, schema);

  auto txn = db.GetInsertTransaction();
  for (int i = 1; i <= 10; ++i) {
    txn.run(
        "With $person1_id as person1_id, $value1 as value1, $person2_id as "
        "person2_id, $value2 as value2\n"
        " CREATE (p1 : PERSON {id : person1_id, value : value1})\n"
        " CREATE (p2 : PERSON {id : person2_id, value : value2})\n"
        " CREATE (p1 : PERSON {id : person1_id})-[:KNOWS]->(p2 : PERSON {id : "
        "person2_id})",
        {{"person1_id", std::to_string(2 * i - 1)},
         {"value1", "70"},
         {"person2_id", std::to_string(2 * i)},
         {"value2", "80"}});
  }

  txn.Commit();
}

void WS1(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
         std::mt19937& gen) {
  auto txn = db.GetUpdateTransaction();
  std::map<std::string, std::string> parameters = {
      {"person1Id", std::to_string(person1_id)},
      {"person2Id", std::to_string(person2_id)}};
  auto res = txn.run(
      "MATCH (p1:PERSON {id: $person1Id})-[:KNOWS]->(p2:PERSON {id: "
      "$person2Id})\n"
      "WHERE p1.value + p2.value >= 100\n"
      "RETURN p1, p2",
      parameters);
  if (!res.empty()) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
    std::uniform_int_distribution<> dist(0, 1);
    int64_t person_id = dist(gen) ? person1_id : person2_id;
    txn.run(
        "MATCH (p:PERSON {id: $personId})\n"
        "SET p.value = p.value - 100",
        {{"personId", std::to_string(person_id)}});
  }
  txn.Commit();
}

std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> WS2(
    GraphDBSession& db) {
  std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> results;
  auto txn = db.GetReadTransaction();
  auto res = txn.run(
      "MATCH (p1:PERSON)-[:KNOWS]->(p2:PERSON {id: p1.id+1})\n"
      "WHERE p1.value + p2.value <= 0\n"
      "RETURN p1.id AS p1id, p1.value AS p1value, p2.id AS p2id, p2.value "
      "AS p2value",
      {});

  gs::Decoder decoder(res.data(), res.size());

  while (!decoder.empty()) {
    int64_t p1id = decoder.get_long();
    int64_t p1value = decoder.get_long();
    int64_t p2id = decoder.get_long();
    int64_t p2value = decoder.get_long();
    results.push_back(std::make_tuple(p1id, p1value, p2id, p2value));
  }
  return results;
}

void WSTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  WSInit(db, work_dir, thread_num);

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 10);
    // write client issue multiple write transactions
    for (int i = 0; i < 1000; ++i) {
      // pick pair randomly
      int64_t person1_id = dist(gen) * 2 - 1;
      int64_t person2_id = person1_id + 1;
      WS1(session, person1_id, person2_id, gen);
    }
  });

  auto results = WS2(db.GetSession(0));

  if (results.empty()) {
    LOG(INFO) << "WSTest passed";
  } else {
    LOG(ERROR) << "WSTest failed";
    for (auto& tup : results) {
      LOG(INFO) << std::get<0>(tup) << " " << std::get<1>(tup) << " "
                << std::get<2>(tup) << " " << std::get<3>(tup);
    }
  }
}

std::string generate_random_string(int length) {
  static const char alphanum[] = "abcdefghijklmnopqrstuvwxyz";

  std::string ret;
  srand(time(0));  // Seed the random number generator

  for (int i = 0; i < length; ++i) {
    ret += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return ret;
}

std::string generate_work_dir(const std::string& prefix) {
  while (true) {
    std::string dir = prefix + generate_random_string(8);
    if (std::filesystem::exists(dir)) {
      continue;
    }

    std::filesystem::create_directories(dir);
    return dir;
  }
}

int main(int argc, char** argv) {
  int thread_num = 8;
  std::string compiler_path = "";

  if (argc > 1) {
    compiler_path = argv[1];
  }
  if (argc > 2) {
    thread_num = std::thread::hardware_concurrency();
  }

  gs::DBInitializer::get().set_compiler_path(compiler_path);
  gs::DBInitializer::get().set_thread_num(thread_num);
  std::string prefix = "/tmp/graphscope_acid_";
  std::string work_dir = generate_work_dir(prefix);
  if (argc > 3) {
    work_dir = argv[2];
  }

  AtomicityCTest(work_dir + "/AtomicityC", thread_num);
  AtomicityRBTest(work_dir + "/AtomicityRB", thread_num);

  G0Test(work_dir + "/G0", thread_num);
  G1ATest(work_dir + "/G1A", thread_num);
  G1BTest(work_dir + "/G1B", thread_num);
  G1CTest(work_dir + "/G1C", thread_num);

  IMPTest(work_dir + "/IMP", thread_num);
  PMPTest(work_dir + "/PMP", thread_num);

  OTVTest(work_dir + "/OTV", thread_num);
  FRTest(work_dir + "/FR", thread_num);

  LUTest(work_dir + "/LU", thread_num);
  WSTest(work_dir + "/WS", thread_num);

  return 0;
}
