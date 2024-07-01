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
                              PropertyType::kInt64,        // id
                              PropertyType::Varchar(256),  // name
                              PropertyType::Varchar(256),  // emails
                          },
                          {"id", "name", "emails"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "oid", 0)},
                          {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem,
                           gs::StorageStrategy::kMem},
                          4096);
  schema.add_edge_label("PERSON", "PERSON", "KNOWS",
                        {
                            PropertyType::kInt64,  // since
                        },
                        {}, gs::EdgeStrategy::kMultiple,
                        gs::EdgeStrategy::kMultiple);

  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto txn = db.GetInsertTransaction();

  int64_t id1 = 1;
  std::string name1 = "Alice";
  std::string email1 = "alice@aol.com";
  int64_t id2 = 2;
  std::string name2 = "Bob";
  std::string email2 = "bob@hotmail.com;bobby@yahoo.com";
  CHECK(txn.AddVertex(person_label_id, generate_id(),
                      {Any::From(id1), Any::From(name1), Any::From(email1)}));
  CHECK(txn.AddVertex(person_label_id, generate_id(),
                      {Any::From(id2), Any::From(name2), Any::From(email2)}));
  txn.Commit();
}

bool AtomicityC(GraphDBSession& db, int64_t person2_id,
                const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  // randomly select a person
  auto vit = get_random_vertex(txn, person_label_id);
  CHECK(vit.IsValid());
  oid_t p1_id = vit.GetId().AsInt64();
  // append an email
  append_string_to_field(vit, 2, new_email);

  oid_t p2_id = generate_id();
  std::string name = "";
  std::string email = "";
  if (!txn.AddVertex(
          person_label_id, p2_id,
          {Any::From(person2_id), Any::From(name), Any::From(email)})) {
    // insert person failed
    txn.Abort();
    return false;
  }
  if (!txn.AddEdge(person_label_id, p1_id, person_label_id, p2_id,
                   knows_label_id, Any::From(since))) {
    // add edge failed
    txn.Abort();
    return false;
  }

  txn.Commit();
  return true;
}

bool AtomicityRB(GraphDBSession& db, int64_t person2_id,
                 const std::string& new_email, int64_t since) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  // randomly select a person
  auto vit1 = get_random_vertex(txn, person_label_id);
  CHECK(vit1.IsValid());
  // append an email
  append_string_to_field(vit1, 2, new_email);

  auto vit2 = txn.GetVertexIterator(person_label_id);
  for (; vit2.IsValid(); vit2.Next()) {
    if (vit2.GetField(0).AsInt64() == person2_id) {
      // person2 exists, abort
      txn.Abort();
      return false;
    }
  }

  oid_t p2_id = generate_id();
  std::string name = "";
  std::string email = "";
  // insert person2
  CHECK(txn.AddVertex(
      person_label_id, p2_id,
      {Any::From(person2_id), Any::From(name), Any::From(email)}));

  txn.Commit();
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

std::pair<int64_t, int64_t> AtomicityCheck(GraphDB& db) {
  auto txn = db.GetReadTransaction();
  int64_t num_persons = 0;
  int64_t num_emails = 0;

  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  for (auto vit = txn.GetVertexIterator(person_label_id); vit.IsValid();
       vit.Next()) {
    ++num_persons;
    num_emails += count_email_num(vit.GetField(2).AsStringView());
  }
  return std::make_pair(num_persons, num_emails);
}

void AtomicityCTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  AtomicityInit(db, work_dir, thread_num);

  auto committed = AtomicityCheck(db);

  std::atomic<int> num_aborted_txns(0);
  std::atomic<int> num_committed_txns(0);

  parallel_transaction(
      db,
      [&](GraphDBSession& session, int txn_id) {
        bool successful =
            AtomicityC(session, 3 + txn_id, "alice@otherdomain.net", 2020);
        if (successful) {
          num_committed_txns.fetch_add(1);
        } else {
          num_aborted_txns.fetch_add(1);
        }
      },
      50);

  committed.first += num_committed_txns.load();
  committed.second += num_committed_txns.load();

  LOG(INFO) << "Number of aborted txns: " << num_aborted_txns;
  auto finalstate = AtomicityCheck(db);

  if (committed == finalstate) {
    LOG(INFO) << "AtomicityCTest passed";
  } else {
    LOG(FATAL) << "AtomicityCTest failed";
  }
}

void AtomicityRBTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  AtomicityInit(db, work_dir, thread_num);

  auto committed = AtomicityCheck(db);

  std::atomic<int> num_aborted_txns(0);
  std::atomic<int> num_committed_txns(0);

  parallel_transaction(
      db,
      [&](GraphDBSession& session, int txn_id) {
        bool successful;

        if (txn_id % 2 == 0) {
          successful = AtomicityRB(session, 2, "alice@otherdomain.net", 2020);
        } else {
          successful =
              AtomicityRB(session, 3 + txn_id, "alice@otherdomain.net", 2020);
        }
        if (successful) {
          num_committed_txns.fetch_add(1);
        } else {
          num_aborted_txns.fetch_add(1);
        }
      },
      50);

  committed.first += num_committed_txns.load();
  committed.second += num_committed_txns.load();

  LOG(INFO) << "Number of aborted txns: " << num_aborted_txns;
  auto finalstate = AtomicityCheck(db);

  if (committed == finalstate) {
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
                              PropertyType::kInt64,        // id
                              PropertyType::Varchar(256),  // version history
                          },
                          {
                              "id",
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
                        {}, EdgeStrategy::kMultiple, EdgeStrategy::kMultiple);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto knows_label_id = schema.get_edge_label_id("KNOWS");

  auto txn = db.GetInsertTransaction();

  std::string value = "0";
  for (int i = 0; i < 100; ++i) {
    auto p1_id = generate_id();
    int64_t p1_id_property = 2 * i + 1;
    CHECK(txn.AddVertex(person_label_id, p1_id,
                        {Any::From(p1_id_property), Any::From(value)}));
    auto p2_id = generate_id();
    int64_t p2_id_property = 2 * i + 2;
    CHECK(txn.AddVertex(person_label_id, p2_id,
                        {Any::From(p2_id_property), Any::From(value)}));
    CHECK(txn.AddEdge(person_label_id, p1_id, person_label_id, p2_id,
                      knows_label_id, Any::From(value)));
  }
  txn.Commit();
}

void G0(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
        int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  auto vit1 = txn.GetVertexIterator(person_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person1_id) {
      break;
    }
  }

  CHECK(vit1.IsValid());
  append_string_to_field(vit1, 1, std::to_string(txn_id));

  auto vit2 = txn.GetVertexIterator(person_label_id);
  for (; vit2.IsValid(); vit2.Next()) {
    if (vit2.GetField(0).AsInt64() == person2_id) {
      break;
    }
  }

  CHECK(vit2.IsValid());
  append_string_to_field(vit2, 1, std::to_string(txn_id));

  auto oeit = txn.GetOutEdgeIterator(person_label_id, vit1.GetIndex(),
                                     person_label_id, knows_label_id);
  while (oeit.IsValid()) {
    if (oeit.GetNeighbor() == vit2.GetIndex()) {
      break;
    }
    oeit.Next();
  }
  CHECK(oeit.IsValid());

  Any cur = oeit.GetData();
  std::string cur_str(cur.value.s);
  if (cur_str.empty()) {
    cur_str = std::to_string(txn_id);
  } else {
    cur_str += ";";
    cur_str += std::to_string(txn_id);
  }
  Any new_value;
  new_value.set_string(cur_str);

  oeit.SetData(new_value);

  txn.Commit();
}

std::tuple<std::string, std::string, std::string> G0Check(GraphDB& db,
                                                          int64_t person1_id,
                                                          int64_t person2_id) {
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  auto vit1 = txn.GetVertexIterator(person_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person1_id) {
      break;
    }
  }
  std::string p1_version_history = std::string(vit1.GetField(1).AsStringView());

  auto vit2 = txn.GetVertexIterator(person_label_id);
  for (; vit2.IsValid(); vit2.Next()) {
    if (vit2.GetField(0).AsInt64() == person2_id) {
      break;
    }
  }
  std::string p2_version_history = std::string(vit2.GetField(1).AsStringView());

  auto oeit = txn.GetOutEdgeIterator(person_label_id, vit1.GetIndex(),
                                     person_label_id, knows_label_id);
  while (oeit.IsValid()) {
    if (oeit.GetNeighbor() == vit2.GetIndex()) {
      break;
    }
    oeit.Next();
  }
  CHECK(oeit.IsValid());
  Any k_version_history_field = oeit.GetData();
  CHECK(k_version_history_field.type == PropertyType::Varchar(256));
  std::string k_version_history(k_version_history_field.value.s);

  return std::make_tuple(p1_version_history, p2_version_history,
                         k_version_history);
}

void G0Test(const std::string& work_dir, int thread_num) {
  GraphDB db;
  G0Init(db, work_dir, thread_num);

  parallel_transaction(
      db,
      [&](GraphDBSession& db, int txn_id) {
        std::random_device rand_dev;
        std::mt19937 gen(rand_dev());
        std::uniform_int_distribution<int> dist(1, 100);
        int picked = dist(gen) * 2 - 1;
        G0(db, picked, picked + 1, txn_id + 1);
      },
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
                              PropertyType::kInt64,
                              PropertyType::kInt64,  // version
                          },
                          {"id", "version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto txn = db.GetInsertTransaction();
  int64_t vertex_data = 1;
  for (int i = 0; i < 100; ++i) {
    int64_t vertex_id_property = i + 1;
    CHECK(
        txn.AddVertex(person_label_id, generate_id(),
                      {Any::From(vertex_id_property), Any::From(vertex_data)}));
  }
  txn.Commit();
}

void G1A1(GraphDBSession& db) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  // select a random person
  auto vit = get_random_vertex(txn, person_label_id);

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  // attempt to set version = 2
  vit.SetField(1, Any::From<int64_t>(2));
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  txn.Abort();
}

int64_t G1A2(GraphDBSession& db) {
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit = get_random_vertex(txn, person_label_id);
  return vit.GetField(1).AsInt64();
}

void G1ATest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  G1AInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& db, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        auto p_version = G1A2(db);
        if (p_version != 1) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        G1A1(db);
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
                              PropertyType::kInt64,  // id
                              PropertyType::kInt64,  // version
                          },
                          {"id", "version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto txn = db.GetInsertTransaction();
  int64_t value = 99;
  for (int i = 0; i < 100; ++i) {
    int64_t vertex_id_property = i + 1;
    CHECK(txn.AddVertex(person_label_id, generate_id(),
                        {Any::From(vertex_id_property), Any::From(value)}));
  }
  txn.Commit();
}

void G1B1(GraphDBSession& db, int64_t even, int64_t odd) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit = get_random_vertex(txn, person_label_id);
  vit.SetField(1, Any::From(even));
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  vit.SetField(1, Any::From(odd));
  txn.Commit();
}

int64_t G1B2(GraphDBSession& db) {
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit = get_random_vertex(txn, person_label_id);
  return vit.GetField(1).AsInt64();
}

void G1BTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  G1BInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        auto p_version = G1B2(session);
        if (p_version % 2 != 1) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        G1B1(session, 0, 1);
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
                              PropertyType::kInt64,  // id
                              PropertyType::kInt64,  // version
                          },
                          {"id", "version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto txn = db.GetInsertTransaction();

  int64_t version_property = 0;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    CHECK(txn.AddVertex(person_label_id, generate_id(),
                        {Any::From(id_property), Any::From(version_property)}));
  }
  txn.Commit();
}

int64_t G1C(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
            int64_t txn_id) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit1 = txn.GetVertexIterator(person_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person1_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());
  vit1.SetField(1, Any::From(txn_id));

  auto vit2 = txn.GetVertexIterator(person_label_id);
  for (; vit2.IsValid(); vit2.Next()) {
    if (vit2.GetField(0).AsInt64() == person2_id) {
      break;
    }
  }
  CHECK(vit2.IsValid());
  int64_t ret = vit2.GetField(1).AsInt64();
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
        std::uniform_int_distribution<int> dist(1, 100);
        // select two different persons randomly
        int64_t person1_id = dist(gen);
        int64_t person2_id;
        do {
          person2_id = dist(gen);
        } while (person1_id == person2_id);
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
                              PropertyType::kInt64,  // id
                              PropertyType::kInt64,  // version
                          },
                          {"id", "version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto txn = db.GetInsertTransaction();
  int64_t version_property = 1;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    CHECK(txn.AddVertex(person_label_id, generate_id(),
                        {Any::From(id_property), Any::From(version_property)}));
  }
  txn.Commit();
}

void IMP1(GraphDBSession& db) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit = get_random_vertex(txn, person_label_id);
  int64_t old_version = vit.GetField(1).AsInt64();
  vit.SetField(1, Any::From(old_version + 1));
  txn.Commit();
}

std::tuple<int64_t, int64_t> IMP2(GraphDBSession& db, int64_t person1_id) {
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto vit0 = txn.GetVertexIterator(person_label_id);
  for (; vit0.IsValid(); vit0.Next()) {
    if (vit0.GetField(0).AsInt64() == person1_id) {
      break;
    }
  }
  CHECK(vit0.IsValid());
  int64_t v1 = vit0.GetField(1).AsInt64();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  auto vit1 = txn.GetVertexIterator(person_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person1_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());
  int64_t v2 = vit1.GetField(1).AsInt64();

  return std::make_tuple(v1, v2);
}

void IMPTest(const std::string& work_dir, int thread_num) {
  GraphDB db;
  IMPInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    if (client_id < rc) {
      std::random_device rand_dev;
      std::mt19937 gen(rand_dev());
      std::uniform_int_distribution<int> dist(1, 100);
      for (int i = 0; i < 1000; ++i) {
        int picked = dist(gen);
        int64_t v1, v2;
        std::tie(v1, v2) = IMP2(session, picked);
        if (v1 != v2) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        IMP1(session);
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
  schema.add_vertex_label("PERSON",
                          {
                              PropertyType::kInt64,
                          },
                          {"id"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "id", 0)},
                          {gs::StorageStrategy::kMem}, 4096);
  schema.add_vertex_label("POST",
                          {
                              PropertyType::kInt64,
                          },
                          {"id"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              gs::PropertyType::kInt64, "id", 0)},
                          {gs::StorageStrategy::kMem}, 4096);
  schema.add_edge_label("PERSON", "POST", "LIKES", {}, {},
                        gs::EdgeStrategy::kMultiple,
                        gs::EdgeStrategy::kMultiple);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto post_label_id = schema.get_vertex_label_id("POST");

  auto txn = db.GetInsertTransaction();
  for (int i = 0; i < 100; ++i) {
    int64_t value = i + 1;
    CHECK(txn.AddVertex(person_label_id, generate_id(), {Any::From(value)}));
    CHECK(txn.AddVertex(post_label_id, generate_id(), {Any::From(value)}));
  }
  txn.Commit();
}

bool PMP1(GraphDBSession& db, int64_t person_id, int64_t post_id) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto post_label_id = db.schema().get_vertex_label_id("POST");
  auto likes_label_id = db.schema().get_edge_label_id("LIKES");

  auto vit0 = txn.GetVertexIterator(person_label_id);
  for (; vit0.IsValid(); vit0.Next()) {
    if (vit0.GetField(0).AsInt64() == person_id) {
      break;
    }
  }
  CHECK(vit0.IsValid());

  auto vit1 = txn.GetVertexIterator(post_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == post_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());

  if (!txn.AddEdge(person_label_id, vit0.GetId(), post_label_id, vit1.GetId(),
                   likes_label_id, Any())) {
    txn.Abort();
    return false;
  }
  txn.Commit();
  return true;
}

std::tuple<int64_t, int64_t> PMP2(GraphDBSession& db, int64_t post_id) {
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto post_label_id = db.schema().get_vertex_label_id("POST");
  auto likes_label_id = db.schema().get_edge_label_id("LIKES");

  auto vit0 = txn.GetVertexIterator(post_label_id);
  for (; vit0.IsValid(); vit0.Next()) {
    if (vit0.GetField(0).AsInt64() == post_id) {
      break;
    }
  }
  int64_t c1 = 0;
  for (auto ieit = txn.GetInEdgeIterator(post_label_id, vit0.GetIndex(),
                                         person_label_id, likes_label_id);
       ieit.IsValid(); ieit.Next()) {
    c1++;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  auto vit1 = txn.GetVertexIterator(post_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == post_id) {
      break;
    }
  }
  int64_t c2 = 0;
  for (auto ieit = txn.GetInEdgeIterator(post_label_id, vit1.GetIndex(),
                                         person_label_id, likes_label_id);
       ieit.IsValid(); ieit.Next()) {
    c2++;
  }
  return std::make_tuple(c1, c2);
}

void PMPTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  PMPInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  std::atomic<int64_t> num_aborted_txns(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        int64_t v1, v2;
        int post_id = dist(gen);
        std::tie(v1, v2) = PMP2(session, post_id);
        if (v1 != v2) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        int person_id = dist(gen);
        int post_id = dist(gen);
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
                              PropertyType::kInt64,        // id
                              PropertyType::Varchar(256),  // name
                              PropertyType::kInt64,        // version
                          },
                          {"id", "name", "version"},
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

  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");
  auto knows_label_id = schema.get_edge_label_id("KNOWS");

  auto txn = db.GetInsertTransaction();
  int64_t value = 0;

  for (int j = 1; j <= 100; j++) {
    std::vector<oid_t> vids;
    for (int i = 1; i <= 4; i++) {
      auto vid = generate_id();
      int64_t id_property = j * 4 + i;
      CHECK(txn.AddVertex(person_label_id, vid,
                          {Any::From(id_property), Any::From(std::to_string(j)),
                           Any::From(value)}));
      vids.push_back(vid);
    }
    for (int i = 0; i < 4; i++) {
      CHECK(txn.AddEdge(person_label_id, vids[i], person_label_id,
                        vids[(i + 1) % 4], knows_label_id, Any()));
    }
  }
  txn.Commit();
}

void OTV1(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  auto vit1 = txn.GetVertexIterator(person_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());
  vid_t vid1 = vit1.GetIndex();
  for (auto eit1 = txn.GetOutEdgeIterator(person_label_id, vid1,
                                          person_label_id, knows_label_id);
       eit1.IsValid(); eit1.Next()) {
    CHECK(eit1.IsValid());
    vid_t vid2 = eit1.GetNeighbor();
    for (auto eit2 = txn.GetOutEdgeIterator(person_label_id, vid2,
                                            person_label_id, knows_label_id);
         eit2.IsValid(); eit2.Next()) {
      CHECK(eit2.IsValid());
      vid_t vid3 = eit2.GetNeighbor();
      for (auto eit3 = txn.GetOutEdgeIterator(person_label_id, vid3,
                                              person_label_id, knows_label_id);
           eit3.IsValid(); eit3.Next()) {
        CHECK(eit3.IsValid());
        vid_t vid4 = eit3.GetNeighbor();
        for (auto eit4 = txn.GetOutEdgeIterator(
                 person_label_id, vid4, person_label_id, knows_label_id);
             eit4.IsValid(); eit4.Next()) {
          CHECK(eit4.IsValid());
          if (eit4.GetNeighbor() == vid1) {
            auto vit = txn.GetVertexIterator(person_label_id);
            vit.Goto(vid1);
            vit.SetField(2, Any::From(vit.GetField(2).AsInt64() + 1));

            vit.Goto(vid2);
            vit.SetField(2, Any::From(vit.GetField(2).AsInt64() + 1));

            vit.Goto(vid3);
            vit.SetField(2, Any::From(vit.GetField(2).AsInt64() + 1));

            vit.Goto(vid4);
            vit.SetField(2, Any::From(vit.GetField(2).AsInt64() + 1));

            txn.Commit();
            return;
          }
        }
      }
    }
  }
}

std::tuple<std::tuple<int64_t, int64_t, int64_t, int64_t>,
           std::tuple<int64_t, int64_t, int64_t, int64_t>>
OTV2(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");
  auto knows_label_id = db.schema().get_edge_label_id("KNOWS");

  auto vit1 = txn.GetVertexIterator(person_label_id);

  auto get_versions = [&]() -> std::tuple<int64_t, int64_t, int64_t, int64_t> {
    CHECK(vit1.IsValid());
    vid_t vid1 = vit1.GetIndex();
    for (auto eit1 = txn.GetOutEdgeIterator(person_label_id, vid1,
                                            person_label_id, knows_label_id);
         eit1.IsValid(); eit1.Next()) {
      CHECK(eit1.IsValid());
      vid_t vid2 = eit1.GetNeighbor();
      for (auto eit2 = txn.GetOutEdgeIterator(person_label_id, vid2,
                                              person_label_id, knows_label_id);
           eit2.IsValid(); eit2.Next()) {
        CHECK(eit2.IsValid());
        vid_t vid3 = eit2.GetNeighbor();
        for (auto eit3 = txn.GetOutEdgeIterator(
                 person_label_id, vid3, person_label_id, knows_label_id);
             eit3.IsValid(); eit3.Next()) {
          CHECK(eit3.IsValid());
          vid_t vid4 = eit3.GetNeighbor();
          for (auto eit4 = txn.GetOutEdgeIterator(
                   person_label_id, vid4, person_label_id, knows_label_id);
               eit4.IsValid(); eit4.Next()) {
            CHECK(eit4.IsValid());
            if (eit4.GetNeighbor() == vid1) {
              auto vit = txn.GetVertexIterator(person_label_id);

              vit.Goto(vid1);
              int64_t v1_version = vit.GetField(2).AsInt64();

              vit.Goto(vid2);
              int64_t v2_version = vit.GetField(2).AsInt64();

              vit.Goto(vid3);
              int64_t v3_version = vit.GetField(2).AsInt64();

              vit.Goto(vid4);
              int64_t v4_version = vit.GetField(2).AsInt64();

              return std::make_tuple(v1_version, v2_version, v3_version,
                                     v4_version);
            }
          }
        }
      }
    }

    return std::make_tuple(0, 0, 0, 0);
  };

  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());
  auto tup1 = get_versions();

  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));

  vit1.Goto(0);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());
  auto tup2 = get_versions();

  return std::make_tuple(tup1, tup2);
}

void OTVTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  OTVInit(db, work_dir, thread_num);

  std::atomic<int64_t> num_incorrect_checks(0);
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = OTV2(session, dist(gen) * 4 + 1);
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
      for (int i = 0; i < 1000; ++i) {
        OTV1(session, dist(gen) * 4 + 1);
      }
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
  int rc = thread_num / 2;

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    if (client_id < rc) {
      for (int i = 0; i < 1000; ++i) {
        std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
        std::tie(tup1, tup2) = FR2(session, dist(gen) * 4 + 1);
        if (tup1 != tup2) {
          num_incorrect_checks.fetch_add(1);
        }
      }
    } else {
      for (int i = 0; i < 1000; ++i) {
        FR1(session, dist(gen) * 4 + 1);
      }
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
          PropertyType::kInt64,  // id
          PropertyType::kInt64,  // num friends
      },
      {
          "id",
          "num_friends",
      },
      {std::tuple<gs::PropertyType, std::string, size_t>(
          gs::PropertyType::kInt64, "id", 0)},
      {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem}, 4096);
  db.Open(schema, work_dir, thread_num);
  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto txn = db.GetInsertTransaction();
  int64_t num_property = 0;
  for (int i = 0; i < 100; ++i) {
    int64_t id_property = i + 1;
    CHECK(txn.AddVertex(person_label_id, generate_id(),
                        {Any::From(id_property), Any::From(num_property)}));
  }

  txn.Commit();
}

bool LU1(GraphDBSession& db, int64_t person_id) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  auto vit = txn.GetVertexIterator(person_label_id);
  for (; vit.IsValid(); vit.Next()) {
    if (vit.GetField(0).AsInt64() == person_id) {
      break;
    }
  }
  if (!vit.IsValid()) {
    txn.Abort();
    return false;
  }

  int64_t num_friends = vit.GetField(1).AsInt64();
  vit.SetField(1, Any::From(num_friends + 1));

  txn.Commit();
  return true;
}

std::map<int64_t, int64_t> LU2(GraphDBSession& db) {
  std::map<int64_t, int64_t> numFriends;
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  auto vit = txn.GetVertexIterator(person_label_id);
  for (; vit.IsValid(); vit.Next()) {
    int64_t person_id = vit.GetField(0).AsInt64();
    int64_t num_friends = vit.GetField(1).AsInt64();
    numFriends.emplace(person_id, num_friends);
  }

  return numFriends;
}

void LUTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  LUInit(db, work_dir, thread_num);

  std::map<int64_t, int64_t> expNumFriends;
  std::mutex mtx;
  std::atomic<int64_t> num_aborted_txns(0);

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
    std::map<int64_t, int64_t> localExpNumFriends;

    for (int i = 0; i < 1000; ++i) {
      int64_t person_id = dist(gen);
      if (LU1(session, person_id)) {
        ++localExpNumFriends[person_id];
      } else {
        num_aborted_txns.fetch_add(1);
      }
    }

    mtx.lock();
    for (auto& pair : localExpNumFriends) {
      expNumFriends[pair.first] += pair.second;
    }
    mtx.unlock();
  });

  LOG(INFO) << "Number of aborted txns: " << num_aborted_txns;

  std::map<int64_t, int64_t> numFriends = LU2(db.GetSession(0));

  if (numFriends == expNumFriends) {
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
                              PropertyType::kInt64,
                              PropertyType::kInt64,  // version
                          },
                          {"id", "version"},
                          {std::tuple<gs::PropertyType, std::string, size_t>(
                              PropertyType::kInt64, "id", 0)},
                          {StorageStrategy::kMem, StorageStrategy::kMem}, 4096);
  db.Open(schema, work_dir, thread_num);

  auto person_label_id = schema.get_vertex_label_id("PERSON");

  auto txn = db.GetInsertTransaction();

  for (int i = 1; i <= 100; i++) {
    int64_t id1 = 2 * i - 1;
    int64_t version1 = 70;
    CHECK(txn.AddVertex(person_label_id, generate_id(),
                        {Any::From(id1), Any::From(version1)}));
    int64_t id2 = 2 * i;
    int64_t version2 = 80;
    CHECK(txn.AddVertex(person_label_id, generate_id(),
                        {Any::From(id2), Any::From(version2)}));
  }
  txn.Commit();
}

void WS1(GraphDBSession& db, int64_t person1_id, int64_t person2_id,
         std::mt19937& gen) {
  auto txn = db.GetUpdateTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  auto vit1 = txn.GetVertexIterator(person_label_id);
  for (; vit1.IsValid(); vit1.Next()) {
    if (vit1.GetField(0).AsInt64() == person1_id) {
      break;
    }
  }
  CHECK(vit1.IsValid());
  int64_t p1_value = vit1.GetField(1).AsInt64();

  auto vit2 = txn.GetVertexIterator(person_label_id);
  for (; vit2.IsValid(); vit2.Next()) {
    if (vit2.GetField(0).AsInt64() == person2_id) {
      break;
    }
  }
  CHECK(vit2.IsValid());
  int64_t p2_value = vit2.GetField(1).AsInt64();

  if (p1_value + p2_value - 100 < 0) {
    txn.Abort();
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MILLI_SEC));
  std::uniform_int_distribution<> dist(0, 1);

  // pick randomly between person1 and person2 and decrement the value property
  if (dist(gen)) {
    vit1.SetField(1, Any::From(p1_value - 100));
  } else {
    vit2.SetField(1, Any::From(p2_value - 100));
  }
  txn.Commit();
}

std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> WS2(
    GraphDBSession& db) {
  std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> results;
  auto txn = db.GetReadTransaction();
  auto person_label_id = db.schema().get_vertex_label_id("PERSON");

  for (auto vit1 = txn.GetVertexIterator(person_label_id); vit1.IsValid();
       vit1.Next()) {
    int64_t person1_id = vit1.GetField(0).AsInt64();
    if (person1_id % 2 != 1) {
      continue;
    }
    int64_t p1_value = vit1.GetField(1).AsInt64();
    for (auto vit2 = txn.GetVertexIterator(person_label_id); vit2.IsValid();
         vit2.Next()) {
      int64_t person2_id = vit2.GetField(0).AsInt64();
      if (person2_id != person1_id + 1) {
        continue;
      }
      int64_t p2_value = vit2.GetField(1).AsInt64();
      if (p1_value + p2_value <= 0) {
        results.emplace_back(person1_id, p1_value, person2_id, p2_value);
      }
    }
  }
  return results;
}

void WSTest(const std::string& work_dir, int thread_num) {
  GraphDB db;

  WSInit(db, work_dir, thread_num);

  parallel_client(db, [&](GraphDBSession& session, int client_id) {
    std::random_device rand_dev;
    std::mt19937 gen(rand_dev());
    std::uniform_int_distribution<int> dist(1, 100);
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
    LOG(FATAL) << "WSTest failed";
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
  if (argc > 1) {
    thread_num = std::thread::hardware_concurrency();
  }

  std::string prefix = "/tmp/graphscope_acid_";
  std::string work_dir = generate_work_dir(prefix);
  if (argc > 2) {
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
