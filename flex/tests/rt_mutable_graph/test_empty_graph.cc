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

int main(int argc, char** argv) {
  std::string work_dir = argv[1];
  {
    std::filesystem::remove_all(work_dir);
    gs::GraphDB db;
    gs::Schema schema;
    schema.add_vertex_label("PERSON",
                            {
                                gs::PropertyType::Varchar(16),  // name
                                gs::PropertyType::Varchar(32),  // emails
                                gs::PropertyType::kStringMap,   // sex
                            },
                            {
                                "name",
                                "emails",
                                "sex",
                            },
                            {std::tuple<gs::PropertyType, std::string, size_t>(
                                gs::PropertyType::kInt64, "id", 0)},
                            {}, 4096);
    schema.add_edge_label("PERSON", "PERSON", "KNOWS",
                          {
                              gs::PropertyType::kInt64,  // since
                          },
                          {}, gs::EdgeStrategy::kMultiple,
                          gs::EdgeStrategy::kMultiple);
    db.Open(schema, work_dir);
    auto person_label_id = schema.get_vertex_label_id("PERSON");
    auto know_label_id = schema.get_edge_label_id("KNOWS");

    int64_t vertex_data = 1;
    int64_t id = 0;
    {
      auto txn = db.GetInsertTransaction();
      for (int i = 0; i < 100; ++i) {
        int64_t vertex_id_property = i + 1;
        std::string s = std::to_string(i % 16);
        CHECK(txn.AddVertex(
            person_label_id, id++,
            {gs::Any::From(std::to_string(vertex_id_property)),
             gs::Any::From(std::to_string(vertex_data)), gs::Any::From(s)}));
      }
      txn.Commit();
      LOG(INFO) << "Add Vertex success\n";
    }
    {
      auto txn = db.GetInsertTransaction();
      for (int64_t i = 0; i + 1 < 100; ++i) {
        CHECK(txn.AddEdge(person_label_id, i, person_label_id, i + 1,
                          know_label_id, i));
      }
      txn.Commit();

      LOG(INFO) << "Add Edge success\n";
    }
  }
  {
    std::filesystem::remove_all(work_dir);
    gs::GraphDB db;
    gs::Schema schema;
    schema.add_vertex_label(
        "PERSON",
        {
            gs::PropertyType::Varchar(16),  // name
            gs::PropertyType::Varchar(32),  // emails
            gs::PropertyType::kStringMap,   // sex
        },
        {"name", "emails", "sex"},
        {
            std::tuple<gs::PropertyType, std::string, size_t>(
                gs::PropertyType::Varchar(32), "id", 0),
        },
        {}, 4096);
    schema.add_edge_label("PERSON", "PERSON", "KNOWS",
                          {
                              gs::PropertyType::Varchar(32),  // since
                          },
                          {}, gs::EdgeStrategy::kMultiple,
                          gs::EdgeStrategy::kMultiple);

    db.Open(schema, work_dir, 1, false, false);
    auto person_label_id = schema.get_vertex_label_id("PERSON");
    auto know_label_id = schema.get_edge_label_id("KNOWS");

    int64_t vertex_data = 1;
    int64_t id = 0;
    {
      auto txn = db.GetInsertTransaction();
      for (int i = 0; i < 100; ++i) {
        int64_t vertex_id_property = i + 1;
        std::string sex{};
        if (i % 2 == 0) {
          sex = "male";
        } else {
          sex = "female";
        }
        CHECK(
            txn.AddVertex(person_label_id, gs::Any::From(std::to_string(id++)),
                          {
                              gs::Any::From(std::to_string(vertex_id_property)),
                              gs::Any::From(std::to_string(vertex_data)),
                              gs::Any::From(sex),
                          }));
      }
      txn.Commit();
      LOG(INFO) << "Add Vertex success\n";
    }
    {
      auto txn = db.GetInsertTransaction();
      for (int64_t i = 0; i + 1 < 100; ++i) {
        CHECK(txn.AddEdge(person_label_id, std::to_string(i), person_label_id,
                          std::to_string(i + 1), know_label_id,
                          gs::Any::From(std::to_string(i))));
      }
      txn.Commit();

      LOG(INFO) << "Add Edge success\n";
    }
  }
}