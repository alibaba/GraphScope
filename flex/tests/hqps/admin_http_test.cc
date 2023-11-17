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

#include <boost/format.hpp>
#include <string>
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "flex/third_party/httplib.h"
#include "yaml-cpp/yaml.h"

#include "glog/logging.h"

static constexpr const char* CREATE_PROCEDURE_PAYLOAD_TEMPLATE =
    "{\"bound_graph\": \"%1%\","
    "\"description\": \"test procedure\","
    "\"enable\": %2%,"
    "\"name\": \"%3%\","
    "\"query\": \"%4%\","
    "\"type\": \"cypher\"}";

std::string generate_start_service_payload(const std::string& graph_name) {
  boost::format formater("{\"graph_name\": \"%1%\"}");
  return (formater % graph_name).str();
}

std::string get_file_name_from_path(const std::string& file_path) {
  auto file_name = file_path.substr(file_path.find_last_of('/') + 1);
  // remove extension
  file_name = file_name.substr(0, file_name.find_last_of('.'));
  return file_name;
}

std::string generate_call_procedure_payload(const std::string& graph_name,
                                            const std::string& procedure_name) {
  query::Query query;
  query.mutable_query_name()->set_name(procedure_name);
  return query.SerializeAsString();
}

std::string generate_update_procedure_payload(const std::string& procedure_name,
                                              bool enabled) {
  boost::format formater("{\"enable\": %1%, \"name\": \"%2%\"}");
  return (formater % (enabled ? "true" : "false") % procedure_name).str();
}

std::string generate_create_procedure_payload(const std::string& graph_name,
                                              const std::string& procedure_path,
                                              bool enabled) {
  boost::format formater(CREATE_PROCEDURE_PAYLOAD_TEMPLATE);
  // read from procedure_path and result into a one-line string
  std::ifstream ifs(procedure_path);
  std::string query((std::istreambuf_iterator<char>(ifs)),
                    (std::istreambuf_iterator<char>()));
  // replace all \n with space
  std::replace(query.begin(), query.end(), '\n', ' ');
  // get file name
  auto file_name = get_file_name_from_path(procedure_path);
  return (formater % graph_name % (enabled ? "true" : "false") % file_name %
          query)
      .str();
}

std::string insert_raw_csv_dir(const std::string& raw_csv_dir,
                               const std::string& import_file_path) {
  // Load the file as yaml, and insert
  // ["loading_config"]["data_source"]["location"]
  YAML::Node node;
  try {
    node = YAML::LoadFile(import_file_path);
  } catch (YAML::BadFile& e) {
    LOG(FATAL) << "load import file failed: " << e.what();
  }
  node["loading_config"]["data_source"]["location"] = raw_csv_dir;
  YAML::Emitter emitter;
  emitter << YAML::DoubleQuoted << YAML::Flow << YAML::BeginSeq << node;
  std::string json(emitter.c_str() + 1);
  return json;
}

void run_builtin_graph_test(
    httplib::Client& admin_client, httplib::Client& query_client,
    const std::string& graph_name,
    const std::vector<std::pair<std::string, std::string>>& queries) {
  //-------0. get graph schema--------------------------------
  auto res = admin_client.Get("/v1/graph/" + graph_name + "/schema");
  if (res->status != 200) {
    LOG(FATAL) << "get graph schema failed for builtin graph" << graph_name
               << ": " << res->body;
  }
  //-------1. create procedures--------------------------------
  {
    for (auto& pair : queries) {
      auto query_name = pair.first;
      auto query_str = pair.second;
      boost::format formater(CREATE_PROCEDURE_PAYLOAD_TEMPLATE);
      formater % graph_name % "true" % query_name % query_str;
      std::string create_proc_payload0 = formater.str();
      auto res = admin_client.Post("/v1/graph/" + graph_name + "/procedure",
                                   create_proc_payload0, "text/plain");
      CHECK(res->status == 200) << "create procedure failed: " << res->body
                                << ", for query: " << create_proc_payload0;
      LOG(INFO) << "Create procedure: " << create_proc_payload0
                << ",response:" << res->body;
    }
  }
  //-------2. now call procedure should fail
  {
    for (auto& pair : queries) {
      auto query_name = pair.first;
      auto query_str = pair.second;
      query::Query query;
      query.mutable_query_name()->set_name(query_name);
      auto res = query_client.Post("/v1/query", query.SerializeAsString(),
                                   "text/plain");
      CHECK(res->status != 200) << "call procedure should fail: " << res->body;
    }
  }
  //-------3.restart service
  {
    // graph_name is not specified, should restart on current graph.
    std::string empty_payload;
    auto res =
        admin_client.Post("/v1/service/restart", empty_payload, "text/plain");
    CHECK(res->status == 200) << "restart service failed: " << res->body;
  }
  {
    //-----3.1 get all procedures.
    auto res = admin_client.Get("/v1/graph/" + graph_name + "/procedure");
    CHECK(res->status == 200) << "get all procedures failed: " << res->body;
    LOG(INFO) << "get all procedures response: " << res->body;
  }
  //------4. now do the query
  {
    for (auto& pair : queries) {
      auto query_name = pair.first;
      auto query_str = pair.second;
      query::Query query;
      query.mutable_query_name()->set_name(query_name);
      auto res = query_client.Post("/v1/query", query.SerializeAsString(),
                                   "text/plain");
      CHECK(res->status == 200)
          << "call procedure should success: " << res->body
          << ", for query: " << query.DebugString();
    }
  }
  LOG(INFO) << "Pass builtin graph test";
}

void run_graph_tests(httplib::Client& cli, const std::string& graph_name,
                     const std::string& schema_path,
                     const std::string& import_path,
                     const std::string& raw_data_dir) {
  //-------0. create graph--------------------------------
  // load schema_path to yaml and output yaml as json
  YAML::Node node;
  try {
    node = YAML::LoadFile(schema_path);
  } catch (YAML::BadFile& e) {
    LOG(FATAL) << "load schema file failed: " << e.what();
  }

  YAML::Emitter emitter;
  emitter << YAML::DoubleQuoted << YAML::Flow << YAML::BeginSeq << node;
  std::string json(emitter.c_str() + 1);
  auto res = cli.Post("/v1/graph/", json, "application/json");
  if (res->status != 200) {
    LOG(FATAL) << "create graph failed: " << res->body;
  }
  auto body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "create graph response: " << body;

  ///----1. get graph schema----------------------------
  res = cli.Get("/v1/graph/" + graph_name + "/schema");
  if (res->status != 200) {
    LOG(FATAL) << "get graph schema failed: " << res->body;
  }
  body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "get graph schema response: " << body;

  //----2. list graph-----------------------------------
  res = cli.Get("/v1/graph/");
  if (res->status != 200) {
    LOG(FATAL) << "list graph failed: " << res->body;
  }
  body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "list graph response: " << body;

  //----3. load graph-----------------------------------
  res = cli.Post("/v1/graph/" + graph_name + "/dataloading",
                 insert_raw_csv_dir(raw_data_dir, import_path), "text/plain");
  if (res->status != 200) {
    LOG(FATAL) << "load graph failed: " << res->body;
  }
  body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "load graph response: " << body;
}

// Create the procedure and call the procedure.
void run_procedure_test(httplib::Client& client, httplib::Client& query_client,
                        const std::string& graph_name,
                        const std::vector<std::pair<std::string, std::string>>&
                            builtin_graph_queries,
                        const std::vector<std::string>& procedures) {
  // First create the procedure with disabled state, then update with enabled
  // state.
  //-----0. get all procedures, should be empty----------------------
  auto res = client.Get("/v1/graph/" + graph_name + "/procedure");
  CHECK(res->status == 200) << "get all procedures failed: " << res->body;

  //-----1. create procedures----------------------------------------
  for (auto& procedure : procedures) {
    auto create_proc_payload =
        generate_create_procedure_payload(graph_name, procedure, false);
    LOG(INFO) << "Creating procedure:" << create_proc_payload;
    res = client.Post("/v1/graph/" + graph_name + "/procedure",
                      create_proc_payload, "text/plain");
    CHECK(res->status == 200) << "create procedure failed: " << res->body
                              << ", for query: " << create_proc_payload;
    LOG(INFO) << "response:" << res->body;
  }
  //-----2. get all procedures--------------------------------------
  res = client.Get("/v1/graph/" + graph_name + "/procedure");
  CHECK(res->status == 200) << "get all procedures failed: " << res->body;
  LOG(INFO) << "get all procedures response: " << res->body;
  // Step4: update procedures
  for (auto& procedure : procedures) {
    auto proc_name = get_file_name_from_path(procedure);
    auto update_proc_payload =
        generate_update_procedure_payload(proc_name, true);
    res = client.Put("/v1/graph/" + graph_name + "/procedure/" + proc_name,
                     update_proc_payload, "text/plain");
    CHECK(res->status == 200) << "update procedure failed: " << res->body
                              << ", for query: " << update_proc_payload;
  }

  //-----3. start service on new graph-----------------------------------
  auto start_service_payload = generate_start_service_payload(graph_name);
  res = client.Post("/v1/service/start", start_service_payload, "text/plain");
  CHECK(res->status == 200) << "start service failed: " << res->body
                            << ", for query: " << start_service_payload;
  {
    //----3.1 call proc on previous procedures on previous graph, should fail.
    for (auto& pair : builtin_graph_queries) {
      auto query_name = pair.first;
      auto query_str = pair.second;
      query::Query query;
      query.mutable_query_name()->set_name(query_name);
      auto res = query_client.Post("/v1/query", query.SerializeAsString(),
                                   "text/plain");
      CHECK(res->status != 200)
          << "call previous procedure on current graph should fail: "
          << res->body;
    }
  }

  //----4. call procedures-----------------------------------------------
  for (auto& procedure : procedures) {
    auto proc_name = get_file_name_from_path(procedure);
    auto call_proc_payload =
        generate_call_procedure_payload(graph_name, proc_name);
    res = query_client.Post("/v1/query", call_proc_payload, "text/plain");
    CHECK(res->status == 200) << "call procedure failed: " << res->body
                              << ", for query: " << call_proc_payload;
  }
  //----5. delete procedure by name-----------------------------------------
  if (procedures.size() > 0) {
    auto proc_name = get_file_name_from_path(procedures[0]);
    res = client.Delete("/v1/graph/" + graph_name + "/procedure/" + proc_name);
    CHECK(res->status == 200) << "delete procedure failed: " << res->body;
  }
  //-----6. call procedure on deleted procedure------------------------------
  // Should return success, since the procedure will be deleted when restart
  // the service.
  if (procedures.size() > 0) {
    auto proc_name = get_file_name_from_path(procedures[0]);
    auto call_proc_payload =
        generate_call_procedure_payload(graph_name, proc_name);
    res = query_client.Post("/v1/query", call_proc_payload, "text/plain");
    CHECK(res->status == 200) << "call procedure failed: " << res->body
                              << ", for query: " << call_proc_payload;
  }
  //-----7. get procedure by name--------------------------------------------
  // get the second procedure by name
  if (procedures.size() > 1) {
    auto proc_name = get_file_name_from_path(procedures[1]);
    res = client.Get("/v1/graph/" + graph_name + "/procedure/" + proc_name);
    CHECK(res->status == 200) << "get procedure failed: " << res->body;
  }
}

void run_get_node_status(httplib::Client& cli) {
  auto res = cli.Get("/v1/node/status");
  if (res->status != 200) {
    LOG(FATAL) << "get node status failed: " << res->body;
  }
  auto body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "get node status response: " << body;
  // get service status
  res = cli.Get("/v1/service/status");
  if (res->status != 200) {
    LOG(FATAL) << "get service status failed: " << res->body;
  }
  body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "get service status response: " << body;
}

void test_delete_graph(httplib::Client& cli, const std::string& graph_name) {
  auto res = cli.Delete("/v1/graph/" + graph_name);
  if (res->status != 200) {
    LOG(FATAL) << "delete graph failed: " << res->body;
  }
  auto body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "delete graph response: " << body;
}

void remove_graph_if_exists(httplib::Client& cli,
                            const std::string& graph_name) {
  auto res = cli.Get("/v1/graph/" + graph_name + "/schema");
  if (res->status == 200) {
    LOG(INFO) << "graph " << graph_name << " exists, delete it";
    test_delete_graph(cli, graph_name);
  }
}

int main(int argc, char** argv) {
  if (argc < 6) {
    std::cerr << "usage: admin_http_test <admin_port> <query_port> "
                 "<graph_schema_file> "
                 "<graph_import_path> <raw_data_dir> [procedure_path1 "
                 "procedure_path2]"
              << std::endl;
    return -1;
  }
  std::string url;
  const char* url_env = std::getenv("GRAPHSCOPE_IP");
  if (url_env == NULL) {
    url = "127.0.0.1";
  } else {
    url = url_env;
  }
  int admin_port = atoi(argv[1]);
  int query_port = atoi(argv[2]);
  auto schema_path = argv[3];
  auto import_path = argv[4];
  auto raw_data_dir = argv[5];
  std::vector<std::string> procedure_paths;

  std::string builtin_graph_name = "modern_graph";

  std::string graph_name;
  {
    // load yaml from schema_path
    YAML::Node node;
    try {
      node = YAML::LoadFile(schema_path);
    } catch (YAML::BadFile& e) {
      LOG(ERROR) << "load schema file failed: " << e.what();
      return -1;
    }
    graph_name = node["name"].as<std::string>();
  }
  LOG(INFO) << "graph name: " << graph_name;

  for (auto i = 6; i < argc; ++i) {
    procedure_paths.emplace_back(argv[i]);
  }
  httplib::Client cli(url, admin_port);
  cli.set_connection_timeout(0, 300000);
  cli.set_read_timeout(60, 0);
  cli.set_write_timeout(60, 0);
  httplib::Client cli_query(url, query_port);
  cli_query.set_connection_timeout(0, 300000);
  cli_query.set_read_timeout(60, 0);
  cli_query.set_write_timeout(60, 0);

  remove_graph_if_exists(cli, graph_name);
  std::vector<std::pair<std::string, std::string>> builtin_graph_queries = {
      {"query0", "MATCH(a) return COUNT(a);"}};
  run_builtin_graph_test(cli, cli_query, builtin_graph_name,
                         builtin_graph_queries);
  run_graph_tests(cli, graph_name, schema_path, import_path, raw_data_dir);
  LOG(INFO) << "run graph tests done";
  run_procedure_test(cli, cli_query, graph_name, builtin_graph_queries,
                     procedure_paths);
  LOG(INFO) << "run procedure tests done";
  run_get_node_status(cli);
  test_delete_graph(cli, graph_name);
  LOG(INFO) << "test delete graph done";
  return 0;
}