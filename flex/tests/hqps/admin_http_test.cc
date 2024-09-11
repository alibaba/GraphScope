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
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "flex/storages/metadata/graph_meta_store.h"
#include "flex/third_party/httplib.h"
#include "flex/utils/yaml_utils.h"
#include "rapidjson/document.h"
#include "yaml-cpp/yaml.h"

#include "glog/logging.h"

static constexpr const char* CREATE_PROCEDURE_PAYLOAD_TEMPLATE =
    "{\"bound_graph\": \"%1%\","
    "\"description\": \"test procedure\","
    "\"enable\": %2%,"
    "\"name\": \"%3%\","
    "\"query\": \"%4%\","
    "\"type\": \"cypher\"}";

std::string generate_start_service_payload(const std::string& graph_id) {
  boost::format formater("{\"graph_id\": \"%1%\"}");
  return (formater % graph_id).str();
}

std::string get_file_name_from_path(const std::string& file_path) {
  auto file_name = file_path.substr(file_path.find_last_of('/') + 1);
  // remove extension
  file_name = file_name.substr(0, file_name.find_last_of('.'));
  // prepend query_ before filename, to avoid name start with number
  file_name = "query_" + file_name;
  return file_name;
}

std::string generate_call_procedure_payload(const std::string& procedure_id) {
  procedure::Query query;
  query.mutable_query_name()->set_name(procedure_id);
  std::string str = query.SerializeAsString();
  // append byte at the tail
  str.push_back(static_cast<uint8_t>(
      gs::GraphDBSession::InputFormat::kCypherProtoProcedure));
  LOG(INFO) << "call procedure payload: " << str.size();
  return str;
}

std::string generate_update_procedure_payload(const std::string& description,
                                              bool enabled) {
  boost::format formater("{\"enable\": %1%, \"description\": \"%2%\"}");
  return (formater % (enabled ? "true" : "false") % description).str();
}

std::string generate_create_procedure_payload(const std::string& graph_id,
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
  return (formater % graph_id % (enabled ? "true" : "false") % file_name %
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
  auto json = gs::get_json_string_from_yaml(node);
  if (json.ok()) {
    return json.value();
  } else {
    LOG(FATAL) << "get json string from yaml failed: "
               << json.status().error_message();
  }
}

void run_builtin_graph_test(
    httplib::Client& admin_client, httplib::Client& query_client,
    const std::string& graph_id,
    const std::vector<std::pair<std::string, std::string>>& queries) {
  //-------0. get graph schema--------------------------------
  auto res = admin_client.Get("/v1/graph/" + graph_id + "/schema");
  if (res->status != 200) {
    LOG(FATAL) << "get graph schema failed for builtin graph" << graph_id
               << ": " << res->body;
  }
  std::vector<gs::PluginId> plugin_ids;
  //-------1. create procedures--------------------------------
  {
    for (auto& pair : queries) {
      auto query_name = pair.first;
      auto query_str = pair.second;
      boost::format formater(CREATE_PROCEDURE_PAYLOAD_TEMPLATE);
      formater % graph_id % "true" % query_name % query_str;
      std::string create_proc_payload0 = formater.str();
      auto res = admin_client.Post("/v1/graph/" + graph_id + "/procedure",
                                   create_proc_payload0, "text/plain");
      CHECK(res->status == 200) << "create procedure failed: " << res->body
                                << ", for query: " << create_proc_payload0;
      LOG(INFO) << "Create procedure: " << create_proc_payload0
                << ",response:" << res->body;
      rapidjson::Document json;
      json.Parse(res->body.c_str());
      if (!json.HasMember("procedure_id")) {
        LOG(FATAL) << "create procedure response does not contain plugin_id: "
                   << res->body;
      }
      plugin_ids.emplace_back(json["procedure_id"].GetString());
    }
  }
  //-------2. now call procedure should fail
  {
    for (auto& proc_id : plugin_ids) {
      auto res = query_client.Post("/v1/graph/current/query",
                                   generate_call_procedure_payload(proc_id),
                                   "text/plain");
      CHECK(res->status != 200);
      LOG(INFO) << "call procedure response: " << res->body;
      // find failed in res->body
      CHECK(res->body.find("failed") != std::string::npos)
          << "call procedure should fail: " << res->body;
    }
  }
  //-------3.restart service
  {
    // graph_id is not specified, should restart on current graph.
    std::string empty_payload;
    auto res =
        admin_client.Post("/v1/service/restart", empty_payload, "text/plain");
    CHECK(res->status == 200) << "restart service failed: " << res->body;
  }
  {
    //-----3.1 get all procedures.
    auto res = admin_client.Get("/v1/graph/" + graph_id + "/procedure");
    CHECK(res->status == 200) << "get all procedures failed: " << res->body;
    LOG(INFO) << "get all procedures response: " << res->body;
  }
  //------4. now do the query
  {
    for (auto& plugin_id : plugin_ids) {
      auto res = query_client.Post("/v1/graph/current/query",
                                   generate_call_procedure_payload(plugin_id),
                                   "text/plain");
      CHECK(res->status == 200)
          << "call procedure should success: " << res->body
          << ", for query: " << plugin_id;
    }
  }
  LOG(INFO) << "Pass builtin graph test";
}

gs::GraphId run_graph_tests(httplib::Client& cli,
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

  auto json_str = gs::get_json_string_from_yaml(node);
  if (!json_str.ok()) {
    LOG(FATAL) << "get json string from yaml failed: "
               << json_str.status().error_message();
  }
  auto res = cli.Post("/v1/graph/", json_str.value(), "application/json");
  if (res->status != 200) {
    LOG(FATAL) << "create graph failed: " << res->body;
  }
  auto body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "create graph response: " << body;
  // parse graph_id from response
  rapidjson::Document j;
  j.Parse(body.c_str());
  if (!j.HasMember("graph_id")) {
    LOG(FATAL) << "create graph response does not contain graph_id: " << body;
  }
  gs::GraphId graph_id = j["graph_id"].GetString();

  ///----1. get graph schema----------------------------
  res = cli.Get("/v1/graph/" + graph_id + "/schema");
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
  res = cli.Post("/v1/graph/" + graph_id + "/dataloading",
                 insert_raw_csv_dir(raw_data_dir, import_path),
                 "application/json");
  if (res->status != 200) {
    LOG(FATAL) << "load graph failed: " << res->body;
  }
  body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "load graph response: " << body;
  return graph_id;
}

// Create the procedure and call the procedure.
void run_procedure_test(httplib::Client& client, httplib::Client& query_client,
                        const std::string& graph_id,
                        const std::vector<std::pair<std::string, std::string>>&
                            builtin_graph_queries,
                        const std::vector<std::string>& procedures) {
  // First create the procedure with disabled state, then update with enabled
  // state.
  //-----0. get all procedures, should be empty----------------------
  auto res = client.Get("/v1/graph/" + graph_id + "/procedure");
  CHECK(res->status == 200) << "get all procedures failed: " << res->body;

  std::vector<gs::PluginId> plugin_ids;
  //-----1. create procedures----------------------------------------
  for (auto& procedure : procedures) {
    auto create_proc_payload =
        generate_create_procedure_payload(graph_id, procedure, false);
    LOG(INFO) << "Creating procedure:" << create_proc_payload;
    res = client.Post("/v1/graph/" + graph_id + "/procedure",
                      create_proc_payload, "text/plain");
    CHECK(res->status == 200) << "create procedure failed: " << res->body
                              << ", for query: " << create_proc_payload;
    LOG(INFO) << "response:" << res->body;
    rapidjson::Document json;
    json.Parse(res->body.c_str());
    if (!json.HasMember("procedure_id")) {
      LOG(FATAL) << "create procedure response does not contain plugin_id: "
                 << res->body;
    }
    plugin_ids.emplace_back(json["procedure_id"].GetString());
  }
  //-----2. get all procedures--------------------------------------
  res = client.Get("/v1/graph/" + graph_id + "/procedure");
  CHECK(res->status == 200) << "get all procedures failed: " << res->body;
  LOG(INFO) << "get all procedures response: " << res->body;
  // Step4: update procedures
  for (size_t i = 0; i < plugin_ids.size(); ++i) {
    auto& proc_id = plugin_ids[i];
    auto update_proc_payload =
        generate_update_procedure_payload("a example procedure", true);
    res = client.Put("/v1/graph/" + graph_id + "/procedure/" + proc_id,
                     update_proc_payload, "text/plain");
    CHECK(res->status == 200) << "update procedure failed: " << res->body
                              << ", for query: " << update_proc_payload;
  }

  //-----3. start service on new graph-----------------------------------
  auto start_service_payload = generate_start_service_payload(graph_id);
  res = client.Post("/v1/service/start", start_service_payload, "text/plain");
  CHECK(res->status == 200) << "start service failed: " << res->body
                            << ", for query: " << start_service_payload;
  {
    //----3.1 call proc on previous procedures on previous graph, should fail.
    auto res = client.Get("/v1/graph/" + graph_id + "/procedure");
    LOG(INFO) << "Current graph has plugins: " << res->body;
    for (auto& pair : builtin_graph_queries) {
      auto query_name = pair.first;
      auto query_str = pair.second;
      auto res = query_client.Post("/v1/graph/current/query",
                                   generate_call_procedure_payload(query_name),
                                   "text/plain");
      CHECK(res->status != 200)
          << "call previous procedure on current graph should fail: "
          << res->body << ", query name; " << query_name;
    }
  }

  //----4. call procedures-----------------------------------------------
  for (auto& proc_id : plugin_ids) {
    auto call_proc_payload = generate_call_procedure_payload(proc_id);
    res = query_client.Post("/v1/graph/current/query", call_proc_payload,
                            "text/plain");
    CHECK(res->status == 200) << "call procedure failed: " << res->body
                              << ", for query: " << call_proc_payload;
  }
  //----5. delete procedure by name-----------------------------------------
  if (procedures.size() > 0) {
    auto proc_id = plugin_ids[0];
    res = client.Delete("/v1/graph/" + graph_id + "/procedure/" + proc_id);
    CHECK(res->status == 200) << "delete procedure failed: " << res->body;
  }
  //-----6. call procedure on deleted procedure------------------------------
  // Should return success, since the procedure will be deleted when restart
  // the service.
  if (procedures.size() > 0) {
    auto call_proc_payload = generate_call_procedure_payload(plugin_ids[0]);
    res = query_client.Post("/v1/graph/current/query", call_proc_payload,
                            "text/plain");
    CHECK(res->status == 200) << "call procedure failed: " << res->body
                              << ", for query: " << call_proc_payload;
  }
  //-----7. get procedure by name--------------------------------------------
  // get the second procedure by name
  if (procedures.size() > 1) {
    res = client.Get("/v1/graph/" + graph_id + "/procedure/" + plugin_ids[1]);
    CHECK(res->status == 200) << "get procedure failed: " << res->body;
  }
}

void run_get_node_status(httplib::Client& cli, const std::string& graph_id) {
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
  // Get current running graph's status
  {
    auto res = cli.Get("/v1/graph/" + graph_id + "/statistics");
    if (res->status != 200) {
      LOG(FATAL) << "get current graph status failed: " << res->body;
    }
    auto body = res->body;
    if (body.empty()) {
      LOG(FATAL) << "Empty response: ";
    }
    // check whether has total_edge_count, total_vertex_count, and the value
    // should be greater than 0
    rapidjson::Document j;
    j.Parse(body.c_str());
    if (!j.HasMember("total_edge_count") ||
        !j.HasMember("total_vertex_count")) {
      LOG(FATAL) << "get current graph status response does not contain "
                    "total_edge_count or total_vertex_count: "
                 << body;
    }
    if (j["total_edge_count"].GetInt() <= 0 ||
        j["total_vertex_count"].GetInt() <= 0) {
      LOG(FATAL) << "get current graph status response total_edge_count or "
                    "total_vertex_count should be greater than 0: "
                 << body;
    }
  }
}

void test_delete_graph(httplib::Client& cli, const std::string& graph_id) {
  auto res = cli.Delete("/v1/graph/" + graph_id);
  if (res->status != 200) {
    LOG(FATAL) << "delete graph failed: " << res->body;
  }
  auto body = res->body;
  if (body.empty()) {
    LOG(FATAL) << "Empty response: ";
  }
  LOG(INFO) << "delete graph response: " << body;
}

void remove_graph_if_exists(httplib::Client& cli, const std::string& graph_id) {
  auto res = cli.Get("/v1/graph/" + graph_id + "/schema");
  if (res->status == 200) {
    LOG(INFO) << "graph " << graph_id << " exists, delete it";
    test_delete_graph(cli, graph_id);
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

  std::string builtin_graph_id = "1";

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

  // remove_graph_if_exists(cli, graph_id);
  std::vector<std::pair<std::string, std::string>> builtin_graph_queries = {
      {"query0", "MATCH(a) return COUNT(a);"}};
  run_builtin_graph_test(cli, cli_query, builtin_graph_id,
                         builtin_graph_queries);
  auto graph_id = run_graph_tests(cli, schema_path, import_path, raw_data_dir);
  LOG(INFO) << "run graph tests done";
  run_procedure_test(cli, cli_query, graph_id, builtin_graph_queries,
                     procedure_paths);
  LOG(INFO) << "run procedure tests done";
  run_get_node_status(cli, graph_id);
  LOG(INFO) << "test delete graph done";
  return 0;
}