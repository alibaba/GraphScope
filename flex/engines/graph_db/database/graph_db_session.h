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

#ifndef GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_
#define GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/compact_transaction.h"
#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/single_edge_insert_transaction.h"
#include "flex/engines/graph_db/database/single_vertex_insert_transaction.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/update_transaction.h"

#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/column.h"
#include "flex/utils/result.h"

#ifdef BUILD_HQPS
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "nlohmann/json.hpp"
#endif  // BUILD_HQPS

namespace gs {

class GraphDB;
class WalWriter;

class GraphDBSession {
 public:
  enum class InputFormat : uint8_t {
    kCppEncoder = 0,
#ifdef BUILD_HQPS
    kCypherJson = 1,               // External usage format
    kCypherInternalAdhoc = 2,      // Internal format for adhoc query
    kCypherInternalProcedure = 3,  // Internal format for procedure
#endif                             // BUILD_HQPS
  };

  static constexpr int32_t MAX_RETRY = 3;
  static constexpr int32_t MAX_PLUGIN_NUM = 256;  // 2^(sizeof(uint8_t)*8)
#ifdef BUILD_HQPS
  static constexpr const char* kCypherJson = "\x01";
  static constexpr const char* kCypherInternalAdhoc = "\x02";
  static constexpr const char* kCypherInternalProcedure = "\x03";
#endif  // BUILD_HQPS
  GraphDBSession(GraphDB& db, Allocator& alloc, WalWriter& logger,
                 const std::string& work_dir, int thread_id)
      : db_(db),
        alloc_(alloc),
        logger_(logger),
        work_dir_(work_dir),
        thread_id_(thread_id),
        eval_duration_(0),
        query_num_(0) {
    for (auto& app : apps_) {
      app = nullptr;
    }
  }
  ~GraphDBSession() {}

  ReadTransaction GetReadTransaction();

  InsertTransaction GetInsertTransaction();

  SingleVertexInsertTransaction GetSingleVertexInsertTransaction();

  SingleEdgeInsertTransaction GetSingleEdgeInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  CompactTransaction GetCompactTransaction();

  bool BatchUpdate(UpdateBatch& batch);

  const MutablePropertyFragment& graph() const;
  MutablePropertyFragment& graph();
  const GraphDB& db() const;

  const Schema& schema() const;

  std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const;

  // Get vertex id column.
  std::shared_ptr<RefColumnBase> get_vertex_id_column(uint8_t label) const;

  Result<std::vector<char>> Eval(const std::string& input);

  void GetAppInfo(Encoder& result);

  int SessionId() const;

  bool Compact();

  double eval_duration() const;

  const AppMetric& GetAppMetric(int idx) const;

  int64_t query_num() const;

  AppBase* GetApp(int idx);

 private:
  /**
   * @brief Parse the input format of the query.
   *        There are four formats:
   *       0. CppEncoder: This format will be used by interactive-sdk to submit
   * c++ stored prcoedure queries. The second last byte is the query id.
   *       1. CypherJson: This format will be sended by interactive-sdk, the
   *        input is a json string + '\x01'
   *         {
   *            "query_name": "example",
   *            "arguments": {
   *               "value": 1,
   *               "type": {
   *                "primitive_type": "DT_SIGNED_INT32"
   *                }
   *            }
   *          }
   *       2. CypherInternalAdhoc: This format will be used by compiler to
   *        submit adhoc query, the input is a string + '\x02', the string is
   *        the path to the dynamic library.
   *       3. CypherInternalProcedure: This format will be used by compiler to
   *        submit procedure query, the input is a proto-encoded string +
   *        '\x03', the string is the path to the dynamic library.
   * @param input The input query.
   * @param str_len The length of the valid payload(other than the format and
   * type bytes)
   * @return The id of the query.
   */
  inline Result<uint8_t> parse_query_type(const std::string& input,
                                          size_t& str_len) {
	  VLOG(10) << "parse query type for " << input;
    char input_tag = input.back();
    VLOG(10) << "input tag: " << static_cast<int>(input_tag);
    size_t len = input.size();
    if (input_tag == static_cast<uint8_t>(InputFormat::kCppEncoder)) {
      // For cpp encoder, the query id is the second last byte, others are all
      // user-defined payload,
      str_len = len - 2;
      return input[len - 2];
    }
#ifdef BUILD_HQPS
    else if (input_tag ==
             static_cast<uint8_t>(InputFormat::kCypherInternalAdhoc)) {
      // For cypher internal adhoc, the query id is the
      // second last byte,which is fixed to 255, and other bytes are a string
      // representing the path to generated dynamic lib.
      str_len = len - 2;
      return input[len - 2];
    } else if (input_tag == static_cast<uint8_t>(InputFormat::kCypherJson)) {
      // For cypherJson there is no query-id provided. The query name is
      // provided in the json string.
      str_len = len - 2;
      std::string_view str_view(input.data(), len - 2);
      VLOG(10) << "string view: " << str_view;
      nlohmann::json j;
      try {
        j = nlohmann::json::parse(str_view);
      } catch (const nlohmann::json::parse_error& e) {
        LOG(ERROR) << "Fail to parse json from input content: " << e.what();
        return Result<uint8_t>(
            StatusCode::InternalError,
            "Fail to parse json from input content:" + std::string(e.what()), 0);
      }
      auto query_name = j["query_name"].get<std::string>();
      const auto& app_name_to_path_index = schema().GetPlugins();
      if (app_name_to_path_index.count(query_name) <= 0) {
        LOG(ERROR) << "Query name is not registered: " << query_name;
        return Result<uint8_t>(StatusCode::NotFound,
                               "Query name is not registered: " + query_name,
                               0);
      }
      if (!j.contains("arguments")){
         LOG(ERROR) << "expect arguments";
	 for (auto& arg : j["arguments"]){
	    VLOG(10) << "arg: " << arg;
	 }
	 return false;
      }
      VLOG(10) << "Query name: " << query_name;
      return app_name_to_path_index.at(query_name).second;
    } else if (input_tag ==
               static_cast<uint8_t>(InputFormat::kCypherInternalProcedure)) {
      // For cypher internal procedure, the query_name is
      // provided in the protobuf message.
      str_len = len - 1;
      procedure::Query cur_query;
      if (!cur_query.ParseFromArray(input.data(), input.size() - 1)) {
        LOG(ERROR) << "Fail to parse query from input content";
        return Result<uint8_t>(StatusCode::InternalError,
                               "Fail to parse query from input content", 0);
      }
      auto query_name = cur_query.query_name().name();
      if (query_name.empty()) {
        LOG(ERROR) << "Query name is empty";
        return Result<uint8_t>(StatusCode::NotFound, "Query name is empty", 0);
      }
      const auto& app_name_to_path_index = schema().GetPlugins();
      if (app_name_to_path_index.count(query_name) <= 0) {
        LOG(ERROR) << "Query name is not registered: " << query_name;
        return Result<uint8_t>(StatusCode::NotFound,
                               "Query name is not registered: " + query_name,
                               0);
      }
      return app_name_to_path_index.at(query_name).second;
    }
#endif  // BUILD_HQPS
    else {
      return Result<uint8_t>(StatusCode::InValidArgument,
                             "Invalid input tag: " + std::to_string(input_tag),
                             0);
    }
  }
  GraphDB& db_;
  Allocator& alloc_;
  WalWriter& logger_;
  std::string work_dir_;
  int thread_id_;

  std::array<AppWrapper, MAX_PLUGIN_NUM> app_wrappers_;
  std::array<AppBase*, MAX_PLUGIN_NUM> apps_;
  std::array<AppMetric, MAX_PLUGIN_NUM> app_metrics_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_
