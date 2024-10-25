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

namespace gs {

class GraphDB;
class WalWriter;

class GraphDBSession {
 public:
  enum class InputFormat : uint8_t {
    kCppEncoder = 0,
    kCypherJson = 1,            // Json format for cypher query
    kCypherProtoAdhoc = 2,      // Protobuf format for adhoc query
    kCypherProtoProcedure = 3,  // Protobuf format for procedure query
  };

  static constexpr int32_t MAX_RETRY = 3;
  static constexpr int32_t MAX_PLUGIN_NUM = 256;  // 2^(sizeof(uint8_t)*8)
  static constexpr const char* kCppEncoderStr = "\x00";
  static constexpr const char* kCypherJsonStr = "\x01";
  static constexpr const char* kCypherProtoAdhocStr = "\x02";
  static constexpr const char* kCypherProtoProcedureStr = "\x03";
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

  ReadTransaction GetReadTransaction() const;

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

  AppBase* GetApp(const std::string& name);

 private:
  Result<std::pair<uint8_t, std::string_view>>
  parse_query_type_from_cypher_json(const std::string_view& input);
  Result<std::pair<uint8_t, std::string_view>>
  parse_query_type_from_cypher_internal(const std::string_view& input);
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
   * @return The id of the query and a string_view which contains the real input
   * of the query, discard the input format and query type.
   */
  inline Result<std::pair<uint8_t, std::string_view>> parse_query_type(
      const std::string& input) {
    const char* str_data = input.data();
    VLOG(10) << "parse query type for " << input << " size: " << input.size();
    char input_tag = input.back();
    VLOG(10) << "input tag: " << static_cast<int>(input_tag);
    size_t len = input.size();
    if (input_tag == static_cast<uint8_t>(InputFormat::kCppEncoder)) {
      // For cpp encoder, the query id is the second last byte, others are all
      // user-defined payload,
      return std::make_pair((uint8_t) input[len - 2],
                            std::string_view(str_data, len - 2));
    } else if (input_tag ==
               static_cast<uint8_t>(InputFormat::kCypherProtoAdhoc)) {
      // For cypher internal adhoc, the query id is the
      // second last byte,which is fixed to 255, and other bytes are a string
      // representing the path to generated dynamic lib.
      return std::make_pair((uint8_t) input[len - 2],
                            std::string_view(str_data, len - 2));
    } else if (input_tag == static_cast<uint8_t>(InputFormat::kCypherJson)) {
      // For cypherJson there is no query-id provided. The query name is
      // provided in the json string.
      // We don't discard the last byte, since we need it to determine the input
      // format when deserializing the input arguments in deserialize() function
      std::string_view str_view(input.data(), len);
      return parse_query_type_from_cypher_json(str_view);
    } else if (input_tag ==
               static_cast<uint8_t>(InputFormat::kCypherProtoProcedure)) {
      // For cypher internal procedure, the query_name is
      // provided in the protobuf message.
      // Same as cypherJson, we don't discard the last byte.
      std::string_view str_view(input.data(), len);
      return parse_query_type_from_cypher_internal(str_view);
    } else {
      return Result<std::pair<uint8_t, std::string_view>>(
          gs::Status(StatusCode::INVALID_ARGUMENT,
                     "Invalid input tag: " + std::to_string(input_tag)));
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
