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

#ifndef ENGINES_HTTP_SERVER_ACTOR_ADMIN_ACT_H_
#define ENGINES_HTTP_SERVER_ACTOR_ADMIN_ACT_H_

#include "flex/engines/http_server/types.h"
#include "flex/engines/graph_db/database/graph_db.h"

#include "flex/storages/metadata/graph_meta_store.h"

#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

#include <memory>
#include <mutex>

namespace server {

class ANNOTATION(actor:impl) admin_actor : public hiactor::actor {
 public:
  admin_actor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr);
  ~admin_actor() override;

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_create_graph(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_get_graph_schema(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_get_graph_meta(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_list_graphs(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_delete_graph(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_graph_loading(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) start_service(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) stop_service(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) service_status(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_procedure_by_procedure_name(procedure_query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_procedures_by_graph_name(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) create_procedure(create_procedure_query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) delete_procedure(procedure_query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) update_procedure(update_procedure_query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) node_status(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_job(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) list_jobs(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) cancel_job(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) run_get_graph_statistic(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) upload_file(query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) create_vertex(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) create_edge(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) delete_vertex(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) delete_edge(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) update_vertex(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) update_edge(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_vertex(graph_management_query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_edge(graph_management_query_param&& param);
  // DECLARE_RUN_QUERIES;
  /// Declare `do_work` func here, no need to implement.
  ACTOR_DO_WORK()

 private:
  std::mutex mtx_;
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
};

// base class for vertex and edge manager
class VertexEdgeManagerBase {
public:
  VertexEdgeManagerBase(std::string &&graph_id);
  ~VertexEdgeManagerBase();
  void parseJson(std::string &&json_str);
  struct DBSession {
    gs::GraphDBSession &db;
    DBSession() : db(gs::GraphDB::get().GetSession(hiactor::local_shard_id())) {}
  };
  bool ok() const;
  void setError(gs::StatusCode code, const std::string &message);
  seastar::future<admin_query_result> errorResponse() const;
  bool checkGraphId(std::shared_ptr<gs::IGraphMetaStore> metadata_store_);
  bool getMetaSchema(std::shared_ptr<gs::IGraphMetaStore> metadata_store_);
  static std::string jsonToString(const nlohmann::json& json) {
    if (json.is_string()) {
      return json.get<std::string>();
    } else {
      return json.dump();
    }
  }
protected:
  std::string graph_id;
  gs::StatusCode error_code;
  std::string error_message;
  nlohmann::json schema_json;
  DBSession *db_session;
  nlohmann::json input_json;
};

// VertexEdgeManagerBase->VertexEdgeManagerInsert
class VertexEdgeManagerInsert : public VertexEdgeManagerBase {
public:
  VertexEdgeManagerInsert(graph_management_param && param);
  ~VertexEdgeManagerInsert() = default;
  bool checkEdgeLabelExists();
  void logEdgeInfo();
protected:
  int edge_num;
  // edge-related data
  std::vector<std::string> properties_array_e = {
              "src_label", "dst_label", "edge_label", 
              "src_primary_key_value", "dst_primary_key_value", "properties"};
  std::vector<std::unordered_map<std::string, std::string>> input_props_e;
  // compute value
  std::vector<gs::Any> src_pk_value_any, dst_pk_value_any,
      property_new_value_any;
  // db related
  std::vector<gs::label_t> src_label_id, dst_label_id, edge_label_id;
};

// VertexEdgeManagerBase->VertexEdgeManagerInsert->CreateVertexManager
class CreateVertexManager : public VertexEdgeManagerInsert {
 public:
  CreateVertexManager(graph_management_param && param);
  ~CreateVertexManager() = default;
  bool checkContainsVertexArray();
  bool inputVertex();
  bool inputEdge();
  void logVertexInfo();
  bool checkVertexLabelExists();
  bool dbCheck();
  void singleInsert();
  void multiInsert();
  bool insert();
protected:
  int vertex_num;
  // vertex-related data
  std::vector<std::unordered_map<std::string, std::string> > input_props_v;
  std::vector<std::string> properties_array_v = {"label", "primary_key_value", "properties"};
  // compute value
  std::vector<std::unordered_map<std::string, gs::Any>> new_properties_map;
  std::vector<std::string> primary_key_name;
  std::vector<std::vector<std::string>> colNames;
};

// VertexEdgeManagerBase->VertexEdgeManagerInsert->CreateEdgeManager
class CreateEdgeManager : public VertexEdgeManagerInsert {
public:
  CreateEdgeManager(graph_management_param && param);
  ~CreateEdgeManager() = default;
  bool checkContainsEdgeArray();
  bool inputEdge();
  bool dbCheck();
  void singleInsert();
  void multiInsert();
  bool insert();
};

// VertexEdgeManagerBase->VertexManager
class VertexManager : public VertexEdgeManagerBase {
public:
  VertexManager(std::string &&graph_id);
  ~VertexManager() = default;
};

// VertexEdgeManagerBase->VertexManager->GetVertexManager
class GetVertexManager : public VertexManager {
public:
  GetVertexManager(graph_management_query_param && param);
  ~GetVertexManager() = default;
  bool checkParams();
  void logVertexInfo();
  bool checkVertexLabelExists();
  std::string query();
protected:
  const std::unordered_map<seastar::sstring, seastar::sstring> &&query_params;
  // input values
  std::unordered_map<std::string, std::string> input_props;
  std::vector<std::string> properties_array = {"label", "primary_key_value"};
  // compute values
  std::string primary_keys_name;
  gs::Any pk_value_any;
  std::vector<seastar::sstring> column_names;
};

// VertexEdgeManagerBase->VertexManager->UpdateVertexManager
class UpdateVertexManager : public VertexManager {
public:
  UpdateVertexManager(graph_management_param && param);
  ~UpdateVertexManager() = default;
  bool checkContainsVertex();
  void logVertexInfo();
  bool checkVertexLabelExists();
  bool update();
protected:
  std::unordered_map<std::string, std::string> input_props;
  std::vector<std::string> properties_array = {"label", "primary_key_value",
                                               "properties"};
  // compute value
  std::vector<std::string> colNames;
  std::string primary_key_name;
  std::unordered_map<std::string, gs::Any> new_properties_map;

};

// VertexEdgeManagerBase->EdgeManager
class EdgeManager : public VertexEdgeManagerBase {
public:
  EdgeManager(std::string &&graph_id);
  ~EdgeManager() = default;
  virtual void logEdgeInfo() = 0;
  bool checkEdgeLabelExists();
protected:
  std::unordered_map<std::string, std::string> input_props;
  gs::Any property_new_value_any, src_pk_value_any, dst_pk_value_any;
  std::string pk_name;
};

// VertexEdgeManagerBase->EdgeManager->GetEdgeManager
class GetEdgeManager : public EdgeManager {
public:
  GetEdgeManager(graph_management_query_param && param);
  ~GetEdgeManager() = default;
  bool checkParams();
  void logEdgeInfo();
  std::string query();
protected:
  const std::unordered_map<seastar::sstring, seastar::sstring> &&query_params;
  std::vector<std::string> properties_array = {
      "src_label", "dst_label", "edge_label", "src_primary_key_value",
      "dst_primary_key_value"};
};

// VertexEdgeManagerBase->EdgeManager->UpdateEdgeManager
class UpdateEdgeManager : public EdgeManager {
public:
  UpdateEdgeManager(graph_management_param && param);
  ~UpdateEdgeManager() = default;
  bool checkContainsVertex();
  void logEdgeInfo();
  bool update();
protected:
  std::vector<std::string> properties_array = {
      "src_label", "dst_label", "edge_label", "src_primary_key_value",
      "dst_primary_key_value", "properties"};
};

// VertexEdgeManagerBase->VertexEdgeManagerOther
class VertexEdgeManagerOther : public VertexEdgeManagerBase {
public:
  VertexEdgeManagerOther(graph_management_param && param);
  ~VertexEdgeManagerOther() = default;
};

// VertexEdgeManagerBase->VertexEdgeManagerOther->DeleteVertexManager
class DeleteVertexManager : public VertexEdgeManagerOther {
public:
  DeleteVertexManager(graph_management_param && param);
  ~DeleteVertexManager() = default;
};

// VertexEdgeManagerBase->VertexEdgeManagerOther->DeleteEdgeManager
class DeleteEdgeManager : public VertexEdgeManagerOther {
public:
  DeleteEdgeManager(graph_management_param && param);
  ~DeleteEdgeManager() = default;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_ACTOR_ADMIN_ACT_H_
