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
#ifndef ENGINES_HTTP_SERVER_STORED_PROCEDURE_H_
#define ENGINES_HTTP_SERVER_STORED_PROCEDURE_H_

#include <dlfcn.h>
#include <string>

#include <yaml-cpp/yaml.h>
#include <climits>
#include <ostream>
#include <string>
#include <utility>
#include "glog/logging.h"

#include "flex/proto_generated_gie/results.pb.h"
#include "flex/proto_generated_gie/stored_procedure.pb.h"

#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/utils/app_utils.h"
#include "flex/utils/yaml_utils.h"

#include <seastar/core/print.hh>

namespace server {

std::string load_and_run(int32_t job_id, const std::string& lib_path);

// get the handle of the dynamic library, throw error if needed
void* open_lib(const char* lib_path);

void* get_func_ptr(const char* lib_path, void* handle, const char* symbol);

// close the handle of the dynamic library, throw error if needed
void close_lib(void* handle, const char* lib_path);

void put_argment(gs::Encoder& encoder, const query::Argument& argment);

template <typename T>
bool get_scalar(YAML::Node node, const std::string& key, T& value) {
  YAML::Node cur = node[key];
  if (cur && cur.IsScalar()) {
    value = cur.as<T>();
    return true;
  }
  return false;
}

template <typename T>
bool get_sequence(YAML::Node node, const std::string& key,
                  std::vector<T>& seq) {
  YAML::Node cur = node[key];
  if (cur && cur.IsSequence()) {
    int num = cur.size();
    seq.clear();
    for (int i = 0; i < num; ++i) {
      seq.push_back(cur[i].as<T>());
    }
    return true;
  }
  return false;
}
struct StoredProcedureMeta {
  std::string name;
  std::string path;
};

std::vector<StoredProcedureMeta> parse_stored_procedures(
    const std::string& stored_procedure_yaml);
std::vector<StoredProcedureMeta> parse_from_multiple_yamls(
    const std::string& plugin_dir,
    const std::vector<std::string>& stored_procedure_yamls,
    const std::vector<std::string>& valid_procedure_names);

enum class StoredProcedureType {
  kCypher = 0,
  kSut = 1,
};

// return a void* ptr with no params
typedef void* CreateAppT(gs::GraphStoreType);

// return void with void* as input
typedef void DeleteAppT(void*, gs::GraphStoreType);

// the root interface of stored produce
class BaseStoredProcedure {
 public:
  BaseStoredProcedure(int32_t procedure_id, std::string procedure_path)
      : procedure_id_(procedure_id),
        procedure_path_(procedure_path),
        dl_handle_(nullptr) {
    dl_handle_ = open_lib(procedure_path.c_str());
    CHECK(dl_handle_);
  }
  virtual ~BaseStoredProcedure() {
    LOG(INFO) << "Destructing stored procedure" << ToString();
  }
  virtual StoredProcedureType GetType() const = 0;

  virtual results::CollectiveResults Query(gs::Decoder& decoder) const = 0;

  virtual void delete_app() = 0;

  virtual std::string ToString() const {
    std::stringstream ss;
    ss << "StoredProcedure{"
       << "procedure_id: " << procedure_id_
       << "}, {procedure_path: " << procedure_path_ << "}";
    return ss.str();
  }

  int32_t GetProcedureId() const { return procedure_id_; }
  std::string GetProcedureName() const { return procedure_path_; }

 protected:
  int32_t procedure_id_;
  std::string procedure_path_;
  void* dl_handle_;
};

template <typename GRAPH_TYPE>
class CypherStoredProcedure;

// Create StoredProcedure
// Why we extract the function here rather then put it in the class?
// To support ad-hoc query, and reuse code.

std::shared_ptr<BaseStoredProcedure> create_stored_procedure_impl(
    int32_t procedure_id, const std::string& procedure_path);

class StoredProcedureManager {
 public:
  static StoredProcedureManager& get();
  StoredProcedureManager() {}

  // expect multiple query.yaml under this directory.
  void LoadFromPluginDir(
      const std::string& plugin_dir,
      const std::vector<std::string>& valid_procedure_names) {
    auto yaml_files = gs::get_yaml_files(plugin_dir);
    auto stored_procedures = parse_from_multiple_yamls(plugin_dir, yaml_files,
                                                       valid_procedure_names);
    CreateStoredProcedures(stored_procedures);
  }

  void LoadFromYaml(const std::string& stored_procedure_yaml) {
    auto stored_procedures = parse_stored_procedures(stored_procedure_yaml);
    CreateStoredProcedures(stored_procedures);
  }

  void CreateStoredProcedures(
      const std::vector<StoredProcedureMeta>& stored_procedures) {
    for (auto i = 0; i < stored_procedures.size(); ++i) {
      stored_procedures_.emplace(
          stored_procedures[i].name,
          server::create_stored_procedure_impl(i, stored_procedures[i].path));
    }

    LOG(INFO) << "Load [" << stored_procedures_.size() << "] stored procedures";
  }

  seastar::future<results::CollectiveResults> Query(
      const query::Query& query_pb) const {
    auto query_name = query_pb.query_name().name();
    if (query_name.empty()) {
      LOG(ERROR) << "Query name is empty";
      return seastar::make_exception_future<results::CollectiveResults>(
          std::runtime_error("Query name is empty"));
    }
    auto it = stored_procedures_.find(query_name);
    if (it != stored_procedures_.end()) {
      // create a decoder to decode the query
      std::vector<char> input_buffer;
      gs::Encoder input_encoder(input_buffer);
      auto& args = query_pb.arguments();
      for (auto i = 0; i < args.size(); ++i) {
        auto& arg = args[i];
        LOG(INFO) << "Putting " << i << "th arg" << arg.DebugString();
        put_argment(input_encoder, arg);
      }
      LOG(INFO) << "Before running " << query_name;
      gs::Decoder input_decoder(input_buffer.data(), input_buffer.size());
      auto result = it->second->Query(input_decoder);
      return seastar::make_ready_future<results::CollectiveResults>(
          std::move(result));
    } else {
      LOG(ERROR) << "No stored procedure with id: " << query_name;
      return seastar::make_exception_future<results::CollectiveResults>(
          std::runtime_error("No stored procedure with id: " + query_name));
    }
  }

 private:
  std::unordered_map<std::string, std::shared_ptr<BaseStoredProcedure>>
      stored_procedures_;
};

// one stored procedure contains one dynamic lib, two function pointer
// one for create app, other for delete app;
template <typename GRAPH_TYPE>
class CypherStoredProcedure : public BaseStoredProcedure {
 public:
  static constexpr const char* CREATOR_APP_FUNC_NAME = "CreateApp";
  static constexpr const char* DELETER_APP_FUNC_NAME = "DeleteApp";

  CypherStoredProcedure(int32_t procedure_id, std::string procedure_path,
                        GRAPH_TYPE&& graph, gs::GraphStoreType graph_store_type)
      : BaseStoredProcedure(procedure_id, procedure_path),
        app_ptr_(nullptr),
        create_app_ptr_(nullptr),
        delete_app_ptr_(nullptr),
        graph_(std::move(graph)),
        graph_store_type_(graph_store_type) {
    LOG(INFO) << "creating stored procedure: v label num: "
              << std::to_string(
                     graph_.GetDBSession().schema().vertex_label_num());
    // get the func_ptr we need for cypher query.
    create_app_ptr_ = reinterpret_cast<CreateAppT*>(get_func_ptr(
        procedure_path_.c_str(), dl_handle_, CREATOR_APP_FUNC_NAME));
    CHECK(create_app_ptr_);
    delete_app_ptr_ = reinterpret_cast<DeleteAppT*>(get_func_ptr(
        procedure_path_.c_str(), dl_handle_, DELETER_APP_FUNC_NAME));
    CHECK(delete_app_ptr_);
    LOG(INFO) << "Successfully get cypher query function pointer";
    app_ptr_ = reinterpret_cast<gs::HqpsAppBase<GRAPH_TYPE>*>(
        create_app_ptr_(graph_store_type_));
    CHECK(app_ptr_);
    LOG(INFO) << "Successfully create app";
  }

  virtual ~CypherStoredProcedure() {
    if (app_ptr_) {
      delete_app();
    }
  }

  StoredProcedureType GetType() const override {
    return StoredProcedureType::kCypher;
  }

  results::CollectiveResults Query(gs::Decoder& decoder) const override {
    CHECK(app_ptr_);
    LOG(INFO) << "Start to query with cypher stored procedure";
    LOG(INFO) << "label num:"
              << graph_.GetDBSession().schema().vertex_label_num();
    return app_ptr_->Query(graph_, decoder);
  }

  void delete_app() override {
    LOG(INFO) << "Start to delete app";
    delete_app_ptr_(static_cast<void*>(app_ptr_), graph_store_type_);
    LOG(INFO) << "Successfully delete app";
  }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "CypherStoredProcedure{"
       << "procedure_id: " << procedure_id_
       << "}, {procedure_path: " << procedure_path_ << "}";
    return ss.str();
  }

 private:
  GRAPH_TYPE graph_;
  gs::GraphStoreType graph_store_type_;
  gs::HqpsAppBase<GRAPH_TYPE>* app_ptr_;

  // func ptr;
  CreateAppT* create_app_ptr_;
  DeleteAppT* delete_app_ptr_;
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_STORED_PROCEDURE_H_
