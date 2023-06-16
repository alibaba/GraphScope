#ifndef STORED_PROCEDURE_MANAGER_H
#define STORED_PROCEDURE_MANAGER_H

#include <dlfcn.h>
#include <string>

#include <yaml-cpp/yaml.h>
#include <climits>
#include <ostream>
#include <string>
#include <utility>
#include "glog/logging.h"

#include "proto_generated_gie/results.pb.h"
#include "proto_generated_gie/stored_procedure.pb.h"

#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

std::string load_and_run(int32_t job_id, const std::string& lib_path);

// get the handle of the dynamic library, throw error if needed
void* open_lib(const char* lib_path);

void* get_func_ptr(const char* lib_path, void* handle, const char* symbol);

// close the handle of the dynamic library, throw error if needed
void close_lib(void* handle, const char* lib_path);

void put_argment(Encoder& encoder, const query::Argument& argment);

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
    const std::vector<std::string>& stored_procedure_yamls);

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

  virtual results::CollectiveResults Query(Decoder& decoder) const = 0;

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

std::vector<std::string> get_yaml_files(const std::string& plugin_dir);

class StoredProcedureManager {
 public:
  static StoredProcedureManager& get();
  StoredProcedureManager() {}

  // expect multiple query.yaml under this directory.
  void LoadFromPluginDir(const std::string& plugin_dir) {
    auto yaml_files = get_yaml_files(plugin_dir);
    auto stored_procedures = parse_from_multiple_yamls(yaml_files);
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
          create_stored_procedure_impl(i, stored_procedures[i].path));
    }

    LOG(INFO) << "Load [" << stored_procedures_.size() << "] stored procedures";
  }

  results::CollectiveResults Query(const query::Query& query_pb) const {
    auto query_name = query_pb.query_name().name();
    if (query_name.empty()) {
      LOG(ERROR) << "Query name is empty";
      return results::CollectiveResults();
    }
    auto it = stored_procedures_.find(query_name);
    if (it != stored_procedures_.end()) {
      // create a decoder to decode the query
      std::vector<char> input_buffer;
      Encoder input_encoder(input_buffer);
      auto& args = query_pb.arguments();
      for (auto i = 0; i < args.size(); ++i) {
        auto& arg = args[i];
        LOG(INFO) << "Putting " << i << "th arg" << arg.DebugString();
        put_argment(input_encoder, arg);
      }
      LOG(INFO) << "Before running " << query_name;
      Decoder input_decoder(input_buffer.data(), input_buffer.size());
      return it->second->Query(input_decoder);
    } else {
      LOG(ERROR) << "No stored procedure with id: " << query_name;
      return results::CollectiveResults();
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
                        const GRAPH_TYPE& graph, int64_t time_stamp,
                        GraphStoreType graph_store_type)
      : BaseStoredProcedure(procedure_id, procedure_path),
        app_ptr_(nullptr),
        create_app_ptr_(nullptr),
        delete_app_ptr_(nullptr),
        graph_(graph),
        time_stamp_(time_stamp),
        graph_store_type_(graph_store_type) {
    // get the func_ptr we need for cypher query.
    create_app_ptr_ = reinterpret_cast<CreateAppT*>(get_func_ptr(
        procedure_path_.c_str(), dl_handle_, CREATOR_APP_FUNC_NAME));
    CHECK(create_app_ptr_);
    delete_app_ptr_ = reinterpret_cast<DeleteAppT*>(get_func_ptr(
        procedure_path_.c_str(), dl_handle_, DELETER_APP_FUNC_NAME));
    CHECK(delete_app_ptr_);
    LOG(INFO) << "Successfully get cypher query function pointer";
    app_ptr_ = reinterpret_cast<HqpsAppBase<GRAPH_TYPE>*>(
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

  results::CollectiveResults Query(Decoder& decoder) const override {
    CHECK(app_ptr_);
    LOG(INFO) << "Start to query with cypher stored procedure";
    return app_ptr_->Query(graph_, time_stamp_, decoder);
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
  const GRAPH_TYPE& graph_;
  GraphStoreType graph_store_type_;
  int64_t time_stamp_;
  HqpsAppBase<GRAPH_TYPE>* app_ptr_;

  // func ptr;
  CreateAppT* create_app_ptr_;
  DeleteAppT* delete_app_ptr_;
};
}  // namespace gs

#endif  // STORED_PROCEDURE_MANAGER_H
