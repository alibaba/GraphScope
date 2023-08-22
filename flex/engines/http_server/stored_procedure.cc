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
#include "flex/engines/http_server/stored_procedure.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/generated/executor_ref.act.autogen.h"

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/http/handlers.hh>

namespace server {

void put_argment(gs::Encoder& encoder, const query::Argument& argment) {
  auto& value = argment.value();
  auto item_case = value.item_case();
  switch (item_case) {
  case common::Value::kI32:
    encoder.put_int(value.i32());
    break;
  case common::Value::kI64:
    encoder.put_long(value.i64());
    break;
  case common::Value::kF64:
    encoder.put_double(value.f64());
    break;
  case common::Value::kStr:
    encoder.put_string(value.str());
    break;
  default:
    LOG(ERROR) << "Not recognizable param type" << static_cast<int>(item_case);
  }
}

// get the handle of the dynamic library, throw error if needed
void* open_lib(const char* lib_path) {
  LOG(INFO) << "try to  open library: " << lib_path;
  void* handle = dlopen(lib_path, RTLD_LAZY);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
    LOG(FATAL) << "Fail to open library: " << lib_path
               << ", error: " << p_error_msg;
  }
  LOG(INFO) << "Successfully open library: " << lib_path;
  return handle;
}

void* get_func_ptr(const char* lib_path, void* handle, const char* symbol) {
  auto* p_func = dlsym(handle, symbol);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
    LOG(FATAL) << "Failed to get symbol " << symbol << " from " << lib_path
               << ". Reason: " << std::string(p_error_msg);
  }
  return p_func;
}

// close the handle of the dynamic library, throw error if needed
void close_lib(void* handle, const char* lib_path) {
  if (handle) {
    auto ret = dlclose(handle);
    if (ret == 0) {
      LOG(INFO) << "Sucessfuly closed library" << lib_path;
    } else {
      auto* p_error_msg = dlerror();
      if (p_error_msg) {
        LOG(FATAL) << "Fail to close library, error: " << p_error_msg;
      }
    }
  } else {
    LOG(WARNING) << "Try to close a null handle," << lib_path;
  }
}

std::vector<std::string> get_yaml_files(const std::string& plugin_dir) {
  std::filesystem::path dir_path = plugin_dir;
  std::string suffix = ".yaml";
  std::vector<std::string> res_yaml_files;

  for (auto& entry : std::filesystem::directory_iterator(dir_path)) {
    if (entry.is_regular_file() && entry.path().extension() == suffix) {
      res_yaml_files.emplace_back(entry.path());
    }
  }
  return res_yaml_files;
}

std::vector<StoredProcedureMeta> parse_from_multiple_yamls(
  const std::string& plugin_dir,
    const std::vector<std::string>& stored_procedure_yamls) {
  std::vector<StoredProcedureMeta> stored_procedures;
  for (auto cur_yaml : stored_procedure_yamls) {
    LOG(INFO) << "Loading for: " << cur_yaml;
    YAML::Node root = YAML::LoadFile(cur_yaml);
    if (!root["name"]) {
      LOG(ERROR) << "Expect name in pre_installed procedure";
    } else if (!root["library"]) {
      LOG(ERROR) << "Expect path in pre_installed procedure";
    } else {
      std::string name = root["name"].as<std::string>();
      std::string path = root["library"].as<std::string>();
      if (!std::filesystem::exists(path)) {
        // in case the path is relative to plugin_dir, prepend plugin_dir
        path = plugin_dir + "/"  +path;
        if (!std::filesystem::exists(path)) {
          LOG(ERROR) << "plugin - " << path << " file not found...";
        }
        else {
          stored_procedures.push_back({name, path});
        }
      } else {
        stored_procedures.push_back({name, path});
      }
    }
  }
  return stored_procedures;
}

std::vector<StoredProcedureMeta> parse_stored_procedures(
    const std::string& stored_procedure_yaml) {
  std::vector<StoredProcedureMeta> stored_procedures;
  YAML::Node root = YAML::LoadFile(stored_procedure_yaml);
  if (root["pre_installed"]) {
    std::vector<YAML::Node> installed_got;
    if (!get_sequence(root, "pre_installed", installed_got)) {
      LOG(ERROR) << "installed_got is not set properly";
    }
    for (auto& procedure : installed_got) {
      if (!procedure["name"]) {
        LOG(ERROR) << "Expect name in pre_installed procedure";
      } else if (!procedure["path"]) {
        LOG(ERROR) << "Expect path in pre_installed procedure";
      } else {
        std::string name = procedure["name"].as<std::string>();
        std::string path = procedure["path"].as<std::string>();
        if (!std::filesystem::exists(path)) {
          LOG(ERROR) << "plugin - " << path << " file not found...";
        } else {
          stored_procedures.push_back({name, path});
        }
      }
    }
  } else {
    LOG(WARNING) << "Expect ntry <pre_installed>: " << stored_procedure_yaml;
  }
  return stored_procedures;
}

std::shared_ptr<BaseStoredProcedure> create_stored_procedure_impl(
    int32_t procedure_id, const std::string& procedure_path) {
  auto& sess = gs::GraphDB::get().GetSession(hiactor::local_shard_id());

  gs::MutableCSRInterface graph_store(sess);

  return std::make_shared<
      server::CypherStoredProcedure<gs::MutableCSRInterface>>(
      procedure_id, procedure_path, std::move(graph_store),
      gs::GraphStoreType::Grape);
}

std::string load_and_run(int32_t job_id, const std::string& lib_path) {
  auto temp_stored_procedure =
      server::create_stored_procedure_impl(job_id, lib_path);
  LOG(INFO) << "Create stored procedure: " << temp_stored_procedure->ToString();
  std::vector<char> empty;
  gs::Decoder input_decoder(empty.data(), empty.size());
  auto res = temp_stored_procedure->Query(input_decoder);
  LOG(INFO) << "Finish running";
  LOG(INFO) << res.DebugString();
  std::string res_str;
  res.SerializeToString(&res_str);
  return res_str;
}

StoredProcedureManager& StoredProcedureManager::get() {
  static StoredProcedureManager instance;
  return instance;
}

}  // namespace server
