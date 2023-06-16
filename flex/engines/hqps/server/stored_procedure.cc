#include "flex/engines/hqps/server/stored_procedure.h"

namespace gs {

void put_argment(Encoder& encoder, const query::Argument& argment) {
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
    LOG(ERROR) << "Not recognizable param type" << static_cast<int>(item_case) ;
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
        LOG(ERROR) << "plugin - " << path << " file not found...";
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
  auto& grape_store = gs::GrapeGraphInterface::get();
  auto time_stamp = std::numeric_limits<int64_t>::max() - 1;
  if (grape_store.Initialized()) {
    return std::make_shared<gs::CypherStoredProcedure<gs::GrapeGraphInterface>>(
        procedure_id, procedure_path, grape_store, time_stamp,
        GraphStoreType::Grape);
  }  else {
    LOG(FATAL) << "No available graph store";
  }
}

std::string load_and_run(int32_t job_id, const std::string& lib_path) {
  auto temp_stored_procedure =
      gs::create_stored_procedure_impl(job_id, lib_path);
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

}  // namespace gs
