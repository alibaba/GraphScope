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
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/graph_db_service.h"
#include "flex/engines/http_server/workdir_manipulator.h"

namespace server {
CodegenProxy& CodegenProxy::get() {
  static CodegenProxy instance;
  return instance;
}

StoredProcedureLibMeta::StoredProcedureLibMeta()
    : status(CodegenStatus::UNINITIALIZED), res_lib_path("") {}

StoredProcedureLibMeta::StoredProcedureLibMeta(CodegenStatus status)
    : status(status), res_lib_path("") {}

StoredProcedureLibMeta::StoredProcedureLibMeta(CodegenStatus status,
                                               std::string res_lib_path)
    : status(status), res_lib_path(res_lib_path) {}

std::string StoredProcedureLibMeta::to_string() const {
  return "status: " + std::to_string(status) +
         ", res_lib_path: " + res_lib_path;
}

CodegenProxy::CodegenProxy() : initialized_(false){};

CodegenProxy::~CodegenProxy() {}

bool CodegenProxy::Initialized() { return initialized_; }

void CodegenProxy::Init(std::string working_dir, std::string codegen_bin,
                        std::string ir_compiler_prop,
                        std::string default_graph_schema_path) {
  working_directory_ = working_dir;
  codegen_bin_ = codegen_bin;
  ir_compiler_prop_ = ir_compiler_prop;
  default_graph_schema_path_ = default_graph_schema_path;
  initialized_ = true;
  LOG(INFO) << "CodegenProxy working dir: " << working_directory_
            << ",codegen bin " << codegen_bin_ << ", ir compiler prop "
            << ir_compiler_prop_ << ", default graph schema "
            << default_graph_schema_path_;
}

seastar::future<std::pair<int32_t, std::string>> CodegenProxy::DoGen(
    const physical::PhysicalPlan& plan) {
  LOG(INFO) << "Start generating for query: ";
  auto next_job_id = plan.plan_id();

  {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock,
             [this, next_job_id] { return !check_job_running(next_job_id); });
  }

  auto cur_graph_schema_path = default_graph_schema_path_;
  if (cur_graph_schema_path.empty()) {
    auto& graph_db_service = server::GraphDBService::get();
    if (graph_db_service.get_metadata_store()) {
      auto running_graph_res =
          graph_db_service.get_metadata_store()->GetRunningGraph();
      if (!running_graph_res.ok()) {
        return seastar::make_exception_future<std::pair<int32_t, std::string>>(
            std::runtime_error("Get running graph failed"));
      }
      cur_graph_schema_path =
          WorkDirManipulator::GetGraphSchemaPath(running_graph_res.value());
    } else {
      LOG(ERROR) << "Graph schema path is empty";
      return seastar::make_exception_future<std::pair<int32_t, std::string>>(
          std::runtime_error("Graph schema path is empty"));
    }
  }

  if (cur_graph_schema_path.empty()) {
    LOG(ERROR) << "Graph schema path is empty";
    return seastar::make_exception_future<std::pair<int32_t, std::string>>(
        std::runtime_error("Graph schema path is empty"));
  }

  return call_codegen_cmd(plan, cur_graph_schema_path)
      .then([this, next_job_id](gs::Result<bool> codegen_res) {
        if (!codegen_res.ok()) {
          LOG(ERROR) << "Compilation failure: "
                     << codegen_res.status().error_message();
          return seastar::make_exception_future<
              std::pair<int32_t, std::string>>(std::runtime_error(
              "Compilation failure: " + codegen_res.status().error_message()));
        }
        return get_res_lib_path_from_cache(next_job_id);
      });
}

seastar::future<gs::Result<bool>> CodegenProxy::call_codegen_cmd(
    const physical::PhysicalPlan& plan,
    const std::string& cur_graph_schema_path) {
  // if the desired query lib for next_job_id is in cache, just return 0
  // otherwise, call codegen cmd
  auto next_job_id = plan.plan_id();
  auto query_name = "query_" + std::to_string(next_job_id);
  auto work_dir = get_work_directory(next_job_id);
  std::string plan_path;

  if (job_id_2_procedures_.find(next_job_id) != job_id_2_procedures_.end() &&
      job_id_2_procedures_[next_job_id].status == CodegenStatus::SUCCESS) {
    return seastar::make_ready_future<gs::Result<bool>>(true);
  }

  insert_or_update(next_job_id, CodegenStatus::RUNNING, "");

  plan_path = prepare_next_job_dir(work_dir, query_name, plan);
  if (plan_path.empty()) {
    insert_or_update(next_job_id, CodegenStatus::FAILED, "");
    return seastar::make_ready_future<gs::Result<bool>>(gs::Result<bool>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to prepare next job dir for " + query_name +
                       ", job id: " + std::to_string(next_job_id) +
                       ", plan path: " + plan_path),
        false));
  }

  std::string expected_res_lib_path = work_dir + "/lib" + query_name + ".so";
  return CallCodegenCmd(codegen_bin_, plan_path, query_name, work_dir, work_dir,
                        cur_graph_schema_path, ir_compiler_prop_)
      .then([this, next_job_id,
             expected_res_lib_path](gs::Result<bool> codegen_res) {
        if (!codegen_res.ok()) {
          LOG(ERROR) << "Compilation failure: "
                     << codegen_res.status().error_message();
          insert_or_update(next_job_id, CodegenStatus::FAILED, "");
          return seastar::make_ready_future<gs::Result<bool>>(codegen_res);
        }
        if (!std::filesystem::exists(expected_res_lib_path)) {
          LOG(ERROR) << "Compilation success, but generated lib not exists: "
                     << expected_res_lib_path;
          insert_or_update(next_job_id, CodegenStatus::FAILED, "");
          VLOG(10) << "Compilation failed, job id: " << next_job_id;
          return seastar::make_ready_future<gs::Result<bool>>(codegen_res);
        }
        VLOG(10) << "Compilation success, job id: " << next_job_id;
        insert_or_update(next_job_id, CodegenStatus::SUCCESS,
                         expected_res_lib_path);
        {
          std::lock_guard<std::mutex> lock(mutex_);
          cv_.notify_all();
        }
        return seastar::make_ready_future<gs::Result<bool>>(true);
      });
}

seastar::future<std::pair<int32_t, std::string>>
CodegenProxy::get_res_lib_path_from_cache(int32_t next_job_id) {
  // the entry must exists
  StoredProcedureLibMeta meta = job_id_2_procedures_[next_job_id];

  if (meta.status == CodegenStatus::SUCCESS) {
    return seastar::make_ready_future<std::pair<int32_t, std::string>>(
        std::make_pair(next_job_id, meta.res_lib_path));
  } else {
    LOG(ERROR) << "Invalid state: " << meta.to_string() << ", "
               << ", compilation failure";
    return seastar::make_exception_future<std::pair<int32_t, std::string>>(
        std::runtime_error("Compilation failed, invalid state: " +
                           meta.to_string()));
  }
}

seastar::future<gs::Result<bool>> CodegenProxy::CallCodegenCmd(
    const std::string& codegen_bin, const std::string& plan_path,
    const std::string& query_name, const std::string& work_dir,
    const std::string& output_dir, const std::string& graph_schema_path,
    const std::string& engine_config, const std::string& procedure_desc) {
  if (query_name.empty()) {
    return seastar::make_exception_future<gs::Result<bool>>(
        std::runtime_error("query_name is empty"));
  }
  // query_name can not start with number, and should only contains digits,
  // letters and underscores
  if (!std::isalpha(query_name[0])) {
    return seastar::make_exception_future<gs::Result<bool>>(
        std::runtime_error("query_name should start with alphabet"));
  }
  if (!std::all_of(query_name.begin(), query_name.end(), [](char c) {
        return std::isalnum(c) || c == '_' || c == '-';
      })) {
    return seastar::make_exception_future<gs::Result<bool>>(
        std::runtime_error("query_name should only contains digits, letters "
                           "and underscores: " +
                           query_name));
  }

  // TODO: different suffix for different platform
  std::string cmd = codegen_bin + " -e=hqps " + " -i=" + plan_path +
                    " -o=" + output_dir + " --procedure_name=" + query_name +
                    " -w=" + work_dir + " --ir_conf=" + engine_config +
                    " --graph_schema_path=" + graph_schema_path;
  std::string desc_file = work_dir + "/" + query_name + ".desc";
  if (!procedure_desc.empty()) {
    std::ofstream ofs(desc_file);
    ofs << procedure_desc;
    ofs.close();
    cmd += " --procedure_desc=" + desc_file;
  }
  LOG(INFO) << "Start call codegen cmd: [" << cmd << "]";

  return hiactor::thread_resource_pool::submit_work([cmd, desc_file] {
    //  auto res = std::system(cmd.c_str());
    boost::process::ipstream stdout_pipe;
    boost::process::ipstream stderr_pipe;
    boost::process::child codegen_process(
        cmd, boost::process::std_out > stdout_pipe,
        boost::process::std_err > stderr_pipe);

    std::stringstream ss;
    std::string stderr_res;
    while (std::getline(stderr_pipe, stderr_res)) {
      ss << stderr_res + "\n";
    }

    codegen_process.wait();
    stderr_pipe.close();
    stdout_pipe.close();
    // remove the desc file if exists
    if (std::filesystem::exists(desc_file)) {
      std::filesystem::remove(desc_file);
    }
    int res = codegen_process.exit_code();
    if (res != 0) {
      return gs::Result<bool>(
          gs::Status(gs::StatusCode::CODEGEN_ERROR, ss.str()), false);
    }

    LOG(INFO) << "Codegen cmd: [" << cmd << "] success! ";
    return gs::Result<bool>(true);
  });
}

std::string CodegenProxy::get_work_directory(int32_t job_id) {
  std::string work_dir = working_directory_ + "/" + std::to_string(job_id);
  ensure_dir_exists(work_dir);
  return work_dir;
}

void CodegenProxy::insert_or_update(int32_t job_id, CodegenStatus status,
                                    std::string path) {
  if (job_id_2_procedures_.find(job_id) != job_id_2_procedures_.end()) {
    job_id_2_procedures_[job_id].status = status;
    job_id_2_procedures_[job_id].res_lib_path = path;
  } else {
    job_id_2_procedures_.emplace(job_id, StoredProcedureLibMeta{status, path});
  }
}

bool CodegenProxy::check_job_running(int32_t job_id) {
  if (job_id_2_procedures_.find(job_id) != job_id_2_procedures_.end()) {
    return job_id_2_procedures_[job_id].status == CodegenStatus::RUNNING;
  } else {
    return false;
  }
}

void CodegenProxy::ensure_dir_exists(const std::string& working_dir) {
  LOG(INFO) << "Ensuring [" << working_dir << "] exists ";
  std::filesystem::path path = working_dir;
  if (!std::filesystem::exists(path)) {
    LOG(INFO) << path << " not exists";
    auto res = std::filesystem::create_directories(path);
    if (!res) {
      LOG(WARNING) << "create " << path << " failed";
    } else {
      LOG(INFO) << "create " << path << " success";
    }
  } else {
    LOG(INFO) << working_dir << " already exists";
  }
}

void CodegenProxy::clear_dir(const std::string& working_dir) {
  LOG(INFO) << "[Cleaning]" << working_dir;
  std::filesystem::path path = working_dir;
  if (std::filesystem::exists(path)) {
    size_t num = 0;
    for (const auto& entry : std::filesystem::directory_iterator(working_dir)) {
      std::filesystem::remove_all(entry.path());
      num += 1;
    }
    LOG(INFO) << "remove " << num << "files under " << path;
  }
}

std::string CodegenProxy::prepare_next_job_dir(
    const std::string& plan_work_dir, const std::string& query_name,
    const physical::PhysicalPlan& plan) {
  // clear directory;
  clear_dir(plan_work_dir);

  // dump plan to file
  std::string plan_path = plan_work_dir + "/" + query_name + ".pb";
  std::ofstream ofs(plan_path, std::ios::binary);
  auto ret = plan.SerializeToOstream(&ofs);
  LOG(INFO) << "Dump plan to: " << plan_path
            << ", ret: " << std::to_string(ret);
  if (!ret) {
    return "";
  }

  return plan_path;
}

}  // namespace server
