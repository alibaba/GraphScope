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
                        std::string compiler_graph_schema) {
  working_directory_ = working_dir;
  codegen_bin_ = codegen_bin;
  ir_compiler_prop_ = ir_compiler_prop;
  compiler_graph_schema_ = compiler_graph_schema;
  initialized_ = true;
  LOG(INFO) << "CodegenProxy working dir: " << working_directory_
            << ",codegen bin " << codegen_bin_ << ", ir compiler prop "
            << ir_compiler_prop_ << ", compiler graph schema "
            << compiler_graph_schema_;
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

  return call_codegen_cmd(plan).then_wrapped([this,
                                              next_job_id](auto&& future) {
    int return_code;
    try {
      return_code = future.get();
    } catch (std::exception& e) {
      LOG(ERROR) << "Compilation failed: " << e.what();
      return seastar::make_ready_future<std::pair<int32_t, std::string>>(
          std::make_pair(next_job_id,
                         std::string("Compilation failed: ") + e.what()));
    }
    if (return_code != 0) {
      LOG(ERROR) << "Codegen failed";
      return seastar::make_exception_future<std::pair<int32_t, std::string>>(
          std::runtime_error("Codegen failed"));
    }
    return get_res_lib_path_from_cache(next_job_id);
  });
}

seastar::future<int> CodegenProxy::call_codegen_cmd(
    const physical::PhysicalPlan& plan) {
  // if the desired query lib for next_job_id is in cache, just return 0
  // otherwise, call codegen cmd
  auto next_job_id = plan.plan_id();
  auto query_name = "query_" + std::to_string(next_job_id);
  auto work_dir = get_work_directory(next_job_id);
  std::string plan_path;

  if (job_id_2_procedures_.find(next_job_id) != job_id_2_procedures_.end() &&
      job_id_2_procedures_[next_job_id].status == CodegenStatus::SUCCESS) {
    return seastar::make_ready_future<int>(0);
  }

  insert_or_update(next_job_id, CodegenStatus::RUNNING, "");

  plan_path = prepare_next_job_dir(work_dir, query_name, plan);
  if (plan_path.empty()) {
    insert_or_update(next_job_id, CodegenStatus::FAILED, "");
    return seastar::make_exception_future<int>(std::runtime_error(
        "Fail to prepare next job dir for " + query_name + ", job id: " +
        std::to_string(next_job_id) + ", plan path: " + plan_path));
  }

  std::string expected_res_lib_path = work_dir + "/lib" + query_name + ".so";
  return CallCodegenCmd(plan_path, query_name, work_dir, work_dir,
                        compiler_graph_schema_, ir_compiler_prop_, codegen_bin_)
      .then([this, next_job_id, expected_res_lib_path](int codegen_res) {
        if (codegen_res != 0 ||
            !std::filesystem::exists(expected_res_lib_path)) {
          LOG(ERROR) << "Expected lib path " << expected_res_lib_path
                     << " not exists, or compilation failure: " << codegen_res
                     << " compilation failed";

          insert_or_update(next_job_id, CodegenStatus::FAILED, "");
          VLOG(10) << "Compilation failed, job id: " << next_job_id;
        } else {
          VLOG(10) << "Compilation success, job id: " << next_job_id;
          insert_or_update(next_job_id, CodegenStatus::SUCCESS,
                           expected_res_lib_path);
        }
        {
          std::lock_guard<std::mutex> lock(mutex_);
          cv_.notify_all();
        }
        return seastar::make_ready_future<int>(codegen_res);
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

seastar::future<int> CodegenProxy::CallCodegenCmd(
    const std::string& plan_path, const std::string& query_name,
    const std::string& work_dir, const std::string& output_dir,
    const std::string& graph_schema_path, const std::string& engine_config,
    const std::string& codegen_bin) {
  // TODO: different suffix for different platform
  std::string cmd = codegen_bin + " -e=hqps " + " -i=" + plan_path +
                    " -o=" + output_dir + " --procedure_name=" + query_name +
                    " -w=" + work_dir + " --ir_conf=" + engine_config +
                    " --graph_schema_path=" + graph_schema_path;
  LOG(INFO) << "Start call codegen cmd: [" << cmd << "]";

  return hiactor::thread_resource_pool::submit_work([cmd] {
           auto res = std::system(cmd.c_str());
           LOG(INFO) << "Codegen cmd: [" << cmd << "] return: " << res;
           return res;
         })
      .then_wrapped([](auto fut) {
        VLOG(10) << "try";
        try {
          VLOG(10) << "Got future ";
          return seastar::make_ready_future<int>(fut.get0());
        } catch (std::exception& e) {
          LOG(ERROR) << "Compilation failed: " << e.what();
          return seastar::make_ready_future<int>(-1);
        }
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
