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
    : status(CodegenStatus::UNINITALIZED), res_lib_path("") {}

StoredProcedureLibMeta::StoredProcedureLibMeta(CodegenStatus status)
    : status(status), res_lib_path("") {}

StoredProcedureLibMeta::StoredProcedureLibMeta(CodegenStatus status,
                                               std::string res_lib_path)
    : status(status), res_lib_path(res_lib_path) {}

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
  auto work_dir = get_work_directory(next_job_id);
  auto query_name = "query_" + std::to_string(next_job_id);
  std::string plan_path = prepare_next_job_dir(work_dir, query_name, plan);
  if (plan_path.empty()) {
    return seastar::make_exception_future<std::pair<int32_t, std::string>>(
        std::runtime_error("Fail to prepare next job dir"));
  }

  if (job_id_2_procedures_.find(next_job_id) == job_id_2_procedures_.end() ||
      job_id_2_procedures_[next_job_id].status == CodegenStatus::FAILED) {
    // Do gen.
    {
      // First lock
      std::lock_guard<std::mutex> lock(mutex_);
      job_id_2_procedures_[next_job_id] =
          StoredProcedureLibMeta{CodegenStatus::RUNNING, ""};
    }
    std::string res_lib_path =
        call_codegen_cmd(plan_path, query_name, work_dir);
    if (!std::filesystem::exists(res_lib_path)) {
      LOG(ERROR) << "Res lib path " << res_lib_path
                 << " not exists, compilation failed";
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (job_id_2_procedures_.find(next_job_id) !=
            job_id_2_procedures_.end()) {
          job_id_2_procedures_[next_job_id].status = CodegenStatus::FAILED;
        } else {
          job_id_2_procedures_.emplace(
              next_job_id, StoredProcedureLibMeta{CodegenStatus::FAILED});
        }
      }
      return seastar::make_exception_future<std::pair<int32_t, std::string>>(
          std::runtime_error("Codegen failed"));
    } else {
      // Add res_lib_path to query_cache.
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (job_id_2_procedures_.find(next_job_id) !=
            job_id_2_procedures_.end()) {
          job_id_2_procedures_[next_job_id].status = CodegenStatus::SUCCESS;
          job_id_2_procedures_[next_job_id].res_lib_path = res_lib_path;
        } else {
          job_id_2_procedures_.emplace(
              next_job_id,
              StoredProcedureLibMeta{CodegenStatus::SUCCESS, res_lib_path});
        }
      }
    }
  }

  return get_res_lib_path_from_cache(next_job_id);
}

seastar::future<std::pair<int32_t, std::string>>
CodegenProxy::get_res_lib_path_from_cache(int32_t next_job_id) {
  // status could be running,
  int retry_times = 0;
  volatile CodegenStatus codegen_status =
      job_id_2_procedures_[next_job_id].status;
  while (codegen_status == CodegenStatus::RUNNING &&
         retry_times < MAX_RETRY_TIMES) {
    // wait for codegen to finish
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    VLOG(10) << "Waiting for codegen to finish: retry times:" << retry_times;
    retry_times += 1;
    codegen_status = job_id_2_procedures_[next_job_id].status;
  }

  if (retry_times >= MAX_RETRY_TIMES) {
    LOG(ERROR) << "Codegen timeout";
    return seastar::make_exception_future<std::pair<int32_t, std::string>>(
        std::runtime_error("Codegen timeout"));
  }

  if (job_id_2_procedures_[next_job_id].status != CodegenStatus::SUCCESS) {
    LOG(ERROR) << "Invalid state: "
               << std::to_string(job_id_2_procedures_[next_job_id].status)
               << ", " << job_id_2_procedures_[next_job_id].res_lib_path
               << ", compilation failure";
    return seastar::make_exception_future<std::pair<int32_t, std::string>>(
        std::runtime_error("Invalid state"));
  }

  return seastar::make_ready_future<std::pair<int32_t, std::string>>(
      std::make_pair(next_job_id,
                     job_id_2_procedures_[next_job_id].res_lib_path));
}

std::string CodegenProxy::call_codegen_cmd(const std::string& plan_path,
                                           const std::string& query_name,
                                           const std::string& work_dir) {
  // TODO: different suffix for different platform
  std::string res_lib_path = work_dir + "/lib" + query_name + ".so";
  std::string cmd = codegen_bin_ + " -e=hqps " + " -i=" + plan_path +
                    " -w=" + work_dir + " --ir_conf=" + ir_compiler_prop_ +
                    " --graph_schema_path=" + compiler_graph_schema_;
  LOG(INFO) << "Start call codegen cmd: [" << cmd << "]";
  auto res = std::system(cmd.c_str());
  if (res != 0) {
    LOG(ERROR) << "call codegen cmd failed: " << cmd;
    return "";
  }
  return res_lib_path;
}

std::string CodegenProxy::get_work_directory(int32_t job_id) {
  std::string work_dir = working_directory_ + "/" + std::to_string(job_id);
  ensure_dir_exists(work_dir);
  return work_dir;
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

const int32_t CodegenProxy::MAX_RETRY_TIMES = 10;

}  // namespace server
