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
#ifndef ENGINES_HQPS_SERVER_CODEGEN_PROXY_H_
#define ENGINES_HQPS_SERVER_CODEGEN_PROXY_H_

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "glog/logging.h"

#include "proto_generated_gie/job_service.pb.h"
#include "proto_generated_gie/physical.pb.h"

namespace server {

// Manages the codegen runner, process the incoming adhoc query, and output to
// the desired directory

class CodegenProxy {
 public:
  static CodegenProxy& get();
  CodegenProxy() : initialized_(false){};

  ~CodegenProxy() = default;

  bool Initialized() { return initialized_; }

  // the last two params are needed temporally, should be remove after all
  // configuration are merged
  void Init(std::string working_dir, std::string codegen_bin,
            std::string ir_compiler_prop, std::string compiler_graph_schema,
            std::string gie_home) {
    working_directory_ = working_dir;
    codegen_bin_ = codegen_bin;
    ir_compiler_prop_ = ir_compiler_prop;
    compiler_graph_schema_ = compiler_graph_schema;
    gie_home_ = gie_home;
    initialized_ = true;
    LOG(INFO) << "CodegenProxy working dir: " << working_directory_
              << ",codegen bin " << codegen_bin_ << ", ir compiler prop "
              << ir_compiler_prop_ << ", compiler graph schema "
              << compiler_graph_schema_;
  }

  // Do gen
  std::optional<std::pair<int32_t, std::string>> do_gen(
      const physical::PhysicalPlan& plan) {
    LOG(INFO) << "Start generating for query: ";
    auto next_job_id = getNextJobId();
    auto work_dir = get_work_directory(next_job_id);
    auto query_name = "query_" + std::to_string(next_job_id);
    std::string plan_path = prepare_next_job_dir(work_dir, query_name, plan);
    if (plan_path.empty()) {
      return {};
    }

    std::string res_lib_path =
        call_codegen_cmd(plan_path, query_name, work_dir);

    // check res_lib_path exists
    if (!std::filesystem::exists(res_lib_path)) {
      LOG(ERROR) << "res lib path " << res_lib_path << " not exists";
      return {};
    }
    return std::make_pair(next_job_id, res_lib_path);
  }

  std::string call_codegen_cmd(const std::string& plan_path,
                               const std::string& query_name,
                               const std::string& work_dir) {
    // TODO: different suffix for different platform
    std::string res_lib_path = work_dir + "/lib" + query_name + ".so";
    std::string cmd = codegen_bin_ + " -e=hqps " + " -i=" + plan_path +
                      " -w=" + work_dir + " --ir_conf=" + ir_compiler_prop_ +
                      " --graph_schema_path=" + compiler_graph_schema_ +
                      " --gie_home=" + gie_home_;
    LOG(INFO) << "Start call codegen cmd: [" << cmd << "]";
    auto res = std::system(cmd.c_str());
    if (res != 0) {
      LOG(ERROR) << "call codegen cmd failed: " << cmd;
      return "";
    }
    return res_lib_path;
  }

 private:
  int32_t getNextJobId() { return next_job_id_.fetch_add(1); }

  std::string get_work_directory(int32_t job_id) {
    std::string work_dir = working_directory_ + "/" + std::to_string(job_id);
    ensure_dir_exists(work_dir);
    return work_dir;
  }

  void ensure_dir_exists(const std::string& working_dir) {
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

  void clear_dir(const std::string& working_dir) {
    LOG(INFO) << "[Cleaning]" << working_dir;
    std::filesystem::path path = working_dir;
    if (std::filesystem::exists(path)) {
      size_t num = 0;
      for (const auto& entry :
           std::filesystem::directory_iterator(working_dir)) {
        std::filesystem::remove_all(entry.path());
        num += 1;
      }
      LOG(INFO) << "remove " << num << "files under " << path;
    }
  }

  std::string prepare_next_job_dir(const std::string& plan_work_dir,
                                   const std::string& query_name,
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

  std::string working_directory_;
  std::string codegen_bin_;
  std::string ir_compiler_prop_;
  std::string compiler_graph_schema_;
  std::string gie_home_;
  std::atomic<int32_t> next_job_id_{0};
  bool initialized_;
};

}  // namespace server

#endif  // ENGINES_HQPS_SERVER_CODEGEN_PROXY_H_
