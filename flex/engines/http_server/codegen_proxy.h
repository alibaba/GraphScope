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

#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"

#include "flex/proto_generated_gie/job_service.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include <boost/program_options.hpp>
#include <hiactor/core/thread_resource_pool.hh>
#include <seastar/core/future.hh>

namespace server {

enum CodegenStatus {
  RUNNING = 0,
  FAILED = 1,
  SUCCESS = 2,
  UNINITIALIZED = 3,
};

struct StoredProcedureLibMeta {
  CodegenStatus status;
  std::string res_lib_path;

  StoredProcedureLibMeta();
  StoredProcedureLibMeta(CodegenStatus status);
  StoredProcedureLibMeta(CodegenStatus status, std::string res_lib_path);

  std::string to_string() const;
};

// Manages the codegen runner, process the incoming adhoc query, and output to
// the desired directory

class CodegenProxy {
 public:
  static CodegenProxy& get();
  static constexpr const char* DEFAULT_CODEGEN_DIR = "/tmp/codegen/";
  CodegenProxy();

  ~CodegenProxy();

  bool Initialized();

  void Init(std::string working_dir, std::string codegen_bin,
            std::string ir_compiler_prop, std::string compiler_graph_schema);

  // Do gen
  // A plan id is given along with the plan, we assume
  // 1. When plan is the same, the plan id might be the same.
  // 2. When plan is different, the plan id must be different.
  // 3. When the plan_id has appeared in the past, we just take the cached
  // res_lib.
  //
  // Consider the critical scenario: when two same plan arrived at the same
  // time, we need to ensure that only one codegen is running.
  seastar::future<std::pair<int32_t, std::string>> DoGen(
      const physical::PhysicalPlan& plan);

  static seastar::future<int> CallCodegenCmd(
      const std::string& plan_path, const std::string& query_name,
      const std::string& work_dir, const std::string& output_dir,
      const std::string& graph_schema_path, const std::string& engine_config,
      const std::string& codegen_bin);

 private:
  seastar::future<int> call_codegen_cmd(const physical::PhysicalPlan& plan);

  seastar::future<std::pair<int32_t, std::string>> get_res_lib_path_from_cache(
      int32_t job_id);

  std::string get_work_directory(int32_t job_id);

  void insert_or_update(int32_t job_id, CodegenStatus status, std::string path);

  bool check_job_running(int32_t job_id);

  void ensure_dir_exists(const std::string& working_dir);

  void clear_dir(const std::string& working_dir);

  std::string prepare_next_job_dir(const std::string& plan_work_dir,
                                   const std::string& query_name,
                                   const physical::PhysicalPlan& plan);

  std::string working_directory_;
  std::string codegen_bin_;
  std::string ir_compiler_prop_;
  std::string compiler_graph_schema_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::unordered_map<int32_t, StoredProcedureLibMeta> job_id_2_procedures_;
  bool initialized_;
};

}  // namespace server

namespace std {
inline std::string to_string(server::CodegenStatus status) {
  switch (status) {
  case server::CodegenStatus::RUNNING:
    return "RUNNING";
  case server::CodegenStatus::FAILED:
    return "FAILED";
  case server::CodegenStatus::SUCCESS:
    return "SUCCESS";
  default:
    return "UNKNOWN";
  }
}
}  // namespace std

#endif  // ENGINES_HQPS_SERVER_CODEGEN_PROXY_H_
