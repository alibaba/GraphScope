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
#ifndef SERVICE_UTILS_H
#define SERVICE_UTILS_H

#include <sys/sysinfo.h>
#include <sys/types.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "flex/utils/result.h"

#include "glog/logging.h"

namespace gs {

static constexpr const char* CODEGEN_BIN = "load_plan_and_gen.sh";
static constexpr const char* GRAPH_LOADER_BIN = "graph_loader";

class FlexException : public std::exception {
 public:
  explicit FlexException(std::string&& error_msg);
  ~FlexException() override;

  const char* what() const noexcept override;

 private:
  std::string _err_msg;
};

// Get the directory of the current executable
std::string get_current_dir();

void run_graph_loading(const std::string& graph_schema_file,
                       const std::string& bulk_load_file,
                       const std::string& data_dir);

std::string get_data_dir(const std::string& workspace,
                         const std::string& graph_name);
std::string get_graph_schema_file(const std::string& workspace,
                                  const std::string& graph_name);
std::string find_codegen_bin();

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage();

void init_cpu_usage_watch();

std::pair<double, double> get_current_cpu_usage();

std::string memory_to_mb_str(uint64_t mem_bytes);

}  // namespace gs

#endif  // SERVICE_UTILS_H