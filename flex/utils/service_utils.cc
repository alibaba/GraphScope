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

#include "flex/utils/service_utils.h"

namespace gs {

static unsigned long long lastTotalUser, lastTotalUserLow, lastTotalSys,
    lastTotalIdle;

FlexException::FlexException(std::string&& error_msg)
    : std::exception(), _err_msg(error_msg) {}

FlexException::~FlexException() {}

const char* FlexException::what() const noexcept { return _err_msg.c_str(); }

// get current executable's directory
std::string get_current_dir() {
  char buf[1024];
  ssize_t len = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
  CHECK_NE(len, -1) << "readlink failed";
  buf[len] = '\0';
  std::string exe_path(buf);
  return exe_path.substr(0, exe_path.rfind('/'));
}

void run_graph_loading(const std::string& graph_schema_file,
                       const std::string& bulk_load_file,
                       const std::string& data_dir) {
  std::string graph_loader_bin = get_current_dir() + "/" + GRAPH_LOADER_BIN;
  // if data_dir is not empty, throw exception
  if (data_dir.empty()) {
    throw FlexException("data_dir for " + graph_schema_file +
                        "is not empty: " + data_dir);
  }
  // if bulk_load_file is empty, throw exception
  if (bulk_load_file.empty()) {
    throw FlexException("bulk_load_file for " + graph_schema_file + "is empty");
  }
  if (graph_schema_file.empty()) {
    throw FlexException("graph_schema_file is empty");
  }
  // check bulk_load_file exists
  if (!std::filesystem::exists(bulk_load_file)) {
    throw FlexException("bulk_load_file " + bulk_load_file + " not exists");
  }
  // mkdir data_dir if not exists
  if (!std::filesystem::exists(data_dir)) {
    std::filesystem::create_directory(data_dir);
  }
  // check data_dir is empty
  if (!std::filesystem::is_empty(data_dir)) {
    LOG(WARNING) << "Graph : " << graph_schema_file << " data_dir: " << data_dir
                 << " is not empty, skip loading";
    return;
  }
  // check graph_schema_file exists
  if (!std::filesystem::exists(graph_schema_file)) {
    throw FlexException("graph_schema_file " + graph_schema_file +
                        " not exists");
  }
  // check graph_loader bin exists
  if (!std::filesystem::exists(graph_loader_bin)) {
    throw FlexException("graph_loader_bin " + graph_loader_bin + " not exists");
  }
  // execute graph_loader with sub process, and check the return value
  std::string cmd = graph_loader_bin + " " + graph_schema_file + " " +
                    bulk_load_file + " " + data_dir;
  VLOG(1) << "Start run graph loading cmd: " << cmd;
  int ret = std::system(cmd.c_str());
  if (ret != 0) {
    throw FlexException("run graph loading cmd failed: " + cmd);
  }
  // check data_dir/init_snapshot.bin exists
  std::string init_snapshot_file = data_dir + "/init_snapshot.bin";
  if (!std::filesystem::exists(init_snapshot_file)) {
    throw FlexException("init_snapshot_file " + init_snapshot_file +
                        " not exists, loading failed");
  }
  VLOG(1) << "Finish run graph loading cmd: " << cmd;
}

std::string get_data_dir(const std::string& workspace,
                         const std::string& graph_name) {
  return workspace + "/data/" + graph_name + "/indices";
}

std::string get_graph_schema_file(const std::string& workspace,
                                  const std::string& graph_name) {
  return workspace + "/data/" + graph_name + "/graph.yaml";
}
std::string find_codegen_bin() {
  // first check whether flex_home env exists
  std::string flex_home;
  std::string codegen_bin;
  char* flex_home_char = getenv("FLEX_HOME");
  if (flex_home_char == nullptr) {
    // infer flex_home from current binary' directory
    char* bin_path = realpath("/proc/self/exe", NULL);
    std::string bin_path_str(bin_path);
    // flex home should be bin_path/../../
    std::string flex_home_str =
        bin_path_str.substr(0, bin_path_str.find_last_of("/"));
    // usr/loca/bin/
    flex_home_str = flex_home_str.substr(0, flex_home_str.find_last_of("/"));
    // usr/local/

    LOG(INFO) << "infer flex_home as installed, flex_home: " << flex_home_str;
    // check codege_bin_path exists
    codegen_bin = flex_home_str + "/bin/" + CODEGEN_BIN;
    // if flex_home exists, return flex_home
    if (std::filesystem::exists(codegen_bin)) {
      return codegen_bin;
    } else {
      // if not found, try as if it is in build directory
      // flex/build/
      flex_home_str = flex_home_str.substr(0, flex_home_str.find_last_of("/"));
      // flex/
      LOG(INFO) << "infer flex_home as build, flex_home: " << flex_home_str;
      codegen_bin = flex_home_str + "/bin/" + CODEGEN_BIN;
      if (std::filesystem::exists(codegen_bin)) {
        return codegen_bin;
      } else {
        LOG(FATAL) << "codegen bin not exists: ";
        return "";
      }
    }
  } else {
    flex_home = std::string(flex_home_char);
    LOG(INFO) << "flex_home env exists, flex_home: " << flex_home;
    codegen_bin = flex_home + "/bin/" + CODEGEN_BIN;
    if (std::filesystem::exists(codegen_bin)) {
      return codegen_bin;
    } else {
      LOG(FATAL) << "codegen bin not exists: ";
      return "";
    }
  }
}

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage() {
  struct sysinfo memInfo;

  sysinfo(&memInfo);
  uint64_t total_mem = memInfo.totalram;
  total_mem *= memInfo.mem_unit;

  uint64_t phy_mem_used = memInfo.totalram - memInfo.freeram;
  phy_mem_used *= memInfo.mem_unit;
  return std::make_pair(phy_mem_used, total_mem);
}

void init_cpu_usage_watch() {
  FILE* file = fopen("/proc/stat", "r");
  fscanf(file, "cpu %llu %llu %llu %llu", &lastTotalUser, &lastTotalUserLow,
         &lastTotalSys, &lastTotalIdle);
  fclose(file);
}

std::pair<double, double> get_current_cpu_usage() {
  double used;
  FILE* file;
  unsigned long long totalUser, totalUserLow, totalSys, totalIdle, total;

  file = fopen("/proc/stat", "r");
  fscanf(file, "cpu %llu %llu %llu %llu", &totalUser, &totalUserLow, &totalSys,
         &totalIdle);
  fclose(file);

  if (totalUser < lastTotalUser || totalUserLow < lastTotalUserLow ||
      totalSys < lastTotalSys || totalIdle < lastTotalIdle) {
    // Overflow detection. Just skip this value.
    used = total = 0.0;
  } else {
    total = (totalUser - lastTotalUser) + (totalUserLow - lastTotalUserLow) +
            (totalSys - lastTotalSys);
    used = total;
    total += (totalIdle - lastTotalIdle);
  }

  lastTotalUser = totalUser;
  lastTotalUserLow = totalUserLow;
  lastTotalSys = totalSys;
  lastTotalIdle = totalIdle;

  return std::make_pair(used, total);
}

std::string memory_to_mb_str(uint64_t mem_bytes) {
  double mem_mb = mem_bytes / 1024.0 / 1024.0;
  return std::to_string(mem_mb) + "MB";
}

}  // namespace gs
