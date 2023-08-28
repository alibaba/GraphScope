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

#include <filesystem>
#include <iostream>
#include "stdlib.h"

#include "flex/engines/http_server/hqps_service.h"

#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/stored_procedure.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>

namespace bpo = boost::program_options;

namespace gs {
static constexpr const char* CODEGEN_BIN = "load_plan_and_gen.sh";
static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
static constexpr const uint32_t DEFAULT_HTTP_PORT = 10000;
static constexpr const char* DEFAULT_CODEGEN_DIR = "/tmp/codegen/";
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

std::string parse_codegen_dir(const bpo::variables_map& vm) {
  std::string codegen_dir;

  if (vm.count("codegen-dir") == 0) {
    LOG(INFO) << "codegen-dir is not specified";
    codegen_dir = DEFAULT_CODEGEN_DIR;
  } else {
    codegen_dir = vm["codegen-dir"].as<std::string>();
  }
  // clear codegen dir
  if (std::filesystem::exists(codegen_dir)) {
    LOG(INFO) << "codegen dir exists, clear directory";
    std::filesystem::remove_all(codegen_dir);
  } else {
    // create codegen_dir
    LOG(INFO) << "codegen dir not exists, create directory";
    std::filesystem::create_directory(codegen_dir);
  }
  return codegen_dir;
}

// parse from yaml
std::tuple<uint32_t, uint32_t> parse_from_server_config(
    const std::string& server_config_path) {
  YAML::Node config = YAML::LoadFile(server_config_path);
  uint32_t shard_num = DEFAULT_SHARD_NUM;
  uint32_t http_port = DEFAULT_HTTP_PORT;
  auto engine_node = config["compute_engine"];
  if (engine_node) {
    auto engine_type = engine_node["type"];
    if (engine_type) {
      auto engine_type_str = engine_type.as<std::string>();
      if (engine_type_str != "hiactor" && engine_type_str != "Hiactor") {
        LOG(FATAL) << "compute_engine type should be hiactor, found: "
                   << engine_type_str;
      }
    }
    auto shard_num_node = engine_node["shared_num"];
    if (shard_num_node) {
      shard_num = shard_num_node.as<uint32_t>();
    } else {
      LOG(INFO) << "shared_num not found, use default value "
                << DEFAULT_SHARD_NUM;
    }
    auto host_node = engine_node["hosts"];
    if (host_node) {
      // host node is a list
      if (host_node.IsSequence()) {
        CHECK(host_node.size() == 1)
            << "only support one host in compute_engine configuration";
        auto host_str = host_node[0].as<std::string>();
        auto port_pos = host_str.find(":");
        if (port_pos != std::string::npos) {
          http_port = std::stoi(host_str.substr(port_pos + 1));
        } else {
          LOG(FATAL) << "host_node not found, use default value ";
        }
      }
    } else {
      LOG(INFO) << "host_node not found, use default value "
                << DEFAULT_HTTP_PORT;
    }
    return std::make_tuple(shard_num, http_port);
  } else {
    LOG(FATAL) << "Fail to find compute_engine configuration";
  }
}

void load_plugins(const bpo::variables_map& vm) {
  if (vm.count("plugin-dir") == 0) {
    LOG(INFO) << "plugin-dir is not specified";
    return;
  }
  std::string plugin_dir = vm["plugin-dir"].as<std::string>();
  if (!std::filesystem::exists(plugin_dir)) {
    LOG(FATAL) << "plugin dir not exists: " << plugin_dir;
  }
  LOG(INFO) << "plugin dir: " << plugin_dir;
  if (!plugin_dir.empty()) {
    LOG(INFO) << "Load plugins from dir: " << plugin_dir;
    server::StoredProcedureManager::get().LoadFromPluginDir(plugin_dir);
  }
}

void init_codegen_proxy(const bpo::variables_map& vm) {
  std::string codegen_dir = parse_codegen_dir(vm);
  std::string codegen_bin;
  std::string gie_home;
  if (vm.count("codegen-bin") == 0) {
    LOG(INFO) << "codegen-bin is not specified";
    codegen_bin = find_codegen_bin();
  } else {
    LOG(INFO) << "codegen-bin is specified";
    codegen_bin = vm["codegen-bin"].as<std::string>();
    if (!std::filesystem::exists(codegen_bin)) {
      LOG(FATAL) << "codegen bin not exists: " << codegen_bin;
    }
  }
  std::string ir_compiler_properties;
  std::string compiler_graph_schema;
  if (vm.count("ir-compiler-prop") == 0) {
    LOG(FATAL) << "ir-compiler-prop is not specified";
  } else {
    ir_compiler_properties = vm["ir-compiler-prop"].as<std::string>();
    if (!std::filesystem::exists(ir_compiler_properties)) {
      LOG(FATAL) << "ir-compiler-prop not exists: " << ir_compiler_properties;
    }
  }
  if (vm.count("compiler-graph-schema") == 0) {
    LOG(FATAL) << "compiler-graph-schema is not specified";
  } else {
    compiler_graph_schema = vm["compiler-graph-schema"].as<std::string>();
    if (!std::filesystem::exists(compiler_graph_schema)) {
      LOG(FATAL) << "compiler-graph-schema not exists: "
                 << compiler_graph_schema;
    }
  }
  if (vm.count("gie-home") == 0) {
    LOG(FATAL) << "gie-home is not specified";
  } else {
    gie_home = vm["gie-home"].as<std::string>();
    if (!std::filesystem::exists(gie_home)) {
      LOG(FATAL) << "gie-home not exists: " << gie_home;
    }
  }
  server::CodegenProxy::get().Init(codegen_dir, codegen_bin,
                                   ir_compiler_properties,
                                   compiler_graph_schema, gie_home);
}
}  // namespace gs

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help,h", "Display help messages")(
      "server-config,c", bpo::value<std::string>(),
      "path to server config yaml")(
      "codegen-dir,d",
      bpo::value<std::string>()->default_value("/tmp/codegen/"),
      "codegen working directory")("codegen-bin,b", bpo::value<std::string>(),
                                   "codegen binary path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,a", bpo::value<std::string>(), "data directory path")(
      "bulk-load,l", bpo::value<std::string>(), "bulk-load config file")(
      "plugin-dir,p", bpo::value<std::string>(), "plugin directory path")(
      "gie-home,h", bpo::value<std::string>(), "path to gie home")(
      "ir-compiler-prop,i", bpo::value<std::string>(),
      "ir compiler property file")("compiler-graph-schema,z",
                                   bpo::value<std::string>(),
                                   "compiler graph schema file");

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }
  //// declare vars
  uint32_t shard_num;
  uint32_t http_port;
  std::string graph_schema_path;
  std::string data_path;
  std::string bulk_load_config_path;
  std::string plugin_dir;

  if (vm.count("server-config") != 0) {
    std::string server_config_path = vm["server-config"].as<std::string>();
    // check file exists
    if (!std::filesystem::exists(server_config_path)) {
      LOG(ERROR) << "server-config not exists: " << server_config_path;
      return 0;
    }
    std::tie(shard_num, http_port) =
        gs::parse_from_server_config(server_config_path);
    LOG(INFO) << "shard_num: " << shard_num << ", http_port: " << http_port;
  } else {
    LOG(FATAL) << "server-config is needed";
  }

  // init graph
  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (vm.count("bulk-load")) {
    bulk_load_config_path = vm["bulk-load"].as<std::string>();
  }

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  auto loading_config =
      gs::LoadingConfig::ParseFromYaml(schema, bulk_load_config_path);
  db.Init(schema, loading_config, data_path, shard_num);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  // loading plugin
  gs::load_plugins(vm);
  gs::init_codegen_proxy(vm);

  server::HQPSService::get().init(shard_num, http_port, false);
  server::HQPSService::get().run_and_wait_for_exit();

  return 0;
}
