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

#include "flex/engines/graph_db/app/cypher_app_utils.h"
#include <glog/logging.h>

#include <sys/wait.h>  // for waitpid()
#include <unistd.h>    // for fork() and execvp()
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace gs {

std::string generate_compiler_config(const std::string& schema,
                                     const std::string& statistics,
                                     const std::vector<std::string>& rules) {
  std::stringstream ss;
  std::string configs =
      "compiler:\n"
      "  planner:\n"
      "    is_on: true\n"
      "    opt: CBO\n"
      "    rules:\n";
  for (const auto& rule : rules) {
    ss << "      - " << rule << "\n";
  }
  std::string rules_str = ss.str();
  configs += rules_str;
  configs +=
      "    trim_class_names: GraphLogicalExpand\n"
      "    join_min_pattern_size: 15\n"
      "  meta:\n"
      "    reader:\n"
      "      schema:\n";
  configs += "        uri: " + schema + "\n";
  configs += "        interval: 1000 # ms\n";

  configs += "      statistics:\n";

  configs += "        uri: " + statistics + "\n";
  configs += "        interval: 86400000 # ms\n";
  configs +=
      "  endpoint:\n"
      "    default_listen_address: localhost\n"
      "  query_timeout: 40000\n"
      "  physical.opt.config: proto\n";
  return configs;
}

void generate_compiler_configs(const std::string& graph_yaml,
                               const std::string& statistics_json,
                               const std::string& path) {
  std::vector<std::string> rules = {
      "FilterIntoJoinRule",   "FilterMatchRule",      "NotMatchToAntiJoinRule",
      "ExtendIntersectRule",  "ExpandGetVFusionRule", "FlatJoinToExpandRule",
      "FlatJoinToCommonRule", "FieldTrimRule"};
  std::string compiler_config =
      generate_compiler_config(graph_yaml, statistics_json, rules);
  std::ofstream out(path);
  out << compiler_config;
  out.close();
}

bool generate_plan(const std::string& query, const std::string& statistics,
                   const std::string& compiler_jar_path,
                   const std::string& compiler_yaml, const std::string& tmp_dir,
                   physical::PhysicalPlan& plan) {
  // dump query to file
  const char* compiler_jar = compiler_jar_path.c_str();
  if (compiler_jar_path == "") {
    std::cerr << "COMPILER_JAR is not set!" << std::endl;
    compiler_jar =
        "../../interactive_engine/compiler/target/"
        "compiler-0.0.1-SNAPSHOT.jar:../../interactive_engine/compiler/target/"
        "libs/*";
  }

  auto id = std::this_thread::get_id();

  std::stringstream ss;
  ss << id;
  std::string thread_id = ss.str();
  const std::string compiler_config_path =
      tmp_dir + "/compiler_config_" + thread_id + ".yaml";
  const std::string query_file = tmp_dir + "/temp" + thread_id + ".cypher";
  const std::string output_file = tmp_dir + "/temp" + thread_id + ".pb";
  const std::string jar_path = compiler_jar;
  const std::string schema_path = "-Dgraph.schema=" + compiler_yaml;
  auto raw_query = query;
  {
    std::ofstream out(query_file);
    out << query;
    out.close();
  }
  generate_compiler_configs(compiler_yaml, statistics, compiler_config_path);

  // call compiler to generate plan
  {
    int pipefd[2];
    if (pipe(pipefd) == -1) {
      LOG(ERROR) << "pipe failed!" << strerror(errno);
      exit(EXIT_FAILURE);
    }

    pid_t pid = fork();

    if (pid == -1) {
      LOG(ERROR) << "fork failed!" << strerror(errno);
      return false;
    } else if (pid == 0) {
      const char* const args[] = {
          "java",
          "-cp",
          jar_path.c_str(),
          "com.alibaba.graphscope.common.ir.tools.GraphPlanner",
          compiler_config_path.c_str(),
          query_file.c_str(),
          output_file.c_str(),
          "temp.cypher.yaml",
          nullptr  // execvp expects a null-terminated array
      };

      close(pipefd[0]);

      if (dup2(pipefd[1], STDERR_FILENO) == -1) {
        LOG(ERROR) << "dup2 failed!" << strerror(errno);
        exit(EXIT_FAILURE);
      }

      close(pipefd[1]);

      execvp(args[0], const_cast<char* const*>(args));

      std::cerr << "Exec failed!" << std::endl;
      return false;
    } else {
      close(pipefd[1]);

      ssize_t count;
      constexpr size_t BUFFSIZ = 4096;
      char buffer[BUFFSIZ];
      std::string error_message;
      while ((count = read(pipefd[0], buffer, sizeof(buffer) - 1)) > 0) {
        buffer[count] = '\0';
        error_message += buffer;
      }

      int status;
      waitpid(pid, &status, 0);
      if (WIFEXITED(status)) {
        VLOG(1) << "Child exited with status " << WEXITSTATUS(status)
                << std::endl;
      }
      close(pipefd[0]);

      {
        std::ifstream file(output_file, std::ios::binary);

        if (!file.is_open()) {
          LOG(ERROR) << "Compiler message: " << error_message;
          return false;
        }

        file.seekg(0, std::ios::end);
        size_t size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::string buffer;
        buffer.resize(size);

        file.read(&buffer[0], size);

        file.close();
        if (!plan.ParseFromString(std::string(buffer))) {
          LOG(ERROR) << "Compiler message: " << error_message;
          return false;
        }
      }
      // clean up temp files
      {
        unlink(output_file.c_str());
        unlink(query_file.c_str());
        unlink(compiler_config_path.c_str());
      }
    }
  }

  return true;
}

void parse_params(std::string_view sw,
                  std::map<std::string, std::string>& params) {
  std::string key, value;
  size_t i = 0;
  while (i < sw.size()) {
    size_t begin = i;
    for (; i < sw.size(); ++i) {
      if (sw[i] == '=') {
        key = std::string(sw.substr(begin, i - begin));
        break;
      }
    }
    begin = ++i;
    for (; i < sw.size(); ++i) {
      if (i + 1 < sw.size() && sw[i] == '&' && sw[i + 1] == '?') {
        value = std::string(sw.substr(begin, i - begin));
        ++i;
        break;
      }
    }
    if (i == sw.size()) {
      value = std::string(sw.substr(begin, i - begin));
    }
    i++;
    params[key] = value;
  }
}

}  // namespace gs