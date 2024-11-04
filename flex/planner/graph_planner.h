/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef PLANNER_GRAPH_PLANNER_H_
#define PLANNER_GRAPH_PLANNER_H_

#include <jni.h>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "flex/proto_generated_gie/physical.pb.h"

#include "glog/logging.h"

namespace gs {
namespace jni {
struct JNIEnvMark {
  JNIEnv* _env;

  JNIEnvMark();
  JNIEnvMark(const std::string& jvm_options);
  ~JNIEnvMark();
  JNIEnv* env();
};

class GraphPlannerWrapper {
 public:
  static constexpr const char* kGraphPlannerClass =
      "com/alibaba/graphscope/common/ir/tools/GraphPlanner";
  static constexpr const char* kGraphPlannerMethod = "generatePhysicalPlan";
  static constexpr const char* kGraphPlannerMethodSignature =
      "(Ljava/lang/String;Ljava/lang/String;)[B";

  GraphPlannerWrapper(const std::string java_path, const std::string& jna_path,
                      const std::string& graph_schema_yaml,
                      const std::string& graph_statistic_json = "")
      : jni_wrapper_(generate_jvm_options(
            java_path, jna_path, graph_schema_yaml, graph_statistic_json)) {
    jclass clz = jni_wrapper_.env()->FindClass(kGraphPlannerClass);
    if (clz == NULL) {
      LOG(ERROR) << "Fail to find class: " << kGraphPlannerClass;
      return;
    }
    graph_planner_clz_ = (jclass) jni_wrapper_.env()->NewGlobalRef(clz);
    jmethodID j_method_id = jni_wrapper_.env()->GetStaticMethodID(
        graph_planner_clz_, kGraphPlannerMethod, kGraphPlannerMethodSignature);
    if (j_method_id == NULL) {
      LOG(ERROR) << "Fail to find method: " << kGraphPlannerMethod;
      return;
    }
    graph_planner_method_id_ = j_method_id;
  }

  ~GraphPlannerWrapper() {
    if (graph_planner_clz_ != NULL) {
      jni_wrapper_.env()->DeleteGlobalRef(graph_planner_clz_);
    }
  }

  inline bool is_valid() {
    return graph_planner_clz_ != NULL && graph_planner_method_id_ != NULL;
  }

  /**
   * @brief Invoker GraphPlanner to generate a physical plan from a cypher
   * query.
   * @param compiler_config_path The path of compiler config file.
   * @param cypher_query_string The cypher query string.
   * @return physical plan in string.
   */
  physical::PhysicalPlan CompilePlan(const std::string& compiler_config_path,
                                     const std::string& cypher_query_string);

 private:
  std::string generate_jvm_options(const std::string java_path,
                                   const std::string& jna_path,
                                   const std::string& graph_schema_yaml,
                                   const std::string& graph_statistic_json);

  // We need to list all files in the directory, if exists.
  // The reason why we need to list all files in the directory is that
  // java -Djava.class.path=dir/* (in jni, which we are using)will not load all
  // jar files in the directory, While java -cp dir/* will load all jar files in
  // the directory.
  std::string expand_directory(const std::string& path);
  std::vector<std::string> list_files(const std::string& path);

  JNIEnvMark jni_wrapper_;
  jclass graph_planner_clz_;
  jmethodID graph_planner_method_id_;
};

}  // namespace jni
}  // namespace gs

#endif  // PLANNER_GRAPH_PLANNER_H_