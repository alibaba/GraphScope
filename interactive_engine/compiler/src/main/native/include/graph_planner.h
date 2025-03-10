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

#include <cstring>
#include <filesystem>
#include <sstream>
#include <jni.h>
#include <string>
#include <vector>

#include "physical.pb.h"

#ifndef GRAPH_PLANNER_JNI_INVOKER
#define GRAPH_PLANNER_JNI_INVOKER 1 // 1: JNI, 0: subprocess
#endif

namespace gs
{

  struct Plan
  {
    std::string error_code;
    std::string full_message;
    physical::PhysicalPlan physical_plan;
    std::string result_schema;
  };

#if (GRAPH_PLANNER_JNI_INVOKER)
  namespace jni
  {
    struct JNIEnvMark
    {
      JNIEnv *_env;

      JNIEnvMark();
      JNIEnvMark(const std::string &jvm_options);
      ~JNIEnvMark();
      JNIEnv *env();
    };

  } // namespace jni
#endif

  class GraphPlannerWrapper
  {
  public:
    static constexpr const char *kGraphPlannerClass =
        "com/alibaba/graphscope/sdk/PlanUtils";
    static constexpr const char *GRAPH_PLANNER_FULL_NAME =
        "com.alibaba.graphscope.sdk.PlanUtils";
    static constexpr const char *kGraphPlannerMethod = "compilePlan";
    static constexpr const char *kGraphPlannerMethodSignature =
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Lcom/alibaba/graphscope/sdk/GraphPlan;";

    /**
     * @brief Constructs a new GraphPlannerWrapper object
     * @param java_path Java class path
     * @param jna_path JNA library path
     * @param graph_schema_yaml Path to the graph schema file in YAML format (optional)
     * @param graph_statistic_json Path to the graph statistics file in JSON format (optional)
     */
    GraphPlannerWrapper(const std::string java_path,
                        const std::string &jna_path,
                        const std::string &graph_schema_yaml = "",
                        const std::string &graph_statistic_json = "")
#if (GRAPH_PLANNER_JNI_INVOKER)
        : jni_wrapper_(generate_jvm_options(
              java_path, jna_path, graph_schema_yaml, graph_statistic_json))
    {
      jclass clz = jni_wrapper_.env()->FindClass(kGraphPlannerClass);
      if (clz == NULL)
      {
        std::cerr << "Fail to find class: " << kGraphPlannerClass << std::endl;
        return;
      }
      graph_planner_clz_ = (jclass)jni_wrapper_.env()->NewGlobalRef(clz);
      jmethodID j_method_id = jni_wrapper_.env()->GetStaticMethodID(
          graph_planner_clz_, kGraphPlannerMethod, kGraphPlannerMethodSignature);
      if (j_method_id == NULL)
      {
        std::cerr << "Fail to find method: " << kGraphPlannerMethod << std::endl;
        return;
      }
      graph_planner_method_id_ = j_method_id;
    }
#else
        : jna_path_("-Djna.library.path=" + jna_path),
          graph_schema_yaml_("-Dgraph.schema=" + graph_schema_yaml),
          graph_statistic_json_("-Dgraph.statistics=" + graph_statistic_json)
    {
      class_path_ = expand_directory(java_path);
    }
#endif

    ~GraphPlannerWrapper()
    {
#if (GRAPH_PLANNER_JNI_INVOKER)
      if (graph_planner_clz_ != NULL)
      {
        jni_wrapper_.env()->DeleteGlobalRef(graph_planner_clz_);
      }
#endif
    }

    inline bool is_valid()
    {
#if (GRAPH_PLANNER_JNI_INVOKER)
      return graph_planner_clz_ != NULL && graph_planner_method_id_ != NULL;
#else
      return true; // just return true, since we don't have a way to check the
                   // validity when calling via subprocess.
#endif
    }

    /**
     * @brief Invoker GraphPlanner to generate a physical plan from a cypher
     * query.
     * @param compiler_config_path The path of compiler config file.
     * @param cypher_query_string The cypher query string.
     * @return physical plan in string.
     */
    Plan CompilePlan(const std::string &compiler_config_path,
                     const std::string &cypher_query_string,
                     const std::string &graph_schema_yaml,
                     const std::string &graph_statistic_json);

  private:
    std::string generate_jvm_options(const std::string java_path,
                                     const std::string &jna_path,
                                     const std::string &graph_schema_yaml,
                                     const std::string &graph_statistic_json);
    // physical::PhysicalPlan compilePlanJNI(const std::string&
    // compiler_config_path,
    //                                       const std::string&
    //                                       cypher_query_string);
    std::string expand_directory(const std::string &path);
#if (GRAPH_PLANNER_JNI_INVOKER)
    // We need to list all files in the directory, if exists.
    // The reason why we need to list all files in the directory is that
    // java -Djava.class.path=dir/* (in jni, which we are using)will not load all
    // jar files in the directory, While java -cp dir/* will load all jar files in
    // the directory.

    gs::jni::JNIEnvMark jni_wrapper_;
    jclass graph_planner_clz_;
    jmethodID graph_planner_method_id_;
#else
    std::string class_path_;
    std::string jna_path_;
    std::string graph_schema_yaml_;
    std::string graph_statistic_json_;
#endif
  };
} // namespace gs

#endif // PLANNER_GRAPH_PLANNER_H_
