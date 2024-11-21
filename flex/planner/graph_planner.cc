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
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>
#include <fstream>

#include <fcntl.h>
#include <flex/planner/graph_planner.h>

namespace gs {
#if (GRAPH_PLANNER_JNI_INVOKER)
namespace jni {

static JavaVM* _jvm = NULL;

JavaVM* CreateJavaVM(const std::string& jvm_options) {
  const char *p, *q;
  const char* jvm_opts;
  if (jvm_options.empty()) {
    jvm_opts = getenv("FLEX_JVM_OPTS");
  } else {
    jvm_opts = jvm_options.c_str();
  }
  if (jvm_opts == NULL) {
    LOG(FATAL) << "Expect FLEX_JVM_OPTS set before initiate jvm";
    return NULL;
  }
  VLOG(1) << "Jvm opts str: " << jvm_opts;

  if (*jvm_opts == '\0')
    return NULL;

  int num_of_opts = 1;
  for (const char* p = jvm_opts; *p; p++) {
    if (*p == ' ')
      num_of_opts++;
  }

  if (num_of_opts == 0)
    return NULL;

  JavaVM* jvm = NULL;
  JNIEnv* env = NULL;
  int i = 0;
  int status = 1;
  JavaVMInitArgs vm_args;

  JavaVMOption* options = new JavaVMOption[num_of_opts];
  memset(options, 0, sizeof(JavaVMOption) * num_of_opts);

  for (p = q = jvm_opts;; p++) {
    if (*p == ' ' || *p == '\0') {
      if (q >= p) {
        goto ret;
      }
      char* opt = new char[p - q + 1];
      memcpy(opt, q, p - q);
      opt[p - q] = '\0';
      options[i++].optionString = opt;
      q = p + 1;  // assume opts are separated by single space
      if (*p == '\0')
        break;
    }
  }

  memset(&vm_args, 0, sizeof(vm_args));
  vm_args.version = JNI_VERSION_1_8;
  vm_args.nOptions = num_of_opts;
  vm_args.options = options;

  status = JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &vm_args);
  if (status == JNI_OK) {
    LOG(INFO) << "Create java virtual machine successfully.";
  } else if (status == JNI_EEXIST) {
    VLOG(1) << "JNI evn already exists.";
  } else {
    LOG(ERROR) << "Error, create java virtual machine failed. return JNI_CODE ("
               << status << ")\n";
  }

ret:
  for (int i = 0; i < num_of_opts; i++) {
    delete[] options[i].optionString;
  }
  delete[] options;
  return jvm;
}

// One process can only create jvm for once.
JavaVM* GetJavaVM(const std::string jvm_options = "") {
  if (_jvm == NULL) {
    // Try to find whether there exists one javaVM
    jsize nVMs;
    JNI_GetCreatedJavaVMs(NULL, 0,
                          &nVMs);  // 1. just get the required array length
    VLOG(1) << "Found " << nVMs << " VMs existing in this process.";
    JavaVM** buffer = new JavaVM*[nVMs];
    JNI_GetCreatedJavaVMs(buffer, nVMs, &nVMs);  // 2. get the data
    for (auto i = 0; i < nVMs; ++i) {
      if (buffer[i] != NULL) {
        _jvm = buffer[i];
        VLOG(1) << "Found index " << i << " VM non null "
                << reinterpret_cast<jlong>(_jvm);
        return _jvm;
      }
    }
    _jvm = CreateJavaVM(jvm_options);
    VLOG(1) << "Created JVM " << reinterpret_cast<jlong>(_jvm);
  }
  return _jvm;
}

JNIEnvMark::JNIEnvMark() : JNIEnvMark::JNIEnvMark("") {}

JNIEnvMark::JNIEnvMark(const std::string& jvm_options) : _env(NULL) {
  if (!GetJavaVM(jvm_options)) {
    return;
  }
  int status =
      GetJavaVM(jvm_options)
          ->AttachCurrentThread(reinterpret_cast<void**>(&_env), nullptr);
  if (status != JNI_OK) {
    LOG(ERROR) << "Error attach current thread: " << status;
  }
}

JNIEnvMark::~JNIEnvMark() {
  if (_env) {
    GetJavaVM()->DetachCurrentThread();
  }
}

JNIEnv* JNIEnvMark::env() { return _env; }

}  // namespace jni

#endif

std::vector<std::string> list_files(const std::string& path) {
  // list all files in the directory
  std::vector<std::string> files;
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    files.push_back(entry.path().string());
  }
  return files;
}

std::string GraphPlannerWrapper::expand_directory(const std::string& path) {
  std::vector<std::string> paths;
  std::string::size_type start = 0;
  std::string::size_type end = path.find(':');
  while (end != std::string::npos) {
    auto sub_path = path.substr(start, end - start);
    if (!sub_path.empty()) {
      if (std::filesystem::is_directory(sub_path)) {
        auto files = list_files(sub_path);
        paths.insert(paths.end(), files.begin(), files.end());
      } else {
        paths.push_back(sub_path);
      }
    }
    start = end + 1;
    end = path.find(':', start);
  }
  auto sub_path = path.substr(start);
  if (!sub_path.empty()) {
    auto files = list_files(sub_path);
    paths.insert(paths.end(), files.begin(), files.end());
  }
  std::stringstream ss;
  for (const auto& p : paths) {
    ss << p << ":";
  }
  return ss.str();
}

#if (GRAPH_PLANNER_JNI_INVOKER)

std::string GraphPlannerWrapper::generate_jvm_options(
    const std::string java_path, const std::string& jna_path,
    const std::string& graph_schema_yaml,
    const std::string& graph_statistic_json) {
  auto expanded_java_path = expand_directory(java_path);
  VLOG(10) << "Expanded java path: " << expanded_java_path;
  std::string jvm_options = "-Djava.class.path=" + expanded_java_path;
  jvm_options += " -Djna.library.path=" + jna_path;
  jvm_options += " -Dgraph.schema=" + graph_schema_yaml;
  if (!graph_statistic_json.empty()) {
    jvm_options += " -Dgraph.statistic=" + graph_statistic_json;
  }
  return jvm_options;
}

physical::PhysicalPlan compilePlanJNI(jclass graph_planner_clz_,
                                      jmethodID graph_planner_method_id_,
                                      JNIEnv* env,
                                      const std::string& compiler_config_path,
                                      const std::string& cypher_query_string) {
  jni::GetJavaVM()->AttachCurrentThread(reinterpret_cast<void**>(&env),
                                        nullptr);
  physical::PhysicalPlan physical_plan;
  if (graph_planner_clz_ == NULL || graph_planner_method_id_ == NULL) {
    LOG(ERROR) << "Invalid GraphPlannerWrapper.";
    return physical_plan;
  }
  jstring param1 = env->NewStringUTF(compiler_config_path.c_str());
  jstring param2 = env->NewStringUTF(cypher_query_string.c_str());

  jbyteArray res = (jbyteArray) env->CallStaticObjectMethod(
      graph_planner_clz_, graph_planner_method_id_, param1, param2);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(ERROR) << "Error in calling GraphPlanner.";
    return physical_plan;
  }
  if (res == NULL) {
    LOG(ERROR) << "Fail to generate plan.";
    return physical_plan;
  }
  jbyte* str = env->GetByteArrayElements(res, NULL);
  jsize len = env->GetArrayLength(res);
  LOG(INFO) << "Physical plan size: " << len;

  physical_plan.ParseFromArray(str, len);
  env->ReleaseByteArrayElements(res, str, 0);
  env->DeleteLocalRef(param1);
  env->DeleteLocalRef(param2);
  env->DeleteLocalRef(res);

  return physical_plan;
}
#endif

#if (!GRAPH_PLANNER_JNI_INVOKER)

void write_query_to_pipe(const std::string& path,
                         const std::string& query_str) {
  LOG(INFO) << "write_query_to_pipe: " << path;

  // mkfifo(path.c_str(), S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH);
  int fd_to_java = open(path.c_str(), O_WRONLY);
  if (fd_to_java < 0) {
    LOG(ERROR) << "Fail to open pipe: " << path;
    return;
  }
  LOG(INFO) << "open pipe done";
  auto len = write(fd_to_java, query_str.c_str(), query_str.size());
  if (len != (int) query_str.size()) {
    LOG(ERROR) << "Fail to write query to pipe:" << len;
    return;
  }
  LOG(INFO) << "write_query_to_pipe done: " << len;
  close(fd_to_java);
}

void write_query_to_file(const std::string& path,
                         const std::string& query_str) {
  std::ofstream query_file(path);
  query_file << query_str;
  query_file.close();
}

physical::PhysicalPlan readPhysicalPlan(const std::string& plan_str) {
  VLOG(10) << "plan str size: " << plan_str.size();
  physical::PhysicalPlan plan;
  if (!plan.ParseFromString(plan_str)) {
    LOG(ERROR) << "Fail to parse physical plan.";
    return physical::PhysicalPlan();
  }
  return plan;
}

physical::PhysicalPlan compilePlanSubprocess(
    const std::string& class_path, const std::string& jna_path,
    const std::string& graph_schema_yaml,
    const std::string& graph_statistic_json,
    const std::string& compiler_config_path,
    const std::string& cypher_query_string) {
  physical::PhysicalPlan physical_plan;
  auto random_prefix = std::to_string(
      std::chrono::system_clock::now().time_since_epoch().count());
  std::string dst_query_path = "/tmp/temp_query_" + random_prefix + ".cypher";
  std::string dst_output_file = "/tmp/temp_output_" + random_prefix + ".pb";
  VLOG(10) << "dst_query_path: " << dst_query_path
           << " dst_output_file: " << dst_output_file;
  mkfifo(dst_query_path.c_str(), S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH);
  mkfifo(dst_output_file.c_str(), S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH);

  pid_t pid = fork();

  if (pid == 0) {
    const char* const command_string_array[] = {
        "java",
        "-cp",
        class_path.c_str(),
        jna_path.c_str(),
        graph_schema_yaml.c_str(),
        graph_statistic_json.c_str(),
        "com.alibaba.graphscope.common.ir.tools.GraphPlanner",
        compiler_config_path.c_str(),
        dst_query_path.c_str(),
        dst_output_file.c_str(),
        "/tmp/temp.cypher.yaml",
        NULL};
    execvp(command_string_array[0],
           const_cast<char* const*>(command_string_array));
  } else if (pid < 0) {
    LOG(ERROR) << "Error in fork.";
  } else {
    write_query_to_pipe(dst_query_path, cypher_query_string);

    int fd_from_java = open(dst_output_file.c_str(), O_RDONLY);
    if (fd_from_java < 0) {
      LOG(ERROR) << "Fail to open pipe: " << dst_output_file;
      return physical_plan;
    }
    std::vector<char> stored_buffer;
    char buffer[128];
    while (true) {
      ssize_t bytesRead = read(fd_from_java, buffer, sizeof(buffer) - 1);
      if (bytesRead <= 0) {
        break;
      }
      stored_buffer.insert(stored_buffer.end(), buffer, buffer + bytesRead);
    }
    physical_plan = readPhysicalPlan(
        std::string(stored_buffer.begin(), stored_buffer.end()));
    close(fd_from_java);

    int status;
    waitpid(pid, &status, 0);
    if (status != 0) {
      LOG(ERROR) << "Error in running command.";
    }
  }
  unlink(dst_query_path.c_str());
  unlink(dst_output_file.c_str());
  return physical_plan;
}
#endif

physical::PhysicalPlan GraphPlannerWrapper::CompilePlan(
    const std::string& compiler_config_path,
    const std::string& cypher_query_string) {
#if (GRAPH_PLANNER_JNI_INVOKER)
  return compilePlanJNI(graph_planner_clz_, graph_planner_method_id_,
                        jni_wrapper_.env(), compiler_config_path,
                        cypher_query_string);
#else
  return compilePlanSubprocess(class_path_, jna_path_, graph_schema_yaml_,
                               graph_statistic_json_, compiler_config_path,
                               cypher_query_string);
#endif
}

}  // namespace gs
