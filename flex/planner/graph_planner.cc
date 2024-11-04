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

#include <flex/planner/graph_planner.h>

namespace gs {
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

physical::PhysicalPlan GraphPlannerWrapper::CompilePlan(
    const std::string& compiler_config_path,
    const std::string& cypher_query_string) {
  physical::PhysicalPlan physical_plan;
  if (!is_valid()) {
    LOG(ERROR) << "Invalid GraphPlannerWrapper.";
    return physical_plan;
  }
  jstring param1 =
      jni_wrapper_.env()->NewStringUTF(compiler_config_path.c_str());
  jstring param2 =
      jni_wrapper_.env()->NewStringUTF(cypher_query_string.c_str());

  jbyteArray res = (jbyteArray) jni_wrapper_.env()->CallStaticObjectMethod(
      graph_planner_clz_, graph_planner_method_id_, param1, param2);
  if (jni_wrapper_.env()->ExceptionCheck()) {
    jni_wrapper_.env()->ExceptionDescribe();
    jni_wrapper_.env()->ExceptionClear();
    LOG(ERROR) << "Error in calling GraphPlanner.";
    return physical_plan;
  }
  if (res == NULL) {
    LOG(ERROR) << "Fail to generate plan.";
    return physical_plan;
  }
  jbyte* str = jni_wrapper_.env()->GetByteArrayElements(res, NULL);
  jsize len = jni_wrapper_.env()->GetArrayLength(res);
  LOG(INFO) << "Physical plan size: " << len;

  physical_plan.ParseFromArray(str, len);
  jni_wrapper_.env()->ReleaseByteArrayElements(res, str, 0);
  jni_wrapper_.env()->DeleteLocalRef(param1);
  jni_wrapper_.env()->DeleteLocalRef(param2);
  jni_wrapper_.env()->DeleteLocalRef(res);

  return physical_plan;
}

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

std::vector<std::string> GraphPlannerWrapper::list_files(
    const std::string& path) {
  // list all files in the directory
  std::vector<std::string> files;
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    files.push_back(entry.path().string());
  }
  return files;
}

}  // namespace jni
}  // namespace gs
