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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

#include "grape/app/context_base.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#include "core/config.h"
#include "core/context/labeled_vertex_property_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"
#include "core/error.h"
#include "core/java/javasdk.h"
#include "core/object/i_fragment_wrapper.h"

namespace gs {
static constexpr const char* APP_CONTEXT_GETTER_CLASS =
    "com/alibaba/graphscope/utils/AppContextGetter";
static constexpr const char* LOAD_LIBRARY_CLASS =
    "com/alibaba/graphscope/utils/LoadLibrary";
static constexpr const char* CONTEXT_UTILS_CLASS =
    "com/alibaba/graphscope/utils/ContextUtils";
static constexpr const char* JSON_CLASS_NAME = "com.alibaba.fastjson.JSON";
static constexpr const char* IFRAGMENT_HELPER_CLASS =
    "com.alibaba.graphscope.runtime.IFragmentHelper";
static constexpr const char* SET_CLASS_LOADER_METHOD_SIG =
    "(Ljava/net/URLClassLoader;)V";

/**
 * @brief JavaContextBase is the base class for JavaPropertyContext and
 * JavaProjectedContext.
 *
 */
template <typename FRAG_T>
class JavaContextBase : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;

  explicit JavaContextBase(const FRAG_T& fragment)
      : app_class_name_(NULL),
        inner_ctx_addr_(0),
        fragment_(fragment),
        app_object_(NULL),
        context_object_(NULL),
        fragment_object_(NULL),
        mm_object_(NULL),
        url_class_loader_object_(NULL) {}

  virtual ~JavaContextBase() {
    if (app_class_name_) {
      delete[] app_class_name_;
    }
    JNIEnvMark m;
    if (m.env()) {
      m.env()->DeleteGlobalRef(url_class_loader_object_);
      VLOG(1) << "Delete URL class loader";
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }
  const fragment_t& fragment() const { return fragment_; }

  const char* app_class_name() const { return app_class_name_; }

  uint64_t inner_context_addr() { return inner_ctx_addr_; }

  const std::string& graph_type_str() const { return graph_type_str_; }

  const jobject& app_object() const { return app_object_; }
  const jobject& context_object() const { return context_object_; }
  const jobject& fragment_object() const { return fragment_object_; }
  const jobject& message_manager_object() const { return mm_object_; }
  const jobject& url_class_loader_object() const {
    return url_class_loader_object_;
  }

  virtual void Output(std::ostream& os) = 0;

  // copy context data stored in java back to cpp context by invoking this
  // method.
  // FIXME(zhanglei) wrap the result
  void WriteBackJVMHeapToCppContext() {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      CHECK_NOTNULL(context_object_);
      jclass context_class = env->GetObjectClass(context_object_);
      CHECK_NOTNULL(context_class);

      jmethodID write_back_method_id =
          env->GetMethodID(context_class, "writeBackVertexData", "()V");
      if (write_back_method_id) {
        env->CallVoidMethod(context_object_, write_back_method_id);
        if (env->ExceptionCheck()) {
          LOG(ERROR) << "Exception occurred when loading user library";
          env->ExceptionDescribe();
          env->ExceptionClear();
          LOG(ERROR) << "Exception occurred when calling write back method";
        }
      } else {
        VLOG(2) << "Not write back method found";
      }
    }
  }

 protected:
  virtual const char* evalDescriptor() = 0;

  // Set frag_group_id to zero inidicate not available.
  void init(jlong messages_addr, const char* java_message_manager_name,
            const std::string& params, const std::string& lib_path,
            int local_num = 1) {
    if (params.empty()) {
      LOG(ERROR) << "no args received";
      return;
    }
    std::string user_library_name, user_class_path, graphx_context_name,
        serial_path;  // the later two should only used by graphx
    std::string args_str = parseParamsAndSetupJVMEnv(
        params, lib_path, user_library_name, user_class_path,
        graphx_context_name, serial_path, local_num);

    JavaVM* jvm = GetJavaVM();
    (void) jvm;
    CHECK_NOTNULL(jvm);
    VLOG(1) << "Successfully get jvm";

    // It is possible for multiple java app run is one grape instance, we need
    // to find the user jar. But it is not possible to restart a jvm with new
    // class path, so we utilize java class loader to load the new jar
    // add_class_path_at_runtime();

    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      // Create a graphscope class loader to load app_class and ctx_class.
      // This means will create a new class loader for each for run_app. The
      // intent is to provide isolation, and avoid class conflicts。
      {
        jobject gs_class_loader_obj = CreateClassLoader(env, user_class_path);
        CHECK_NOTNULL(gs_class_loader_obj);
        url_class_loader_object_ = env->NewGlobalRef(gs_class_loader_obj);
      }

      VLOG(1) << "Creating app object: " << app_class_name_;
      app_object_ = LoadAndCreate(env, url_class_loader_object_,
                                  app_class_name_, serial_path.c_str());
      VLOG(1) << "Successfully created app object with class loader:"
              << &url_class_loader_object_
              << ", of type: " << std::string(app_class_name_);

      createContextObj(env, graphx_context_name, serial_path);
      jclass context_class = env->GetObjectClass(context_object_);
      CHECK_NOTNULL(context_class);

      jmethodID init_method_id =
          env->GetMethodID(context_class, "Init", evalDescriptor());
      CHECK_NOTNULL(init_method_id);

      jobject fragObject = CreateFFIPointer(
          env, graph_type_str_.c_str(), url_class_loader_object_,
          reinterpret_cast<jlong>(&fragment_));
      CHECK_NOTNULL(fragObject);
      fragment_object_ = wrapFragObj(env, fragObject);

      // 2. Create Message manager Java object
      jobject messagesObject =
          CreateFFIPointer(env, java_message_manager_name,
                           url_class_loader_object_, messages_addr);
      CHECK_NOTNULL(messagesObject);
      mm_object_ = env->NewGlobalRef(messagesObject);

      // 3. Create arguments array
      {
        jobject json_object = createArgsObject(env, args_str);
        // 3.1 If we find a setClassLoaderMethod, then we invoke.(NOt
        // neccessary) this is specially for giraph adaptors
        setContextClassLoader(env, context_class);

        // 4. Invoke java method
        env->CallVoidMethod(context_object_, init_method_id, fragment_object_,
                            mm_object_, json_object);
        if (env->ExceptionCheck()) {
          env->ExceptionDescribe();
          env->ExceptionClear();
          LOG(ERROR) << "Exception in context Init";
        }
        VLOG(1) << "Successfully invokd ctx init method.";
        // 5. to output the result, we need the c++ context held by java
        // object.
        jfieldID inner_ctx_address_field =
            env->GetFieldID(context_class, "ffiContextAddress", "J");
        CHECK_NOTNULL(inner_ctx_address_field);

        inner_ctx_addr_ =
            env->GetLongField(context_object_, inner_ctx_address_field);
        CHECK_NE(inner_ctx_addr_, 0);
        VLOG(1) << "Successfully obtained inner ctx address";
      }
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

 private:
  /**
   * @brief Generate user class path, i.e. URLClassLoader class path, from lib
   * path.
   *
   * @note lib_path is the path to user_app lib, if empty, we will skip
   * llvm4jni and gs-ffi-gen in generated class path, and we will take
   * jar_name as full path.
   *
   */
  std::string libPath2UserClassPath(const boost::filesystem::path& lib_dir,
                                    const boost::filesystem::path& lib_path,
                                    const std::string& jar_name) {
    char user_class_path[40000];
    if (!lib_path.empty() && !lib_dir.empty()) {
      std::string udf_work_space = lib_dir.branch_path().string();

      std::string llvm4jni_output_dir = udf_work_space;
      llvm4jni_output_dir += "/user-llvm4jni-output-";
      llvm4jni_output_dir += lib_path.filename().string();

      std::string java_codegen_cp = udf_work_space;
      java_codegen_cp += "/gs-ffi-";
      std::string lib_path_str = lib_path.filename().string();
      java_codegen_cp += lib_path_str.substr(3, lib_path_str.size() - 6);

      // There are cases(giraph) where jar_name can be full
      // path(/tmp/gs/session/resource/....), so we judge whether this case.

      snprintf(user_class_path, sizeof(user_class_path),
               "%s:/usr/local/lib:/opt/graphscope/lib:%s:%s/CLASS_OUTPUT/:%s",
               lib_dir.string().c_str(), llvm4jni_output_dir.c_str(),
               java_codegen_cp.c_str(), jar_name.c_str());
    } else {
      // for giraph_runner testing, user jar can be absolute path.
      snprintf(user_class_path, sizeof(user_class_path),
               "/usr/local/lib:/opt/graphscope/lib:%s", jar_name.c_str());
    }

    return std::string(user_class_path);
  }
  // user library name should be absolute
  // serial path is used in graphx, to specify the path to serializaed class
  // objects of vd,ed.etc.
  std::string parseParamsAndSetupJVMEnv(const std::string& params,
                                        const std::string lib_path,
                                        std::string& user_library_name,
                                        std::string& user_class_path,
                                        std::string& graphx_context_class_name,
                                        std::string& serial_path,
                                        int local_num) {
    boost::property_tree::ptree pt;
    std::stringstream ss;
    {
      ss << params;
      try {
        boost::property_tree::read_json(ss, pt);
      } catch (boost::property_tree::ptree_error& r) {
        LOG(ERROR) << "parse json failed: " << params;
      }
    }

    std::string frag_name = pt.get<std::string>("frag_name");
    CHECK(!frag_name.empty());
    VLOG(1) << "Parse frag name: " << frag_name;
    graph_type_str_ = frag_name;
    // pt.erase("frag_name");

    std::string jar_name = pt.get<std::string>("jar_name");
    CHECK(!jar_name.empty());
    VLOG(1) << "Parse jar name: " << jar_name;

    std::string app_class_name = pt.get<std::string>("app_class");
    CHECK(!app_class_name.empty());
    VLOG(1) << "Parse app class name: " << app_class_name;
    const char* ch = app_class_name.c_str();
    app_class_name_ = new char[strlen(ch) + 1];
    memcpy(app_class_name_, ch, strlen(ch));
    app_class_name_[strlen(ch)] = '\0';
    pt.erase("app_class");

    auto iter = pt.find("graphx_context_class");
    if (iter != pt.not_found()) {
      graphx_context_class_name = pt.get<std::string>("graphx_context_class");
    }

    serial_path = pt.get<std::string>("serial_path", "");

    boost::filesystem::path lib_path_fs, lib_dir;
    if (!lib_path.empty()) {
      lib_path_fs = lib_path;
      lib_dir = lib_path_fs.branch_path();

      user_library_name = lib_path_fs.string();
      CHECK(!user_library_name.empty());
      VLOG(1) << "User library name " << user_library_name;
    }

    user_class_path = libPath2UserClassPath(lib_dir, lib_path_fs, jar_name);
    VLOG(10) << "user class path: " << user_class_path;

    // Giraph adaptor context need to map java graph data to
    // vineyard_id(frag_group_id)
    // pt.put("vineyard_id", frag_group_id);

    // JVM runtime opt should consists of java.libaray.path and
    // java.class.path maybe this should be set by the backend not user.
    std::string grape_jvm_opt = generate_jvm_opts();
    if (!grape_jvm_opt.empty()) {
      putenv(const_cast<char*>(grape_jvm_opt.data()));
      VLOG(10) << "Find GRAPE_JVM_OPTS in params, setting to env..."
               << grape_jvm_opt;
    }

    if (getenv("GRAPE_JVM_OPTS")) {
      VLOG(1) << "OK, GRAPE_JVM_OPTS has been set.";
    } else {
      LOG(ERROR) << "Cannot find GRAPE_JVM_OPTS env";
    }
    SetupEnv(local_num);
    ss.str("");  // reset the stream buffer
    boost::property_tree::json_parser::write_json(ss, pt);
    return ss.str();
  }

  // get the java context name with is bounded to app_object_.
  std::string getCtxClassNameFromAppObject(JNIEnv* env) {
    jclass app_context_getter_class = (jclass) LoadClassWithClassLoader(
        env, url_class_loader_object_, APP_CONTEXT_GETTER_CLASS);
    if (env->ExceptionCheck()) {
      LOG(ERROR) << "Exception in loading class: "
                 << std::string(APP_CONTEXT_GETTER_CLASS);
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(ERROR) << "exiting since exception occurred";
    }

    CHECK_NOTNULL(app_context_getter_class);

    jmethodID app_context_getter_method =
        env->GetStaticMethodID(app_context_getter_class, "getContextName",
                               "(Ljava/lang/Object;)Ljava/lang/String;");
    CHECK_NOTNULL(app_context_getter_method);
    // Pass app class's class object
    jstring context_class_jstring = (jstring) env->CallStaticObjectMethod(
        app_context_getter_class, app_context_getter_method, app_object_);
    if (env->ExceptionCheck()) {
      LOG(ERROR) << "Exception occurred when get context class string";
      env->ExceptionDescribe();
      env->ExceptionClear();
    }
    CHECK_NOTNULL(context_class_jstring);
    return JString2String(env, context_class_jstring);
  }
  // Judge the jar contained in jar_name actually exists
  bool preprocess_jar_name(const std::string jar_name) {
    // first split by ";"
    std::string delimiter_char = ":";
    std::string token;
    size_t pos = 0, first = 0;
    std::vector<std::string> res;
    while ((pos = jar_name.find(delimiter_char, first)) != std::string::npos) {
      token = jar_name.substr(first, pos - first);
      res.push_back(token);
      first = pos + 1;
    }
    res.push_back(jar_name.substr(first));
    //
    if (res.empty()) {
      LOG(ERROR) << "Empty jar name";
      return false;
    }
    for (auto jar : res) {
      std::ifstream f(jar.c_str());
      if (!f.good()) {
        return false;
      }
      f.close();
    }
    return true;
  }

  void loadJNILibrary(JNIEnv* env, const std::string& user_library_name) {
    if (!user_library_name.empty()) {
      // Since we load loadLibraryClass with urlClassLoader, the
      // fromClass.classLoader literal, which is used in System.load,
      // should be urlClassLoader.
      jclass load_library_class = (jclass) LoadClassWithClassLoader(
          env, url_class_loader_object_, LOAD_LIBRARY_CLASS);
      CHECK_NOTNULL(load_library_class);
      jstring user_library_jstring =
          env->NewStringUTF(user_library_name.c_str());
      jmethodID load_library_methodID = env->GetStaticMethodID(
          load_library_class, "invoke", "(Ljava/lang/String;)V");

      // call static method
      env->CallStaticVoidMethod(load_library_class, load_library_methodID,
                                user_library_jstring);
      if (env->ExceptionCheck()) {
        LOG(ERROR) << "Exception occurred when loading user library";
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exiting since exception occurred";
      }
      VLOG(1) << "Loaded specified user jni library: " << user_library_name;
    }
  }

  void createContextObj(JNIEnv* env, const std::string& graphx_context_name,
                        const std::string& serial_path) {
    if (graphx_context_name.size() != 0 &&
        (graphx_context_name.find("com.alibaba.graphscope.context."
                                  "GraphXParallelAdaptorContext") !=
         std::string::npos)) {
      context_object_ =
          LoadAndCreate(env, url_class_loader_object_,
                        graphx_context_name.c_str(), serial_path.c_str());
      VLOG(1) << "Succcessfully loaded graphx context: " << context_object_;
    } else {
      std::string _context_class_name_str = getCtxClassNameFromAppObject(env);
      VLOG(1) << "Context class name: " << _context_class_name_str;
      context_object_ =
          LoadAndCreate(env, url_class_loader_object_,
                        _context_class_name_str.c_str(), serial_path.c_str());
      VLOG(1) << "Successfully created ctx object with class loader:"
              << &url_class_loader_object_
              << ", of type: " << _context_class_name_str;
    }
  }

  jobject wrapFragObj(JNIEnv* env, jobject& fragObject) {
    if (graph_type_str_.find("Immutable") != std::string::npos ||
        graph_type_str_.find("ArrowProjected") != std::string::npos) {
      VLOG(10) << "Creating IFragment";
      // jobject fragment_object_impl_ = env->NewGlobalRef(fragObject);
      // For immutableFragment and ArrowProjectedFragment, we use a wrapper
      // Load IFragmentHelper class, and call it functions.
      jclass ifragment_helper_clz = (jclass) LoadClassWithClassLoader(
          env, url_class_loader_object_, IFRAGMENT_HELPER_CLASS);
      CHECK_NOTNULL(ifragment_helper_clz);
      jmethodID adapt2SimpleFragment_methodID = env->GetStaticMethodID(
          ifragment_helper_clz, "adapt2SimpleFragment",
          "(Ljava/lang/Object;)Lcom/alibaba/graphscope/fragment/"
          "IFragment;");
      CHECK_NOTNULL(adapt2SimpleFragment_methodID);

      jobject res = (jobject) env->CallStaticObjectMethod(
          ifragment_helper_clz, adapt2SimpleFragment_methodID, fragObject);
      CHECK_NOTNULL(res);
      return env->NewGlobalRef(res);
    } else {
      return env->NewGlobalRef(fragObject);
      VLOG(1) << "Creating ArrowFragment";
    }
  }

  jobject createArgsObject(JNIEnv* env, const std::string& args_str) {
    jclass json_class = (jclass) LoadClassWithClassLoader(
        env, url_class_loader_object_, JSON_CLASS_NAME);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(ERROR) << "Exception in loading json class ";
    }
    CHECK_NOTNULL(json_class);
    jmethodID parse_method = env->GetStaticMethodID(
        json_class, "parseObject",
        "(Ljava/lang/String;)Lcom/alibaba/fastjson/JSONObject;");
    CHECK_NOTNULL(parse_method);
    VLOG(10) << "User defined kw args: " << args_str;
    jstring args_jstring = env->NewStringUTF(args_str.c_str());
    jobject json_object =
        env->CallStaticObjectMethod(json_class, parse_method, args_jstring);
    CHECK_NOTNULL(json_object);
    return json_object;
  }

  void setContextClassLoader(JNIEnv* env, jclass& context_class) {
    jmethodID set_class_loader_method = env->GetMethodID(
        context_class, "setClassLoader", SET_CLASS_LOADER_METHOD_SIG);
    if (set_class_loader_method) {
      env->CallVoidMethod(context_object_, set_class_loader_method,
                          url_class_loader_object_);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in set Class loader";
        return;
      }
      VLOG(1) << "Successfully set class loader";
    } else {
      VLOG(2) << "No class loader available to set for ctx";
    }
  }

  std::string graph_type_str_;
  char* app_class_name_;
  uint64_t inner_ctx_addr_;
  const fragment_t& fragment_;

  jobject app_object_;
  jobject context_object_;
  jobject fragment_object_;
  jobject mm_object_;
  jobject url_class_loader_object_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_
