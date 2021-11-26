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
      InvokeGC(m.env());
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

 protected:
  virtual const char* evalDescriptor() = 0;
  void init(jlong messages_addr, const char* java_message_manager_name,
            const std::string& params, const std::string& lib_path) {
    if (params.empty()) {
      LOG(ERROR) << "no args received";
      return;
    }
    std::string user_library_name;
    std::string user_class_path;
    std::string args_str = parseParamsAndSetupJVMEnv(
        params, lib_path, user_library_name, user_class_path);

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

      // Create a graphscope class loader to load app_class and ctx_class. This
      // means will create a new class loader for each for run_app.
      // The intent is to provide isolation, and avoid class conflicts。
      {
        jobject gs_class_loader_obj = CreateClassLoader(env, user_class_path);
        CHECK_NOTNULL(gs_class_loader_obj);
        url_class_loader_object_ = env->NewGlobalRef(gs_class_loader_obj);
      }

      {
        if (!user_library_name.empty()) {
          // Since we load loadLibraryClass with urlClassLoader, the
          // fromClass.classLoader literal, which is used in System.load, should
          // be urlClassLoader.
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

      {
        VLOG(1) << "Creating app object: " << app_class_name_;
        app_object_ =
            LoadAndCreate(env, url_class_loader_object_, app_class_name_);
        VLOG(1) << "Successfully created app object with class loader:"
                << &url_class_loader_object_
                << ", of type: " << std::string(app_class_name_);
      }

      {
        std::string _context_class_name_str = getCtxClassNameFromAppObject(env);
        VLOG(1) << "Context class name: " << _context_class_name_str;
        context_object_ = LoadAndCreate(env, url_class_loader_object_,
                                        _context_class_name_str.c_str());
        VLOG(1) << "Successfully created ctx object with class loader:"
                << &url_class_loader_object_
                << ", of type: " << _context_class_name_str;
      }
      jclass context_class = env->GetObjectClass(context_object_);
      CHECK_NOTNULL(context_class);

      jmethodID init_method_id =
          env->GetMethodID(context_class, "init", evalDescriptor());
      CHECK_NOTNULL(init_method_id);

      jobject fragObject = CreateFFIPointer(
          env, graph_type_str_.c_str(), url_class_loader_object_,
          reinterpret_cast<jlong>(&fragment_));
      CHECK_NOTNULL(fragObject);
      fragment_object_ = env->NewGlobalRef(fragObject);

      // 2. Create Message manager Java object
      jobject messagesObject =
          CreateFFIPointer(env, java_message_manager_name,
                           url_class_loader_object_, messages_addr);
      CHECK_NOTNULL(messagesObject);
      mm_object_ = env->NewGlobalRef(messagesObject);

      // 3. Create arguments array
      {
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
        VLOG(1) << "User defined kw args: " << args_str;
        jstring args_jstring = env->NewStringUTF(args_str.c_str());
        jobject json_object =
            env->CallStaticObjectMethod(json_class, parse_method, args_jstring);
        CHECK_NOTNULL(json_object);

        // 4. Invoke java method
        env->CallVoidMethod(context_object_, init_method_id, fragment_object_,
                            mm_object_, json_object);
        if (env->ExceptionCheck()) {
          env->ExceptionDescribe();
          env->ExceptionClear();
          LOG(ERROR) << "Exception in context Init";
        }
        VLOG(1) << "Successfully invokd ctx init method.";
        // 5. to output the result, we need the c++ context held by java object.
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
   * @note lib_path is the path to user_app lib, if empty, we will skip llvm4jni
   * and gs-ffi-gen in generated class path, and we will take jar_name as full
   * path.
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

      std::string jar_unpack_path = lib_dir.string();
      jar_unpack_path += "/";
      jar_unpack_path += jar_name;

      snprintf(user_class_path, sizeof(user_class_path),
               "%s:/usr/local/lib:/opt/graphscope/lib:%s:%s:%s",
               lib_dir.string().c_str(), llvm4jni_output_dir.c_str(),
               java_codegen_cp.c_str(), jar_unpack_path.c_str());
    } else {
      snprintf(user_class_path, sizeof(user_class_path),
               "/usr/local/lib:/opt/graphscope/lib:%s", jar_name.c_str());
    }

    return std::move(std::string(user_class_path));
  }
  // user library name should be absolute
  std::string parseParamsAndSetupJVMEnv(const std::string& params,
                                        const std::string lib_path,
                                        std::string& user_library_name,
                                        std::string& user_class_path) {
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

    VLOG(1) << "Received json: " << params;
    std::string frag_name = pt.get<std::string>("frag_name");
    CHECK(!frag_name.empty());
    VLOG(1) << "Parse frag name: " << frag_name;
    graph_type_str_ = frag_name;
    pt.erase("frag_name");

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
    boost::filesystem::path lib_path_fs, lib_dir;
    if (!lib_path.empty()) {
      lib_path_fs = lib_path;
      lib_dir = lib_path_fs.branch_path();

      user_library_name = lib_path_fs.string();
      CHECK(!user_library_name.empty());
      VLOG(1) << "User library name " << user_library_name;
    }

    user_class_path = libPath2UserClassPath(lib_dir, lib_path_fs, jar_name);
    VLOG(1) << "user cp: " << user_class_path;

    // JVM runtime opt should consists of java.libaray.path and
    // java.class.path maybe this should be set by the backend not user.
    if (getenv("GRAPE_JVM_OPTS")) {
      VLOG(1) << "OK, GRAPE_JVM_OPTS has been set.";
    } else {
      LOG(ERROR) << "Cannot find GRAPE_JVM_OPTS env";
    }
    SetupEnv(1);
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
    CHECK_NOTNULL(context_class_jstring);
    return JString2String(env, context_class_jstring);
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
