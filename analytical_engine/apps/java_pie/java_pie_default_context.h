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

#ifndef ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>

#include <iomanip>
#include <limits>
#include <string>
#include <vector>

#include "grape/app/context_base.h"
#include "grape/grape.h"

#include "core/error.h"
#include "core/java/javasdk.h"

namespace gs {

/**
 * @brief Driver context for Java context, work along with @see
 * gs::JavaPIEDefaultApp.
 *
 * @tparam FRAG_T Should be grape::ImmutableEdgecutFragment<...>
 */
template <typename FRAG_T>
class JavaPIEDefaultContext : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;
  explicit JavaPIEDefaultContext(const FRAG_T& fragment)
      : fragment_(fragment),
        app_class_name_(NULL),
        context_class_name_(NULL),
        app_object_(NULL),
        context_object_(NULL),
        fragment_object_(NULL),
        mm_object_(NULL),
        url_class_loader_object_(NULL) {}

  virtual ~JavaPIEDefaultContext() {
    delete[] app_class_name_;
    delete[] context_class_name_;
    JNIEnvMark m;
    if (m.env()) {
      m.env()->DeleteGlobalRef(app_object_);
      m.env()->DeleteGlobalRef(context_object_);
      m.env()->DeleteGlobalRef(fragment_object_);
      m.env()->DeleteGlobalRef(mm_object_);
      m.env()->DeleteGlobalRef(url_class_loader_object_);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }
  const fragment_t& fragment() { return fragment_; }
  const char* app_class_name() const { return app_class_name_; }
  const char* context_class_name() const { return context_class_name_; }
  const jobject& app_object() const { return app_object_; }
  const jobject& context_object() const { return context_object_; }
  const jobject& fragment_object() const { return fragment_object_; }
  const jobject& message_manager_object() const { return mm_object_; }
  const jobject& url_class_loader_object() const {
    return url_class_loader_object_;
  }

  void Init(grape::DefaultMessageManager& messages, std::string& frag_name,
            std::string& app_class_name, std::string& app_context_name,
            std::vector<std::string>& args) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      CHECK_NOTNULL(!initClassNames(app_class_name, app_context_name));

      {
        // Create a gs class loader obj which has same classPath with parent
        // classLoader.
        jobject gs_class_loader_obj = CreateClassLoader(env);
        CHECK_NOTNULL(gs_class_loader_obj);
        url_class_loader_object_ = env->NewGlobalRef(gs_class_loader_obj);
      }

      context_object_ =
          LoadAndCreate(env, url_class_loader_object_, context_class_name_);
      app_object_ =
          LoadAndCreate(env, url_class_loader_object_, app_class_name_);

      java_frag_type_name_ = frag_name;
      fragment_object_ = CreateFFIPointer(env, java_frag_type_name_.c_str(),
                                          url_class_loader_object_,
                                          reinterpret_cast<jlong>(&fragment_));
      mm_object_ = CreateFFIPointer(env, parallel_java_message_mananger_name_,
                                    url_class_loader_object_,
                                    reinterpret_cast<jlong>(&messages));

      jobject args_object = CreateFFIPointer(env, "std::vector<std::string>",
                                             url_class_loader_object_,
                                             reinterpret_cast<jlong>(&args));

      const char* descriptor =
          "(Lcom/alibaba/graphscope/fragment/IFragment;"
          "Lcom/alibaba/graphscope/parallel/DefaultMessageManager;"
          "Lcom/alibaba/graphscope/stdcxx/StdVector;)V";

      jclass context_class = env->FindClass(context_class_name_);
      CHECK_NOTNULL(context_class);
      jmethodID init_methodID =
          env->GetMethodID(context_class, "Init", descriptor);
      CHECK_NOTNULL(init_methodID);

      env->CallVoidMethod(context_object_, init_methodID, fragment_object_,
                          mm_object_, args_object);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

  void Output(std::ostream& os) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jclass context_class = env->FindClass(context_class_name_);
      CHECK_NOTNULL(context_class);

      const char* descriptor = "(Lcom/alibaba/graphscope/fragment/IFragment;)V";
      jmethodID output_methodID =
          env->GetMethodID(context_class, "Output", descriptor);
      CHECK_NOTNULL(output_methodID);
      env->CallVoidMethod(context_object_, output_methodID, fragment_object_);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

 private:
  bool initClassNames(const std::string& app_class,
                      const std::string& context_class) {
    CHECK(!app_class.empty() && !context_class.empty());
    app_class_name_ = JavaClassNameDashToSlash(app_class);
    context_class_name_ = JavaClassNameDashToSlash(context_class);
    return true;
  }
  char* app_class_name_;
  char* context_class_name_;
  const fragment_t& fragment_;
  static const char* default_java_message_mananger_name_ =
      "grape::DefaultMessageManager";
  std::string java_frag_type_name_;
  jobject app_object_;
  jobject context_object_;
  jobject fragment_object_;
  jobject mm_object_;
  jobject url_class_loader_object_;
};
}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
