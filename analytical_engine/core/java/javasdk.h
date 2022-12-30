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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_JAVASDK_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_JAVASDK_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/error.h"

namespace gs {

std::string JString2String(JNIEnv* env, jstring jStr);
bool InitWellKnownClasses(JNIEnv* env);

inline uint64_t getTotalSystemMemory();

void SetupEnv(const int local_num);

JavaVM* CreateJavaVM();

// One process can only create jvm for once.
JavaVM* GetJavaVM();

struct JNIEnvMark {
  JNIEnv* _env;

  JNIEnvMark();
  ~JNIEnvMark();
  JNIEnv* env();
};

// Create a URL class loader
jobject CreateClassLoader(JNIEnv* env, const std::string& class_path);

// For pie_default and pie_parallel context, we create a url class loader with
// no extra class path.
jobject CreateClassLoader(JNIEnv* env);

jobject CreateFFIPointer(JNIEnv* env, const char* type_name,
                         const jobject& url_class_loader, jlong pointer);

jobject LoadAndCreate(JNIEnv* env, const jobject& url_class_loader_obj,
                      const char* class_name, const char* serial_path = "");

void InvokeGC(JNIEnv* env);

std::string GetJobjectClassName(JNIEnv* env, jobject object);

char* JavaClassNameDashToSlash(const std::string& str);

// judge whether java app class instance of Communicator, if yes, we call
// the init communicator method.
void InitJavaCommunicator(JNIEnv* env, const jobject& url_class_loader,
                          const jobject& java_app, jlong app_address);

std::string GetJavaProperty(JNIEnv* env, const char* property_name);

jclass LoadClassWithClassLoader(JNIEnv* env, const jobject& url_class_loader,
                                const char* class_name);

jobject CreateGiraphAdaptor(JNIEnv* env, const char* app_class_name,
                            const jobject& fragment_obj);

jobject CreateGiraphAdaptorContext(JNIEnv* env, const char* context_class_name,
                                   const jobject& fragment_obj);

std::string exec(const char* cmd);

std::string generate_jvm_opts();
}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_JAVASDK_H_
