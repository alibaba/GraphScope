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

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

#include <dlfcn.h>

namespace gs {

AppBase::AppMode ReadAppBase::mode() const { return AppMode::kRead; }

AppBase::AppType ReadAppBase::type() const { return AppType::kCppProcedure; }

bool ReadAppBase::run(GraphDBSession& db, Decoder& input, Encoder& output) {
  return this->Query(db, input, output);
}

AppBase::AppMode WriteAppBase::mode() const { return AppMode::kWrite; }

AppBase::AppType WriteAppBase::type() const { return AppType::kCppProcedure; }

bool WriteAppBase::run(GraphDBSession& db, Decoder& input, Encoder& output) {
  return this->Query(db, input, output);
}

SharedLibraryAppFactory::SharedLibraryAppFactory(const std::string& path)
    : app_path_(path) {
  app_handle_ = dlopen(app_path_.c_str(), RTLD_LAZY);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
    LOG(ERROR) << "Fail to open library: " << path
               << ", error: " << p_error_msg;
  }

  *(void**) (&func_creator_) = dlsym(app_handle_, "CreateApp");
  p_error_msg = dlerror();
  if (p_error_msg) {
    LOG(ERROR) << "Failed to get symbol CreateApp from " << path
               << ". Reason: " << std::string(p_error_msg);
  }
  *(void**) (&func_deletor_) = dlsym(app_handle_, "DeleteApp");
  p_error_msg = dlerror();
  if (p_error_msg) {
    LOG(ERROR) << "Failed to get symbol DeleteApp from " << path
               << ". Reason: " << std::string(p_error_msg);
  }
}

SharedLibraryAppFactory::~SharedLibraryAppFactory() {
  if (app_handle_ != NULL) {
    dlclose(app_handle_);
  }
}

AppWrapper SharedLibraryAppFactory::CreateApp(const GraphDB& db) {
  if (func_creator_ == NULL) {
    LOG(ERROR) << "Failed to create app from " << app_path_
               << ". Reason: func_creator_ is NULL";
    return AppWrapper();
  }
  AppBase* app = static_cast<AppBase*>(func_creator_(db));
  return AppWrapper(app, func_deletor_);
}

}  // namespace gs

namespace std {

std::istream& operator>>(std::istream& is, gs::AppBase::AppType& type) {
  int t;
  is >> t;
  type = static_cast<gs::AppBase::AppType>(t);
  return is;
}

std::ostream& operator<<(std::ostream& os, const gs::AppBase::AppType& type) {
  os << static_cast<int>(type);
  return os;
}

std::istream& operator>>(std::istream& is, gs::AppBase::AppMode& mode) {
  int m;
  is >> m;
  mode = static_cast<gs::AppBase::AppMode>(m);
  return is;
}

std::ostream& operator<<(std::ostream& os, const gs::AppBase::AppMode& mode) {
  os << static_cast<int>(mode);
  return os;
}

}  // namespace std
