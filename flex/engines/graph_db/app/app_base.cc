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

SharedLibraryAppFactory::SharedLibraryAppFactory(const std::string& path)
    : app_path_(path) {
  app_handle_ = dlopen(app_path_.c_str(), RTLD_LAZY);

  *(void**) (&func_creator_) = dlsym(app_handle_, "CreateApp");
  *(void**) (&func_deletor_) = dlsym(app_handle_, "DeleteApp");
}

SharedLibraryAppFactory::~SharedLibraryAppFactory() {
  if (app_handle_ != NULL) {
    dlclose(app_handle_);
  }
}

AppWrapper SharedLibraryAppFactory::CreateApp(GraphDBSession& db) {
  AppBase* app = static_cast<AppBase*>(func_creator_(db));
  return AppWrapper(app, func_deletor_);
}

}  // namespace gs
