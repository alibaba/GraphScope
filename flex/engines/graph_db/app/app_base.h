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

#ifndef GRAPHSCOPE_APP_BASE_H_
#define GRAPHSCOPE_APP_BASE_H_

#include "flex/engines/graph_db/app/app_utils.h"

#include <dlfcn.h>
#include <string.h>

#include <string>
#include <vector>

namespace gs {

class AppBase {
 public:
  AppBase() {}
  virtual ~AppBase() {}

  virtual bool Query(Decoder& input, Encoder& output) = 0;
};

class AppWrapper {
 public:
  AppWrapper() : app_(NULL), func_deletor_(NULL) {}
  AppWrapper(AppBase* app, void (*func_deletor)(void*))
      : app_(app), func_deletor_(func_deletor) {}
  AppWrapper(AppWrapper&& rhs) {
    app_ = rhs.app_;
    func_deletor_ = rhs.func_deletor_;
    rhs.app_ = NULL;
    rhs.func_deletor_ = NULL;
  }
  ~AppWrapper() {
    if (app_ != NULL && func_deletor_ != NULL) {
      func_deletor_(app_);
    } else if (app_ != NULL) {
      delete app_;
    }
  }

  AppWrapper& operator=(AppWrapper&& rhs) {
    app_ = rhs.app_;
    func_deletor_ = rhs.func_deletor_;
    rhs.app_ = NULL;
    rhs.func_deletor_ = NULL;
    return *this;
  }

  AppBase* app() { return app_; }
  const AppBase* app() const { return app_; }

 private:
  AppBase* app_;
  void (*func_deletor_)(void*);
};

class GraphDBSession;

class AppFactoryBase {
 public:
  AppFactoryBase() {}
  virtual ~AppFactoryBase() {}

  virtual AppWrapper CreateApp(GraphDBSession& db) = 0;
};

class SharedLibraryAppFactory : public AppFactoryBase {
 public:
  SharedLibraryAppFactory(const std::string& path);

  ~SharedLibraryAppFactory();

  AppWrapper CreateApp(GraphDBSession& db) override;

 private:
  std::string app_path_;
  void* app_handle_;

  void* (*func_creator_)(GraphDBSession&);
  void (*func_deletor_)(void*);
};

}  // namespace gs

#endif  // GRAPHSCOPE_APP_BASE_H_
