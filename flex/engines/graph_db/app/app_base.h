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

#include "flex/utils/app_utils.h"

#include <dlfcn.h>
#include <string.h>

#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace gs {

class GraphDBSession;
class GraphDB;
class AppBase {
 public:
  enum class AppType : uint8_t {
    kCppProcedure = 0,
    kCypherProcedure = 1,
    kCypherAdhoc = 2,
    kBuiltIn = 3,
  };

  enum class AppMode : uint8_t {
    kRead = 0,
    kWrite = 1,
  };

  virtual AppType type() const = 0;
  virtual AppMode mode() const = 0;
  virtual bool run(GraphDBSession& db, Decoder& input, Encoder& output) = 0;
  virtual ~AppBase() {}
};

class ReadAppBase : public AppBase {
 public:
  AppMode mode() const override;

  AppType type() const override;

  bool run(GraphDBSession& db, Decoder& input, Encoder& output) override;

  virtual bool Query(const GraphDBSession& db, Decoder& input,
                     Encoder& output) = 0;
};

class WriteAppBase : public AppBase {
 public:
  AppMode mode() const override;

  AppType type() const override;

  bool run(GraphDBSession& db, Decoder& input, Encoder& output) override;

  virtual bool Query(GraphDBSession& db, Decoder& input, Encoder& output) = 0;
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

  virtual AppWrapper CreateApp(const GraphDB& db) = 0;
};

class SharedLibraryAppFactory : public AppFactoryBase {
 public:
  SharedLibraryAppFactory(const std::string& path);

  ~SharedLibraryAppFactory();

  AppWrapper CreateApp(const GraphDB& db) override;

 private:
  std::string app_path_;
  void* app_handle_;

  void* (*func_creator_)(const GraphDB&);
  void (*func_deletor_)(void*);
};

struct AppMetric {
  AppMetric()
      : total_(0),
        min_val_(std::numeric_limits<int64_t>::max()),
        max_val_(0),
        count_(0) {}
  ~AppMetric() {}

  void add_record(int64_t val) {
    total_ += val;
    min_val_ = std::min(min_val_, val);
    max_val_ = std::max(max_val_, val);
    ++count_;
  }

  bool empty() const { return (count_ == 0); }

  AppMetric& operator+=(const AppMetric& rhs) {
    total_ += rhs.total_;
    min_val_ = std::min(min_val_, rhs.min_val_);
    max_val_ = std::max(max_val_, rhs.max_val_);
    count_ += rhs.count_;

    return *this;
  }

  void output(const std::string& name) const {
    LOG(INFO) << "Query - " << name << ":";
    LOG(INFO) << "\tcount: " << count_;
    LOG(INFO) << "\tmin: " << min_val_;
    LOG(INFO) << "\tmax: " << max_val_;
    LOG(INFO) << "\tavg: "
              << static_cast<double>(total_) / static_cast<double>(count_);
  }

  int64_t total_;
  int64_t min_val_;
  int64_t max_val_;
  int64_t count_;
};

}  // namespace gs

namespace std {
std::istream& operator>>(std::istream& in, gs::AppBase::AppType& type);
std::ostream& operator<<(std::ostream& out, const gs::AppBase::AppType& type);

std::istream& operator>>(std::istream& in, gs::AppBase::AppMode& mode);
std::ostream& operator<<(std::ostream& out, const gs::AppBase::AppMode& mode);

}  // namespace std

#endif  // GRAPHSCOPE_APP_BASE_H_
