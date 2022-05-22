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

#ifndef ANALYTICAL_ENGINE_CORE_OBJECT_OBJECT_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_OBJECT_OBJECT_MANAGER_H_

#include <glog/logging.h>

#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "boost/leaf/result.hpp"

#include "core/error.h"
#include "core/object/gs_object.h"

namespace bl = boost::leaf;

namespace gs {
/**
 * @brief ObjectManager manages GSObject like fragment wrapper, loaded app and
 * more.
 */
class ObjectManager {
 public:
  bl::result<void> PutObject(std::shared_ptr<GSObject> obj) {
    auto& id = obj->id();

    DLOG(INFO) << "[object manager] putting " << id;

    if (objects.find(id) != objects.end()) {
      auto existed_obj_type = objects[id]->type();
      std::stringstream ss;
      ss << "Object " << id << "[" << ObjectTypeToString(existed_obj_type)
         << "]"
         << " already exists.";
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError, ss.str());
    }
    objects[id] = std::move(obj);
    return {};
  }

  bl::result<void> RemoveObject(const std::string& id) {
    DLOG(INFO) << "[object manager] removing " << id;
    if (objects.find(id) != objects.end()) {
      objects.erase(id);
    }
    return {};
  }

  bl::result<std::shared_ptr<GSObject>> GetObject(const std::string& id) {
    DLOG(INFO) << "[object manager] getting " << id;
    if (objects.find(id) == objects.end()) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                      "Object " + id + " does not exist");
    }
    return objects[id];
  }

  template <typename T>
  bl::result<std::shared_ptr<T>> GetObject(const std::string& id) {
    DLOG(INFO) << "[object manager] getting typed " << id;
    if (objects.find(id) == objects.end()) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                      "Object " + id + " does not exist");
    }
    auto obj = std::dynamic_pointer_cast<T>(objects[id]);

    if (obj == nullptr) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "GSObject failed to cast. T may be invalid");
    }
    return obj;
  }

  bool HasObject(const std::string& id) {
    DLOG(INFO) << "[object manager] has " << id;
    return objects.find(id) != objects.end();
  }

 private:
  std::map<std::string, std::shared_ptr<GSObject>> objects;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_OBJECT_MANAGER_H_
