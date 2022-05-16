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

#ifndef ANALYTICAL_ENGINE_CORE_OBJECT_GS_OBJECT_H_
#define ANALYTICAL_ENGINE_CORE_OBJECT_GS_OBJECT_H_

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <utility>

namespace gs {
enum class ObjectType {
  kFragmentWrapper,
  kLabeledFragmentWrapper,
  kAppEntry,
  kContextWrapper,
  kPropertyGraphUtils,
  kProjectUtils
};

inline const char* ObjectTypeToString(ObjectType ob_type) {
  switch (ob_type) {
  case ObjectType::kFragmentWrapper:
    return "FragmentWrapper";
  case ObjectType::kLabeledFragmentWrapper:
    return "LabeledFragmentWrapper";
  case ObjectType::kAppEntry:
    return "AppEntry";
  case ObjectType::kContextWrapper:
    return "ContextWrapper";
  case ObjectType::kPropertyGraphUtils:
    return "PropertyGraphUtils";
  case ObjectType::kProjectUtils:
    return "ProjectUtils";
  default:
    CHECK(false);
  }

  return "";
}

/**
 * @brief GSObject is the base class of GraphScope object. Every object which is
 * managed by ObjectManager should inherit GSObject.
 */
class GSObject {
 public:
  explicit GSObject(std::string id, ObjectType type)
      : id_(std::move(id)), type_(type) {}

  virtual ~GSObject() {
    VLOG(10) << "Object " << id_ << "[" << ObjectTypeToString(type_) << "]"
             << " is destructed.";
  }

  const std::string& id() const { return id_; }

  ObjectType type() const { return type_; }

  virtual std::string ToString() const {
    std::stringstream ss;
    ss << "Object " << id_ << "[" << ObjectTypeToString(type_) << "]";
    return ss.str();
  }

 private:
  std::string id_;
  ObjectType type_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_GS_OBJECT_H_
