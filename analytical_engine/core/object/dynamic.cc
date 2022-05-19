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

#ifdef NETWORKX
#include "core/object/dynamic.h"

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>

/* static definition */
gs::dynamic::AllocatorT gs::dynamic::Value::allocator_{};

std::size_t gs::dynamic::Value::hash() const {
  switch (GetType()) {
  case rapidjson::kNullType:
    return 0xBAAAAAAD;
  case rapidjson::kArrayType: {
    std::size_t hash_value = 0;
    for (const auto& val : GetArray()) {
      if (val.IsString()) {
        hash_value += std::hash<std::string>()(val.GetString());
      } else if (val.IsInt64()) {
        hash_value += std::hash<int64_t>()(val.GetInt64());
      } else if (val.IsDouble()) {
        hash_value += std::hash<double>()(val.GetDouble());
      }
    }
    return hash_value;
  }
  case rapidjson::kNumberType: {
    if (IsDouble()) {
      return std::hash<double>()(GetDouble());
    } else {
      return std::hash<int64_t>()(GetInt64());
    }
  }
  case rapidjson::kTrueType:
  case rapidjson::kFalseType:
    return std::hash<bool>()(GetBool());
  case rapidjson::kStringType:
    return std::hash<std::string>()(GetString());
  case rapidjson::kObjectType:
    throw std::runtime_error("Object value can't not be hashed.");
  }
  return rapidjson::kNullType;  // avoid -Wreturn-type warnings
}

#endif  // NETWORKX
