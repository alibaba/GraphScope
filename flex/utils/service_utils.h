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
#ifndef SERVICE_UTILS_H
#define SERVICE_UTILS_H

#include <fcntl.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "flex/utils/property/types.h"
#include "flex/utils/result.h"
#include "flex/utils/yaml_utils.h"
#include "nlohmann/json.hpp"

#include <glog/logging.h>
#include <boost/filesystem.hpp>

namespace gs {

static constexpr const char* CODEGEN_BIN = "load_plan_and_gen.sh";

/// Util functions.
inline int64_t GetCurrentTimeStamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline std::string toUpper(const std::string str) {
  std::string upper_str = str;
  std::transform(upper_str.begin(), upper_str.end(), upper_str.begin(),
                 ::toupper);
  return upper_str;
}

// With the help of the following functions, we can serialize and deserialize
// by json.get<PropertyType>() and operator <</operator =;
// These two functions are inlined to avoid linking library in codegen.
inline void to_json(nlohmann::json& j, const PropertyType& p) {
  if (p == PropertyType::Empty()) {
    j = "empty";
  } else if (p == PropertyType::Bool() || p == PropertyType::UInt8() ||
             p == PropertyType::UInt16() || p == PropertyType::Int32() ||
             p == PropertyType::UInt32() || p == PropertyType::Float() ||
             p == PropertyType::Int64() || p == PropertyType::UInt64() ||
             p == PropertyType::Double()) {
    j["primitive_type"] = config_parsing::PrimitivePropertyTypeToString(p);
  } else if (p == PropertyType::Date()) {
    j["temporal"]["timestamp"] = {};
  } else if (p == PropertyType::Day()) {
    j["temporal"]["date32"] = {};
  } else if (p == PropertyType::StringView() ||
             p == PropertyType::StringMap()) {
    j["string"]["long_text"] = {};
  } else if (p.IsVarchar()) {
    j["string"]["var_char"]["max_length"] = p.additional_type_info.max_length;
  } else {
    LOG(ERROR) << "Unknown property type";
  }
}

inline void from_json(const nlohmann::json& j, PropertyType& p) {
  if (j.contains("primitive_type")) {
    p = config_parsing::StringToPrimitivePropertyType(
        j["primitive_type"].get<std::string>());
  } else if (j.contains("string")) {
    if (j["string"].contains("long_text")) {
      p = PropertyType::String();
    } else if (j.contains("string") && j["string"].contains("var_char")) {
      if (j["string"]["var_char"].contains("max_length")) {
        p = PropertyType::Varchar(
            j["string"]["var_char"]["max_length"].get<int32_t>());
      } else {
        p = PropertyType::Varchar(PropertyType::STRING_DEFAULT_MAX_LENGTH);
      }
    } else {
      throw std::invalid_argument("Unknown string type");
    }
  } else if (j.contains("temporal")) {
    if (j["temporal"].contains("timestamp")) {
      p = PropertyType::Date();
    } else if (j["temporal"].contains("date32")) {
      p = PropertyType::Day();
    } else {
      throw std::invalid_argument("Unknown temporal type");
    }
  } else {
    LOG(ERROR) << "Unknown property type";
  }
}

inline boost::filesystem::path get_current_binary_directory() {
  return boost::filesystem::canonical("/proc/self/exe").parent_path();
}

inline std::string jsonToString(const nlohmann::json& json) {
  if (json.is_string()) {
    return json.get<std::string>();
  } else {
    return json.dump();
  }
}

// Get the directory of the current executable
std::string get_current_dir();

std::string find_codegen_bin();

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage();

void init_cpu_usage_watch();

std::pair<double, double> get_current_cpu_usage();

std::string memory_to_mb_str(uint64_t mem_bytes);

}  // namespace gs

#endif  // SERVICE_UTILS_H