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
#include <rapidjson/pointer.h>
#include <rapidjson/rapidjson.h>
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
#include "flex/utils/yaml_utils.h"

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <boost/filesystem.hpp>

namespace gs {

static constexpr const char* CODEGEN_BIN = "load_plan_and_gen.sh";

/// Util functions.

inline void blockSignal(int sig) {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, sig);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
    perror("pthread_sigmask");
  }
}

inline int64_t GetCurrentTimeStamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline std::string rapidjson_stringify(const rapidjson::Value& value,
                                       int indent = -1) {
  rapidjson::StringBuffer buffer;
  if (indent == -1) {
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return buffer.GetString();
  } else {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', indent);
    value.Accept(writer);
    return buffer.GetString();
  }
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
inline bool to_json(rapidjson::Document& j, const PropertyType& p) {
  if (p == PropertyType::Empty()) {
    j.AddMember("empty", "empty", j.GetAllocator());
  } else if (p == PropertyType::Bool() || p == PropertyType::UInt8() ||
             p == PropertyType::UInt16() || p == PropertyType::Int32() ||
             p == PropertyType::UInt32() || p == PropertyType::Float() ||
             p == PropertyType::Int64() || p == PropertyType::UInt64() ||
             p == PropertyType::Double()) {
    j.AddMember("primitive_type",
                gs::config_parsing::PrimitivePropertyTypeToString(p),
                j.GetAllocator());
  } else if (p == PropertyType::Date()) {
    rapidjson::Document temporal(rapidjson::kObjectType, &j.GetAllocator());
    temporal.AddMember("timestamp", "", j.GetAllocator());
    j.AddMember("temporal", temporal, j.GetAllocator());
  } else if (p == PropertyType::Day()) {
    rapidjson::Document temporal(rapidjson::kObjectType, &j.GetAllocator());
    temporal.AddMember("date32", "", j.GetAllocator());
    j.AddMember("temporal", temporal, j.GetAllocator());
  } else if (p == PropertyType::StringView() ||
             p == PropertyType::StringMap()) {
    rapidjson::Document long_text(rapidjson::kObjectType, &j.GetAllocator());
    long_text.AddMember("long_text", "", j.GetAllocator());
    j.AddMember("string", long_text, j.GetAllocator());
  } else if (p.IsVarchar()) {
    rapidjson::Document string(rapidjson::kObjectType, &j.GetAllocator());
    rapidjson::Document var_char(rapidjson::kObjectType, &j.GetAllocator());
    var_char.AddMember("max_length", p.additional_type_info.max_length,
                       j.GetAllocator());
    string.AddMember("var_char", var_char, j.GetAllocator());
    j.AddMember("string", string, j.GetAllocator());
  } else {
    LOG(ERROR) << "Unknown property type";
    return false;
  }
  return true;
}

inline rapidjson::Document to_json(
    const PropertyType& p,
    rapidjson::Document::AllocatorType* allocator = nullptr) {
  rapidjson::Document j;
  if (allocator) {
    j = rapidjson::Document(rapidjson::kObjectType, allocator);
  } else {
    j = rapidjson::Document(rapidjson::kObjectType);
  }
  if (!to_json(j, p)) {
    LOG(ERROR) << "Failed to convert PropertyType to json";
  }
  return j;
}

inline bool from_json(const rapidjson::Value& j, PropertyType& p) {
  if (j.HasMember("primitive_type")) {
    p = config_parsing::StringToPrimitivePropertyType(
        j["primitive_type"].GetString());
  } else if (j.HasMember("string")) {
    if (j["string"].HasMember("long_text")) {
      p = PropertyType::String();
    } else if (j.HasMember("string") && j["string"].HasMember("var_char")) {
      if (j["string"]["var_char"].HasMember("max_length")) {
        p = PropertyType::Varchar(
            j["string"]["var_char"]["max_length"].GetInt());
      } else {
        p = PropertyType::Varchar(PropertyType::GetStringDefaultMaxLength());
      }
    } else {
      throw std::invalid_argument("Unknown string type: " +
                                  rapidjson_stringify(j));
    }
  } else if (j.HasMember("temporal")) {
    if (j["temporal"].HasMember("timestamp")) {
      p = PropertyType::Date();
    } else if (j["temporal"].HasMember("date32")) {
      p = PropertyType::Day();
    } else {
      throw std::invalid_argument("Unknown temporal type");
    }
  } else {
    LOG(ERROR) << "Unknown property type";
    return false;
  }
  return true;
}

inline PropertyType from_json(const rapidjson::Value& j) {
  PropertyType p;
  if (!from_json(j, p)) {
    LOG(ERROR) << "Failed to convert json to PropertyType";
  }
  return p;
}

inline boost::filesystem::path get_current_binary_directory() {
  return boost::filesystem::canonical("/proc/self/exe").parent_path();
}

inline std::string jsonToString(const rapidjson::Value& json) {
  if (json.IsString()) {
    return json.GetString();
  } else {
    return rapidjson_stringify(json);
  }
}

// Get the directory of the current executable
std::string get_current_dir();

std::string find_codegen_bin();

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage();

void init_cpu_usage_watch();

std::pair<double, double> get_current_cpu_usage();

std::string memory_to_mb_str(uint64_t mem_bytes);

size_t human_readable_to_bytes(const std::string& human_readable);

}  // namespace gs

#endif  // SERVICE_UTILS_H