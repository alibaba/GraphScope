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
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>
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
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <rapidjson/prettywriter.h>

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

// With the help of the following functions, we can serialize and deserialize
// by json.get<PropertyType>() and operator <</operator =;
// These two functions are inlined to avoid linking library in codegen.
inline void to_json(rapidjson::Document& j, const PropertyType& p) {
  if (p == PropertyType::Empty()) {
    rapidjson::Pointer("/").Set(j, "empty");
  } else if (p == PropertyType::Bool() || p == PropertyType::UInt8() ||
             p == PropertyType::UInt16() || p == PropertyType::Int32() ||
             p == PropertyType::UInt32() || p == PropertyType::Float() ||
             p == PropertyType::Int64() || p == PropertyType::UInt64() ||
             p == PropertyType::Double()) {
    rapidjson::Pointer("/primitive_type").Set(
        j, config_parsing::PrimitivePropertyTypeToString(p));
  } else if (p == PropertyType::Date()) {
    rapidjson::Pointer("/temporal/timestamp").Set(j, {});
  } else if (p == PropertyType::Day()) {
    rapidjson::Pointer("/temporal/date32").Set(j, {});
  } else if (p == PropertyType::StringView() ||
             p == PropertyType::StringMap()) {
    rapidjson::Pointer("/string/long_text").Set(j, {});
  } else if (p.IsVarchar()) {
    rapidjson::Pointer("/string/var_char/max_length").Set(j,
                                                         p.additional_type_info.max_length);
  } else {
    LOG(ERROR) << "Unknown property type";
  }
}

inline void from_json(const rapidjson::Value& j, PropertyType& p) {
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
        p = PropertyType::Varchar(PropertyType::STRING_DEFAULT_MAX_LENGTH);
      }
    } else {
      throw std::invalid_argument("Unknown string type");
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
  }
}

inline boost::filesystem::path get_current_binary_directory() {
  return boost::filesystem::canonical("/proc/self/exe").parent_path();
}

inline std::string rapidjson_stringify(const rapidjson::Value& value, int indent = -1) {
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

inline std::string jsonToString(const rapidjson::Value& json) {
  if (json.IsString()) {
    return json.GetString();
  } else {
    return rapidjson_stringify(json);
  }
}

class FlexException : public std::exception {
 public:
  explicit FlexException(std::string&& error_msg);
  ~FlexException() override;

  const char* what() const noexcept override;

 private:
  std::string _err_msg;
};

// Get the directory of the current executable
std::string get_current_dir();

std::string find_codegen_bin();

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage();

void init_cpu_usage_watch();

std::pair<double, double> get_current_cpu_usage();

std::string memory_to_mb_str(uint64_t mem_bytes);

}  // namespace gs

#endif  // SERVICE_UTILS_H