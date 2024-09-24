
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

#include "flex/utils/yaml_utils.h"
#include <rapidjson/document.h>
#include <rapidjson/pointer.h>
#include <rapidjson/prettywriter.h>
#include <fstream>
#include "service_utils.h"

namespace gs {
std::vector<std::string> get_yaml_files(const std::string& plugin_dir) {
  std::filesystem::path dir_path = plugin_dir;
  std::vector<std::string> res_yaml_files;
  if (!std::filesystem::exists(dir_path)) {
    return res_yaml_files;
  }

  for (auto& entry : std::filesystem::directory_iterator(dir_path)) {
    if (entry.is_regular_file() && ((entry.path().extension() == ".yaml") ||
                                    (entry.path().extension() == ".yml"))) {
      res_yaml_files.emplace_back(entry.path());
    }
  }
  return res_yaml_files;
}

void convert_yaml_node_to_json(const YAML::Node& node,
                               rapidjson::Document::AllocatorType& allocator,
                               rapidjson::Value& json) {
  try {
    switch (node.Type()) {
    case YAML::NodeType::Null: {
      json.SetNull();
      break;
    }
    case YAML::NodeType::Scalar: {
      try {
        json.SetInt(node.as<int>());
      } catch (const YAML::BadConversion& e) {
        try {
          json.SetDouble(node.as<double>());
        } catch (const YAML::BadConversion& e) {
          try {
            json.SetBool(node.as<bool>());
          } catch (const YAML::BadConversion& e) {
            json.SetString(node.as<std::string>().c_str(), allocator);
          }
        }
      }
      break;
    }
    case YAML::NodeType::Sequence: {
      json.SetArray();
      for (const auto& item : node) {
        rapidjson::Value element;
        convert_yaml_node_to_json(item, allocator, element);
        json.PushBack(element, allocator);
      }
      break;
    }
    case YAML::NodeType::Map:
      json.SetObject();
      for (const auto& pair : node) {
        rapidjson::Value key(pair.first.as<std::string>().c_str(), allocator);
        rapidjson::Value value;
        convert_yaml_node_to_json(pair.second, allocator, value);
        json.AddMember(key, value, allocator);
      }
      break;
    default:
      throw std::runtime_error("Unsupported YAML node type" +
                               std::to_string(node.Type()));
      break;
    }
  } catch (const YAML::BadConversion& e) {
    throw Status{StatusCode::IO_ERROR, e.what()};
  }
}

Result<std::string> get_json_string_from_yaml(const std::string& file_path) {
  try {
    YAML::Node config = YAML::LoadFile(file_path);
    // output config to json string
    return get_json_string_from_yaml(config);
  } catch (const YAML::BadFile& e) {
    return Result<std::string>(Status{StatusCode::IO_ERROR, e.what()});
  }
}

Result<std::string> get_json_string_from_yaml(const YAML::Node& node) {
  try {
    if (node.IsNull()) {
      return Result<std::string>(Status{StatusCode::OK, "{}"});
    }
    rapidjson::Document doc;
    convert_yaml_node_to_json(node, doc.GetAllocator(), doc);
    // return json.dump(2);  // 2 indents
    return std::string(rapidjson_stringify(doc, 2));
  } catch (const YAML::BadConversion& e) {
    return Result<std::string>(Status{StatusCode::IO_ERROR, e.what()});
  } catch (const std::runtime_error& e) {
    return Result<std::string>(Status{StatusCode::IO_ERROR, e.what()});
  } catch (...) {
    return Result<std::string>(Status{StatusCode::IO_ERROR, "Unknown error"});
  }
}

Result<std::string> get_yaml_string_from_yaml_node(const YAML::Node& node) {
  try {
    YAML::Emitter emitter;
    auto status = write_yaml_node_to_yaml_string(node, emitter);
    if (!status.ok()) {
      return Result<std::string>(status);
    }
    return std::string(emitter.c_str());
  } catch (const YAML::BadConversion& e) {
    return Result<std::string>(Status{StatusCode::IO_ERROR, e.what()});
  }
}

Status write_yaml_node_to_yaml_string(const YAML::Node& node,
                                      YAML::Emitter& emitter) {
  if (node.IsNull()) {
    emitter << YAML::Null;
    return Status::OK();
  }
  try {
    switch (node.Type()) {
    case YAML::NodeType::Scalar: {
      emitter << node.as<std::string>();
      break;
    }
    case YAML::NodeType::Sequence: {
      emitter << YAML::BeginSeq;
      for (const auto& item : node) {
        write_yaml_node_to_yaml_string(item, emitter);
      }
      emitter << YAML::EndSeq;
      break;
    }
    case YAML::NodeType::Map: {
      emitter << YAML::BeginMap;
      for (const auto& pair : node) {
        emitter << YAML::Key << pair.first.as<std::string>();
        emitter << YAML::Value;
        write_yaml_node_to_yaml_string(pair.second, emitter);
      }
      emitter << YAML::EndMap;
      break;
    }
    default:
      throw std::runtime_error("Unsupported YAML node type" +
                               std::to_string(node.Type()));
      break;
    }
  } catch (const YAML::BadConversion& e) {
    return Status{StatusCode::IO_ERROR, e.what()};
  }
  return Status::OK();
}

}  // namespace gs
