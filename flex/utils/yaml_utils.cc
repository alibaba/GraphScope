
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
namespace gs {
std::vector<std::string> get_yaml_files(const std::string& plugin_dir) {
  std::filesystem::path dir_path = plugin_dir;
  std::vector<std::string> res_yaml_files;

  for (auto& entry : std::filesystem::directory_iterator(dir_path)) {
    if (entry.is_regular_file() && ((entry.path().extension() == ".yaml") ||
                                    (entry.path().extension() == ".yml"))) {
      res_yaml_files.emplace_back(entry.path());
    }
  }
  return res_yaml_files;
}

Result<std::string> get_string_from_yaml(const std::string& file_path) {
  try {
    YAML::Node config = YAML::LoadFile(file_path);
    // output config to string
    return get_string_from_yaml(config);
  } catch (const YAML::BadFile& e) {
    return Result<std::string>(Status{StatusCode::IOError, e.what()});
  } catch (const YAML::ParserException& e) {
    return Result<std::string>(Status{StatusCode::IOError, e.what()});
  } catch (const YAML::BadConversion& e) {
    return Result<std::string>(Status{StatusCode::IOError, e.what()});
  }
}

Result<std::string> get_string_from_yaml(const YAML::Node& node) {
  try {
    // output config to string
    YAML::Emitter emitter;
    emitter << YAML::DoubleQuoted << YAML::Flow << YAML::BeginSeq << node;
    std::string json(emitter.c_str() + 1);
    return Result<std::string>(Status{StatusCode::OK, "Success"}, json);
  } catch (...) {
    return Result<std::string>(
        Status{StatusCode::IOError, "Failed to convert yaml to json"});
  }
}

}  // namespace gs
