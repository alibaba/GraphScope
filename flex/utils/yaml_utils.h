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

#ifndef UTILS_YAML_UTILS_H_
#define UTILS_YAML_UTILS_H_

#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <string>
#include <vector>
#include "flex/utils/result.h"

#include "glog/logging.h"

namespace gs {

std::vector<std::string> get_yaml_files(const std::string& plugin_dir);

Result<std::string> get_json_string_from_yaml(const std::string& file_path);

Result<std::string> get_json_string_from_yaml(const YAML::Node& yaml_node);

Status write_yaml_node_to_yaml_string(const YAML::Node& node,
                                      YAML::Emitter& emitter);

Result<std::string> get_yaml_string_from_yaml_node(const YAML::Node& node);

namespace config_parsing {
template <typename T>
bool get_scalar(YAML::Node node, const std::string& key, T& value) {
  YAML::Node cur = node[key];
  if (cur && cur.IsScalar()) {
    value = cur.as<T>();
    return true;
  }
  return false;
}

template <typename T>
bool get_sequence(YAML::Node node, const std::string& key,
                  std::vector<T>& seq) {
  YAML::Node cur = node[key];
  if (cur && cur.IsSequence()) {
    int num = cur.size();
    seq.clear();
    for (int i = 0; i < num; ++i) {
      seq.push_back(cur[i].as<T>());
    }
    return true;
  }
  return false;
}

template <typename V>
static bool expect_config(YAML::Node root, const std::string& key,
                          const V& value) {
  V got;
  if (!get_scalar(root, key, got)) {
    LOG(ERROR) << "Expect key: " << key << " set to " << value
               << " but not set";
    return false;
  }
  if (got != value) {
    LOG(ERROR) << "Expect key: " << key << " set to " << value << " but got "
               << got;
    return false;
  }
  return true;
}

}  // namespace config_parsing
}  // namespace gs

#endif  // UTILS_YAML_UTILS_H_