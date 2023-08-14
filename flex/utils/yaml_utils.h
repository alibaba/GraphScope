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

namespace gs {
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
    LOG(ERROR) << "Expect key: " << key << "set to " << value << " but not set";
    return false;
  }
  if (got != value) {
    LOG(ERROR) << "Expect key: " << key << "set to " << value << " but got "
               << got;
    return false;
  }
  return true;
}

static bool access_file(std::string& file_path) {
  if (file_path.size() == 0) {
    return false;
  }
  if (file_path[0] == '/') {
    std::filesystem::path path(file_path);
    return std::filesystem::exists(path);
  }
  char* flex_data_dir = std::getenv("FLEX_DATA_DIR");
  if (flex_data_dir != NULL) {
    auto temp = std::string(flex_data_dir) + "/" + file_path;
    std::filesystem::path path(temp);
    if (std::filesystem::exists(path)) {
      file_path = temp;
      return true;
    }
  }
  file_path =
      std::filesystem::current_path().generic_string() + "/" + file_path;
  std::filesystem::path path(file_path);
  return std::filesystem::exists(path);
}

}  // namespace config_parsing
}  // namespace gs

#endif  // UTILS_YAML_UTILS_H_