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

#include "glog/logging.h"

namespace gs {

std::vector<std::string> get_yaml_files(const std::string& plugin_dir);

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
// When file_path is absolute path, try to find the file in the absolute path.
// When data_location is give, try to find the file in data_location first.
// When data_location is not given, try to find the file Under FLEX_DATA_DIR
// When FLEX_DATA_DIR is not set, try to find the file under current path.

static bool access_file(const std::string data_location,
                        std::string& file_path) {
  if (file_path.size() == 0) {
    return false;
  }
  if (file_path[0] == '/') {
    std::filesystem::path path(file_path);
    return std::filesystem::exists(path);
  }

  std::string real_location;
  if (!data_location.empty()) {
    real_location = data_location;
  } else if (std::getenv("FLEX_DATA_DIR") != NULL) {
    real_location = std::string(std::getenv("FLEX_DATA_DIR"));
  } else {
    real_location = std::filesystem::current_path().generic_string();
  }

  file_path = real_location + "/" + file_path;
  std::filesystem::path path(file_path);
  return std::filesystem::exists(path);
}

}  // namespace config_parsing
}  // namespace gs

#endif  // UTILS_YAML_UTILS_H_