
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
    if (entry.is_regular_file() && (entry.path().extension() == ".yaml") ||
        (entry.path().extension() == ".yml")) {
      res_yaml_files.emplace_back(entry.path());
    }
  }
  return res_yaml_files;
}

}  // namespace gs
