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

#include "flex/utils/file_utils.h"

namespace gs {

bool read_string_from_file(const std::string& file_path, std::string& content) {
  std::ifstream inputFile(file_path);  // Open the file for reading

  if (!inputFile.is_open()) {
    LOG(ERROR) << "Error: Could not open the file " << file_path;
    return false;
  }
  // Use a stringstream to read the entire content of the file
  std::ostringstream buffer;
  buffer << inputFile.rdbuf();
  content = buffer.str();
  return true;
}

bool write_string_to_file(const std::string& content,
                          const std::string& file_path) {
  // Open the file for writing, override the previous content
  std::ofstream outputFile(file_path, std::ios::out | std::ios::trunc);

  if (!outputFile.is_open()) {
    LOG(ERROR) << "Error: Could not open the file " << file_path;
    return false;
  }
  // Write the content to the file
  outputFile << content;
  return true;
}

}  // namespace gs
