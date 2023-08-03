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

#ifndef STORAGE_RT_MUTABLE_GRAPH_LOAD_CONFIG_H_
#define STORAGE_RT_MUTABLE_GRAPH_LOAD_CONFIG_H_

#include <string>

namespace gs {
// Provide meta info about bulk loading.
struct LoadConfig {
  std::string data_source_;  // "file", "hdfs", "oss", "s3"
  std::string delimiter_;    // "\t", ",", " ", "|"
  std::string method_;       // init, append, overwrite
};
}  // namespace gs

#endif  // STORAGE_RT_MUTABLE_GRAPH_LOAD_CONFIG_H_