/** Copyright 2020 Alibaba Group Holding Limited.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
Author: Ma JingYuan
*/

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_UTILS_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_UTILS_H_

#include <string>
#include <vector>

#include "folly/dynamic.h"

namespace gs {
template <typename T>
void ExtractOidArrayFromDynamic(folly::dynamic node_array,
                                std::vector<T>& oid_array) {}

template <>
void ExtractOidArrayFromDynamic<int64_t>(folly::dynamic node_array,
                                         std::vector<int64_t>& oid_array) {
  for (const auto& val : node_array) {
    oid_array.push_back(val.asInt());
  }
}

template <>
void ExtractOidArrayFromDynamic<std::string>(
    folly::dynamic node_array, std::vector<std::string>& oid_array) {
  for (const auto& val : node_array) {
    oid_array.push_back(val.asString());
  }
}

template <>
void ExtractOidArrayFromDynamic<folly::dynamic>(
    folly::dynamic node_array, std::vector<folly::dynamic>& oid_array) {
  for (const auto& val : node_array) {
    oid_array.push_back(val);
  }
}
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_UTILS_H_
