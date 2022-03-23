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

#ifndef ANALYTICAL_ENGINE_APPS_BOUNDARY_UTILS_H_
#define ANALYTICAL_ENGINE_APPS_BOUNDARY_UTILS_H_

#include <string>

#include "core/object/dynamic.h"

namespace gs {
template <typename oid_t>
oid_t dynamic_to_oid(const rapidjson::Value& node) {}

template <>
int64_t dynamic_to_oid<int64_t>(const rapidjson::Value& node) {
  return node.GetInt64();
}

template <>
std::string dynamic_to_oid<std::string>(const rapidjson::Value& node) {
  return node.GetString();
}

template <>
dynamic::Value dynamic_to_oid<dynamic::Value>(const rapidjson::Value& node) {
  return dynamic::Value(node);
}
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDARY_UTILS_H_
