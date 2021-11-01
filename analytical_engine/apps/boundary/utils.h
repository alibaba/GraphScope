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

#include "folly/dynamic.h"

namespace gs {
template <typename oid_t>
oid_t dynamic_to_oid(const folly::dynamic& node) {}

template <>
int64_t dynamic_to_oid<int64_t>(const folly::dynamic& node) {
  return node.asInt();
}

template <>
std::string dynamic_to_oid<std::string>(const folly::dynamic& node) {
  return node.asString();
}

template <>
folly::dynamic dynamic_to_oid<folly::dynamic>(const folly::dynamic& node) {
  return node;
}
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_BOUNDARY_UTILS_H_
