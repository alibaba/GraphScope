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
*/

#ifndef STORAGES_RT_MUTABLE_GRAPH_TYPES_H_
#define STORAGES_RT_MUTABLE_GRAPH_TYPES_H_

#include <stdint.h>
#include <iostream>

namespace gs {

enum class EdgeStrategy {
  kNone,
  kSingle,
  kMultiple,
};

using timestamp_t = uint32_t;
using vid_t = uint32_t;
using label_t = uint8_t;

// primitive types
static constexpr const char* DT_SIGNED_INT32 = "DT_SIGNED_INT32";
static constexpr const char* DT_UNSIGNED_INT32 = "DT_UNSIGNED_INT32";
static constexpr const char* DT_SIGNED_INT64 = "DT_SIGNED_INT64";
static constexpr const char* DT_UNSIGNED_INT64 = "DT_UNSIGNED_INT64";
static constexpr const char* DT_BOOL = "DT_BOOL";
static constexpr const char* DT_FLOAT = "DT_FLOAT";
static constexpr const char* DT_DOUBLE = "DT_DOUBLE";
static constexpr const char* DT_STRING = "DT_STRING";
static constexpr const char* DT_STRINGMAP = "DT_STRINGMAP";
static constexpr const char* DT_DATE = "DT_DATE32";

}  // namespace gs

namespace std {

// operator << for EdgeStrategy
inline ostream& operator<<(ostream& os, const gs::EdgeStrategy& strategy) {
  switch (strategy) {
  case gs::EdgeStrategy::kNone:
    os << "None";
    break;
  case gs::EdgeStrategy::kSingle:
    os << "Single";
    break;
  case gs::EdgeStrategy::kMultiple:
    os << "Multiple";
    break;
  default:
    os << "Unknown";
    break;
  }
  return os;
}
}  // namespace std

#endif  // STORAGES_RT_MUTABLE_GRAPH_TYPES_H_
