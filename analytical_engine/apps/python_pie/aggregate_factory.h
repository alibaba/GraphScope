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

#ifndef ANALYTICAL_ENGINE_APPS_PYTHON_PIE_AGGREGATE_FACTORY_H_
#define ANALYTICAL_ENGINE_APPS_PYTHON_PIE_AGGREGATE_FACTORY_H_

#include <string>

namespace gs {

enum PIEAggregateType {
  kMinAggregate = 0,
  kMaxAggregate = 1,
  kSumAggregate = 2,
  kProductAggregate = 3,
  kOverwriteAggregate = 4,
  kTextAppendAggregate = 5,
  kEmptyAggregate = 100,
};

class AggregateFactory {
 public:
  template <typename T>
  static std::function<bool(T*, T&&)> CreateAggregate(
      const PIEAggregateType type);
};

template <typename T>
inline std::function<bool(T*, T&&)> AggregateFactory::CreateAggregate(
    const PIEAggregateType type) {
  if (type == PIEAggregateType::kMinAggregate) {
    return [](T* lhs, T&& rhs) {
      if (*lhs > rhs) {
        *lhs = rhs;
        return true;
      }
      return false;
    };
  }
  if (type == PIEAggregateType::kMaxAggregate) {
    return [](T* lhs, T&& rhs) {
      if (*lhs < rhs) {
        *lhs = rhs;
        return true;
      }
      return false;
    };
  }
  if (type == PIEAggregateType::kSumAggregate) {
    return [](T* lhs, T&& rhs) {
      *lhs += rhs;
      return true;
    };
  }
  if (type == PIEAggregateType::kProductAggregate) {
    return [](T* lhs, T&& rhs) {
      *lhs *= rhs;
      return true;
    };
  }
  if (type == PIEAggregateType::kOverwriteAggregate) {
    return [](T* lhs, T&& rhs) {
      *lhs = rhs;
      return true;
    };
  }
  LOG(ERROR) << "Unexpected python pregel aggregator type " << type;
  return nullptr;
}

template <>
inline std::function<bool(std::string*, std::string&&)>
AggregateFactory::CreateAggregate(const PIEAggregateType type) {
  if (type == PIEAggregateType::kTextAppendAggregate) {
    return [](std::string* lhs, std::string&& rhs) {
      lhs->append(rhs);
      return true;
    };
  }
  LOG(ERROR) << "Unexpected python pie aggregator type" << type;
  return nullptr;
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PYTHON_PIE_AGGREGATE_FACTORY_H_
