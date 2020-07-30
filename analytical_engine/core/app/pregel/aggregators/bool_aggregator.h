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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_BOOL_AGGREGATOR_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_BOOL_AGGREGATOR_H_

#include <string>

#include "core/app/pregel/aggregators/aggregator.h"

namespace gs {

/**
 * @brief Pregel aggregator for bool type. The booleans will be aggregated with
 * AND logic.
 */
class BoolAndAggregator : public Aggregator<bool> {
 public:
  void Aggregate(bool value) override {
    Aggregator<bool>::SetCurrentValue(Aggregator<bool>::GetCurrentValue() &&
                                      value);
  }

  void Init() override { Aggregator<bool>::SetCurrentValue(true); }

  void Reset() override { Aggregator<bool>::SetCurrentValue(true); }
};

/**
 * @brief Pregel aggregator for bool type. The booleans will be aggregated with
 * OR logic.
 */
class BoolOrAggregator : public Aggregator<bool> {
 public:
  void Aggregate(bool value) override {
    Aggregator<bool>::SetCurrentValue(Aggregator<bool>::GetCurrentValue() ||
                                      value);
  }

  void Init() override { Aggregator<bool>::SetCurrentValue(false); }

  void Reset() override { Aggregator<bool>::SetCurrentValue(false); }
};
/**
 * @brief Pregel aggregator for bool type. The aggregator only keeps the last
 * value.
 */
class BoolOverwriteAggregator : public Aggregator<bool> {
 public:
  void Aggregate(bool value) override {
    Aggregator<bool>::SetCurrentValue(value);
  }

  void Init() override { Aggregator<bool>::SetCurrentValue(false); }

  void Reset() override { Aggregator<bool>::SetCurrentValue(false); }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_BOOL_AGGREGATOR_H_
