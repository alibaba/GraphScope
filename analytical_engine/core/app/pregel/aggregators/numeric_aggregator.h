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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_NUMERIC_AGGREGATOR_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_NUMERIC_AGGREGATOR_H_

#include <algorithm>
#include <limits>
#include <string>

#include "core/app/pregel/aggregators/aggregator.h"

namespace gs {
/**
 * @brief A pregel aggregator for the numeric data type. The aggregator
 * aggregates numeric data with MIN logic.
 * @tparam AGGR_TYPE
 */
template <typename AGGR_TYPE>
class NumericMinAggregator : public Aggregator<AGGR_TYPE> {
 public:
  void Aggregate(AGGR_TYPE value) override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        std::min(Aggregator<AGGR_TYPE>::GetCurrentValue(), value));
  }

  void Init() override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        std::numeric_limits<AGGR_TYPE>::max());
  }

  void Reset() override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        std::numeric_limits<AGGR_TYPE>::max());
  }
};
/**
 * @brief A pregel aggregator for the numeric data type. The aggregator
 * aggregates numeric data with MAX logic.
 * @tparam AGGR_TYPE
 */
template <typename AGGR_TYPE>
class NumericMaxAggregator : public Aggregator<AGGR_TYPE> {
 public:
  void Aggregate(AGGR_TYPE value) override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        std::max(Aggregator<AGGR_TYPE>::GetCurrentValue(), value));
  }

  void Init() override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        std::numeric_limits<AGGR_TYPE>::min());
  }

  void Reset() override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        std::numeric_limits<AGGR_TYPE>::min());
  }
};
/**
 * @brief A pregel aggregator for the numeric data type. The aggregator
 * aggregates numeric data with SUM logic.
 * @tparam AGGR_TYPE
 */
template <typename AGGR_TYPE>
class NumericSumAggregator : public Aggregator<AGGR_TYPE> {
 public:
  void Aggregate(AGGR_TYPE value) override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        Aggregator<AGGR_TYPE>::GetCurrentValue() + value);
  }

  void Init() override { Aggregator<AGGR_TYPE>::SetCurrentValue(0); }

  void Reset() override { Aggregator<AGGR_TYPE>::SetCurrentValue(0); }
};
/**
 * @brief A pregel aggregator for the numeric data type. The aggregator
 * aggregates numeric data with PRODUCT logic.
 * @tparam AGGR_TYPE
 */
template <typename AGGR_TYPE>
class NumericProductAggregator : public Aggregator<AGGR_TYPE> {
 public:
  void Aggregate(AGGR_TYPE value) override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(
        Aggregator<AGGR_TYPE>::GetCurrentValue() * value);
  }

  void Init() override { Aggregator<AGGR_TYPE>::SetCurrentValue(1); }

  void Reset() override { Aggregator<AGGR_TYPE>::SetCurrentValue(1); }
};
/**
 * @brief A pregel aggregator for the numeric data type. This aggregator only
 * keeps last value.
 * @tparam AGGR_TYPE
 */
template <typename AGGR_TYPE>
class NumericOverwriteAggregator : public Aggregator<AGGR_TYPE> {
 public:
  void Aggregate(AGGR_TYPE value) override {
    Aggregator<AGGR_TYPE>::SetCurrentValue(value);
  }

  void Init() override { Aggregator<AGGR_TYPE>::SetCurrentValue(0); }

  void Reset() override { Aggregator<AGGR_TYPE>::SetCurrentValue(0); }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_NUMERIC_AGGREGATOR_H_
