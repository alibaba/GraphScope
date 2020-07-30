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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_AGGREGATOR_FACTORY_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_AGGREGATOR_FACTORY_H_

#include <memory>

#include "core/app/pregel/aggregators/bool_aggregator.h"
#include "core/app/pregel/aggregators/numeric_aggregator.h"
#include "core/app/pregel/aggregators/text_aggregator.h"

namespace gs {

/**
 * @brief AggregatorFactory is dedicated to creating pregel aggregator.
 */
class AggregatorFactory {
 public:
  static std::shared_ptr<IAggregator> CreateAggregator(
      const PregelAggregatorType type) {
    if (type == PregelAggregatorType::kBoolAndAggregator) {
      return std::shared_ptr<IAggregator>(new BoolAndAggregator());
    }
    if (type == PregelAggregatorType::kBoolOrAggregator) {
      return std::shared_ptr<IAggregator>(new BoolOrAggregator());
    }
    if (type == PregelAggregatorType::kBoolOverwriteAggregator) {
      return std::shared_ptr<IAggregator>(new BoolOverwriteAggregator());
    }
    if (type == PregelAggregatorType::kDoubleMaxAggregator) {
      return std::shared_ptr<IAggregator>(new NumericMaxAggregator<double>());
    }
    if (type == PregelAggregatorType::kDoubleMinAggregator) {
      return std::shared_ptr<IAggregator>(new NumericMinAggregator<double>());
    }
    if (type == PregelAggregatorType::kDoubleSumAggregator) {
      return std::shared_ptr<IAggregator>(new NumericSumAggregator<double>());
    }
    if (type == PregelAggregatorType::kDoubleProductAggregator) {
      return std::shared_ptr<IAggregator>(
          new NumericProductAggregator<double>());
    }
    if (type == PregelAggregatorType::kDoubleOverwriteAggregator) {
      return std::shared_ptr<IAggregator>(
          new NumericOverwriteAggregator<double>());
    }
    if (type == PregelAggregatorType::kInt64MaxAggregator) {
      return std::shared_ptr<IAggregator>(new NumericMaxAggregator<int64_t>());
    }
    if (type == PregelAggregatorType::kInt64MinAggregator) {
      return std::shared_ptr<IAggregator>(new NumericMinAggregator<int64_t>());
    }
    if (type == PregelAggregatorType::kInt64SumAggregator) {
      return std::shared_ptr<IAggregator>(new NumericSumAggregator<int64_t>());
    }
    if (type == PregelAggregatorType::kInt64ProductAggregator) {
      return std::shared_ptr<IAggregator>(
          new NumericProductAggregator<int64_t>());
    }
    if (type == PregelAggregatorType::kInt64OverwriteAggregator) {
      return std::shared_ptr<IAggregator>(
          new NumericOverwriteAggregator<int64_t>());
    }
    if (type == PregelAggregatorType::kTextAppendAggregator) {
      return std::shared_ptr<IAggregator>(new TextAppendAggregator());
    }
    LOG(ERROR) << "Unexpected python pregel aggregator type " << type;
    return nullptr;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_AGGREGATOR_FACTORY_H_
