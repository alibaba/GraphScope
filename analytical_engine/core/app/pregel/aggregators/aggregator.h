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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_AGGREGATOR_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_AGGREGATOR_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/app/pregel/i_vertex_program.h"

namespace gs {

enum PregelAggregatorType {
  kBoolAndAggregator = 0,
  kBoolOrAggregator = 1,
  kBoolOverwriteAggregator = 2,
  kDoubleMinAggregator = 10,
  kDoubleMaxAggregator = 11,
  kDoubleSumAggregator = 12,
  kDoubleProductAggregator = 13,
  kDoubleOverwriteAggregator = 14,
  kInt64MinAggregator = 20,
  kInt64MaxAggregator = 21,
  kInt64SumAggregator = 22,
  kInt64ProductAggregator = 23,
  kInt64OverwriteAggregator = 24,
  kTextAppendAggregator = 30,
  kEmptyAggregator = 100,
};

/**
 * @brief Aggregator is a base class for pregel program
 * @tparam AGGR_TYPE
 */
template <typename AGGR_TYPE>
class Aggregator : public IAggregator {
 public:
  virtual void Aggregate(AGGR_TYPE value) = 0;

  void SetCurrentValue(AGGR_TYPE value) { curr_value_ = value; }

  AGGR_TYPE GetCurrentValue() const { return curr_value_; }

  AGGR_TYPE GetAggregatedValue() const { return last_value_; }

  void Serialize(grape::InArchive& arc) override { arc << curr_value_; }

  void DeserializeAndAggregate(grape::OutArchive& arc) override {
    AGGR_TYPE value;
    while (!arc.Empty()) {
      arc >> value;
      Aggregate(value);
    }
  }

  void DeserializeAndAggregate(std::vector<grape::InArchive>& arcs) override {
    AGGR_TYPE value;
    for (auto& arc : arcs) {
      grape::OutArchive oarc(std::move(arc));
      while (!oarc.Empty()) {
        oarc >> value;
        Aggregate(value);
      }
    }
  }

  void StartNewRound() override {
    std::swap(curr_value_, last_value_);
    Reset();
  }

  std::shared_ptr<IAggregator> clone() override { return nullptr; }

  std::string ToString() override { return std::to_string(curr_value_); }

 private:
  // The global aggregated value are stored in `last_value_` variable,
  // which can be used in next compute step
  AGGR_TYPE curr_value_;
  AGGR_TYPE last_value_;
};

/**
 * @brief This is a specialized pregel aggregator for string type
 */
template <>
class Aggregator<std::string> : public IAggregator {
 public:
  virtual void Aggregate(std::string value) = 0;

  void SetCurrentValue(std::string value) { curr_value_ = value; }

  std::string GetCurrentValue() const { return curr_value_; }

  std::string GetAggregatedValue() const { return last_value_; }

  void Serialize(grape::InArchive& arc) override { arc << curr_value_; }

  void DeserializeAndAggregate(grape::OutArchive& arc) override {
    std::string value;
    while (!arc.Empty()) {
      arc >> value;
      Aggregate(value);
    }
  }

  void DeserializeAndAggregate(std::vector<grape::InArchive>& arcs) override {
    std::string value;
    for (auto& arc : arcs) {
      grape::OutArchive oarc(std::move(arc));
      while (!oarc.Empty()) {
        oarc >> value;
        Aggregate(value);
      }
    }
  }

  void StartNewRound() override {
    std::swap(curr_value_, last_value_);
    Reset();
  }

  std::shared_ptr<IAggregator> clone() override { return nullptr; }

  std::string ToString() override { return curr_value_; }

 private:
  std::string curr_value_;
  std::string last_value_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_AGGREGATORS_AGGREGATOR_H_
