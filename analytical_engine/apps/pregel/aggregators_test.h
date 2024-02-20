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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_AGGREGATORS_TEST_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_AGGREGATORS_TEST_H_

#include <limits>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/app/pregel/aggregators/aggregator.h"
#include "core/app/pregel/i_vertex_program.h"
#include "core/app/pregel/pregel_compute_context.h"
#include "core/app/pregel/pregel_property_app_base.h"

namespace gs {

class AggregatorsTest
    : public IPregelProgram<
          PregelPropertyVertex<
              vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                                      vineyard::property_graph_types::VID_TYPE>,
              double, double>,
          PregelPropertyComputeContext<
              vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                                      vineyard::property_graph_types::VID_TYPE>,
              double, double>> {
  using fragment_t =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;

 public:
  void Init(PregelPropertyVertex<fragment_t, double, double>& v,
            PregelPropertyComputeContext<fragment_t, double, double>& context)
      override {
    context.register_aggregator(INT64_SUM_AGGREGATOR,
                                PregelAggregatorType::kInt64SumAggregator);
    context.register_aggregator(INT64_MAX_AGGREGATOR,
                                PregelAggregatorType::kInt64MaxAggregator);
    context.register_aggregator(INT64_MIN_AGGREGATOR,
                                PregelAggregatorType::kInt64MinAggregator);
    context.register_aggregator(INT64_PRODUCT_AGGREGATOR,
                                PregelAggregatorType::kInt64ProductAggregator);
    context.register_aggregator(
        INT64_OVERWRITE_AGGREGATOR,
        PregelAggregatorType::kInt64OverwriteAggregator);

    context.register_aggregator(DOUBLE_SUM_AGGREGATOR,
                                PregelAggregatorType::kDoubleSumAggregator);
    context.register_aggregator(DOUBLE_MAX_AGGREGATOR,
                                PregelAggregatorType::kDoubleMaxAggregator);
    context.register_aggregator(DOUBLE_MIN_AGGREGATOR,
                                PregelAggregatorType::kDoubleMinAggregator);
    context.register_aggregator(DOUBLE_PRODUCT_AGGREGATOR,
                                PregelAggregatorType::kDoubleProductAggregator);
    context.register_aggregator(
        DOUBLE_OVERWRITE_AGGREGATOR,
        PregelAggregatorType::kDoubleOverwriteAggregator);

    context.register_aggregator(BOOL_AND_AGGREGATOR,
                                PregelAggregatorType::kBoolAndAggregator);
    context.register_aggregator(BOOL_OR_AGGREGATOR,
                                PregelAggregatorType::kBoolOrAggregator);
    context.register_aggregator(BOOL_OVERWRITE_AGGREGATOR,
                                PregelAggregatorType::kBoolOverwriteAggregator);

    context.register_aggregator(TEXT_APPEND_AGGREGAROR,
                                PregelAggregatorType::kTextAppendAggregator);
  }

  void Compute(grape::IteratorPair<double*> messages,
               PregelPropertyVertex<fragment_t, double, double>& v,
               PregelPropertyComputeContext<fragment_t, double, double>&
                   context) override {
    if (context.superstep() == 0) {
      context.aggregate<int64_t>(INT64_SUM_AGGREGATOR, 1);
      context.aggregate<int64_t>(INT64_MAX_AGGREGATOR, std::stoi(v.id()));
      context.aggregate<int64_t>(INT64_MIN_AGGREGATOR, std::stoi(v.id()));
      context.aggregate<int64_t>(INT64_PRODUCT_AGGREGATOR, 1);
      context.aggregate<int64_t>(INT64_OVERWRITE_AGGREGATOR, 1);

      context.aggregate<double>(DOUBLE_SUM_AGGREGATOR, 1.0);
      context.aggregate<double>(DOUBLE_MAX_AGGREGATOR, std::stod(v.id()));
      context.aggregate<double>(DOUBLE_MIN_AGGREGATOR, std::stod(v.id()));
      context.aggregate<double>(DOUBLE_PRODUCT_AGGREGATOR, 1.0);
      context.aggregate<double>(DOUBLE_OVERWRITE_AGGREGATOR, 1.0);

      context.aggregate<bool>(BOOL_AND_AGGREGATOR, true);
      context.aggregate<bool>(BOOL_OR_AGGREGATOR, false);
      context.aggregate<bool>(BOOL_OVERWRITE_AGGREGATOR, true);

      std::string text = v.id() + ',';
      context.aggregate<std::string>(TEXT_APPEND_AGGREGAROR, text);

    } else {
      if (v.id().compare("0") == 0) {
        assert(context.get_aggregated_value<int64_t>(INT64_SUM_AGGREGATOR) ==
               81307);
        assert(context.get_aggregated_value<int64_t>(INT64_MAX_AGGREGATOR) ==
               81306);
        assert(context.get_aggregated_value<int64_t>(INT64_MIN_AGGREGATOR) ==
               0);
        assert(context.get_aggregated_value<int64_t>(
                   INT64_PRODUCT_AGGREGATOR) == 1);
        assert(context.get_aggregated_value<int64_t>(
                   INT64_OVERWRITE_AGGREGATOR) == 1);

        assert(context.get_aggregated_value<double>(DOUBLE_SUM_AGGREGATOR) ==
               81307);
        assert(context.get_aggregated_value<double>(DOUBLE_MAX_AGGREGATOR) ==
               81306);
        assert(context.get_aggregated_value<double>(DOUBLE_MIN_AGGREGATOR) ==
               0);
        assert(context.get_aggregated_value<double>(
                   DOUBLE_PRODUCT_AGGREGATOR) == 1);
        assert(context.get_aggregated_value<double>(
                   DOUBLE_OVERWRITE_AGGREGATOR) == 1);

        assert(context.get_aggregated_value<bool>(BOOL_AND_AGGREGATOR) == true);
        assert(context.get_aggregated_value<bool>(BOOL_OR_AGGREGATOR) == false);
        assert(context.get_aggregated_value<bool>(BOOL_OVERWRITE_AGGREGATOR) ==
               true);

        std::vector<std::string> rlt;
        ::boost::split(
            rlt,
            context.get_aggregated_value<std::string>(TEXT_APPEND_AGGREGAROR),
            ::boost::is_any_of(","));
        assert(rlt.size() == 81308);
      }

      // terminate iterator
      v.vote_to_halt();
    }
  }

  const std::string INT64_SUM_AGGREGATOR = "int64_sum_aggregator";
  const std::string INT64_MIN_AGGREGATOR = "int64_min_aggregator";
  const std::string INT64_MAX_AGGREGATOR = "int64_max_aggregator";
  const std::string INT64_PRODUCT_AGGREGATOR = "int64_product_aggregator";
  const std::string INT64_OVERWRITE_AGGREGATOR = "int64_overwrite_aggregator";

  const std::string DOUBLE_SUM_AGGREGATOR = "double_sum_aggregator";
  const std::string DOUBLE_MIN_AGGREGATOR = "double_min_aggregator";
  const std::string DOUBLE_MAX_AGGREGATOR = "double_max_aggregator";
  const std::string DOUBLE_PRODUCT_AGGREGATOR = "double_product_aggregator";
  const std::string DOUBLE_OVERWRITE_AGGREGATOR = "double_overwrite_aggregator";

  const std::string BOOL_AND_AGGREGATOR = "bool_and_aggregator";
  const std::string BOOL_OR_AGGREGATOR = "bool_or_aggregator";
  const std::string BOOL_OVERWRITE_AGGREGATOR = "bool_overwrite_aggregator";

  const std::string TEXT_APPEND_AGGREGAROR = "text_append_aggregator";
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_AGGREGATORS_TEST_H_
