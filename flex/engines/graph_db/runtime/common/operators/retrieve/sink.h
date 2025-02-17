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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_SINK_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_SINK_H_

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/utils/app_utils.h"

namespace gs {
namespace runtime {

class Sink {
 public:
  template <typename GraphInterface>
  static void sink(const Context& ctx, const GraphInterface& graph,
                   Encoder& output) {
    size_t row_num = ctx.row_num();
    results::CollectiveResults results;
    for (size_t i = 0; i < row_num; ++i) {
      auto result = results.add_results();
      for (size_t j : ctx.tag_ids) {
        auto col = ctx.get(j);
        if (col == nullptr) {
          continue;
        }
        auto column = result->mutable_record()->add_columns();
        auto val = col->get_elem(i);
        val.sink(graph, j, column);
      }
    }
    auto res = results.SerializeAsString();
    output.put_bytes(res.data(), res.size());
  }

  template <typename GraphInterface>
  static void sink_encoder(const Context& ctx, const GraphInterface& graph,
                           Encoder& encoder) {
    size_t row_num = ctx.row_num();
    for (size_t i = 0; i < row_num; ++i) {
      for (size_t j : ctx.tag_ids) {
        auto col = ctx.get(j);
        if (col == nullptr) {
          continue;
        }

        auto val = col->get_elem(i);
        val.sink(graph, encoder);
      }
    }
  }

  // for debug
  template <typename GraphInterface>
  static void sink_beta(const Context& ctx, const GraphInterface& graph,
                        Encoder& output) {
    size_t row_num = ctx.row_num();
    std::stringstream ss;

    for (size_t i = 0; i < row_num; ++i) {
      for (size_t j : ctx.tag_ids) {
        auto col = ctx.get(j);
        if (col == nullptr) {
          continue;
        }
        auto val = col->get_elem(i);
        ss << val.to_string() << "|";
      }
      ss << std::endl;
    }
    ss << "========================================================="
       << std::endl;
    // std::cout << ss.str();
    auto res = ss.str();
    output.put_bytes(res.data(), res.size());
  }
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_SINK_H_