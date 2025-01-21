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
#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_ORDER_BY_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_ORDER_BY_H_

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"

#include <queue>

namespace gs {

namespace runtime {

class OrderBy {
 public:
  template <typename Comparer>
  static Context order_by_with_limit(const GraphReadInterface& graph,
                                     Context&& ctx, const Comparer& cmp,
                                     size_t low, size_t high) {
    if (low == 0 && high >= ctx.row_num()) {
      std::vector<size_t> offsets(ctx.row_num());
      std::iota(offsets.begin(), offsets.end(), 0);
      std::sort(offsets.begin(), offsets.end(),
                [&](size_t lhs, size_t rhs) { return cmp(lhs, rhs); });
      ctx.reshuffle(offsets);
      return ctx;
    }
    size_t row_num = ctx.row_num();
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (size_t i = 0; i < row_num; ++i) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    std::vector<size_t> offsets;
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }

    ctx.reshuffle(offsets);
    return ctx;
  }

  template <typename Comparer>
  static Context staged_order_by_with_limit(
      const GraphReadInterface& graph, Context&& ctx, const Comparer& cmp,
      size_t low, size_t high, const std::vector<size_t>& indices) {
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (auto i : indices) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    std::vector<size_t> offsets;
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }

    ctx.reshuffle(offsets);
    return ctx;
  }

  template <typename Comparer>
  static Context order_by_with_limit_with_indices(
      const GraphReadInterface& graph, Context&& ctx,
      std::function<std::optional<std::vector<size_t>>(
          const GraphReadInterface&, const Context& ctx)>
          indices,
      const Comparer& cmp, size_t low, size_t high) {
    size_t row_num = ctx.row_num();
    if (row_num <= high) {
      return order_by_with_limit(graph, std::move(ctx), cmp, low, high);
    }
    auto _indices = indices(graph, ctx);
    if (!_indices.has_value()) {
      return order_by_with_limit(graph, std::move(ctx), cmp, low, high);
    } else {
      return staged_order_by_with_limit(graph, std::move(ctx), cmp, low, high,
                                        _indices.value());
    }
  }
};
}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_ORDER_BY_H_