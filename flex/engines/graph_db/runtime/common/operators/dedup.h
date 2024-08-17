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

#ifndef RUNTIME_COMMON_OPERATORS_DEDUP_H_
#define RUNTIME_COMMON_OPERATORS_DEDUP_H_

#include <set>
#include <unordered_set>

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Dedup {
 public:
  static void dedup(const ReadTransaction& txn, Context& ctx,
                    const std::vector<size_t>& cols);
  static void dedup(const ReadTransaction& txn, Context& ctx,
                    const std::vector<size_t>& cols,
                    const std::vector<std::function<RTAny(size_t)>>& vars);
  template <typename... Keys>
  static Context dedup(const ReadTransaction& txn, Context&& ctx) {
    auto row_num = ctx.row_num();
    std::vector<size_t> offsets;
    std::vector<std::tuple<decltype(std::declval<Keys>()(0))...>> vars;
    for (size_t i = 0; i < row_num; ++i) {
      offsets.emplace_back(i);
      vars.emplace_back(Keys(i)...);
    }
    std::sort(offsets.begin(), offsets.end(),
              [&vars](size_t a, size_t b) { return vars[a] < vars[b]; });
    std::vector<size_t> vec;
    for (size_t i = 0; i < offsets.size(); ++i) {
      if (i == 0 || vars[offsets[i]] != vars[offsets[i - 1]]) {
        vec.emplace_back(offsets[i]);
      }
    }
    ctx.reshuffle(vec);
    return ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_DEDUP_H_