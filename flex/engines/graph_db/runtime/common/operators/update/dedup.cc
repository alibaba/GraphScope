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

#include "flex/engines/graph_db/runtime/common/operators/update/dedup.h"

namespace gs {

namespace runtime {

WriteContext Dedup::dedup(const GraphInsertInterface& graph, WriteContext&& ctx,
                          const std::vector<size_t>& keys) {
  int row_num = ctx.row_num();
  if (row_num == 0) {
    return ctx;
  }
  if (keys.size() == 1) {
    std::vector<std::tuple<WriteContext::WriteParams, int>> keys_tuples;
    for (int i = 0; i < ctx.row_num(); ++i) {
      keys_tuples.emplace_back(ctx.get(keys[0]).get(i), i);
    }
    std::sort(keys_tuples.begin(), keys_tuples.end());
    std::vector<size_t> offsets;
    offsets.emplace_back(std::get<1>(keys_tuples[0]));
    for (int i = 1; i < ctx.row_num(); ++i) {
      if (!(std::get<0>(keys_tuples[i]) == std::get<0>(keys_tuples[i - 1]))) {
        offsets.emplace_back(std::get<1>(keys_tuples[i]));
      }
    }
    ctx.reshuffle(offsets);
    return ctx;
  } else if (keys.size() == 2) {
    std::vector<
        std::tuple<WriteContext::WriteParams, WriteContext::WriteParams, int>>
        keys_tuples;
    for (int i = 0; i < ctx.row_num(); ++i) {
      keys_tuples.emplace_back(ctx.get(keys[0]).get(i), ctx.get(keys[1]).get(i),
                               i);
    }
    std::sort(keys_tuples.begin(), keys_tuples.end());
    std::vector<size_t> offsets;
    offsets.emplace_back(std::get<2>(keys_tuples[0]));
    for (int i = 1; i < ctx.row_num(); ++i) {
      if (!(std::get<0>(keys_tuples[i]) == std::get<0>(keys_tuples[i - 1]) &&
            std::get<1>(keys_tuples[i]) == std::get<1>(keys_tuples[i - 1]))) {
        offsets.emplace_back(std::get<2>(keys_tuples[i]));
      }
    }
    ctx.reshuffle(offsets);
    return ctx;
  } else if (keys.size() == 3) {
    std::vector<std::tuple<WriteContext::WriteParams, WriteContext::WriteParams,
                           WriteContext::WriteParams, int>>
        keys_tuples;
    for (int i = 0; i < ctx.row_num(); ++i) {
      keys_tuples.emplace_back(ctx.get(keys[0]).get(i), ctx.get(keys[1]).get(i),
                               ctx.get(keys[2]).get(i), i);
    }
    std::sort(keys_tuples.begin(), keys_tuples.end());
    std::vector<size_t> offsets;
    offsets.emplace_back(std::get<3>(keys_tuples[0]));
    for (int i = 1; i < ctx.row_num(); ++i) {
      if (!(std::get<0>(keys_tuples[i]) == std::get<0>(keys_tuples[i - 1]) &&
            std::get<1>(keys_tuples[i]) == std::get<1>(keys_tuples[i - 1]) &&
            std::get<2>(keys_tuples[i]) == std::get<2>(keys_tuples[i - 1]))) {
        offsets.emplace_back(std::get<3>(keys_tuples[i]));
      }
    }
    ctx.reshuffle(offsets);

    return ctx;
  } else {
    std::vector<std::vector<WriteContext::WriteParams>> keys_tuples;
    for (int i = 0; i < ctx.row_num(); ++i) {
      std::vector<WriteContext::WriteParams> key;
      for (size_t k : keys) {
        key.emplace_back(ctx.get(k).get(i));
      }
      keys_tuples.emplace_back(std::move(key));
    }
    std::vector<size_t> offsets(ctx.row_num());
    std::iota(offsets.begin(), offsets.end(), 0);
    std::sort(offsets.begin(), offsets.end(),
              [&keys_tuples](size_t lhs, size_t rhs) {
                for (size_t k = 0; k < keys_tuples[lhs].size(); ++k) {
                  if (keys_tuples[lhs][k] < keys_tuples[rhs][k]) {
                    return true;
                  } else if (keys_tuples[rhs][k] < keys_tuples[lhs][k]) {
                    return false;
                  }
                }
                return lhs < rhs;
              });
    std::vector<size_t> new_offsets;
    new_offsets.push_back(offsets[0]);
    for (size_t i = 1; i < offsets.size(); ++i) {
      if (keys_tuples[offsets[i]] != keys_tuples[offsets[i - 1]]) {
        new_offsets.push_back(offsets[i]);
      }
    }
    ctx.reshuffle(new_offsets);
    return ctx;
  }
}

}  // namespace runtime

}  // namespace gs
