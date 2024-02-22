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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_LIMIT_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_LIMIT_H_

#include <string>
#include <tuple>
#include <vector>

#include "flex/engines/hqps_db/core/context.h"

namespace gs {

class LimitOp {
 public:
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV>
  static auto Limit(Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                    size_t lower_bound, size_t upper_bound) {
    auto& cur_ = ctx.GetMutableHead();
    size_t cur_offset = 0;
    std::vector<size_t> new_offsets;
    new_offsets.emplace_back(0);
    upper_bound = std::min(upper_bound, cur_.Size());
    for (size_t cur_ind = 0; cur_ind < cur_.Size(); ++cur_ind) {
      if (cur_ind >= lower_bound && cur_ind < upper_bound) {
        cur_offset += 1;
      }
      new_offsets.push_back(cur_offset);
    }

    std::vector<size_t> selected_indices;
    for (size_t i = lower_bound; i < upper_bound; ++i) {
      selected_indices.push_back(i);
    }
    cur_.SubSetWithIndices(selected_indices);
    ctx.merge_offset_with_back(new_offsets);
    return std::move(ctx);
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_LIMIT_H_