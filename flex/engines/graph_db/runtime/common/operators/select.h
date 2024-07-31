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

#ifndef RUNTIME_COMMON_OPERATORS_SELECT_H_
#define RUNTIME_COMMON_OPERATORS_SELECT_H_

#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Select {
 public:
  template <typename PRED_T>
  static void select(Context& ctx, const PRED_T& pred) {
    size_t row_num = ctx.row_num();
    std::vector<size_t> offsets;
    for (size_t k = 0; k < row_num; ++k) {
      if (pred(k)) {
        offsets.push_back(k);
      }
    }

    ctx.reshuffle(offsets);
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_SELECT_H_