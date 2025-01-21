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
#ifndef RUNTIME_COMMON_COLUMNS_COLUMNS_UTILS_H_
#define RUNTIME_COMMON_COLUMNS_COLUMNS_UTILS_H_

#include <algorithm>
#include <numeric>
#include <vector>

namespace gs {
class ColumnsUtils {
 public:
  template <typename VEC_T>
  static void generate_dedup_offset(const VEC_T& vec, size_t row_num,
                                    std::vector<size_t>& offsets) {
    std::vector<size_t> row_indices(row_num);
    row_indices.resize(row_num);
    std::iota(row_indices.begin(), row_indices.end(), 0);
    std::sort(row_indices.begin(), row_indices.end(),
              [&vec](size_t a, size_t b) {
                auto a_val = vec[a];
                auto b_val = vec[b];
                if (a_val == b_val) {
                  return a < b;
                }
                return a_val < b_val;
              });
    offsets.clear();
    offsets.push_back(row_indices[0]);
    for (size_t i = 1; i < row_indices.size(); ++i) {
      if (!(vec[row_indices[i]] == vec[row_indices[i - 1]])) {
        offsets.push_back(row_indices[i]);
      }
    }
  }
};
}  // namespace gs
#endif  // RUNTIME_COMMON_COLUMNS_COLUMNS_UTILS_H_