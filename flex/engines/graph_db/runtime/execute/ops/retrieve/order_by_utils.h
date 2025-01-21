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

#ifndef RUNTIME_EXECUTE_RETRIEVE_OPS_ORDER_BY_UTILS_H_
#define RUNTIME_EXECUTE_RETRIEVE_OPS_ORDER_BY_UTILS_H_
#include "flex/engines/graph_db/runtime/utils/var.h"

namespace gs {
namespace runtime {
namespace ops {
class GeneralComparer {
 public:
  GeneralComparer() : keys_num_(0) {}
  ~GeneralComparer() {}

  void add_keys(Var&& key, bool asc) {
    keys_.emplace_back(std::move(key));
    order_.push_back(asc);
    ++keys_num_;
  }

  bool operator()(size_t lhs, size_t rhs) const {
    for (size_t k = 0; k < keys_num_; ++k) {
      auto& v = keys_[k];
      auto asc = order_[k];
      RTAny lhs_val = v.get(lhs);
      RTAny rhs_val = v.get(rhs);
      if (lhs_val < rhs_val) {
        return asc;
      } else if (rhs_val < lhs_val) {
        return !asc;
      }
    }

    return lhs < rhs;
  }

 private:
  std::vector<Var> keys_;
  std::vector<bool> order_;
  size_t keys_num_;
};
}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_EXECUTE_RETRIEVE_OPS_ORDER_BY_UTILS_H_