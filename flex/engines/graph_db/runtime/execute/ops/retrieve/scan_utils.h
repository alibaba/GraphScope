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

#ifndef RUNTIME_EXECUTE_RETRIEVE_OPS_SCAN_UTILS_H_
#define RUNTIME_EXECUTE_RETRIEVE_OPS_SCAN_UTILS_H_
#include "flex/engines/graph_db/runtime/execute/operator.h"

namespace gs {
namespace runtime {
namespace ops {

class ScanUtils {
 public:
  static std::function<
      std::vector<Any>(const std::map<std::string, std::string>&)>
  parse_ids_with_type(PropertyType type,
                      const algebra::IndexPredicate& triplet);

  static bool check_idx_predicate(const physical::Scan& scan_opr,
                                  bool& scan_oid);
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_EXECUTE_RETRIEVE_OPS_SCAN_UTILS_H_