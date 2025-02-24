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

#ifndef RUNTIME_UTILS_PARAMS_H_
#define RUNTIME_UTILS_PARAMS_H_

#include "flex/engines/graph_db/runtime/common/types.h"

namespace gs {
namespace runtime {

struct ScanParams {
  int alias;
  std::vector<label_t> tables;
  int32_t limit;

  ScanParams() : alias(-1), limit(std::numeric_limits<int32_t>::max()) {}
};

struct GetVParams {
  VOpt opt;
  int tag;
  std::vector<label_t> tables;
  int alias;
};

struct EdgeExpandParams {
  int v_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  bool is_optional;
};

}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_UTILS_PARAMS_H_