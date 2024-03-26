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

#ifndef FLEX_INTERACTIVE_EXAMPLES_LDBC_PLUGINS_INTERACTIVE_UTILS_H_
#define FLEX_INTERACTIVE_EXAMPLES_LDBC_PLUGINS_INTERACTIVE_UTILS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"

namespace gs {
void encode_ic1_result(const results::CollectiveResults& ic1_result,
                       Encoder& encoder) {
  auto size = ic1_result.results_size();
  for (int32_t i = 0; i < size; ++i) {
    auto& result = ic1_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i32();
    const auto& personLastName =
        result.record().columns(0).entry().element().object().str();
    const auto& person_distance =
        result.record().columns(0).entry().element().object().i32();
    LOG(INFO) << "personId: " << personId
              << " personLastName: " << personLastName
              << " person_distance: " << person_distance;

    LOG(INFO) << result.DebugString();
  }
}
}  // namespace gs

#endif  // FLEX_INTERACTIVE_EXAMPLES_LDBC_PLUGINS_INTERACTIVE_UTILS_H_