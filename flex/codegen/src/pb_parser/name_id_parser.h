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
#ifndef CODEGEN_SRC_PB_PARSER_NAME_ID_PARSER_H_
#define CODEGEN_SRC_PB_PARSER_NAME_ID_PARSER_H_

#include <string>
#include "flex/proto_generated_gie/common.pb.h"

namespace gs {

bool get_name_from_name_or_id(const common::NameOrId& name_or_id,
                              std::string& name) {
  if (name_or_id.item_case() == common::NameOrId::kName) {
    name = name_or_id.name();
    return true;
  } else {
    return false;
  }
}

template <
    typename LabelT,
    typename std::enable_if<std::is_same_v<LabelT, uint8_t>>::type* = nullptr>
static LabelT try_get_label_from_name_or_id(
    const common::NameOrId& name_or_id) {
  if (name_or_id.item_case() != common::NameOrId::kId) {
    LOG(FATAL) << "no id is found";
  }
  return name_or_id.id();
}

template <typename LabelT,
          typename std::enable_if<std::is_same_v<LabelT, std::string>>::type* =
              nullptr>
static LabelT try_get_label_from_name_or_id(
    const common::NameOrId& name_or_id) {
  if (name_or_id.item_case() != common::NameOrId::kName) {
    LOG(FATAL) << "no name is found";
  }
  return name_or_id.name();
}

}  // namespace gs

#endif  // CODEGEN_SRC_PB_PARSER_NAME_ID_PARSER_H_