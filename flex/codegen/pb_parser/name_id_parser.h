#ifndef NAME_ID_PARSER_H
#define NAME_ID_PARSER_H

#include <string>
#include "proto_generated_gie/common.pb.h"

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
  // if (!name_or_id.has_id()) {
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

#endif  // NAME_ID_PARSER_H