

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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/scan_utils.h"

namespace gs {
namespace runtime {
namespace ops {

typedef const std::map<std::string, std::string>& ParamsType;
template <typename T>
void parse_ids_from_idx_predicate(
    const algebra::IndexPredicate& predicate,
    std::function<std::vector<Any>(ParamsType)>& ids) {
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    std::vector<Any> ret;
    if (triplet.const_().item_case() == common::Value::kI32) {
      ret.emplace_back(static_cast<T>(triplet.const_().i32()));
    } else if (triplet.const_().item_case() == common::Value::kI64) {
      ret.emplace_back(static_cast<T>(triplet.const_().i64()));
    } else if (triplet.const_().item_case() == common::Value::kI64Array) {
      const auto& arr = triplet.const_().i64_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(static_cast<T>(arr.item(i)));
      }
    } else if (triplet.const_().item_case() == common::Value::kI32Array) {
      const auto& arr = triplet.const_().i32_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(static_cast<T>(arr.item(i)));
      }
    }
    ids = [ret = std::move(ret)](ParamsType) { return ret; };
  }

  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    auto param_type = parse_from_ir_data_type(triplet.param().data_type());

    if (param_type == RTAnyType::kI32Value) {
      ids = [triplet](ParamsType params) {
        return std::vector<Any>{
            static_cast<T>(std::stoi(params.at(triplet.param().name())))};
      };
    } else if (param_type == RTAnyType::kI64Value) {
      ids = [triplet](ParamsType params) {
        return std::vector<Any>{
            static_cast<T>(std::stoll(params.at(triplet.param().name())))};
      };
    }
  }
  default:
    break;
  }
}

void parse_ids_from_idx_predicate(
    const algebra::IndexPredicate& predicate,
    std::function<std::vector<Any>(ParamsType)>& ids) {
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  std::vector<Any> ret;
  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    if (triplet.const_().item_case() == common::Value::kStr) {
      ret.emplace_back(triplet.const_().str());
      ids = [ret = std::move(ret)](ParamsType) { return ret; };

    } else if (triplet.const_().item_case() == common::Value::kStrArray) {
      const auto& arr = triplet.const_().str_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(arr.item(i));
      }
      ids = [ret = std::move(ret)](ParamsType) { return ret; };
    }
  }

  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    auto param_type = parse_from_ir_data_type(triplet.param().data_type());

    if (param_type == RTAnyType::kStringValue) {
      ids = [triplet](ParamsType params) {
        return std::vector<Any>{params.at(triplet.param().name())};
      };
    }
  }
  default:
    break;
  }
}
std::function<std::vector<Any>(const std::map<std::string, std::string>&)>
ScanUtils::parse_ids_with_type(PropertyType type,
                               const algebra::IndexPredicate& triplet) {
  std::function<std::vector<Any>(const std::map<std::string, std::string>&)>
      ids;
  switch (type.type_enum) {
  case impl::PropertyTypeImpl::kInt64: {
    parse_ids_from_idx_predicate<int64_t>(triplet, ids);
  } break;
  case impl::PropertyTypeImpl::kInt32: {
    parse_ids_from_idx_predicate<int32_t>(triplet, ids);
  } break;
  case impl::PropertyTypeImpl::kStringView: {
    parse_ids_from_idx_predicate(triplet, ids);
  } break;
  default:
    LOG(FATAL) << "unsupported type" << static_cast<int>(type.type_enum);
    break;
  }
  return ids;
}
bool ScanUtils::check_idx_predicate(const physical::Scan& scan_opr,
                                    bool& scan_oid) {
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    return false;
  }

  if (!scan_opr.has_params()) {
    return false;
  }

  if (!scan_opr.has_idx_predicate()) {
    return false;
  }
  const algebra::IndexPredicate& predicate = scan_opr.idx_predicate();
  if (predicate.or_predicates_size() != 1) {
    return false;
  }
  if (predicate.or_predicates(0).predicates_size() != 1) {
    return false;
  }
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  if (!triplet.has_key()) {
    return false;
  }
  auto key = triplet.key();
  if (key.has_key()) {
    scan_oid = true;
  } else if (key.has_id()) {
    scan_oid = false;
  } else {
    LOG(ERROR) << "Invalid key type" << key.DebugString();
    return false;
  }

  if (triplet.cmp() != common::Logical::EQ &&
      triplet.cmp() != common::Logical::WITHIN) {
    return false;
  }

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
  } break;
  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
  } break;
  default: {
    return false;
  } break;
  }

  return true;
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs