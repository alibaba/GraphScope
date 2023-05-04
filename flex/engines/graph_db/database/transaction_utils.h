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

#ifndef GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_
#define GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_

#include "flex/utils/property/types.h"
#include "glog/logging.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

inline void serialize_field(grape::InArchive& arc, const Property& prop) {
  switch (prop.type()) {
  case PropertyType::kInt32:
    arc << prop.get_value<int>();
    break;
  case PropertyType::kDate:
    arc << prop.get_value<Date>().milli_second;
    break;
  case PropertyType::kString:
    arc << prop.get_value<std::string>();
    break;
  case PropertyType::kStringView:
    arc << prop.get_value<std::string_view>();
    break;
  case PropertyType::kEmpty:
    break;
  case PropertyType::kInt64:
    arc << prop.get_value<int64_t>();
    break;
  case PropertyType::kList:
    arc << prop.get_value<std::vector<Property>>();
  default:
    LOG(FATAL) << "Unexpected property type";
  }
}

inline void deserialize_field(grape::OutArchive& arc, Property& prop) {
  switch (prop.type()) {
  case PropertyType::kInt32: {
    int val;
    arc >> val;
    prop.set_value<int>(val);
  } break;
  case PropertyType::kDate: {
    Date val;
    arc >> val.milli_second;
    prop.set_value<Date>(val);
  } break;
  case PropertyType::kString: {
    std::string val;
    arc >> val;
    prop.set_value<std::string>(val);
  } break;
  case PropertyType::kStringView: {
    std::string_view val;
    arc >> val;
    prop.set_value<std::string_view>(val);
  } break;
  case PropertyType::kEmpty:
    break;
  case PropertyType::kInt64: {
    int64_t val;
    arc >> val;
    prop.set_value<int64_t>(val);
  } break;
  case PropertyType::kList: {
    std::vector<Property> content;
    arc >> content;
    prop.set_value<std::vector<Property>>(content);
  } break;
  default:
    LOG(FATAL) << "Unexpected property type";
  }
}

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_
