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

#include "flex/engines/hqps_db/app/interactive_app_base.h"

namespace gs {
// void put_argument(gs::Encoder& encoder, const query::Argument& argument) {
//   auto& value = argument.value();
//   auto item_case = value.item_case();
//   switch (item_case) {
//   case common::Value::kI32:
//     encoder.put_int(value.i32());
//     break;
//   case common::Value::kI64:
//     encoder.put_long(value.i64());
//     break;
//   case common::Value::kF64:
//     encoder.put_double(value.f64());
//     break;
//   case common::Value::kStr:
//     encoder.put_string(value.str());
//     break;
//   default:
//     LOG(ERROR) << "Not recognizable param type" <<
//     static_cast<int>(item_case);
//   }
// }

// bool parse_input_argument(gs::Decoder& raw_input,
//                           gs::Encoder& argument_encoder) {
//   if (raw_input.size() <= 0) {
//     LOG(ERROR) << "Invalid input size: " << raw_input.size();
//     return false;
//   }
//   query::Query cur_query;
//   if (!cur_query.ParseFromArray(raw_input.data(), raw_input.size())) {
//     LOG(ERROR) << "Fail to parse query from input content";
//     return false;
//   }
//   auto& args = cur_query.arguments();
//   for (int32_t i = 0; i < args.size(); ++i) {
//     put_argument(argument_encoder, args[i]);
//   }
//   VLOG(10) << ", num args: " << args.size();
//   return true;
// }
}  // namespace gs

// #endif  // BUILD_HQPS