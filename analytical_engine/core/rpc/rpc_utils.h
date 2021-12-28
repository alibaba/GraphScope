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

#ifndef ANALYTICAL_ENGINE_CORE_RPC_RPC_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_RPC_RPC_UTILS_H_

#include <exception>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <utility>

#include "core/error.h"
#include "core/rpc/command_detail.h"
#include "core/utils/grape_utils.h"

#include "google/protobuf/util/json_util.h"
#include "proto/graphscope/proto/op_def.pb.h"

namespace gs {
namespace rpc {

inline boost::leaf::result<DagDef> ReadDagFromFile(
    const std::string& location) {
  std::ifstream ifs(location);
  std::string dag_str((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));

  DagDef dag_def;
  auto stat = google::protobuf::util::JsonStringToMessage(dag_str, &dag_def);
  if (!stat.ok()) {
    RETURN_GS_ERROR(ErrorCode::kIOError,
                    "Failed to parse: " + stat.message().ToString());
  }
  return dag_def;
}

inline CommandDetail OpToCmd(const OpDef& op) {
  auto op_type = op.op();
  std::map<int, rpc::AttrValue> params;

  for (auto& pair : op.attr()) {
    params[pair.first] = pair.second;
  }
  return op.has_query_args()
             ? CommandDetail(op_type, std::move(params), op.query_args())
             : CommandDetail(op_type, std::move(params));
}
}  // namespace rpc
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_RPC_RPC_UTILS_H_
