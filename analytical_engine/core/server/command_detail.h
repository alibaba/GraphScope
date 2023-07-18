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

#ifndef ANALYTICAL_ENGINE_CORE_SERVER_COMMAND_DETAIL_H_
#define ANALYTICAL_ENGINE_CORE_SERVER_COMMAND_DETAIL_H_

#include <map>
#include <utility>

#include "proto/attr_value.pb.h"
#include "proto/types.pb.h"

namespace grape {
class InArchive;
class OutArchive;
}  // namespace grape

namespace gs {

/**
 * @brief CommandDetail encapsulates an operation with parameters from user via
 * rpc. The class should implement archives interfaces to serialize and transmit
 * the object across the cluster.
 */
struct CommandDetail {
  CommandDetail() = default;

  CommandDetail(const rpc::OperationType& op_type,
                std::map<int, rpc::AttrValue>&& op_params,
                const rpc::LargeAttrValue& large_attr)
      : type(op_type), params(std::move(op_params)), large_attr(large_attr) {}

  CommandDetail(const rpc::OperationType& op_type,
                std::map<int, rpc::AttrValue>&& op_params,
                const rpc::LargeAttrValue& large_attr, rpc::QueryArgs args)
      : type(op_type),
        params(std::move(op_params)),
        large_attr(large_attr),
        query_args(std::move(args)) {}

  rpc::OperationType type{};
  std::map<int, rpc::AttrValue> params;
  rpc::LargeAttrValue large_attr;
  rpc::QueryArgs query_args;
};

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const CommandDetail& cd);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              CommandDetail& cd);

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_SERVER_COMMAND_DETAIL_H_
