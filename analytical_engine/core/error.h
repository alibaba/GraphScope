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

#ifndef ANALYTICAL_ENGINE_CORE_ERROR_H_
#define ANALYTICAL_ENGINE_CORE_ERROR_H_

#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "boost/leaf.hpp"

#include "vineyard/graph/utils/error.h"

#include "proto/graphscope/proto/error_codes.pb.h"
#include "utils/mpi_utils.h"

namespace gs {

inline rpc::Code ErrorCodeToProto(vineyard::ErrorCode ec) {
  switch (ec) {
  case vineyard::ErrorCode::kOk:
    return rpc::Code::OK;
  case vineyard::ErrorCode::kVineyardError:
    return rpc::Code::VINEYARD_ERROR;
  case vineyard::ErrorCode::kNetworkError:
    return rpc::Code::NETWORK_ERROR;
  case vineyard::ErrorCode::kUnimplementedMethod:
    return rpc::Code::UNIMPLEMENTED_ERROR;
  default:
    return rpc::Code::ANALYTICAL_ENGINE_INTERNAL_ERROR;
  }
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_ERROR_H_
