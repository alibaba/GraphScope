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

#ifndef RUNTIME_CODEGEN_BUILDERS_BUILDERS_H_
#define RUNTIME_CODEGEN_BUILDERS_BUILDERS_H_
#include <sstream>
#include <string>

#include "flex/engines/graph_db/runtime/codegen/building_context.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
namespace runtime {

std::string BuildScan(BuildingContext& context, const physical::Scan& opr);

std::string BuildSink(BuildingContext& context);

std::string BuildLimit(BuildingContext& context, const algebra::Limit& opr);

std::string BuildGetV(BuildingContext& context, const physical::GetV& opr);

}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_CODEGEN_BUILDERS_BUILDERS_H_