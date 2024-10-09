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
#ifndef RUNTIME_ADHOC_RUNTIME_H_
#define RUNTIME_ADHOC_RUNTIME_H_

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "boost/leaf.hpp"

namespace bl = boost::leaf;

namespace gs {

namespace runtime {

bl::result<Context> runtime_eval(
    const physical::PhysicalPlan& plan, const ReadTransaction& txn,
    const std::map<std::string, std::string>& params);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_H_