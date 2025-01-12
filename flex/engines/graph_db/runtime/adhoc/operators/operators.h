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

#ifndef RUNTIME_ADHOC_OPERATORS_OPERATORS_H_
#define RUNTIME_ADHOC_OPERATORS_OPERATORS_H_

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/utils/app_utils.h"

namespace gs {

namespace runtime {

class OprTimer;

Context eval_select(const algebra::Select& opr, const GraphReadInterface& graph,
                    Context&& ctx,
                    const std::map<std::string, std::string>& params,
                    OprTimer& timer);

bool tc_fusable(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr);

Context eval_tc(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr,
                const GraphReadInterface& graph, Context&& ctx,
                const std::map<std::string, std::string>& params,
                const physical::PhysicalOpr_MetaData& meta0,
                const physical::PhysicalOpr_MetaData& meta1,
                const physical::PhysicalOpr_MetaData& meta2);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_OPERATORS_H_