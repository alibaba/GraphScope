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

#ifndef RUNTIME_ADHOC_UTILS_H_
#define RUNTIME_ADHOC_UTILS_H_

#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
#include "flex/engines/graph_db/runtime/common/types.h"

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"
#include "flex/proto_generated_gie/type.pb.h"

namespace gs {

namespace runtime {

Direction parse_direction(const physical::EdgeExpand_Direction& dir);

std::vector<label_t> parse_tables(const algebra::QueryParams& query_params);

std::vector<LabelTriplet> parse_label_triplets(
    const physical::PhysicalOpr_MetaData& meta);

std::shared_ptr<IContextColumn> create_column(
    const common::IrDataType& data_type);

std::shared_ptr<IContextColumn> create_column_beta(RTAnyType type);

std::shared_ptr<IContextColumn> build_column(
    const common::IrDataType& data_type, const Expr& expr, size_t row_num);

std::shared_ptr<IContextColumn> build_column_beta(const Expr& expr,
                                                  size_t row_num);

std::shared_ptr<IContextColumnBuilder> create_column_builder(RTAnyType type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_UTILS_H_