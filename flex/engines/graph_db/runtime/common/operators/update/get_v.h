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

#ifndef RUNTIME_COMMON_OPERATORS_UPDATE_GET_V_H_
#define RUNTIME_COMMON_OPERATORS_UPDATE_GET_V_H_
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"
#include "flex/engines/graph_db/runtime/utils/params.h"

namespace gs {
namespace runtime {
class UGetV {
 public:
  template <typename PRED_T>
  static bl::result<Context> get_vertex_from_edge(
      const GraphUpdateInterface& graph, Context&& ctx,
      const GetVParams& params, const PRED_T& pred) {
    auto col = ctx.get(params.tag);
    std::vector<size_t> shuffle_offsets;
    if (col->column_type() != ContextColumnType::kEdge) {
      LOG(ERROR) << "current only support edge column" << col->column_info();
      RETURN_BAD_REQUEST_ERROR("current only support edge column");
    }
    const auto input_edge_list = dynamic_cast<const IEdgeColumn*>(col.get());
    MLVertexColumnBuilder builder;
    if (input_edge_list->edge_column_type() == EdgeColumnType::kBDML) {
      const auto bdml_edge_list =
          dynamic_cast<const BDMLEdgeColumn*>(input_edge_list);
      bdml_edge_list->foreach_edge([&](size_t index, const LabelTriplet& label,
                                       vid_t src, vid_t dst,
                                       const EdgeData& edata, Direction dir) {
        if (pred(label.src_label, src, index)) {
          if (params.opt == VOpt::kStart) {
            builder.push_back_vertex(VertexRecord{label.src_label, src});
          } else if (params.opt == VOpt::kEnd) {
            builder.push_back_vertex(VertexRecord{label.dst_label, dst});
          } else if (params.opt == VOpt::kOther) {
            if (dir == Direction::kOut) {
              builder.push_back_vertex(VertexRecord{label.dst_label, dst});
            } else {
              builder.push_back_vertex(VertexRecord{label.src_label, src});
            }
          }
          shuffle_offsets.push_back(index);
        }
      });
    } else {
      LOG(ERROR) << "current only support BDML edge column";
      RETURN_BAD_REQUEST_ERROR("current only support BDML edge column");
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offsets);
    return ctx;
  }

  template <typename PRED_T>
  static bl::result<Context> get_vertex_from_vertices(
      const GraphUpdateInterface& graph, Context&& ctx,
      const GetVParams& params, const PRED_T& pred) {
    auto col = ctx.get(params.tag);
    std::vector<size_t> shuffle_offsets;
    if (col->column_type() != ContextColumnType::kVertex) {
      LOG(ERROR) << "current only support vertex column" << col->column_info();
      RETURN_BAD_REQUEST_ERROR("current only support vertex column");
    }
    const auto input_vertex_list =
        dynamic_cast<const IVertexColumn*>(col.get());
    MLVertexColumnBuilder builder;
    foreach_vertex(*input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     if (pred(label, v, index)) {
                       builder.push_back_vertex(VertexRecord{label, v});
                       shuffle_offsets.push_back(index);
                     }
                   });
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offsets);
    return ctx;
  }
};
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_COMMON_OPERATORS_UPDATE_GET_V_H_