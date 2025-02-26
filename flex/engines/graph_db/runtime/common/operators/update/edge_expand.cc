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
#include "flex/engines/graph_db/runtime/common/operators/update/edge_expand.h"

namespace gs {
namespace runtime {
bl::result<Context> UEdgeExpand::edge_expand_v_without_pred(
    const GraphUpdateInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  const auto& input_vertex_list =
      dynamic_cast<const IVertexColumn&>(*ctx.get(params.v_tag).get());
  std::vector<size_t> shuffle_offset;
  auto builder = MLVertexColumnBuilder::builder();
  if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (label == triplet.dst_label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              for (; ie_iter.IsValid(); ie_iter.Next()) {
                builder.push_back_vertex(VertexRecord{
                    ie_iter.GetNeighborLabel(), ie_iter.GetNeighbor()});
                shuffle_offset.push_back(index);
              }
            }
          }
        });
  }

  if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (label == triplet.src_label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              for (; oe_iter.IsValid(); oe_iter.Next()) {
                builder.push_back_vertex(VertexRecord{
                    oe_iter.GetNeighborLabel(), oe_iter.GetNeighbor()});
                shuffle_offset.push_back(index);
              }
            }
          }
        });
  }
  ctx.set_with_reshuffle(params.alias, builder.finish(nullptr), shuffle_offset);
  return ctx;
}

bl::result<Context> UEdgeExpand::edge_expand_e_without_pred(
    const GraphUpdateInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  const auto& input_vertex_list =
      dynamic_cast<const IVertexColumn&>(*ctx.get(params.v_tag).get());
  std::vector<size_t> shuffle_offset;
  const auto& v_labels = input_vertex_list.get_labels_set();
  std::vector<LabelTriplet> labels;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels;
  for (auto& label : params.labels) {
    if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
      if (v_labels.find(label.dst_label) != v_labels.end()) {
        labels.push_back(label);
        const auto& props = graph.schema().get_edge_properties(
            label.src_label, label.dst_label, label.edge_label);
        if (props.size() == 0) {
          edge_labels.emplace_back(label, PropertyType::kEmpty);

        } else if (props.size() == 1) {
          edge_labels.emplace_back(label, props[0]);
        } else {
          edge_labels.emplace_back(label, PropertyType::kRecordView);
        }
        continue;
      }
    }
    if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
      if (v_labels.find(label.src_label) != v_labels.end()) {
        labels.push_back(label);
        const auto& props = graph.schema().get_edge_properties(
            label.src_label, label.dst_label, label.edge_label);
        if (props.size() == 0) {
          edge_labels.emplace_back(label, PropertyType::kEmpty);
        } else if (props.size() == 1) {
          edge_labels.emplace_back(label, props[0]);
        } else {
          edge_labels.emplace_back(label, PropertyType::kRecordView);
        }
      }
    }
  }
  BDMLEdgeColumnBuilder builder(edge_labels);

  if (params.dir == Direction::kBoth) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (triplet.src_label == label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              for (; oe_iter.IsValid(); oe_iter.Next()) {
                builder.push_back_opt(triplet, v, oe_iter.GetNeighbor(),
                                      oe_iter.GetData(), Direction::kOut);
                shuffle_offset.push_back(index);
              }
            }
            if (triplet.dst_label == label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              for (; ie_iter.IsValid(); ie_iter.Next()) {
                builder.push_back_opt(triplet, ie_iter.GetNeighbor(), v,
                                      ie_iter.GetData(), Direction::kIn);
                shuffle_offset.push_back(index);
              }
            }
          }
        });
    ctx.set_with_reshuffle(params.alias, builder.finish(nullptr),
                           shuffle_offset);
    return ctx;
  } else if (params.dir == Direction::kIn) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (triplet.dst_label == label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              for (; ie_iter.IsValid(); ie_iter.Next()) {
                builder.push_back_opt(triplet, ie_iter.GetNeighbor(), v,
                                      ie_iter.GetData(), Direction::kIn);
                shuffle_offset.push_back(index);
              }
            }
          }
        });
    ctx.set_with_reshuffle(params.alias, builder.finish(nullptr),
                           shuffle_offset);
    return ctx;
  } else if (params.dir == Direction::kOut) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (triplet.src_label == label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              for (; oe_iter.IsValid(); oe_iter.Next()) {
                builder.push_back_opt(triplet, v, oe_iter.GetNeighbor(),
                                      oe_iter.GetData(), Direction::kOut);
                shuffle_offset.push_back(index);
              }
            }
          }
        });
    ctx.set_with_reshuffle(params.alias, builder.finish(nullptr),
                           shuffle_offset);
    return ctx;
  }
  LOG(ERROR) << "should not reach here: " << static_cast<int>(params.dir);
  RETURN_UNSUPPORTED_ERROR("should not reach here " +
                           std::to_string(static_cast<int>(params.dir)));
}

}  // namespace runtime
}  // namespace gs