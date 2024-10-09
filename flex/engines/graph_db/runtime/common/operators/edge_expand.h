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

#ifndef RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_

#include <set>

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

#include "glog/logging.h"

namespace gs {
namespace runtime {

struct EdgeExpandParams {
  int v_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
};

class EdgeExpand {
 public:
  template <typename PRED_T>
  static bl::result<Context> expand_edge(const ReadTransaction& txn,
                                         Context&& ctx,
                                         const EdgeExpandParams& params,
                                         const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    if (params.labels.size() == 1) {
      if (params.dir == Direction::kIn) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        label_t output_vertex_label = params.labels[0].src_label;
        label_t edge_label = params.labels[0].edge_label;

        auto& props = txn.schema().get_edge_properties(
            params.labels[0].src_label, params.labels[0].dst_label,
            params.labels[0].edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        if (props.size() > 1) {
          pt = PropertyType::kRecordView;
        }

        SDSLEdgeColumnBuilder builder(Direction::kIn, params.labels[0], pt,
                                      props);

        foreach_vertex(input_vertex_list,
                       [&](size_t index, label_t label, vid_t v) {
                         auto ie_iter = txn.GetInEdgeIterator(
                             label, v, output_vertex_label, edge_label);
                         while (ie_iter.IsValid()) {
                           auto nbr = ie_iter.GetNeighbor();
                           if (pred(params.labels[0], nbr, v, ie_iter.GetData(),
                                    Direction::kIn, index)) {
                             CHECK(ie_iter.GetData().type == pt);
                             builder.push_back_opt(nbr, v, ie_iter.GetData());
                             shuffle_offset.push_back(index);
                           }
                           ie_iter.Next();
                         }
                       });

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else if (params.dir == Direction::kOut) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        label_t output_vertex_label = params.labels[0].dst_label;
        label_t edge_label = params.labels[0].edge_label;
        label_t src_label = params.labels[0].src_label;

        auto& props = txn.schema().get_edge_properties(
            params.labels[0].src_label, params.labels[0].dst_label,
            params.labels[0].edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        if (props.size() > 1) {
          pt = PropertyType::kRecordView;
        }

        SDSLEdgeColumnBuilder builder(Direction::kOut, params.labels[0], pt,
                                      props);

        foreach_vertex(input_vertex_list,
                       [&](size_t index, label_t label, vid_t v) {
                         if (label != src_label) {
                           return;
                         }
                         auto oe_iter = txn.GetOutEdgeIterator(
                             label, v, output_vertex_label, edge_label);
                         while (oe_iter.IsValid()) {
                           auto nbr = oe_iter.GetNeighbor();
                           if (pred(params.labels[0], v, nbr, oe_iter.GetData(),
                                    Direction::kOut, index)) {
                             CHECK(oe_iter.GetData().type == pt);
                             builder.push_back_opt(v, nbr, oe_iter.GetData());
                             shuffle_offset.push_back(index);
                           }
                           oe_iter.Next();
                         }
                       });

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else {
        LOG(ERROR) << "Unsupported direction: " << params.dir;
        RETURN_UNSUPPORTED_ERROR("Unsupported direction: " +
                                 std::to_string(params.dir));
      }
    } else {
      if (params.dir == Direction::kBoth) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
        for (auto& triplet : params.labels) {
          auto& props = txn.schema().get_edge_properties(
              triplet.src_label, triplet.dst_label, triplet.edge_label);
          PropertyType pt = PropertyType::kEmpty;
          if (!props.empty()) {
            pt = props[0];
          }
          label_props.emplace_back(triplet, pt);
        }
        BDMLEdgeColumnBuilder builder(label_props);

        foreach_vertex(
            input_vertex_list, [&](size_t index, label_t label, vid_t v) {
              for (auto& label_prop : label_props) {
                auto& triplet = label_prop.first;
                auto& pt = label_prop.second;
                if (label == triplet.src_label) {
                  auto oe_iter = txn.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    if (pred(triplet, v, nbr, oe_iter.GetData(),
                             Direction::kOut, index)) {
                      CHECK(oe_iter.GetData().type == pt);
                      builder.push_back_opt(triplet, v, nbr, oe_iter.GetData(),
                                            Direction::kOut);
                      shuffle_offset.push_back(index);
                    }
                    oe_iter.Next();
                  }
                }
                if (label == triplet.dst_label) {
                  auto ie_iter = txn.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    if (pred(triplet, nbr, v, ie_iter.GetData(), Direction::kIn,
                             index)) {
                      CHECK(ie_iter.GetData().type == pt);
                      builder.push_back_opt(triplet, nbr, v, ie_iter.GetData(),
                                            Direction::kIn);
                      shuffle_offset.push_back(index);
                    }
                    ie_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else if (params.dir == Direction::kOut) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
        for (auto& triplet : params.labels) {
          auto& props = txn.schema().get_edge_properties(
              triplet.src_label, triplet.dst_label, triplet.edge_label);
          PropertyType pt = PropertyType::kEmpty;
          if (!props.empty()) {
            pt = props[0];
          }
          label_props.emplace_back(triplet, pt);
        }
        SDMLEdgeColumnBuilder builder(Direction::kOut, label_props);

        foreach_vertex(
            input_vertex_list, [&](size_t index, label_t label, vid_t v) {
              for (auto& label_prop : label_props) {
                auto& triplet = label_prop.first;
                auto& pt = label_prop.second;
                if (label != triplet.src_label)
                  continue;
                auto oe_iter = txn.GetOutEdgeIterator(
                    label, v, triplet.dst_label, triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut,
                           index)) {
                    CHECK(oe_iter.GetData().type == pt);
                    builder.push_back_opt(triplet, v, nbr, oe_iter.GetData());
                    shuffle_offset.push_back(index);
                  }
                  oe_iter.Next();
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else {
        LOG(ERROR) << "Unsupported direction: " << params.dir;
        RETURN_UNSUPPORTED_ERROR("Unsupported direction" +
                                 std::to_string(params.dir));
      }
    }
  }

  static bl::result<Context> expand_edge_without_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const EdgeExpandParams& params);

  template <typename PRED_T>
  static bl::result<Context> expand_vertex(const ReadTransaction& txn,
                                           Context&& ctx,
                                           const EdgeExpandParams& params,
                                           const PRED_T& pred) {
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list->vertex_column_type();

    std::set<label_t> output_vertex_set;
    const std::set<label_t>& input_vertex_set =
        input_vertex_list->get_labels_set();
    if (params.dir == Direction::kOut) {
      for (auto& triplet : params.labels) {
        if (input_vertex_set.find(triplet.src_label) !=
            input_vertex_set.end()) {
          output_vertex_set.insert(triplet.dst_label);
        }
      }
    } else if (params.dir == Direction::kIn) {
      for (auto& triplet : params.labels) {
        if (input_vertex_set.find(triplet.dst_label) !=
            input_vertex_set.end()) {
          output_vertex_set.insert(triplet.src_label);
        }
      }
    } else {
      for (auto& triplet : params.labels) {
        if (input_vertex_set.find(triplet.src_label) !=
            input_vertex_set.end()) {
          output_vertex_set.insert(triplet.dst_label);
        }
        if (input_vertex_set.find(triplet.dst_label) !=
            input_vertex_set.end()) {
          output_vertex_set.insert(triplet.src_label);
        }
      }
    }

    if (output_vertex_set.empty()) {
      LOG(ERROR) << "No output vertex label found...";
      RETURN_UNSUPPORTED_ERROR("No output vertex label found...");
    }

    std::vector<size_t> shuffle_offset;

    if (output_vertex_set.size() == 1) {
      label_t output_vertex_label = *output_vertex_set.begin();
      SLVertexColumnBuilder builder(output_vertex_label);

      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
        label_t input_vertex_label = casted_input_vertex_list->label();
        if (params.labels.size() == 1) {
          auto& label_triplet = params.labels[0];
          if (params.dir == Direction::kBoth &&
              label_triplet.src_label == label_triplet.dst_label &&
              label_triplet.src_label == output_vertex_label &&
              output_vertex_label == input_vertex_label) {
            casted_input_vertex_list->foreach_vertex(
                [&](size_t index, label_t label, vid_t v) {
                  auto oe_iter = txn.GetOutEdgeIterator(
                      label, v, label, label_triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    if (pred(label_triplet, v, nbr, oe_iter.GetData(),
                             Direction::kOut, index)) {
                      builder.push_back_opt(nbr);
                      shuffle_offset.push_back(index);
                    }
                    oe_iter.Next();
                  }
                  auto ie_iter = txn.GetInEdgeIterator(
                      label, v, label, label_triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    if (pred(label_triplet, nbr, v, ie_iter.GetData(),
                             Direction::kIn, index)) {
                      builder.push_back_opt(nbr);
                      shuffle_offset.push_back(index);
                    }
                    ie_iter.Next();
                  }
                });

            ctx.set_with_reshuffle(params.alias, builder.finish(),
                                   shuffle_offset);
          } else if (params.dir == Direction::kIn &&
                     label_triplet.src_label == output_vertex_label &&
                     label_triplet.dst_label == input_vertex_label) {
            casted_input_vertex_list->foreach_vertex(
                [&](size_t index, label_t label, vid_t v) {
                  auto ie_iter = txn.GetInEdgeIterator(
                      label, v, output_vertex_label, label_triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    if (pred(label_triplet, nbr, v, ie_iter.GetData(),
                             Direction::kIn, index)) {
                      builder.push_back_opt(nbr);
                      shuffle_offset.push_back(index);
                    }
                    ie_iter.Next();
                  }
                });
            ctx.set_with_reshuffle(params.alias, builder.finish(),
                                   shuffle_offset);
          } else {
            LOG(ERROR) << "Unsupported direction and label triplet...";
            RETURN_UNSUPPORTED_ERROR(
                "Unsupported direction and label triplet...");
          }
        } else {
          LOG(ERROR) << "multiple label triplet...";
          RETURN_UNSUPPORTED_ERROR("multiple label triplet...");
        }
      } else {
        LOG(ERROR) << "edge expand vertex input multiple vertex label";
        RETURN_UNSUPPORTED_ERROR(
            "edge expand vertex input multiple vertex label");
      }
    } else {
      MLVertexColumnBuilder builder;

      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
        label_t input_vertex_label = casted_input_vertex_list->label();
        for (label_t output_vertex_label : output_vertex_set) {
          if (params.dir == Direction::kBoth) {
            LOG(ERROR) << "expand vertex with both direction is not supported";
            RETURN_UNSUPPORTED_ERROR(
                "expand vertex with both direction is not supported");
          } else if (params.dir == Direction::kIn) {
            for (auto& triplet : params.labels) {
              if (triplet.dst_label == input_vertex_label &&
                  triplet.src_label == output_vertex_label) {
                casted_input_vertex_list->foreach_vertex(
                    [&](size_t index, label_t label, vid_t v) {
                      auto ie_iter = txn.GetInEdgeIterator(
                          label, v, output_vertex_label, triplet.edge_label);
                      while (ie_iter.IsValid()) {
                        auto nbr = ie_iter.GetNeighbor();
                        if (pred(triplet, nbr, v, ie_iter.GetData(),
                                 Direction::kIn, index)) {
                          builder.push_back_vertex(
                              std::make_pair(output_vertex_label, nbr));
                          shuffle_offset.push_back(index);
                        }
                        ie_iter.Next();
                      }
                    });
              }
            }
          } else if (params.dir == Direction::kOut) {
            LOG(ERROR) << "expand vertex with out direction is not supported";
            RETURN_UNSUPPORTED_ERROR(
                "expand vertex with out direction is not supported");
          } else {
            // Must be both
            LOG(ERROR) << "Unknow direction";
            RETURN_UNSUPPORTED_ERROR("Unknow direction");
          }
        }
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      } else {
        LOG(ERROR) << "edge expand vertex input multiple vertex label";
        RETURN_UNSUPPORTED_ERROR(
            "edge expand vertex input multiple vertex label");
      }
    }

    return ctx;
  }

  static bl::result<Context> expand_vertex_without_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const EdgeExpandParams& params);

  static bl::result<Context> expand_2d_vertex_without_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const EdgeExpandParams& params1, const EdgeExpandParams& params2);
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_