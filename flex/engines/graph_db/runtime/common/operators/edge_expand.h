#ifndef RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_

#include <set>

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"

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
  static Context expand_edge(const ReadTransaction& txn, Context&& ctx,
                             const EdgeExpandParams& params,
                             const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    LOG(INFO) << "expand edge" << (int) params.dir << " "
              << params.labels.size();
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

        SDSLEdgeColumnBuilder builder(Direction::kIn, params.labels[0], pt);

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
            *std::dynamic_pointer_cast<SLVertexColumn>(ctx.get(params.v_tag));
        label_t output_vertex_label = params.labels[0].dst_label;
        label_t edge_label = params.labels[0].edge_label;

        auto& props = txn.schema().get_edge_properties(
            params.labels[0].src_label, params.labels[0].dst_label,
            params.labels[0].edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }

        SDSLEdgeColumnBuilder builder(Direction::kOut, params.labels[0], pt);

        input_vertex_list.foreach_vertex([&](size_t index, label_t label,
                                             vid_t v) {
          auto oe_iter =
              txn.GetOutEdgeIterator(label, v, output_vertex_label, edge_label);
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
        LOG(INFO) << "expand edge both";
      }
    }

    LOG(FATAL) << "not support";
  }

  static Context expand_edge_without_predicate(const ReadTransaction& txn,
                                               Context&& ctx,
                                               const EdgeExpandParams& params);

  template <typename PRED_T>
  static Context expand_vertex(const ReadTransaction& txn, Context&& ctx,
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
      LOG(FATAL) << "output vertex label set is empty...";
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
            LOG(FATAL) << "xxx, " << (int) params.dir;
          }
        } else {
          LOG(FATAL) << "multiple label triplet...";
        }
      } else {
        LOG(FATAL) << "edge expand vertex input multiple vertex label";
      }
    } else {
      MLVertexColumnBuilder builder;

      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
        label_t input_vertex_label = casted_input_vertex_list->label();
        for (label_t output_vertex_label : output_vertex_set) {
          if (params.dir == Direction::kBoth) {
            LOG(FATAL) << "AAAAA";
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
            LOG(FATAL) << "AAAAA";
          }
        }
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      } else {
        LOG(FATAL) << "edge expand vertex input multiple vertex label";
      }
    }

    return ctx;
  }

  static Context expand_vertex_without_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const EdgeExpandParams& params);

  static Context expand_2d_vertex_without_predicate(
      const ReadTransaction& txn, Context&& ctx,
      const EdgeExpandParams& params1, const EdgeExpandParams& params2);
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_EDGE_EXPAND_H_