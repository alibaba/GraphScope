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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_EDGE_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_EDGE_EXPAND_H_

#include <set>

#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand_impl.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

#include "glog/logging.h"

namespace gs {
namespace runtime {

struct EdgeExpandParams {
  int v_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  bool is_optional;
};

class OprTimer;

class EdgeExpand {
 public:
  template <typename PRED_T>
  static Context expand_edge(const GraphReadInterface& graph, Context&& ctx,
                             const EdgeExpandParams& params,
                             const PRED_T& pred) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    std::vector<size_t> shuffle_offset;
    std::shared_ptr<IVertexColumn> input_vertex_list_ptr =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list_ptr->vertex_column_type();
    if (params.labels.size() == 1) {
      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list_ptr);
        auto pair =
            expand_edge_impl<PRED_T>(graph, *casted_input_vertex_list,
                                     params.labels[0], pred, params.dir);
        if (pair.first != nullptr) {
          ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
          return ctx;
        }
      }
      LOG(INFO) << "not hit, fallback";
      if (params.dir == Direction::kIn) {
        auto& input_vertex_list = *input_vertex_list_ptr;
        label_t output_vertex_label = params.labels[0].src_label;
        label_t edge_label = params.labels[0].edge_label;

        auto& props = graph.schema().get_edge_properties(
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
                         auto ie_iter = graph.GetInEdgeIterator(
                             label, v, output_vertex_label, edge_label);
                         while (ie_iter.IsValid()) {
                           auto nbr = ie_iter.GetNeighbor();
                           if (pred(params.labels[0], nbr, v, ie_iter.GetData(),
                                    Direction::kIn, index)) {
                             assert(ie_iter.GetData().type == pt);
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

        auto& props = graph.schema().get_edge_properties(
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
                         auto oe_iter = graph.GetOutEdgeIterator(
                             label, v, output_vertex_label, edge_label);
                         while (oe_iter.IsValid()) {
                           auto nbr = oe_iter.GetNeighbor();
                           if (pred(params.labels[0], v, nbr, oe_iter.GetData(),
                                    Direction::kOut, index)) {
                             assert(oe_iter.GetData().type == pt);
                             builder.push_back_opt(v, nbr, oe_iter.GetData());
                             shuffle_offset.push_back(index);
                           }
                           oe_iter.Next();
                         }
                       });

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else {
        LOG(FATAL) << "expand edge both";
      }
    } else {
      LOG(INFO) << "not hit, fallback";
      if (params.dir == Direction::kBoth) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
        for (auto& triplet : params.labels) {
          auto& props = graph.schema().get_edge_properties(
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
                if (label == triplet.src_label) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    if (pred(triplet, v, nbr, oe_iter.GetData(),
                             Direction::kOut, index)) {
                      assert(oe_iter.GetData().type == label_prop.second);
                      builder.push_back_opt(triplet, v, nbr, oe_iter.GetData(),
                                            Direction::kOut);
                      shuffle_offset.push_back(index);
                    }
                    oe_iter.Next();
                  }
                }
                if (label == triplet.dst_label) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    if (pred(triplet, nbr, v, ie_iter.GetData(), Direction::kIn,
                             index)) {
                      assert(ie_iter.GetData().type == label_prop.second);
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
          auto& props = graph.schema().get_edge_properties(
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
                if (label != triplet.src_label)
                  continue;
                auto oe_iter = graph.GetOutEdgeIterator(
                    label, v, triplet.dst_label, triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut,
                           index)) {
                    assert(oe_iter.GetData().type == label_prop.second);
                    builder.push_back_opt(triplet, v, nbr, oe_iter.GetData());
                    shuffle_offset.push_back(index);
                  }
                  oe_iter.Next();
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    }
    LOG(FATAL) << "not support";
  }

  static Context expand_edge_with_special_edge_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const SPEdgePredicate& pred);

  static Context expand_edge_without_predicate(const GraphReadInterface& graph,
                                               Context&& ctx,
                                               const EdgeExpandParams& params,
                                               OprTimer& timer);

  template <typename PRED_T>
  static Context expand_vertex(const GraphReadInterface& graph, Context&& ctx,
                               const EdgeExpandParams& params,
                               const PRED_T& pred) {
    if (params.is_optional) {
      LOG(FATAL) << "not support optional edge expand";
    }
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    VertexColumnType input_vertex_list_type =
        input_vertex_list->vertex_column_type();

    if (input_vertex_list_type == VertexColumnType::kSingle) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MLVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
      auto pair = expand_vertex_impl<PRED_T>(graph, *casted_input_vertex_list,
                                             params.labels, params.dir, pred);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else {
      LOG(FATAL) << "unexpected to reach here...";
      return ctx;
    }
  }

  static Context expand_vertex_ep_lt(const GraphReadInterface& graph,
                                     Context&& ctx,
                                     const EdgeExpandParams& params,
                                     const std::string& ep_val);
  static Context expand_vertex_ep_gt(const GraphReadInterface& graph,
                                     Context&& ctx,
                                     const EdgeExpandParams& params,
                                     const std::string& ep_val);
  template <typename PRED_T>
  struct SPVPWrapper {
    SPVPWrapper(const PRED_T& pred) : pred_(pred) {}

    inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                           const Any& edata, Direction dir,
                           size_t path_idx) const {
      if (dir == Direction::kOut) {
        return pred_(label.dst_label, dst);
      } else {
        return pred_(label.src_label, src);
      }
    }

    const PRED_T& pred_;
  };

  static Context expand_vertex_with_special_vertex_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params, const SPVertexPredicate& pred);

  static Context expand_vertex_without_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const EdgeExpandParams& params);

  template <typename T1, typename T2, typename T3>
  static Context tc(
      const GraphReadInterface& graph, Context&& ctx,
      const std::array<std::tuple<label_t, label_t, label_t, Direction>, 3>&
          labels,
      int input_tag, int alias1, int alias2, bool LT, const std::string& val) {
    std::shared_ptr<IVertexColumn> input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(input_tag));
    CHECK(input_vertex_list->vertex_column_type() == VertexColumnType::kSingle);
    auto casted_input_vertex_list =
        std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
    label_t input_label = casted_input_vertex_list->label();
    auto dir0 = std::get<3>(labels[0]);
    auto dir1 = std::get<3>(labels[1]);
    auto dir2 = std::get<3>(labels[2]);
    auto d0_nbr_label = std::get<1>(labels[0]);
    auto d0_e_label = std::get<2>(labels[0]);
    auto csr0 = (dir0 == Direction::kOut)
                    ? graph.GetOutgoingGraphView<T1>(input_label, d0_nbr_label,
                                                     d0_e_label)
                    : graph.GetIncomingGraphView<T1>(input_label, d0_nbr_label,
                                                     d0_e_label);
    auto d1_nbr_label = std::get<1>(labels[1]);
    auto d1_e_label = std::get<2>(labels[1]);
    auto csr1 = (dir1 == Direction::kOut)
                    ? graph.GetOutgoingGraphView<T2>(input_label, d1_nbr_label,
                                                     d1_e_label)
                    : graph.GetIncomingGraphView<T2>(input_label, d1_nbr_label,
                                                     d1_e_label);
    auto d2_nbr_label = std::get<1>(labels[2]);
    auto d2_e_label = std::get<2>(labels[2]);
    auto csr2 = (dir2 == Direction::kOut)
                    ? graph.GetOutgoingGraphView<T3>(d1_nbr_label, d2_nbr_label,
                                                     d2_e_label)
                    : graph.GetIncomingGraphView<T3>(d1_nbr_label, d2_nbr_label,
                                                     d2_e_label);

    T1 param = TypedConverter<T1>::typed_from_string(val);

    SLVertexColumnBuilder builder1(d1_nbr_label);
    SLVertexColumnBuilder builder2(d2_nbr_label);
    std::vector<size_t> offsets;

    size_t idx = 0;
    static thread_local GraphReadInterface::vertex_array_t<bool> d0_set;
    static thread_local std::vector<vid_t> d0_vec;

    d0_set.Init(graph.GetVertexSet(d0_nbr_label), false);
    for (auto v : casted_input_vertex_list->vertices()) {
      if (LT) {
        csr0.foreach_edges_lt(v, param, [&](vid_t u, const Date& date) {
          d0_set[u] = true;
          d0_vec.push_back(u);
        });
      } else {
        csr0.foreach_edges_gt(v, param, [&](vid_t u, const Date& date) {
          d0_set[u] = true;
          d0_vec.push_back(u);
        });
      }
      for (auto& e1 : csr1.get_edges(v)) {
        auto nbr1 = e1.get_neighbor();
        for (auto& e2 : csr2.get_edges(nbr1)) {
          auto nbr2 = e2.get_neighbor();
          if (d0_set[nbr2]) {
            builder1.push_back_opt(nbr1);
            builder2.push_back_opt(nbr2);
            offsets.push_back(idx);
          }
        }
      }
      for (auto u : d0_vec) {
        d0_set[u] = false;
      }
      d0_vec.clear();
      ++idx;
    }

    std::shared_ptr<IContextColumn> col1 = builder1.finish();
    std::shared_ptr<IContextColumn> col2 = builder2.finish();
    ctx.set_with_reshuffle(alias1, col1, offsets);
    ctx.set(alias2, col2);
    return ctx;
  }
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_EDGE_EXPAND_H_