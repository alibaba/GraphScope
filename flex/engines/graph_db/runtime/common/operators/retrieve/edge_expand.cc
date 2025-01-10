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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand.h"
#include "flex/engines/graph_db/runtime/adhoc/opr_timer.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand_impl.h"

namespace gs {

namespace runtime {

static std::vector<LabelTriplet> get_expand_label_set(
    const GraphReadInterface& graph, const std::set<label_t>& label_set,
    const std::vector<LabelTriplet>& labels, Direction dir) {
  std::vector<LabelTriplet> label_triplets;
  if (dir == Direction::kOut) {
    for (auto& triplet : labels) {
      if (label_set.count(triplet.src_label)) {
        label_triplets.emplace_back(triplet);
      }
    }
  } else if (dir == Direction::kIn) {
    for (auto& triplet : labels) {
      if (label_set.count(triplet.dst_label)) {
        label_triplets.emplace_back(triplet);
      }
    }
  } else {
    for (auto& triplet : labels) {
      if (label_set.count(triplet.src_label) ||
          label_set.count(triplet.dst_label)) {
        label_triplets.emplace_back(triplet);
      }
    }
  }
  return label_triplets;
}

static Context expand_edge_without_predicate_optional_impl(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  // has only one label
  if (params.labels.size() == 1) {
    // both direction and src_label == dst_label
    if (params.dir == Direction::kBoth &&
        params.labels[0].src_label == params.labels[0].dst_label) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      CHECK(!input_vertex_list.is_optional())
          << "not support optional vertex column as input currently";
      auto& triplet = params.labels[0];
      auto props = graph.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }
      if (props.size() > 1) {
        pt = PropertyType::kRecordView;
      }
      OptionalBDSLEdgeColumnBuilder builder(triplet, pt);
      foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                            vid_t v) {
        bool has_edge = false;
        if (label == triplet.src_label) {
          auto oe_iter = graph.GetOutEdgeIterator(label, v, triplet.dst_label,
                                                  triplet.edge_label);
          while (oe_iter.IsValid()) {
            auto nbr = oe_iter.GetNeighbor();
            builder.push_back_opt(v, nbr, oe_iter.GetData(), Direction::kOut);
            shuffle_offset.push_back(index);
            has_edge = true;
            oe_iter.Next();
          }
        }
        if (label == triplet.dst_label) {
          auto ie_iter = graph.GetInEdgeIterator(label, v, triplet.src_label,
                                                 triplet.edge_label);
          while (ie_iter.IsValid()) {
            auto nbr = ie_iter.GetNeighbor();
            builder.push_back_opt(nbr, v, ie_iter.GetData(), Direction::kIn);
            shuffle_offset.push_back(index);
            has_edge = true;
            ie_iter.Next();
          }
        }
        if (!has_edge) {
          builder.push_back_null();
          shuffle_offset.push_back(index);
        }
      });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else if (params.dir == Direction::kOut) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      CHECK(!input_vertex_list.is_optional())
          << "not support optional vertex column as input currently";
      auto& triplet = params.labels[0];
      auto props = graph.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }
      if (props.size() > 1) {
        pt = PropertyType::kRecordView;
      }
      OptionalSDSLEdgeColumnBuilder builder(Direction::kOut, triplet, pt);
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       if (label == triplet.src_label) {
                         auto oe_iter = graph.GetOutEdgeIterator(
                             label, v, triplet.dst_label, triplet.edge_label);
                         bool has_edge = false;
                         while (oe_iter.IsValid()) {
                           auto nbr = oe_iter.GetNeighbor();
                           builder.push_back_opt(v, nbr, oe_iter.GetData());
                           shuffle_offset.push_back(index);
                           oe_iter.Next();
                           has_edge = true;
                         }
                         if (!has_edge) {
                           builder.push_back_null();
                           shuffle_offset.push_back(index);
                         }
                       } else {
                         builder.push_back_null();
                         shuffle_offset.push_back(index);
                       }
                     });

      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    }
  }
  LOG(FATAL) << "not support" << params.labels.size() << " "
             << (int) params.dir;
  return ctx;
}

Context EdgeExpand::expand_edge_without_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, OprTimer& timer) {
  if (params.is_optional) {
    TimerUnit tx;
    tx.start();
    auto ret = expand_edge_without_predicate_optional_impl(
        graph, std::move(ctx), params);
    timer.record_routine("#### expand_edge_without_predicate_optional", tx);
    return ret;
  }
  std::vector<size_t> shuffle_offset;

  if (params.labels.size() == 1) {
    if (params.dir == Direction::kIn) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      label_t output_vertex_label = params.labels[0].src_label;
      label_t edge_label = params.labels[0].edge_label;

      auto& props = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (props.size() > 1) {
        pt = PropertyType::kRecordView;

      } else if (!props.empty()) {
        pt = props[0];
      }

      SDSLEdgeColumnBuilder builder(Direction::kIn, params.labels[0], pt,
                                    props);

      label_t dst_label = params.labels[0].dst_label;
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       if (label != dst_label) {
                         return;
                       }
                       auto ie_iter = graph.GetInEdgeIterator(
                           label, v, output_vertex_label, edge_label);
                       while (ie_iter.IsValid()) {
                         auto nbr = ie_iter.GetNeighbor();
                         assert(ie_iter.GetData().type == pt);
                         builder.push_back_opt(nbr, v, ie_iter.GetData());
                         shuffle_offset.push_back(index);
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
      label_t src_label = params.labels[0].src_label;
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       if (label != src_label) {
                         return;
                       }
                       auto oe_iter = graph.GetOutEdgeIterator(
                           label, v, output_vertex_label, edge_label);

                       while (oe_iter.IsValid()) {
                         auto nbr = oe_iter.GetNeighbor();
                         assert(oe_iter.GetData().type == pt);
                         builder.push_back_opt(v, nbr, oe_iter.GetData());
                         shuffle_offset.push_back(index);
                         oe_iter.Next();
                       }
                     });

      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      auto props = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }
      BDSLEdgeColumnBuilder builder(params.labels[0], pt);
      foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                            vid_t v) {
        if (label == params.labels[0].src_label) {
          auto oe_iter =
              graph.GetOutEdgeIterator(label, v, params.labels[0].dst_label,
                                       params.labels[0].edge_label);
          while (oe_iter.IsValid()) {
            auto nbr = oe_iter.GetNeighbor();
            builder.push_back_opt(v, nbr, oe_iter.GetData(), Direction::kOut);
            shuffle_offset.push_back(index);
            oe_iter.Next();
          }
        }
        if (label == params.labels[0].dst_label) {
          auto ie_iter =
              graph.GetInEdgeIterator(label, v, params.labels[0].src_label,
                                      params.labels[0].edge_label);
          while (ie_iter.IsValid()) {
            auto nbr = ie_iter.GetNeighbor();
            builder.push_back_opt(nbr, v, ie_iter.GetData(), Direction::kIn);
            shuffle_offset.push_back(index);
            ie_iter.Next();
          }
        }
      });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    }
  } else {
    auto column =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    auto label_set = column->get_labels_set();
    auto labels =
        get_expand_label_set(graph, label_set, params.labels, params.dir);
    std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
    std::vector<std::vector<PropertyType>> props_vec;
    for (auto& triplet : labels) {
      auto& props = graph.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }
      if (props.size() > 1) {
        pt = PropertyType::kRecordView;
      }
      props_vec.emplace_back(props);
      label_props.emplace_back(triplet, pt);
    }
    if (params.dir == Direction::kOut || params.dir == Direction::kIn) {
      if (labels.size() == 1) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        if (params.dir == Direction::kOut) {
          auto& triplet = labels[0];
          SDSLEdgeColumnBuilder builder(Direction::kOut, triplet,
                                        label_props[0].second, props_vec[0]);
          foreach_vertex(
              input_vertex_list, [&](size_t index, label_t label, vid_t v) {
                if (label == triplet.src_label) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    builder.push_back_opt(v, nbr, oe_iter.GetData());
                    shuffle_offset.push_back(index);
                    oe_iter.Next();
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        } else if (params.dir == Direction::kIn) {
          auto& triplet = labels[0];
          SDSLEdgeColumnBuilder builder(Direction::kIn, triplet,
                                        label_props[0].second, props_vec[0]);
          foreach_vertex(
              input_vertex_list, [&](size_t index, label_t label, vid_t v) {
                if (label == triplet.dst_label) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    builder.push_back_opt(nbr, v, ie_iter.GetData());
                    shuffle_offset.push_back(index);
                    ie_iter.Next();
                  }
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
          return ctx;
        }
      } else if (labels.size() > 1 || labels.size() == 0) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));

        SDMLEdgeColumnBuilder builder(params.dir, label_props);
        if (params.dir == Direction::kOut) {
          foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                                vid_t v) {
            for (auto& triplet : labels) {
              if (triplet.src_label == label) {
                if (params.dir == Direction::kOut) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    builder.push_back_opt(triplet, v, nbr, oe_iter.GetData());
                    shuffle_offset.push_back(index);
                    oe_iter.Next();
                  }
                }
              }
            }
          });
        } else {
          foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                                vid_t v) {
            for (auto& triplet : labels) {
              if (triplet.dst_label == label) {
                if (params.dir == Direction::kIn) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    builder.push_back_opt(triplet, nbr, v, ie_iter.GetData());
                    shuffle_offset.push_back(index);
                    ie_iter.Next();
                  }
                }
              }
            }
          });
        }

        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    } else if (params.dir == Direction::kBoth) {
      if (labels.size() == 1) {
        BDSLEdgeColumnBuilder builder(labels[0], label_props[0].second);
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                              vid_t v) {
          if (label == labels[0].src_label) {
            auto oe_iter = graph.GetOutEdgeIterator(
                label, v, labels[0].dst_label, labels[0].edge_label);
            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              builder.push_back_opt(v, nbr, oe_iter.GetData(), Direction::kOut);
              shuffle_offset.push_back(index);
              oe_iter.Next();
            }
          }
          if (label == labels[0].dst_label) {
            auto ie_iter = graph.GetInEdgeIterator(
                label, v, labels[0].src_label, labels[0].edge_label);
            while (ie_iter.IsValid()) {
              auto nbr = ie_iter.GetNeighbor();
              builder.push_back_opt(nbr, v, ie_iter.GetData(), Direction::kIn);
              shuffle_offset.push_back(index);
              ie_iter.Next();
            }
          }
        });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      } else {
        BDMLEdgeColumnBuilder builder(label_props);
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        foreach_vertex(
            input_vertex_list, [&](size_t index, label_t label, vid_t v) {
              for (auto& triplet : labels) {
                if (triplet.src_label == label) {
                  auto oe_iter = graph.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    builder.push_back_opt(triplet, v, nbr, oe_iter.GetData(),
                                          Direction::kOut);
                    shuffle_offset.push_back(index);
                    oe_iter.Next();
                  }
                }
                if (triplet.dst_label == label) {
                  auto ie_iter = graph.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    builder.push_back_opt(triplet, nbr, v, ie_iter.GetData(),
                                          Direction::kIn);
                    shuffle_offset.push_back(index);
                    ie_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    }
  }

  LOG(FATAL) << "not support";
  return ctx;
}

Context EdgeExpand::expand_vertex_without_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  std::shared_ptr<IVertexColumn> input_vertex_list =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
  VertexColumnType input_vertex_list_type =
      input_vertex_list->vertex_column_type();
  if (input_vertex_list_type == VertexColumnType::kSingle) {
    //    LOG(INFO) << "input vertex size: " << input_vertex_list->size();
    if (input_vertex_list->is_optional()) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumnBase>(input_vertex_list);
      auto pair = expand_vertex_without_predicate_optional_impl(
          graph, *casted_input_vertex_list, params.labels, params.dir);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    } else {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
      // optional edge expand
      if (params.is_optional) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumnBase>(input_vertex_list);
        auto pair = expand_vertex_without_predicate_optional_impl(
            graph, *casted_input_vertex_list, params.labels, params.dir);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      } else {
        auto pair = expand_vertex_without_predicate_impl(
            graph, *casted_input_vertex_list, params.labels, params.dir);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      }
    }
  } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
    if (input_vertex_list->is_optional() || params.is_optional) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MLVertexColumnBase>(input_vertex_list);
      auto pair = expand_vertex_without_predicate_optional_impl(
          graph, *casted_input_vertex_list, params.labels, params.dir);
      ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
      return ctx;
    }
    auto casted_input_vertex_list =
        std::dynamic_pointer_cast<MLVertexColumn>(input_vertex_list);
    auto pair = expand_vertex_without_predicate_impl(
        graph, *casted_input_vertex_list, params.labels, params.dir);
    ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
    return ctx;
  } else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
    if (input_vertex_list->is_optional() || params.is_optional) {
      LOG(FATAL) << "not support optional vertex column as input currently";
    }
    auto casted_input_vertex_list =
        std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
    auto pair = expand_vertex_without_predicate_impl(
        graph, *casted_input_vertex_list, params.labels, params.dir);
    ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
    return ctx;
  } else {
    LOG(FATAL) << "unexpected to reach here";
    return ctx;
  }
}

template <typename T>
static Context _expand_edge_with_special_edge_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SPEdgePredicate& pred) {
  if (pred.type() == SPPredicateType::kPropertyGT) {
    return EdgeExpand::expand_edge<EdgePropertyGTPredicate<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const EdgePropertyGTPredicate<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return EdgeExpand::expand_edge<EdgePropertyLTPredicate<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const EdgePropertyLTPredicate<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyEQ) {
    return EdgeExpand::expand_edge<EdgePropertyEQPredicate<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const EdgePropertyEQPredicate<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return EdgeExpand::expand_edge<EdgePropertyNEPredicate<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const EdgePropertyNEPredicate<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return EdgeExpand::expand_edge<EdgePropertyLEPredicate<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const EdgePropertyLEPredicate<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return EdgeExpand::expand_edge<EdgePropertyGEPredicate<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const EdgePropertyGEPredicate<T>&>(pred));
  } else {
    LOG(FATAL) << "not support edge property type "
               << static_cast<int>(pred.type());
  }
}

Context EdgeExpand::expand_edge_with_special_edge_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SPEdgePredicate& pred) {
  if (params.is_optional) {
    LOG(FATAL) << "not support optional edge expand";
  }
  if (pred.data_type() == RTAnyType::kI64Value) {
    return _expand_edge_with_special_edge_predicate<int64_t>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _expand_edge_with_special_edge_predicate<int32_t>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _expand_edge_with_special_edge_predicate<double>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _expand_edge_with_special_edge_predicate<std::string_view>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _expand_edge_with_special_edge_predicate<Date>(graph, std::move(ctx),
                                                          params, pred);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _expand_edge_with_special_edge_predicate<double>(
        graph, std::move(ctx), params, pred);
  } else {
    LOG(FATAL) << "not support edge property type "
               << static_cast<int>(pred.data_type());
  }
  return Context();
}

Context EdgeExpand::expand_vertex_ep_lt(const GraphReadInterface& graph,
                                        Context&& ctx,
                                        const EdgeExpandParams& params,
                                        const std::string& ep_val) {
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
    label_t input_label = casted_input_vertex_list->label();
    std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
    std::vector<PropertyType> ed_types;
    for (auto& triplet : params.labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if (triplet.src_label == input_label &&
          ((params.dir == Direction::kOut) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
      if (triplet.dst_label == input_label &&
          ((params.dir == Direction::kIn) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
    }
    grape::DistinctSort(label_dirs);
    bool se = (label_dirs.size() == 1);
    bool sp = true;
    if (!se) {
      for (size_t k = 1; k < ed_types.size(); ++k) {
        if (ed_types[k] != ed_types[0]) {
          sp = false;
          break;
        }
      }
    }
    if (!sp) {
      LOG(FATAL) << "not support multiple edge types";
    }
    const PropertyType& ed_type = ed_types[0];
    if (ed_type == PropertyType::Date()) {
      Date max_value(std::stoll(ep_val));
      std::vector<GraphReadInterface::graph_view_t<Date>> views;
      for (auto& t : label_dirs) {
        label_t nbr_label = std::get<0>(t);
        label_t edge_label = std::get<1>(t);
        Direction dir = std::get<2>(t);
        if (dir == Direction::kOut) {
          views.emplace_back(graph.GetOutgoingGraphView<Date>(
              input_label, nbr_label, edge_label));
        } else {
          CHECK(dir == Direction::kIn);
          views.emplace_back(graph.GetIncomingGraphView<Date>(
              input_label, nbr_label, edge_label));
        }
      }
      MSVertexColumnBuilder builder;
      size_t csr_idx = 0;
      std::vector<size_t> offsets;
      for (auto& csr : views) {
        label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
        // label_t edge_label = std::get<1>(label_dirs[csr_idx]);
        // Direction dir = std::get<2>(label_dirs[csr_idx]);
        size_t idx = 0;
        builder.start_label(nbr_label);
        for (auto v : casted_input_vertex_list->vertices()) {
          csr.foreach_edges_lt(v, max_value, [&](vid_t nbr, const Date& e) {
            builder.push_back_opt(nbr);
            offsets.push_back(idx);
          });
          ++idx;
        }
        ++csr_idx;
      }
      std::shared_ptr<IContextColumn> col = builder.finish();
      ctx.set_with_reshuffle(params.alias, col, offsets);
      return ctx;
    } else {
      LOG(FATAL) << "not support edge type";
    }
  } else {
    LOG(FATAL) << "unexpected to reach here...";
    return ctx;
  }
}

Context EdgeExpand::expand_vertex_ep_gt(const GraphReadInterface& graph,
                                        Context&& ctx,
                                        const EdgeExpandParams& params,
                                        const std::string& ep_val) {
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
    label_t input_label = casted_input_vertex_list->label();
    std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
    std::vector<PropertyType> ed_types;
    for (auto& triplet : params.labels) {
      if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                                triplet.edge_label)) {
        continue;
      }
      if (triplet.src_label == input_label &&
          ((params.dir == Direction::kOut) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                                Direction::kOut);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
      if (triplet.dst_label == input_label &&
          ((params.dir == Direction::kIn) ||
           (params.dir == Direction::kBoth))) {
        label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                                Direction::kIn);
        const auto& properties = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        if (properties.empty()) {
          ed_types.push_back(PropertyType::Empty());
        } else {
          CHECK_EQ(properties.size(), 1);
          ed_types.push_back(properties[0]);
        }
      }
    }
    grape::DistinctSort(label_dirs);
    bool se = (label_dirs.size() == 1);
    bool sp = true;
    if (!se) {
      for (size_t k = 1; k < ed_types.size(); ++k) {
        if (ed_types[k] != ed_types[0]) {
          sp = false;
          break;
        }
      }
    }
    if (!sp) {
      LOG(FATAL) << "not support multiple edge types";
    }
    const PropertyType& ed_type = ed_types[0];
    if (se) {
      if (ed_type == PropertyType::Date()) {
        Date max_value(std::stoll(ep_val));
        std::vector<GraphReadInterface::graph_view_t<Date>> views;
        for (auto& t : label_dirs) {
          label_t nbr_label = std::get<0>(t);
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            views.emplace_back(graph.GetOutgoingGraphView<Date>(
                input_label, nbr_label, edge_label));
          } else {
            CHECK(dir == Direction::kIn);
            views.emplace_back(graph.GetIncomingGraphView<Date>(
                input_label, nbr_label, edge_label));
          }
        }
        SLVertexColumnBuilder builder(std::get<0>(label_dirs[0]));
        size_t csr_idx = 0;
        std::vector<size_t> offsets;
        for (auto& csr : views) {
          size_t idx = 0;
          for (auto v : casted_input_vertex_list->vertices()) {
            csr.foreach_edges_gt(v, max_value, [&](vid_t nbr, const Date& val) {
              builder.push_back_opt(nbr);
              offsets.push_back(idx);
            });
            ++idx;
          }
          ++csr_idx;
        }
        std::shared_ptr<IContextColumn> col = builder.finish();
        ctx.set_with_reshuffle(params.alias, col, offsets);
        return ctx;
      } else {
        LOG(FATAL) << "not support edge type";
      }
    } else {
      if (ed_type == PropertyType::Date()) {
        Date max_value(std::stoll(ep_val));
        std::vector<GraphReadInterface::graph_view_t<Date>> views;
        for (auto& t : label_dirs) {
          label_t nbr_label = std::get<0>(t);
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            views.emplace_back(graph.GetOutgoingGraphView<Date>(
                input_label, nbr_label, edge_label));
          } else {
            CHECK(dir == Direction::kIn);
            views.emplace_back(graph.GetIncomingGraphView<Date>(
                input_label, nbr_label, edge_label));
          }
        }
        MSVertexColumnBuilder builder;
        size_t csr_idx = 0;
        std::vector<size_t> offsets;
        for (auto& csr : views) {
          label_t nbr_label = std::get<0>(label_dirs[csr_idx]);
          size_t idx = 0;
          builder.start_label(nbr_label);
          LOG(INFO) << "start label: " << static_cast<int>(nbr_label);
          for (auto v : casted_input_vertex_list->vertices()) {
            csr.foreach_edges_gt(v, max_value, [&](vid_t nbr, const Date& val) {
              builder.push_back_opt(nbr);
              offsets.push_back(idx);
            });
            ++idx;
          }
          ++csr_idx;
        }
        std::shared_ptr<IContextColumn> col = builder.finish();
        ctx.set_with_reshuffle(params.alias, col, offsets);
        return ctx;
      } else {
        LOG(FATAL) << "not support edge type";
      }
    }
  } else {
    LOG(FATAL) << "unexpected to reach here...";
    return ctx;
  }
}

template <typename T>
static Context _expand_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SPVertexPredicate& pred) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<VertexPropertyEQPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const VertexPropertyEQPredicateBeta<T>&>(pred)));
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<VertexPropertyLTPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const VertexPropertyLTPredicateBeta<T>&>(pred)));
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<VertexPropertyGTPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const VertexPropertyGTPredicateBeta<T>&>(pred)));
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<VertexPropertyLEPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const VertexPropertyLEPredicateBeta<T>&>(pred)));
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<VertexPropertyGEPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const VertexPropertyGEPredicateBeta<T>&>(pred)));
  } else if (pred.type() == SPPredicateType::kPropertyBetween) {
    return EdgeExpand::expand_vertex<
        EdgeExpand::SPVPWrapper<VertexPropertyBetweenPredicateBeta<T>>>(
        graph, std::move(ctx), params,
        EdgeExpand::SPVPWrapper(
            dynamic_cast<const VertexPropertyBetweenPredicateBeta<T>&>(pred)));
  } else {
    LOG(FATAL) << "not support vertex property type "
               << static_cast<int>(pred.type());
  }
}

Context EdgeExpand::expand_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const EdgeExpandParams& params, const SPVertexPredicate& pred) {
  if (params.is_optional) {
    LOG(FATAL) << "not support optional edge expand";
  }

  if (pred.data_type() == RTAnyType::kI64Value) {
    return _expand_vertex_with_special_vertex_predicate<int64_t>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _expand_vertex_with_special_vertex_predicate<Date>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _expand_vertex_with_special_vertex_predicate<double>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _expand_vertex_with_special_vertex_predicate<std::string_view>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _expand_vertex_with_special_vertex_predicate<int32_t>(
        graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kDate32) {
    return _expand_vertex_with_special_vertex_predicate<Day>(
        graph, std::move(ctx), params, pred);
  }
  LOG(FATAL) << static_cast<int>(pred.type()) << "not impl";
  return ctx;
}

}  // namespace runtime

}  // namespace gs
