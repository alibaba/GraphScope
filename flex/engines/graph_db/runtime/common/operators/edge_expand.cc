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

#include "flex/engines/graph_db/runtime/common/operators/edge_expand.h"

namespace gs {

namespace runtime {

static std::vector<LabelTriplet> get_expand_label_set(
    const ReadTransaction& txn, const std::set<label_t>& label_set,
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

Context EdgeExpand::expand_edge_without_predicate(
    const ReadTransaction& txn, Context&& ctx, const EdgeExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  LOG(INFO) << "expand edge" << (int) params.labels[0].src_label << " "
            << (int) params.labels[0].dst_label << " "
            << (int) params.labels[0].edge_label;

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
      if (props.size() > 1) {
        pt = PropertyType::kRecordView;
      } else if (!props.empty()) {
        pt = props[0];
      }

      SDSLEdgeColumnBuilder builder(Direction::kIn, params.labels[0], pt);

      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       auto ie_iter = txn.GetInEdgeIterator(
                           label, v, output_vertex_label, edge_label);
                       while (ie_iter.IsValid()) {
                         auto nbr = ie_iter.GetNeighbor();
                         CHECK(ie_iter.GetData().type == pt)
                             << ie_iter.GetData().type << " " << pt;
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

      auto& props = txn.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }

      SDSLEdgeColumnBuilder builder(Direction::kOut, params.labels[0], pt);

      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       auto oe_iter = txn.GetOutEdgeIterator(
                           label, v, output_vertex_label, edge_label);
                       while (oe_iter.IsValid()) {
                         auto nbr = oe_iter.GetNeighbor();
                         CHECK(oe_iter.GetData().type == pt);
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
      auto props = txn.schema().get_edge_properties(
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
              txn.GetOutEdgeIterator(label, v, params.labels[0].dst_label,
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
              txn.GetInEdgeIterator(label, v, params.labels[0].src_label,
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
    LOG(INFO) << "expand edge multiple";
    auto column =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
    auto label_set = column->get_labels_set();
    auto labels =
        get_expand_label_set(txn, label_set, params.labels, params.dir);
    std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
    for (auto& triplet : labels) {
      auto& props = txn.schema().get_edge_properties(
          triplet.src_label, triplet.dst_label, triplet.edge_label);
      PropertyType pt = PropertyType::kEmpty;
      if (!props.empty()) {
        pt = props[0];
      }
      label_props.emplace_back(triplet, pt);
    }
    if (params.dir == Direction::kOut || params.dir == Direction::kIn) {
      if (labels.size() == 1) {
      } else if (labels.size() > 1) {
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));

        SDMLEdgeColumnBuilder builder(params.dir, label_props);
        if (params.dir == Direction::kOut) {
          foreach_vertex(input_vertex_list, [&](size_t index, label_t label,
                                                vid_t v) {
            for (auto& triplet : labels) {
              if (triplet.src_label == label) {
                if (params.dir == Direction::kOut) {
                  auto oe_iter = txn.GetOutEdgeIterator(
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
                  auto ie_iter = txn.GetInEdgeIterator(
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
      if (labels.size() > 1) {
        BDMLEdgeColumnBuilder builder(label_props);
        auto& input_vertex_list =
            *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
        foreach_vertex(
            input_vertex_list, [&](size_t index, label_t label, vid_t v) {
              for (auto& triplet : labels) {
                if (triplet.src_label == label) {
                  auto oe_iter = txn.GetOutEdgeIterator(
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
                  auto ie_iter = txn.GetInEdgeIterator(
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
    const ReadTransaction& txn, Context&& ctx, const EdgeExpandParams& params) {
  std::shared_ptr<IVertexColumn> input_vertex_list =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
  VertexColumnType input_vertex_list_type =
      input_vertex_list->vertex_column_type();

  std::set<label_t> output_vertex_set;
  const std::set<label_t>& input_vertex_set =
      input_vertex_list->get_labels_set();
  if (params.dir == Direction::kOut) {
    for (auto& triplet : params.labels) {
      if (input_vertex_set.find(triplet.src_label) != input_vertex_set.end()) {
        output_vertex_set.insert(triplet.dst_label);
      }
    }
  } else if (params.dir == Direction::kIn) {
    for (auto& triplet : params.labels) {
      if (input_vertex_set.find(triplet.dst_label) != input_vertex_set.end()) {
        output_vertex_set.insert(triplet.src_label);
      }
    }
  } else {
    for (auto& triplet : params.labels) {
      if (input_vertex_set.find(triplet.src_label) != input_vertex_set.end()) {
        output_vertex_set.insert(triplet.dst_label);
      }
      if (input_vertex_set.find(triplet.dst_label) != input_vertex_set.end()) {
        output_vertex_set.insert(triplet.src_label);
      }
    }
  }

  // if (output_vertex_set.empty()) {
  //   LOG(FATAL) << "output vertex label set is empty...";
  // }

  std::vector<size_t> shuffle_offset;

  if (output_vertex_set.size() <= 1) {
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
                auto oe_iter = txn.GetOutEdgeIterator(label, v, label,
                                                      label_triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  LOG(INFO) << nbr;
                  builder.push_back_opt(nbr);
                  shuffle_offset.push_back(index);
                  oe_iter.Next();
                }
                auto ie_iter = txn.GetInEdgeIterator(label, v, label,
                                                     label_triplet.edge_label);
                while (ie_iter.IsValid()) {
                  auto nbr = ie_iter.GetNeighbor();
                  LOG(INFO) << nbr;
                  builder.push_back_opt(nbr);
                  shuffle_offset.push_back(index);
                  ie_iter.Next();
                }
              });
          LOG(INFO) << "alias" << params.alias;
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
        } else if (params.dir == Direction::kIn &&
                   label_triplet.src_label == output_vertex_label &&
                   label_triplet.dst_label == input_vertex_label) {
          auto& props = txn.schema().get_edge_properties(
              label_triplet.src_label, label_triplet.dst_label,
              label_triplet.edge_label);
          if (props.empty()) {
            casted_input_vertex_list->foreach_vertex(
                [&](size_t index, label_t label, vid_t v) {
                  auto iter = txn.GetInEdgeIterator(
                      label, v, output_vertex_label, label_triplet.edge_label);
                  while (iter.IsValid()) {
                    builder.push_back_opt(iter.GetNeighbor());
                    shuffle_offset.push_back(index);
                    iter.Next();
                  }
                });
          } else if (props[0] == PropertyType::kDate) {
            // also check csr mutability
            const TypedMutableCsrBase<Date>* csr =
                dynamic_cast<const TypedMutableCsrBase<Date>*>(
                    txn.graph().get_ie_csr(label_triplet.dst_label,
                                           label_triplet.src_label,
                                           label_triplet.edge_label));
            casted_input_vertex_list->foreach_vertex(
                [&](size_t index, label_t label, vid_t v) {
                  auto slice = csr->get_edges(v);
                  for (auto& e : slice) {
                    builder.push_back_opt(e.neighbor);
                    shuffle_offset.push_back(index);
                  }
                });
          } else {
            LOG(FATAL) << "XXXX";
          }
          // casted_input_vertex_list->foreach_vertex(
          //     [&](size_t index, label_t label, vid_t v) {
          //       auto ie_iter = txn.GetInEdgeIterator(
          //           label, v, output_vertex_label,
          //           label_triplet.edge_label);
          //       while (ie_iter.IsValid()) {
          //         auto nbr = ie_iter.GetNeighbor();
          //         builder.push_back_opt(nbr);
          //         shuffle_offset.push_back(index);
          //         ie_iter.Next();
          //       }
          //     });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
        } else if (params.dir == Direction::kOut &&
                   label_triplet.src_label == input_vertex_label &&
                   label_triplet.dst_label == output_vertex_label) {
          casted_input_vertex_list->foreach_vertex(
              [&](size_t index, label_t label, vid_t v) {
                auto oe_iter = txn.GetOutEdgeIterator(
                    label, v, output_vertex_label, label_triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  builder.push_back_opt(nbr);
                  shuffle_offset.push_back(index);
                  oe_iter.Next();
                }
              });
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
        } else {
          LOG(FATAL) << "xxx";
        }
      } else {
        MLVertexColumnBuilder builder;
        if (params.dir == Direction::kOut || params.dir == Direction::kIn) {
          if (params.dir == Direction::kOut) {
            casted_input_vertex_list->foreach_vertex(
                [&](size_t index, label_t label, vid_t v) {
                  for (auto& triplet : params.labels) {
                    if (triplet.src_label == label) {
                      auto oe_iter = txn.GetOutEdgeIterator(
                          label, v, triplet.dst_label, triplet.edge_label);
                      while (oe_iter.IsValid()) {
                        auto nbr = oe_iter.GetNeighbor();
                        builder.push_back_vertex(
                            std::make_pair(triplet.dst_label, nbr));
                        shuffle_offset.push_back(index);
                        oe_iter.Next();
                      }
                    }
                  }
                });
          } else {
            casted_input_vertex_list->foreach_vertex(
                [&](size_t index, label_t label, vid_t v) {
                  for (auto& triplet : params.labels) {
                    if (triplet.dst_label == label) {
                      auto ie_iter = txn.GetInEdgeIterator(
                          label, v, triplet.src_label, triplet.edge_label);
                      while (ie_iter.IsValid()) {
                        auto nbr = ie_iter.GetNeighbor();
                        builder.push_back_vertex(
                            std::make_pair(triplet.src_label, nbr));
                        shuffle_offset.push_back(index);
                        ie_iter.Next();
                      }
                    }
                  }
                });
          }
          ctx.set_with_reshuffle(params.alias, builder.finish(),
                                 shuffle_offset);
        }
      }
    } else if (input_vertex_list_type == VertexColumnType::kMultiple) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MLVertexColumn>(input_vertex_list);
      if (params.dir == Direction::kBoth) {
        LOG(FATAL) << "AAAAAAAAA";
      } else if (params.dir == Direction::kIn) {
        casted_input_vertex_list->foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              for (auto& triplet : params.labels) {
                if (triplet.dst_label == label) {
                  auto ie_iter = txn.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    builder.push_back_opt(nbr);
                    shuffle_offset.push_back(index);
                    ie_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      } else if (params.dir == Direction::kOut) {
        casted_input_vertex_list->foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              for (auto& triplet : params.labels) {
                if (triplet.src_label == label) {
                  auto oe_iter = txn.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    builder.push_back_opt(nbr);
                    shuffle_offset.push_back(index);
                    oe_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      } else {
        LOG(FATAL) << "xxx";
      }
    } else if (input_vertex_list_type == VertexColumnType::kMultiSegment) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<MSVertexColumn>(input_vertex_list);
      if (params.dir == Direction::kBoth) {
        LOG(FATAL) << "AAAAAAAAA";
      } else if (params.dir == Direction::kIn) {
        casted_input_vertex_list->foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              for (auto& triplet : params.labels) {
                if (triplet.dst_label == label) {
                  auto ie_iter = txn.GetInEdgeIterator(
                      label, v, triplet.src_label, triplet.edge_label);
                  while (ie_iter.IsValid()) {
                    auto nbr = ie_iter.GetNeighbor();
                    builder.push_back_opt(nbr);
                    shuffle_offset.push_back(index);
                    ie_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      } else if (params.dir == Direction::kOut) {
        casted_input_vertex_list->foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              for (auto& triplet : params.labels) {
                if (triplet.src_label == label) {
                  auto oe_iter = txn.GetOutEdgeIterator(
                      label, v, triplet.dst_label, triplet.edge_label);
                  while (oe_iter.IsValid()) {
                    auto nbr = oe_iter.GetNeighbor();
                    builder.push_back_opt(nbr);
                    shuffle_offset.push_back(index);
                    oe_iter.Next();
                  }
                }
              }
            });
        ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      } else {
        LOG(FATAL) << "xxx";
      }
    } else {
      LOG(FATAL) << "unexpected input vertex list type";
    }
  } else {
    if (input_vertex_list_type == VertexColumnType::kSingle) {
      auto casted_input_vertex_list =
          std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
      label_t input_vertex_label = casted_input_vertex_list->label();
#if 0
      MLVertexColumnBuilder builder;
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
                      builder.push_back_vertex(
                          std::make_pair(output_vertex_label, nbr));
                      shuffle_offset.push_back(index);
                      ie_iter.Next();
                    }
                  });
            }
          }
        } else if (params.dir == Direction::kOut) {
          LOG(FATAL) << "AAAAA";
        }
      }
#else
      MSVertexColumnBuilder builder;
      for (label_t output_vertex_label : output_vertex_set) {
        builder.start_label(output_vertex_label);
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
                      builder.push_back_opt(nbr);
                      shuffle_offset.push_back(index);
                      ie_iter.Next();
                    }
                  });
            }
          }
        } else if (params.dir == Direction::kOut) {
          LOG(FATAL) << "AAAAA";
        }
      }
#endif

      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    } else {
      LOG(FATAL) << "edge expand vertex input multiple vertex label";
    }
  }

  return ctx;
}

Context EdgeExpand::expand_2d_vertex_without_predicate(
    const ReadTransaction& txn, Context&& ctx, const EdgeExpandParams& params1,
    const EdgeExpandParams& params2) {
  std::shared_ptr<IVertexColumn> input_vertex_list =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params1.v_tag));
  VertexColumnType input_vertex_list_type =
      input_vertex_list->vertex_column_type();

  std::vector<size_t> shuffle_offset;

  if (params1.labels.size() == 1 && params2.labels.size() == 1) {
    if (params1.dir == Direction::kOut && params2.dir == Direction::kOut) {
      label_t d0_label = params1.labels[0].src_label;
      label_t d1_label = params1.labels[0].dst_label;
      label_t d2_label = params2.labels[0].dst_label;
      label_t e0_label = params1.labels[0].edge_label;
      label_t e1_label = params2.labels[0].edge_label;

      SLVertexColumnBuilder builder(d2_label);

      if (input_vertex_list_type == VertexColumnType::kSingle) {
        auto casted_input_vertex_list =
            std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
        auto oe_csr0 =
            txn.GetOutgoingSingleImmutableGraphView<grape::EmptyType>(
                d0_label, d1_label, e0_label);
        auto oe_csr1 = txn.GetOutgoingGraphView<grape::EmptyType>(
            d1_label, d2_label, e1_label);
        casted_input_vertex_list->foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              if (oe_csr0.exist(v)) {
                auto& oe0 = oe_csr0.get_edge(v);
                auto oe1_slice = oe_csr1.get_edges(oe0.neighbor);
                for (auto& e : oe1_slice) {
                  builder.push_back_opt(e.neighbor);
                  shuffle_offset.push_back(index);
                }
              }

              // auto oe_iter0 =
              //     txn.GetOutEdgeIterator(d0_label, v, d1_label, e0_label);
              // while (oe_iter0.IsValid()) {
              //   auto nbr = oe_iter0.GetNeighbor();
              //   auto oe_iter1 =
              //       txn.GetOutEdgeIterator(d1_label, nbr, d2_label,
              //       e1_label);
              //   while (oe_iter1.IsValid()) {
              //     auto nbr2 = oe_iter1.GetNeighbor();
              //     builder.push_back_opt(nbr2);
              //     shuffle_offset.push_back(index);
              //     oe_iter1.Next();
              //   }
              //   oe_iter0.Next();
              // }
            });

        ctx.set_with_reshuffle(params2.alias, builder.finish(), shuffle_offset);
        return ctx;
      }
    }
  }
  LOG(FATAL) << "XXXX";

  return ctx;
}

}  // namespace runtime

}  // namespace gs
