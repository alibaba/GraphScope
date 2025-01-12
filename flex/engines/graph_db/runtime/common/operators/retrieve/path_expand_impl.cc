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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/path_expand_impl.h"

namespace gs {

namespace runtime {

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex(const GraphReadInterface& graph,
                        const SLVertexColumn& input, label_t edge_label,
                        Direction dir, int lower, int upper) {
  int input_label = input.label();
  SLVertexColumnBuilder builder(input_label);
  std::vector<size_t> offsets;
  if (upper == lower) {
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      builder.push_back_opt(v);
      offsets.push_back(idx++);
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  }

  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }

  int depth = 0;
  if (dir == Direction::kOut) {
    while (!output_list.empty()) {
      input_list.clear();
      std::swap(input_list, output_list);
      if (depth >= lower && depth < upper) {
        if (depth == (upper - 1)) {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.first);
            offsets.push_back(pair.second);
          }
        } else {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.first);
            offsets.push_back(pair.second);

            auto it = graph.GetOutEdgeIterator(input_label, pair.first,
                                               input_label, edge_label);
            while (it.IsValid()) {
              output_list.emplace_back(it.GetNeighbor(), pair.second);
              it.Next();
            }
          }
        }
      } else if (depth < lower) {
        for (auto& pair : input_list) {
          auto it = graph.GetOutEdgeIterator(input_label, pair.first,
                                             input_label, edge_label);
          while (it.IsValid()) {
            output_list.emplace_back(it.GetNeighbor(), pair.second);
            it.Next();
          }
        }
      }
      ++depth;
    }
  } else if (dir == Direction::kIn) {
    while (!output_list.empty()) {
      input_list.clear();
      std::swap(input_list, output_list);
      if (depth >= lower && depth < upper) {
        if (depth == (upper - 1)) {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.first);
            offsets.push_back(pair.second);
          }
        } else {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.first);
            offsets.push_back(pair.second);

            auto it = graph.GetInEdgeIterator(input_label, pair.first,
                                              input_label, edge_label);
            while (it.IsValid()) {
              output_list.emplace_back(it.GetNeighbor(), pair.second);
              it.Next();
            }
          }
        }
      } else if (depth < lower) {
        for (auto& pair : input_list) {
          auto it = graph.GetInEdgeIterator(input_label, pair.first,
                                            input_label, edge_label);
          while (it.IsValid()) {
            output_list.emplace_back(it.GetNeighbor(), pair.second);
            it.Next();
          }
        }
      }
      ++depth;
    }
  } else {
    while (!output_list.empty()) {
      input_list.clear();
      std::swap(input_list, output_list);
      if (depth >= lower && depth < upper) {
        if (depth == (upper - 1)) {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.first);
            offsets.push_back(pair.second);
          }
        } else {
          for (auto& pair : input_list) {
            builder.push_back_opt(pair.first);
            offsets.push_back(pair.second);

            auto it0 = graph.GetInEdgeIterator(input_label, pair.first,
                                               input_label, edge_label);
            while (it0.IsValid()) {
              output_list.emplace_back(it0.GetNeighbor(), pair.second);
              it0.Next();
            }
            auto it1 = graph.GetOutEdgeIterator(input_label, pair.first,
                                                input_label, edge_label);
            while (it1.IsValid()) {
              output_list.emplace_back(it1.GetNeighbor(), pair.second);
              it1.Next();
            }
          }
        }
      } else if (depth < lower) {
        for (auto& pair : input_list) {
          auto it0 = graph.GetInEdgeIterator(input_label, pair.first,
                                             input_label, edge_label);
          while (it0.IsValid()) {
            output_list.emplace_back(it0.GetNeighbor(), pair.second);
            it0.Next();
          }
          auto it1 = graph.GetOutEdgeIterator(input_label, pair.first,
                                              input_label, edge_label);
          while (it1.IsValid()) {
            output_list.emplace_back(it1.GetNeighbor(), pair.second);
            it1.Next();
          }
        }
      }
      ++depth;
    }
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
path_expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const SLVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper) {
  if (labels.size() == 1) {
    if (labels[0].src_label == labels[0].dst_label &&
        labels[0].src_label == input.label()) {
      label_t v_label = labels[0].src_label;
      label_t e_label = labels[0].edge_label;
      const auto& properties =
          graph.schema().get_edge_properties(v_label, v_label, e_label);
      if (properties.size() <= 1) {
        if (dir == Direction::kBoth) {
          if (properties.empty() || properties[0] == PropertyType::Empty()) {
            auto iview = graph.GetIncomingGraphView<grape::EmptyType>(
                v_label, v_label, e_label);
            auto oview = graph.GetOutgoingGraphView<grape::EmptyType>(
                v_label, v_label, e_label);
            return iterative_expand_vertex_on_dual_graph_view(
                iview, oview, input, lower, upper);
          } else if (properties[0] == PropertyType::Int32()) {
            auto iview =
                graph.GetIncomingGraphView<int>(v_label, v_label, e_label);
            auto oview =
                graph.GetOutgoingGraphView<int>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_dual_graph_view(
                iview, oview, input, lower, upper);
          } else if (properties[0] == PropertyType::Int64()) {
            auto iview =
                graph.GetIncomingGraphView<int64_t>(v_label, v_label, e_label);
            auto oview =
                graph.GetOutgoingGraphView<int64_t>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_dual_graph_view(
                iview, oview, input, lower, upper);
          } else if (properties[0] == PropertyType::Date()) {
            auto iview =
                graph.GetIncomingGraphView<Date>(v_label, v_label, e_label);
            auto oview =
                graph.GetOutgoingGraphView<Date>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_dual_graph_view(
                iview, oview, input, lower, upper);
          }
        } else if (dir == Direction::kIn) {
          if (properties.empty() || properties[0] == PropertyType::Empty()) {
            auto iview = graph.GetIncomingGraphView<grape::EmptyType>(
                v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(iview, input, lower,
                                                         upper);
          } else if (properties[0] == PropertyType::Int32()) {
            auto iview =
                graph.GetIncomingGraphView<int>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(iview, input, lower,
                                                         upper);
          } else if (properties[0] == PropertyType::Int64()) {
            auto iview =
                graph.GetIncomingGraphView<int64_t>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(iview, input, lower,
                                                         upper);
          } else if (properties[0] == PropertyType::Date()) {
            auto iview =
                graph.GetIncomingGraphView<Date>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(iview, input, lower,
                                                         upper);
          }
        } else if (dir == Direction::kOut) {
          if (properties.empty() || properties[0] == PropertyType::Empty()) {
            auto oview = graph.GetOutgoingGraphView<grape::EmptyType>(
                v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(oview, input, lower,
                                                         upper);
          } else if (properties[0] == PropertyType::Int32()) {
            auto oview =
                graph.GetOutgoingGraphView<int>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(oview, input, lower,
                                                         upper);
          } else if (properties[0] == PropertyType::Int64()) {
            auto oview =
                graph.GetOutgoingGraphView<int64_t>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(oview, input, lower,
                                                         upper);
          } else if (properties[0] == PropertyType::Date()) {
            auto oview =
                graph.GetOutgoingGraphView<Date>(v_label, v_label, e_label);
            return iterative_expand_vertex_on_graph_view(oview, input, lower,
                                                         upper);
          }
        }

        return iterative_expand_vertex(graph, input, e_label, dir, lower,
                                       upper);
      }
    }
  }
  LOG(FATAL) << "not implemented...";
  std::shared_ptr<IContextColumn> ret(nullptr);
  return std::make_pair(ret, std::vector<size_t>());
}

}  // namespace runtime

}  // namespace gs
