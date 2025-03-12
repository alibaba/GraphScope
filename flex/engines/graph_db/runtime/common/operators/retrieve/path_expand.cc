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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/path_expand.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/path_expand_impl.h"
#include "flex/engines/graph_db/runtime/common/utils/bitset.h"

namespace gs {

namespace runtime {

bl::result<Context> PathExpand::edge_expand_v(const GraphReadInterface& graph,
                                              Context&& ctx,
                                              const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  if (params.labels.size() == 1 &&
      ctx.get(params.start_tag)->column_type() == ContextColumnType::kVertex &&
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag))
              ->vertex_column_type() == VertexColumnType::kSingle) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<SLVertexColumn>(ctx.get(params.start_tag));
    auto pair = path_expand_vertex_without_predicate_impl(
        graph, input_vertex_list, params.labels, params.dir, params.hop_lower,
        params.hop_upper);
    ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
    return ctx;
  } else {
    if (params.dir == Direction::kOut) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
      std::set<label_t> labels;
      std::vector<std::vector<LabelTriplet>> out_labels_map(
          graph.schema().vertex_label_num());
      for (const auto& label : params.labels) {
        labels.emplace(label.dst_label);
        out_labels_map[label.src_label].emplace_back(label);
      }

      auto builder = MLVertexColumnBuilder::builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       output.emplace_back(label, v, index);
                     });
      int depth = 0;
      while (depth < params.hop_upper && (!output.empty())) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_vertex({std::get<0>(tuple), std::get<1>(tuple)});
            shuffle_offset.push_back(std::get<2>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (const auto& label_triplet : out_labels_map[label]) {
            auto oe_iter = graph.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              output.emplace_back(label_triplet.dst_label, nbr, index);
              oe_iter.Next();
            }
          }
        }
        ++depth;
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(nullptr),
                             shuffle_offset);
      return ctx;
    } else if (params.dir == Direction::kIn) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
      std::set<label_t> labels;
      std::vector<std::vector<LabelTriplet>> in_labels_map(
          graph.schema().vertex_label_num());
      for (auto& label : params.labels) {
        labels.emplace(label.src_label);
        in_labels_map[label.dst_label].emplace_back(label);
      }

      auto builder = MLVertexColumnBuilder::builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       output.emplace_back(label, v, index);
                     });
      int depth = 0;
      while (depth < params.hop_upper && (!output.empty())) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (const auto& tuple : input) {
            builder.push_back_vertex({std::get<0>(tuple), std::get<1>(tuple)});
            shuffle_offset.push_back(std::get<2>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (const auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (const auto& label_triplet : in_labels_map[label]) {
            auto oe_iter = graph.GetInEdgeIterator(label_triplet.dst_label, v,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              output.emplace_back(label_triplet.src_label, nbr, index);
              oe_iter.Next();
            }
          }
        }
        ++depth;
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(nullptr),
                             shuffle_offset);
      return ctx;
    } else if (params.dir == Direction::kBoth) {
      std::set<label_t> labels;
      std::vector<std::vector<LabelTriplet>> in_labels_map(
          graph.schema().vertex_label_num()),
          out_labels_map(graph.schema().vertex_label_num());
      for (const auto& label : params.labels) {
        labels.emplace(label.dst_label);
        in_labels_map[label.dst_label].emplace_back(label);
        out_labels_map[label.src_label].emplace_back(label);
      }

      auto builder = MLVertexColumnBuilder::builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      auto input_vertex_list =
          std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
      if (input_vertex_list->vertex_column_type() ==
          VertexColumnType::kMultiple) {
        auto& input_vertex_list = *std::dynamic_pointer_cast<MLVertexColumn>(
            ctx.get(params.start_tag));

        input_vertex_list.foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              output.emplace_back(label, v, index);
            });
      } else {
        foreach_vertex(*input_vertex_list,
                       [&](size_t index, label_t label, vid_t v) {
                         output.emplace_back(label, v, index);
                       });
      }
      int depth = 0;
      while (depth < params.hop_upper && (!output.empty())) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_vertex({std::get<0>(tuple), std::get<1>(tuple)});
            shuffle_offset.push_back(std::get<2>(tuple));
          }
        }

        if (depth + 1 >= params.hop_upper) {
          break;
        }

        for (auto& tuple : input) {
          auto label = std::get<0>(tuple);
          auto v = std::get<1>(tuple);
          auto index = std::get<2>(tuple);
          for (const auto& label_triplet : out_labels_map[label]) {
            auto oe_iter = graph.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              output.emplace_back(label_triplet.dst_label, nbr, index);
              oe_iter.Next();
            }
          }
          for (const auto& label_triplet : in_labels_map[label]) {
            auto ie_iter = graph.GetInEdgeIterator(label_triplet.dst_label, v,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto nbr = ie_iter.GetNeighbor();
              output.emplace_back(label_triplet.src_label, nbr, index);
              ie_iter.Next();
            }
          }
        }
        depth++;
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(nullptr),
                             shuffle_offset);
      return ctx;
    }
  }
  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

bl::result<Context> PathExpand::edge_expand_p(const GraphReadInterface& graph,
                                              Context&& ctx,
                                              const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  std::vector<std::vector<LabelTriplet>> out_labels_map(
      graph.schema().vertex_label_num()),
      in_labels_map(graph.schema().vertex_label_num());
  for (const auto& triplet : labels) {
    out_labels_map[triplet.src_label].emplace_back(triplet);
    in_labels_map[triplet.dst_label].emplace_back(triplet);
  }
  auto dir = params.dir;
  std::vector<std::pair<std::unique_ptr<PathImpl>, size_t>> input;
  std::vector<std::pair<std::unique_ptr<PathImpl>, size_t>> output;

  GeneralPathColumnBuilder builder;
  std::shared_ptr<Arena> arena = std::make_shared<Arena>();
  if (dir == Direction::kOut) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(std::move(p), index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth + 1 < params.hop_upper) {
        for (auto& [path, index] : input) {
          auto end = path->get_end();
          for (const auto& label_triplet : out_labels_map[end.label_]) {
            auto oe_iter = graph.GetOutEdgeIterator(end.label_, end.vid_,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              std::unique_ptr<PathImpl> new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              oe_iter.Next();
            }
          }
        }
      }

      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path(path.get()));
          arena->emplace_back(std::move(path));
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(arena), shuffle_offset);

    return ctx;
  } else if (dir == Direction::kIn) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(std::move(p), index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();

      if (depth + 1 < params.hop_upper) {
        for (const auto& [path, index] : input) {
          auto end = path->get_end();
          for (const auto& label_triplet : in_labels_map[end.label_]) {
            auto ie_iter = graph.GetInEdgeIterator(end.label_, end.vid_,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              std::unique_ptr<PathImpl> new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.src_label, ie_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              ie_iter.Next();
            }
          }
        }
      }

      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path(path.get()));
          arena->emplace_back(std::move(path));
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(arena), shuffle_offset);

    return ctx;

  } else if (dir == Direction::kBoth) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(std::move(p), index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth + 1 < params.hop_upper) {
        for (auto& [path, index] : input) {
          auto end = path->get_end();
          for (const auto& label_triplet : out_labels_map[end.label_]) {
            auto oe_iter = graph.GetOutEdgeIterator(end.label_, end.vid_,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              oe_iter.Next();
            }
          }

          for (const auto& label_triplet : in_labels_map[end.label_]) {
            auto ie_iter = graph.GetInEdgeIterator(end.label_, end.vid_,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.edge_label,
                               label_triplet.src_label, ie_iter.GetNeighbor());
              output.emplace_back(std::move(new_path), index);
              ie_iter.Next();
            }
          }
        }
      }

      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path(path.get()));
          arena->emplace_back(std::move(path));
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(arena), shuffle_offset);
    return ctx;
  }
  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

static bool single_source_single_dest_shortest_path_impl(
    const GraphReadInterface& graph, const ShortestPathParams& params,
    vid_t src, vid_t dst, std::vector<vid_t>& path) {
  std::queue<vid_t> q1;
  std::queue<vid_t> q2;
  std::queue<vid_t> tmp;

  label_t v_label = params.labels[0].src_label;
  label_t e_label = params.labels[0].edge_label;
  auto vertices = graph.GetVertexSet(v_label);
  GraphReadInterface::vertex_array_t<int> pre(vertices, -1);
  GraphReadInterface::vertex_array_t<int> dis(vertices, 0);
  q1.push(src);
  dis[src] = 1;
  q2.push(dst);
  dis[dst] = -1;

  while (true) {
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        int x = q1.front();
        if (dis[x] >= params.hop_upper + 1) {
          return false;
        }
        q1.pop();
        auto oe_iter = graph.GetOutEdgeIterator(v_label, x, v_label, e_label);
        while (oe_iter.IsValid()) {
          int y = oe_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(v_label, x, v_label, e_label);
        while (ie_iter.IsValid()) {
          int y = ie_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          ie_iter.Next();
        }
      }
      std::swap(q1, tmp);
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        int x = q2.front();
        if (dis[x] <= -params.hop_upper - 1) {
          return false;
        }
        q2.pop();
        auto oe_iter = graph.GetOutEdgeIterator(v_label, x, v_label, e_label);
        while (oe_iter.IsValid()) {
          int y = oe_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(v_label, x, v_label, e_label);
        while (ie_iter.IsValid()) {
          int y = ie_iter.GetNeighbor();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            return len >= params.hop_lower && len < params.hop_upper;
          }
          ie_iter.Next();
        }
      }
      std::swap(q2, tmp);
    }
  }
  return false;
}

bl::result<Context> PathExpand::single_source_single_dest_shortest_path(
    const GraphReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, std::pair<label_t, vid_t>& dest) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  if (labels.size() != 1 || label_sets.size() != 1) {
    LOG(ERROR) << "only support one label triplet";
    RETURN_UNSUPPORTED_ERROR("only support one label triplet");
  }
  auto label_triplet = labels[0];
  if (label_triplet.src_label != label_triplet.dst_label ||
      params.dir != Direction::kBoth) {
    LOG(ERROR) << "only support same src and dst label and both direction";
    RETURN_UNSUPPORTED_ERROR(
        "only support same src and dst label and both "
        "direction");
  }
  auto builder = SLVertexColumnBuilder::builder(label_triplet.dst_label);
  GeneralPathColumnBuilder path_builder;
  std::shared_ptr<Arena> arena = std::make_shared<Arena>();
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<vid_t> path;
    if (single_source_single_dest_shortest_path_impl(graph, params, v,
                                                     dest.second, path)) {
      builder.push_back_opt(dest.second);
      shuffle_offset.push_back(index);
      auto impl = PathImpl::make_path_impl(label_triplet.src_label,
                                           label_triplet.edge_label, path);
      path_builder.push_back_opt(Path(impl.get()));
      arena->emplace_back(std::move(impl));
    }
  });

  ctx.set_with_reshuffle(params.v_alias, builder.finish(nullptr),
                         shuffle_offset);
  ctx.set(params.alias, path_builder.finish(arena));
  return ctx;
}

static void dfs(const GraphReadInterface& graph, vid_t src, vid_t dst,
                const GraphReadInterface::vertex_array_t<bool>& visited,
                const GraphReadInterface::vertex_array_t<int8_t>& dist,
                const ShortestPathParams& params,
                std::vector<std::vector<vid_t>>& paths,
                std::vector<vid_t>& cur_path) {
  cur_path.push_back(src);
  if (src == dst) {
    paths.emplace_back(cur_path);
    cur_path.pop_back();
    return;
  }
  auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].src_label, src,
                                          params.labels[0].dst_label,
                                          params.labels[0].edge_label);
  while (oe_iter.IsValid()) {
    vid_t nbr = oe_iter.GetNeighbor();
    if (visited[nbr] && dist[nbr] == dist[src] + 1) {
      dfs(graph, nbr, dst, visited, dist, params, paths, cur_path);
    }
    oe_iter.Next();
  }
  auto ie_iter = graph.GetInEdgeIterator(params.labels[0].dst_label, src,
                                         params.labels[0].src_label,
                                         params.labels[0].edge_label);
  while (ie_iter.IsValid()) {
    vid_t nbr = ie_iter.GetNeighbor();
    if (visited[nbr] && dist[nbr] == dist[src] + 1) {
      dfs(graph, nbr, dst, visited, dist, params, paths, cur_path);
    }

    ie_iter.Next();
  }
  cur_path.pop_back();
}
static void all_shortest_path_with_given_source_and_dest_impl(
    const GraphReadInterface& graph, const ShortestPathParams& params,
    vid_t src, vid_t dst, std::vector<std::vector<vid_t>>& paths) {
  GraphReadInterface::vertex_array_t<int8_t> dist_from_src(
      graph.GetVertexSet(params.labels[0].src_label), -1);
  GraphReadInterface::vertex_array_t<int8_t> dist_from_dst(
      graph.GetVertexSet(params.labels[0].dst_label), -1);
  dist_from_src[src] = 0;
  dist_from_dst[dst] = 0;
  std::queue<vid_t> q1, q2, tmp;
  q1.push(src);
  q2.push(dst);
  std::vector<vid_t> vec;
  int8_t src_dep = 0, dst_dep = 0;

  while (true) {
    if (src_dep >= params.hop_upper || dst_dep >= params.hop_upper ||
        !vec.empty()) {
      break;
    }
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        vid_t v = q1.front();
        q1.pop();
        auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].src_label, v,
                                                params.labels[0].dst_label,
                                                params.labels[0].edge_label);
        while (oe_iter.IsValid()) {
          vid_t nbr = oe_iter.GetNeighbor();
          if (dist_from_src[nbr] == -1) {
            dist_from_src[nbr] = src_dep + 1;
            tmp.push(nbr);
            if (dist_from_dst[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(params.labels[0].dst_label, v,
                                               params.labels[0].src_label,
                                               params.labels[0].edge_label);
        while (ie_iter.IsValid()) {
          vid_t nbr = ie_iter.GetNeighbor();
          if (dist_from_src[nbr] == -1) {
            dist_from_src[nbr] = src_dep + 1;
            tmp.push(nbr);
            if (dist_from_dst[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          ie_iter.Next();
        }
      }
      std::swap(q1, tmp);
      ++src_dep;
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        vid_t v = q2.front();
        q2.pop();
        auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].dst_label, v,
                                                params.labels[0].src_label,
                                                params.labels[0].edge_label);
        while (oe_iter.IsValid()) {
          vid_t nbr = oe_iter.GetNeighbor();
          if (dist_from_dst[nbr] == -1) {
            dist_from_dst[nbr] = dst_dep + 1;
            tmp.push(nbr);
            if (dist_from_src[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          oe_iter.Next();
        }
        auto ie_iter = graph.GetInEdgeIterator(params.labels[0].src_label, v,
                                               params.labels[0].dst_label,
                                               params.labels[0].edge_label);
        while (ie_iter.IsValid()) {
          vid_t nbr = ie_iter.GetNeighbor();
          if (dist_from_dst[nbr] == -1) {
            dist_from_dst[nbr] = dst_dep + 1;
            tmp.push(nbr);
            if (dist_from_src[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
          ie_iter.Next();
        }
      }
      std::swap(q2, tmp);
      ++dst_dep;
    }
  }

  while (!q1.empty()) {
    q1.pop();
  }
  if (vec.empty()) {
    return;
  }
  if (src_dep + dst_dep >= params.hop_upper) {
    return;
  }
  GraphReadInterface::vertex_array_t<bool> visited(
      graph.GetVertexSet(params.labels[0].src_label), false);
  for (auto v : vec) {
    q1.push(v);
    visited[v] = true;
  }
  while (!q1.empty()) {
    auto v = q1.front();
    q1.pop();
    auto oe_iter = graph.GetOutEdgeIterator(params.labels[0].src_label, v,
                                            params.labels[0].dst_label,
                                            params.labels[0].edge_label);
    while (oe_iter.IsValid()) {
      vid_t nbr = oe_iter.GetNeighbor();
      if (visited[nbr]) {
        oe_iter.Next();
        continue;
      }
      if (dist_from_src[nbr] != -1 &&
          dist_from_src[nbr] + 1 == dist_from_src[v]) {
        q1.push(nbr);
        visited[nbr] = true;
      }
      if (dist_from_dst[nbr] != -1 &&
          dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
        q1.push(nbr);
        visited[nbr] = true;
        dist_from_src[nbr] = dist_from_src[v] + 1;
      }
      oe_iter.Next();
    }

    auto ie_iter = graph.GetInEdgeIterator(params.labels[0].dst_label, v,
                                           params.labels[0].src_label,
                                           params.labels[0].edge_label);
    while (ie_iter.IsValid()) {
      vid_t nbr = ie_iter.GetNeighbor();
      if (visited[nbr]) {
        ie_iter.Next();
        continue;
      }
      if (dist_from_src[nbr] != -1 &&
          dist_from_src[nbr] + 1 == dist_from_src[v]) {
        q1.push(nbr);
        visited[nbr] = true;
      }
      if (dist_from_dst[nbr] != -1 &&
          dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
        q1.push(nbr);
        visited[nbr] = true;
        dist_from_src[nbr] = dist_from_src[v] + 1;
      }
      ie_iter.Next();
    }
  }
  std::vector<vid_t> cur_path;
  dfs(graph, src, dst, visited, dist_from_src, params, paths, cur_path);
}

bl::result<Context> PathExpand::all_shortest_paths_with_given_source_and_dest(
    const GraphReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, const std::pair<label_t, vid_t>& dest) {
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  if (labels.size() != 1 || label_sets.size() != 1) {
    LOG(ERROR) << "only support one label triplet";
    RETURN_UNSUPPORTED_ERROR("only support one label triplet");
  }
  auto label_triplet = labels[0];
  if (label_triplet.src_label != label_triplet.dst_label) {
    LOG(ERROR) << "only support same src and dst label";
    RETURN_UNSUPPORTED_ERROR("only support same src and dst label");
  }
  auto dir = params.dir;
  if (dir != Direction::kBoth) {
    LOG(ERROR) << "only support both direction";
    RETURN_UNSUPPORTED_ERROR("only support both direction");
  }

  if (dest.first != label_triplet.dst_label) {
    LOG(ERROR) << "only support same src and dst label";
    RETURN_UNSUPPORTED_ERROR("only support same src and dst label");
  }
  auto builder = SLVertexColumnBuilder::builder(label_triplet.dst_label);
  GeneralPathColumnBuilder path_builder;
  std::vector<size_t> shuffle_offset;
  std::shared_ptr<Arena> arena = std::make_shared<Arena>();
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<std::vector<vid_t>> paths;
    all_shortest_path_with_given_source_and_dest_impl(graph, params, v,
                                                      dest.second, paths);
    for (auto& path : paths) {
      auto ptr = PathImpl::make_path_impl(label_triplet.src_label,
                                          label_triplet.edge_label, path);
      builder.push_back_opt(dest.second);
      path_builder.push_back_opt(Path(ptr.get()));
      arena->emplace_back(std::move(ptr));
      shuffle_offset.push_back(index);
    }
  });
  ctx.set_with_reshuffle(params.v_alias, builder.finish(nullptr),
                         shuffle_offset);
  ctx.set(params.alias, path_builder.finish(arena));
  return ctx;
}

template <typename T>
static bl::result<Context> _single_shortest_path(
    const GraphReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, const SPVertexPredicate& pred) {
  if (pred.type() == SPPredicateType::kPropertyLT) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyLTPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyLTPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyGTPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyGTPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyLEPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyLEPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyGEPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyGEPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyBetween) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyBetweenPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyBetweenPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyEQ) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyEQPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyEQPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return PathExpand::single_source_shortest_path<
        VertexPropertyNEPredicateBeta<T>>(
        graph, std::move(ctx), params,
        dynamic_cast<const VertexPropertyNEPredicateBeta<T>&>(pred));
  } else {
    LOG(ERROR) << "not support edge property type "
               << static_cast<int>(pred.type());
    RETURN_UNSUPPORTED_ERROR("not support edge property type");
  }
}

bl::result<Context>
PathExpand::single_source_shortest_path_with_special_vertex_predicate(
    const GraphReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, const SPVertexPredicate& pred) {
  if (pred.data_type() == RTAnyType::kI64Value) {
    return _single_shortest_path<int64_t>(graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _single_shortest_path<std::string_view>(graph, std::move(ctx),
                                                   params, pred);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _single_shortest_path<Date>(graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _single_shortest_path<double>(graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _single_shortest_path<int>(graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kDate32) {
    return _single_shortest_path<Day>(graph, std::move(ctx), params, pred);
  } else if (pred.data_type() == RTAnyType::kEmpty) {
    return _single_shortest_path<grape::EmptyType>(graph, std::move(ctx),
                                                   params, pred);
  } else {
    LOG(ERROR) << "not support edge property type "
               << static_cast<int>(pred.type());
    RETURN_UNSUPPORTED_ERROR("not support edge property type");
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs
