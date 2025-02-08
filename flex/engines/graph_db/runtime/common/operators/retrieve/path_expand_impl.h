
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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_IMPL_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_IMPL_H_

#include <memory>
#include <utility>
#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"

namespace gs {
namespace runtime {

template <typename EDATA_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex_on_graph_view(
    const GraphReadInterface::graph_view_t<EDATA_T>& view,
    const SLVertexColumn& input, int lower, int upper) {
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
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }
  int depth = 0;
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

          auto es = view.get_edges(pair.first);
          for (auto& e : es) {
            output_list.emplace_back(e.get_neighbor(), pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto es = view.get_edges(pair.first);
        for (auto& e : es) {
          output_list.emplace_back(e.get_neighbor(), pair.second);
        }
      }
    }
    ++depth;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename EDATA_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex_on_dual_graph_view(
    const GraphReadInterface::graph_view_t<EDATA_T>& iview,
    const GraphReadInterface::graph_view_t<EDATA_T>& oview,
    const SLVertexColumn& input, int lower, int upper) {
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
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }
  int depth = 0;
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

          auto ies = iview.get_edges(pair.first);
          for (auto& e : ies) {
            output_list.emplace_back(e.get_neighbor(), pair.second);
          }
          auto oes = oview.get_edges(pair.first);
          for (auto& e : oes) {
            output_list.emplace_back(e.get_neighbor(), pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto ies = iview.get_edges(pair.first);
        for (auto& e : ies) {
          output_list.emplace_back(e.get_neighbor(), pair.second);
        }
        auto oes = oview.get_edges(pair.first);
        for (auto& e : oes) {
          output_list.emplace_back(e.get_neighbor(), pair.second);
        }
      }
    }
    ++depth;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
path_expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const SLVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper);

template <typename EDATA_T, typename PRED_T>
void sssp_dir(const GraphReadInterface::graph_view_t<EDATA_T>& view,
              label_t v_label, vid_t v,
              const GraphReadInterface::vertex_set_t& vertices, size_t idx,
              int lower, int upper, SLVertexColumnBuilder& dest_col_builder,
              GeneralPathColumnBuilder& path_col_builder,
              std::vector<std::unique_ptr<CpxValueBase>>& path_impls,
              std::vector<size_t>& offsets, const PRED_T& pred) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  GraphReadInterface::vertex_array_t<vid_t> parent(
      vertices, GraphReadInterface::kInvalidVid);

  while (depth < upper && !cur.empty()) {
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path(impl.get()));
            path_impls.emplace_back(std::move(impl));
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path(impl.get()));
            path_impls.emplace_back(std::move(impl));
            offsets.push_back(idx);
          }
          for (auto& e : view.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (parent[nbr] == GraphReadInterface::kInvalidVid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        for (auto& e : view.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (parent[nbr] == GraphReadInterface::kInvalidVid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename EDATA_T, typename PRED_T>
void sssp_both_dir(const GraphReadInterface::graph_view_t<EDATA_T>& view0,
                   const GraphReadInterface::graph_view_t<EDATA_T>& view1,
                   label_t v_label, vid_t v,
                   const GraphReadInterface::vertex_set_t& vertices, size_t idx,
                   int lower, int upper,
                   SLVertexColumnBuilder& dest_col_builder,
                   GeneralPathColumnBuilder& path_col_builder,
                   std::vector<std::unique_ptr<CpxValueBase>>& path_impls,
                   std::vector<size_t>& offsets, const PRED_T& pred) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  GraphReadInterface::vertex_array_t<vid_t> parent(
      vertices, GraphReadInterface::kInvalidVid);

  while (depth < upper && !cur.empty()) {
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path(impl.get()));
            path_impls.emplace_back(std::move(impl));
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, path);
            path_col_builder.push_back_opt(Path(impl.get()));
            path_impls.emplace_back(std::move(impl));
            offsets.push_back(idx);
          }
          for (auto& e : view0.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (parent[nbr] == GraphReadInterface::kInvalidVid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
          for (auto& e : view1.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (parent[nbr] == GraphReadInterface::kInvalidVid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        for (auto& e : view0.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (parent[nbr] == GraphReadInterface::kInvalidVid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
        for (auto& e : view1.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (parent[nbr] == GraphReadInterface::kInvalidVid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename EDATA_T, typename PRED_T>
void sssp_both_dir_with_order_by_length_limit(
    const GraphReadInterface::graph_view_t<EDATA_T>& view0,
    const GraphReadInterface::graph_view_t<EDATA_T>& view1, label_t v_label,
    vid_t v, const GraphReadInterface::vertex_set_t& vertices, size_t idx,
    int lower, int upper, SLVertexColumnBuilder& dest_col_builder,
    ValueColumnBuilder<int>& path_len_builder, std::vector<size_t>& offsets,
    const PRED_T& pred, int limit_upper) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  GraphReadInterface::vertex_array_t<bool> vis(vertices, false);
  vis[v] = true;

  while (depth < upper && !cur.empty()) {
    if (offsets.size() >= static_cast<size_t>(limit_upper)) {
      break;
    }
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            dest_col_builder.push_back_opt(u);

            path_len_builder.push_back_opt(depth);
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u)) {
            dest_col_builder.push_back_opt(u);

            path_len_builder.push_back_opt(depth);
            offsets.push_back(idx);
          }
          for (auto& e : view0.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (!vis[nbr]) {
              vis[nbr] = true;
              next.push_back(nbr);
            }
          }
          for (auto& e : view1.get_edges(u)) {
            auto nbr = e.get_neighbor();
            if (!vis[nbr]) {
              vis[nbr] = true;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        for (auto& e : view0.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (!vis[nbr]) {
            vis[nbr] = true;
            next.push_back(nbr);
          }
        }
        for (auto& e : view1.get_edges(u)) {
          auto nbr = e.get_neighbor();
          if (!vis[nbr]) {
            vis[nbr] = true;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}
template <typename EDATA_T, typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
single_source_shortest_path_with_order_by_length_limit_impl(
    const GraphReadInterface& graph, const IVertexColumn& input,
    label_t e_label, Direction dir, int lower, int upper, const PRED_T& pred,
    int limit_upper) {
  label_t v_label = *input.get_labels_set().begin();
  auto vertices = graph.GetVertexSet(v_label);
  SLVertexColumnBuilder dest_col_builder(v_label);
  ValueColumnBuilder<int32_t> path_len_builder;

  std::vector<size_t> offsets;
  {
    CHECK(dir == Direction::kBoth);
    auto oe_view =
        graph.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    auto ie_view =
        graph.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir_with_order_by_length_limit(
          oe_view, ie_view, v_label, v, vertices, idx, lower, upper,
          dest_col_builder, path_len_builder, offsets, pred, limit_upper);
    });
  }

  return std::make_tuple(dest_col_builder.finish(), path_len_builder.finish(),
                         std::move(offsets));
}

template <typename EDATA_T, typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
single_source_shortest_path_impl(
    std::vector<std::unique_ptr<CpxValueBase>>& path_impls,
    const GraphReadInterface& graph, const IVertexColumn& input,
    label_t e_label, Direction dir, int lower, int upper, const PRED_T& pred) {
  label_t v_label = *input.get_labels_set().begin();
  auto vertices = graph.GetVertexSet(v_label);
  SLVertexColumnBuilder dest_col_builder(v_label);
  GeneralPathColumnBuilder path_col_builder;
  std::vector<size_t> offsets;
  if (dir == Direction::kIn || dir == Direction::kOut) {
    auto view =
        (dir == Direction::kIn)
            ? graph.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label)
            : graph.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_dir(view, label, v, vertices, idx, lower, upper, dest_col_builder,
               path_col_builder, path_impls, offsets, pred);
    });
  } else {
    CHECK(dir == Direction::kBoth);
    auto oe_view =
        graph.GetOutgoingGraphView<EDATA_T>(v_label, v_label, e_label);
    auto ie_view =
        graph.GetIncomingGraphView<EDATA_T>(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir(oe_view, ie_view, v_label, v, vertices, idx, lower, upper,
                    dest_col_builder, path_col_builder, path_impls, offsets,
                    pred);
    });
  }
  return std::make_tuple(dest_col_builder.finish(), path_col_builder.finish(),
                         std::move(offsets));
}

template <typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
default_single_source_shortest_path_impl(
    std::vector<std::unique_ptr<CpxValueBase>>& path_impls,
    const GraphReadInterface& graph, const IVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper, const PRED_T& pred) {
  label_t label_num = graph.schema().vertex_label_num();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> labels_map(
      label_num);
  const auto& input_labels_set = input.get_labels_set();
  std::set<label_t> dest_labels;
  for (auto& triplet : labels) {
    if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                              triplet.edge_label)) {
      continue;
    }
    if (dir == Direction::kOut || dir == Direction::kBoth) {
      if (input_labels_set.find(triplet.src_label) != input_labels_set.end()) {
        labels_map[triplet.src_label].emplace_back(
            triplet.dst_label, triplet.edge_label, Direction::kOut);
        dest_labels.insert(triplet.dst_label);
      }
    }
    if (dir == Direction::kIn || dir == Direction::kBoth) {
      if (input_labels_set.find(triplet.dst_label) != input_labels_set.end()) {
        labels_map[triplet.dst_label].emplace_back(
            triplet.src_label, triplet.edge_label, Direction::kIn);
        dest_labels.insert(triplet.src_label);
      }
    }
  }
  GeneralPathColumnBuilder path_col_builder;
  std::vector<size_t> offsets;

  std::shared_ptr<IContextColumn> dest_col(nullptr);
  if (dest_labels.size() == 1) {
    SLVertexColumnBuilder dest_col_builder(*dest_labels.begin());

    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      std::vector<std::pair<label_t, vid_t>> cur;
      std::vector<std::pair<label_t, vid_t>> next;
      cur.emplace_back(label, v);
      std::map<std::pair<label_t, vid_t>, std::pair<label_t, vid_t>> parent;
      int depth = 0;
      while (depth < upper && !cur.empty()) {
        for (auto u : cur) {
          if (depth >= lower && pred(u.first, u.second)) {
            std::vector<VertexRecord> path;
            auto x = u;
            while (!(x.first == label && x.second == v)) {
              path.emplace_back(VertexRecord{x.first, x.second});
              x = parent[x];
            }
            path.emplace_back(VertexRecord{label, v});
            std::reverse(path.begin(), path.end());

            if (path.size() > 1) {
              auto impl = PathImpl::make_path_impl(std::move(path));
              path_col_builder.push_back_opt(Path(impl.get()));
              path_impls.emplace_back(std::move(impl));

              dest_col_builder.push_back_opt(u.second);
              offsets.push_back(idx);
            }
          }

          for (auto& l : labels_map[u.first]) {
            label_t nbr_label = std::get<0>(l);
            auto iter = (std::get<2>(l) == Direction::kOut)
                            ? graph.GetOutEdgeIterator(
                                  u.first, u.second, nbr_label, std::get<1>(l))
                            : graph.GetInEdgeIterator(
                                  u.first, u.second, nbr_label, std::get<1>(l));
            while (iter.IsValid()) {
              auto nbr = std::make_pair(nbr_label, iter.GetNeighbor());
              if (parent.find(nbr) == parent.end()) {
                parent[nbr] = u;
                next.push_back(nbr);
              }
              iter.Next();
            }
          }

          ++depth;
          cur.clear();
          std::swap(cur, next);
        }
      }
    });

    dest_col = dest_col_builder.finish();
  } else {
    MLVertexColumnBuilder dest_col_builder;

    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      std::vector<std::pair<label_t, vid_t>> cur;
      std::vector<std::pair<label_t, vid_t>> next;
      cur.emplace_back(label, v);
      std::map<std::pair<label_t, vid_t>, std::pair<label_t, vid_t>> parent;
      int depth = 0;
      while (depth < upper && !cur.empty()) {
        for (auto u : cur) {
          if (depth >= lower && pred(u.first, u.second)) {
            std::vector<VertexRecord> path;
            auto x = u;
            while (!(x.first == label && x.second == v)) {
              path.emplace_back(VertexRecord{x.first, x.second});
              x = parent[x];
            }
            path.emplace_back(VertexRecord{label, v});
            std::reverse(path.begin(), path.end());

            if (path.size() > 1) {
              auto impl = PathImpl::make_path_impl(std::move(path));

              path_col_builder.push_back_opt(Path(impl.get()));
              path_impls.emplace_back(std::move(impl));

              dest_col_builder.push_back_vertex({u.first, u.second});
              offsets.push_back(idx);
            }
          }

          for (auto& l : labels_map[u.first]) {
            label_t nbr_label = std::get<0>(l);
            auto iter = (std::get<2>(l) == Direction::kOut)
                            ? graph.GetOutEdgeIterator(
                                  u.first, u.second, nbr_label, std::get<1>(l))
                            : graph.GetInEdgeIterator(
                                  u.first, u.second, nbr_label, std::get<1>(l));
            while (iter.IsValid()) {
              auto nbr = std::make_pair(nbr_label, iter.GetNeighbor());
              if (parent.find(nbr) == parent.end()) {
                parent[nbr] = u;
                next.push_back(nbr);
              }
              iter.Next();
            }
          }

          ++depth;
          cur.clear();
          std::swap(cur, next);
        }
      }
    });

    dest_col = dest_col_builder.finish();
  }
  return std::make_tuple(dest_col, path_col_builder.finish(),
                         std::move(offsets));
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_