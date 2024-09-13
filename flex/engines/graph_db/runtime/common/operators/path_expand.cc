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

#include "flex/engines/graph_db/runtime/common/operators/path_expand.h"

namespace gs {

namespace runtime {

bl::result<Context> PathExpand::edge_expand_v(const ReadTransaction& txn,
                                              Context&& ctx,
                                              const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  if (params.labels.size() == 1) {
    if (params.dir == Direction::kOut) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<SLVertexColumn>(ctx.get(params.start_tag));
      label_t output_vertex_label = params.labels[0].dst_label;
      label_t edge_label = params.labels[0].edge_label;
      SLVertexColumnBuilder builder(output_vertex_label);

      std::vector<vid_t> input;
      std::vector<vid_t> output;
      input_vertex_list.foreach_vertex(
          [&](size_t index, label_t label, vid_t v) {
            int depth = 0;
            input.clear();
            output.clear();
            input.push_back(v);
            while (depth < params.hop_upper && !input.empty()) {
              if (depth >= params.hop_lower) {
                for (auto u : input) {
                  builder.push_back_opt(u);
                  shuffle_offset.push_back(index);

                  auto oe_iter = txn.GetOutEdgeIterator(
                      label, u, output_vertex_label, edge_label);
                  while (oe_iter.IsValid()) {
                    output.push_back(oe_iter.GetNeighbor());
                    oe_iter.Next();
                  }
                }
              } else {
                for (auto u : input) {
                  auto oe_iter = txn.GetOutEdgeIterator(
                      label, u, output_vertex_label, edge_label);
                  while (oe_iter.IsValid()) {
                    output.push_back(oe_iter.GetNeighbor());
                    oe_iter.Next();
                  }
                }
              }
              ++depth;
              input.clear();
              std::swap(input, output);
            }
          });

      ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                  shuffle_offset, params.keep_cols);
      return ctx;
    } else if (params.dir == Direction::kBoth &&
               params.labels[0].src_label == params.labels[0].dst_label) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<SLVertexColumn>(ctx.get(params.start_tag));
      label_t output_vertex_label = params.labels[0].dst_label;
      label_t edge_label = params.labels[0].edge_label;

      SLVertexColumnBuilder builder(output_vertex_label);

      std::vector<std::pair<size_t, vid_t>> input;
      std::vector<std::pair<size_t, vid_t>> output;
      std::set<vid_t> exclude;
      CHECK_GE(params.hop_lower, 0);
      CHECK_GE(params.hop_upper, params.hop_lower);
      if (params.hop_lower == 0) {
        RETURN_BAD_REQUEST_ERROR("hop_lower should be greater than 0");
      } else {
        if (params.hop_upper == 1) {
          RETURN_BAD_REQUEST_ERROR("hop_upper should be greater than 1");
        } else {
          input_vertex_list.foreach_vertex(
              [&](size_t index, label_t label, vid_t v) {
                output.emplace_back(index, v);
              });
        }
        int depth = 0;
        while (depth < params.hop_upper) {
          input.clear();
          std::swap(input, output);

          if (depth >= params.hop_lower) {
            for (auto& pair : input) {
              builder.push_back_opt(pair.second);
              shuffle_offset.push_back(pair.first);
            }
          }

          if (depth + 1 >= params.hop_upper) {
            break;
          }

          auto label = params.labels[0].src_label;
          for (auto& pair : input) {
            auto index = pair.first;
            auto v = pair.second;
            auto oe_iter = txn.GetOutEdgeIterator(label, v, output_vertex_label,
                                                  edge_label);
            while (oe_iter.IsValid()) {
              auto nbr = oe_iter.GetNeighbor();
              if (exclude.find(nbr) == exclude.end()) {
                output.emplace_back(index, nbr);
              }
              oe_iter.Next();
            }

            auto ie_iter = txn.GetInEdgeIterator(label, v, output_vertex_label,
                                                 edge_label);
            while (ie_iter.IsValid()) {
              auto nbr = ie_iter.GetNeighbor();
              if (exclude.find(nbr) == exclude.end()) {
                output.emplace_back(index, nbr);
              }
              ie_iter.Next();
            }
          }

          ++depth;
        }
      }
      ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                  shuffle_offset, params.keep_cols);
      return ctx;
    }
  } else {
    if (params.dir == Direction::kOut) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
      std::set<label_t> labels;
      for (auto& label : params.labels) {
        labels.emplace(label.dst_label);
      }

      MLVertexColumnBuilder builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       output.emplace_back(label, v, index);
                     });
      int depth = 0;
      while (depth < params.hop_upper) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_vertex(
                std::make_pair(std::get<0>(tuple), std::get<1>(tuple)));
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
          for (auto& label_triplet : params.labels) {
            if (label_triplet.src_label == label) {
              auto oe_iter = txn.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                output.emplace_back(label_triplet.dst_label, nbr, index);
                oe_iter.Next();
              }
            }
          }
        }
        ++depth;
      }
      ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                  shuffle_offset, params.keep_cols);
      return ctx;
    } else if (params.dir == Direction::kBoth) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<MLVertexColumn>(ctx.get(params.start_tag));
      std::set<label_t> labels;
      for (auto& label : params.labels) {
        labels.emplace(label.dst_label);
      }

      MLVertexColumnBuilder builder(labels);
      std::vector<std::tuple<label_t, vid_t, size_t>> input;
      std::vector<std::tuple<label_t, vid_t, size_t>> output;
      input_vertex_list.foreach_vertex(
          [&](size_t index, label_t label, vid_t v) {
            output.emplace_back(label, v, index);
          });
      int depth = 0;
      while (depth < params.hop_upper) {
        input.clear();
        std::swap(input, output);
        if (depth >= params.hop_lower) {
          for (auto& tuple : input) {
            builder.push_back_vertex(
                std::make_pair(std::get<0>(tuple), std::get<1>(tuple)));
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
          for (auto& label_triplet : params.labels) {
            if (label_triplet.src_label == label) {
              auto oe_iter = txn.GetOutEdgeIterator(label_triplet.src_label, v,
                                                    label_triplet.dst_label,
                                                    label_triplet.edge_label);

              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                output.emplace_back(label_triplet.dst_label, nbr, index);
                oe_iter.Next();
              }
            }
            if (label_triplet.dst_label == label) {
              auto ie_iter = txn.GetInEdgeIterator(label_triplet.dst_label, v,
                                                   label_triplet.src_label,
                                                   label_triplet.edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                output.emplace_back(label_triplet.src_label, nbr, index);
                ie_iter.Next();
              }
            }
          }
        }
        depth++;
      }
      ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                  shuffle_offset, params.keep_cols);
      return ctx;
    } else {
      LOG(ERROR) << "Not implemented yet";
      RETURN_NOT_IMPLEMENTED_ERROR("Not implemented yet for direction in");
    }
  }
  return ctx;
}

bl::result<Context> PathExpand::edge_expand_p(const ReadTransaction& txn,
                                              Context&& ctx,
                                              const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  auto dir = params.dir;
  std::vector<std::pair<std::shared_ptr<PathImpl>, size_t>> input;
  std::vector<std::pair<std::shared_ptr<PathImpl>, size_t>> output;
  std::vector<std::shared_ptr<PathImpl>> path_impls;

  GeneralPathColumnBuilder builder;
  if (dir == Direction::kOut) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(p, index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path::make_path(path));
          path_impls.emplace_back(path);
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (auto& [path, index] : input) {
        auto end = path->get_end();
        for (auto& label_triplet : labels) {
          if (label_triplet.src_label == end.first) {
            auto oe_iter = txn.GetOutEdgeIterator(end.first, end.second,
                                                  label_triplet.dst_label,
                                                  label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              std::shared_ptr<PathImpl> new_path =
                  path->expand(label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(new_path, index);
              oe_iter.Next();
            }
          }
        }
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    builder.set_path_impls(path_impls);
    ctx.set_with_reshuffle_beta(params.alias, builder.finish(), shuffle_offset,
                                params.keep_cols);

    return ctx;
  } else if (dir == Direction::kBoth) {
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     auto p = PathImpl::make_path_impl(label, v);
                     input.emplace_back(p, index);
                   });
    int depth = 0;
    while (depth < params.hop_upper) {
      output.clear();
      if (depth >= params.hop_lower) {
        for (auto& [path, index] : input) {
          builder.push_back_opt(Path::make_path(path));
          path_impls.emplace_back(path);
          shuffle_offset.push_back(index);
        }
      }
      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (auto& [path, index] : input) {
        auto end = path->get_end();
        for (auto& label_triplet : labels) {
          if (label_triplet.src_label == end.first) {
            auto oe_iter = txn.GetOutEdgeIterator(end.first, end.second,
                                                  label_triplet.dst_label,
                                                  label_triplet.edge_label);
            while (oe_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.dst_label, oe_iter.GetNeighbor());
              output.emplace_back(new_path, index);
              oe_iter.Next();
            }
          }
          if (label_triplet.dst_label == end.first) {
            auto ie_iter = txn.GetInEdgeIterator(end.first, end.second,
                                                 label_triplet.src_label,
                                                 label_triplet.edge_label);
            while (ie_iter.IsValid()) {
              auto new_path =
                  path->expand(label_triplet.src_label, ie_iter.GetNeighbor());
              output.emplace_back(new_path, index);
              ie_iter.Next();
            }
          }
        }
      }

      input.clear();
      std::swap(input, output);
      ++depth;
    }
    builder.set_path_impls(path_impls);
    ctx.set_with_reshuffle_beta(params.alias, builder.finish(), shuffle_offset,
                                params.keep_cols);
    return ctx;
  } else {
    LOG(ERROR) << "Not implemented yet";
    RETURN_NOT_IMPLEMENTED_ERROR("Not implemented yet for direction in");
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs
