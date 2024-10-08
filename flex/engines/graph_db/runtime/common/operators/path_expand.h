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

#ifndef RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_

#include <vector>

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/types.h"

#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {

namespace runtime {

struct PathExpandParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
  std::set<int> keep_cols;
};

class PathExpand {
 public:
  // PathExpand(expandOpt == Vertex && alias == -1 && resultOpt == END_V) +
  // GetV(opt == END)
  static bl::result<Context> edge_expand_v(const ReadTransaction& txn,
                                           Context&& ctx,
                                           const PathExpandParams& params);
  static bl::result<Context> edge_expand_p(const ReadTransaction& txn,
                                           Context&& ctx,
                                           const PathExpandParams& params);

  template <typename PRED_T>
  static bl::result<Context> edge_expand_v_pred(const ReadTransaction& txn,
                                                Context&& ctx,
                                                const PathExpandParams& params,
                                                const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label) {
      if (params.dir == Direction::kOut) {
        auto& input_vertex_list = *std::dynamic_pointer_cast<SLVertexColumn>(
            ctx.get(params.start_tag));
        label_t output_vertex_label = params.labels[0].dst_label;
        label_t edge_label = params.labels[0].edge_label;
        label_t vertex_label = params.labels[0].src_label;
        SLVertexColumnBuilder builder(output_vertex_label);

#if 0
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
                    if (pred(label, u)) {
                      builder.push_back_opt(u);
                      shuffle_offset.push_back(index);
                    }

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
#else
        std::vector<std::pair<size_t, vid_t>> input;
        std::vector<std::pair<size_t, vid_t>> output;
        input_vertex_list.foreach_vertex(
            [&](size_t index, label_t label, vid_t v) {
              output.emplace_back(index, v);
            });
        int depth = 0;
        auto oe_csr = txn.GetOutgoingSingleImmutableGraphView<grape::EmptyType>(
            vertex_label, vertex_label, edge_label);
        while (depth < params.hop_upper && !output.empty()) {
          input.clear();
          std::swap(input, output);

          for (auto& pair : input) {
            if (pred(vertex_label, pair.second, pair.first)) {
              builder.push_back_opt(pair.second);
              shuffle_offset.push_back(pair.first);
            }
          }
          if (depth + 1 >= params.hop_upper) {
            break;
          }
          for (auto& pair : input) {
            auto index = pair.first;
            auto v = pair.second;
            if (oe_csr.exist(v)) {
              output.emplace_back(index, oe_csr.get_edge(v).neighbor);
            }
            // auto oe_iter = txn.GetOutEdgeIterator(
            //     vertex_label, v, output_vertex_label, edge_label);
            // while (oe_iter.IsValid()) {
            //   auto nbr = oe_iter.GetNeighbor();
            //   output.emplace_back(index, nbr);
            //   oe_iter.Next();
            // }
          }

          ++depth;
        }
#endif

        ctx.set_with_reshuffle_beta(params.alias, builder.finish(),
                                    shuffle_offset, params.keep_cols);
        return ctx;
      }
    }
    RETURN_UNSUPPORTED_ERROR(
        "Unsupported path expand. Currently only support "
        "single edge label expand with src_label = dst_label.");
    return ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_PATH_EXPAND_H_