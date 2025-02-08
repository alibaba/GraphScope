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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_

#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/path_expand_impl.h"
#include "flex/engines/graph_db/runtime/common/types.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

namespace gs {

namespace runtime {

struct PathExpandParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
};

struct ShortestPathParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  int v_alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
};

class PathExpand {
 public:
  // PathExpand(expandOpt == Vertex && alias == -1 && resultOpt == END_V) +
  // GetV(opt == END)
  static bl::result<Context> edge_expand_v(const GraphReadInterface& graph,
                                           Context&& ctx,
                                           const PathExpandParams& params);
  static bl::result<Context> edge_expand_p(const GraphReadInterface& graph,
                                           Context&& ctx,
                                           const PathExpandParams& params);

  static bl::result<Context> all_shortest_paths_with_given_source_and_dest(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const std::pair<label_t, vid_t>& dst);
  // single dst
  static bl::result<Context> single_source_single_dest_shortest_path(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, std::pair<label_t, vid_t>& dest);

  template <typename PRED_T>
  static bl::result<Context>
  single_source_shortest_path_with_order_by_length_limit(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const PRED_T& pred, int limit_upper) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        input_vertex_col->get_labels_set().size() == 1) {
      const auto& properties = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      if (properties.empty()) {
        auto tup = single_source_shortest_path_with_order_by_length_limit_impl<
            grape::EmptyType, PRED_T>(
            graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
            params.hop_lower, params.hop_upper, pred, limit_upper);
        ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                               std::get<2>(tup));
        ctx.set(params.alias, std::get<1>(tup));
        return ctx;
      } else if (properties.size() == 1) {
        if (properties[0] == PropertyType::Int32()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  int, PRED_T>(graph, *input_vertex_col,
                               params.labels[0].edge_label, params.dir,
                               params.hop_lower, params.hop_upper, pred,
                               limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Int64()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  int64_t, PRED_T>(graph, *input_vertex_col,
                                   params.labels[0].edge_label, params.dir,
                                   params.hop_lower, params.hop_upper, pred,
                                   limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Date()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  Date, PRED_T>(graph, *input_vertex_col,
                                params.labels[0].edge_label, params.dir,
                                params.hop_lower, params.hop_upper, pred,
                                limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::StringView()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  std::string_view, PRED_T>(
                  graph, *input_vertex_col, params.labels[0].edge_label,
                  params.dir, params.hop_lower, params.hop_upper, pred,
                  limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Double()) {
          auto tup =
              single_source_shortest_path_with_order_by_length_limit_impl<
                  double, PRED_T>(graph, *input_vertex_col,
                                  params.labels[0].edge_label, params.dir,
                                  params.hop_lower, params.hop_upper, pred,
                                  limit_upper);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        }
      }
    }

    LOG(ERROR) << "not support edge property type ";
    RETURN_UNSUPPORTED_ERROR("not support edge property type ");
  }

  template <typename PRED_T>
  static bl::result<Context> single_source_shortest_path(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        input_vertex_col->get_labels_set().size() == 1) {
      const auto& properties = graph.schema().get_edge_properties(
          params.labels[0].src_label, params.labels[0].dst_label,
          params.labels[0].edge_label);
      if (properties.empty()) {
        auto tup = single_source_shortest_path_impl<grape::EmptyType, PRED_T>(
            *ctx.value_collection, graph, *input_vertex_col,
            params.labels[0].edge_label, params.dir, params.hop_lower,
            params.hop_upper, pred);
        ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                               std::get<2>(tup));
        ctx.set(params.alias, std::get<1>(tup));
        return ctx;
      } else if (properties.size() == 1) {
        if (properties[0] == PropertyType::Int32()) {
          auto tup = single_source_shortest_path_impl<int, PRED_T>(
              *ctx.value_collection, graph, *input_vertex_col,
              params.labels[0].edge_label, params.dir, params.hop_lower,
              params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Int64()) {
          auto tup = single_source_shortest_path_impl<int64_t, PRED_T>(
              *ctx.value_collection, graph, *input_vertex_col,
              params.labels[0].edge_label, params.dir, params.hop_lower,
              params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Date()) {
          auto tup = single_source_shortest_path_impl<Date, PRED_T>(
              *ctx.value_collection, graph, *input_vertex_col,
              params.labels[0].edge_label, params.dir, params.hop_lower,
              params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        } else if (properties[0] == PropertyType::Double()) {
          auto tup = single_source_shortest_path_impl<double, PRED_T>(
              *ctx.value_collection, graph, *input_vertex_col,
              params.labels[0].edge_label, params.dir, params.hop_lower,
              params.hop_upper, pred);
          ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                                 std::get<2>(tup));
          ctx.set(params.alias, std::get<1>(tup));
          return ctx;
        }
      }
    }
    auto tup = default_single_source_shortest_path_impl<PRED_T>(
        *ctx.value_collection, graph, *input_vertex_col, params.labels,
        params.dir, params.hop_lower, params.hop_upper, pred);
    ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
    ctx.set(params.alias, std::get<1>(tup));
    return ctx;
  }

  static bl::result<Context>
  single_source_shortest_path_with_special_vertex_predicate(
      const GraphReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const SPVertexPredicate& pred);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_