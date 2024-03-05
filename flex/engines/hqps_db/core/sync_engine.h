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
#ifndef ENGINES_HQPS_ENGINE_SYNC_ENGINE_H_
#define ENGINES_HQPS_ENGINE_SYNC_ENGINE_H_

#include <climits>

#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/multi_label_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/path.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/column.h"
#include "grape/utils/bitset.h"

#include "flex/engines/hqps_db/core/base_engine.h"
#include "flex/engines/hqps_db/core/operator/edge_expand.h"
#include "flex/engines/hqps_db/core/operator/get_v.h"
#include "flex/engines/hqps_db/core/operator/group_by.h"
#include "flex/engines/hqps_db/core/operator/path_expand.h"
#include "flex/engines/hqps_db/core/operator/scan.h"
#include "flex/engines/hqps_db/core/operator/shortest_path.h"
#include "flex/engines/hqps_db/core/operator/sink.h"
#include "flex/engines/hqps_db/core/utils/props.h"

namespace gs {

template <typename GRAPH_INTERFACE>
class SyncEngine : public BaseEngine {
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using default_vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;
  using two_label_set_t =
      TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType>;

  template <typename... T>
  using vertex_set_t = RowVertexSet<label_id_t, vertex_id_t, T...>;

 public:
  ///////////////////////////// Scan Vertex/////////////////////////////

  /// @brief Scan for one label, with append_opt == AppendOpt::Persist
  /// @tparam EXPR
  /// @tparam COL_T
  /// @tparam ...SELECTOR
  /// @tparam append_opt
  /// @param graph
  /// @param v_label
  /// @param filter
  /// @return
  template <AppendOpt append_opt, typename EXPR, typename... SELECTOR,
            typename std::enable_if<(append_opt == AppendOpt::Persist)>::type* =
                nullptr,
            typename COL_T = default_vertex_set_t>
  static Context<COL_T, 0, 0, grape::EmptyType> ScanVertex(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label,
      Filter<EXPR, SELECTOR...>&& filter) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertex(
        graph, v_label, std::move(filter));

    return Context<COL_T, 0, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  // implement for append_opt == AppendOpt::temp
  template <
      AppendOpt append_opt, typename EXPR, typename... SELECTOR,
      typename std::enable_if<(append_opt == AppendOpt::Temp)>::type* = nullptr,
      typename COL_T = default_vertex_set_t>
  static Context<COL_T, -1, 0, grape::EmptyType> ScanVertex(
      const GRAPH_INTERFACE& graph, const label_id_t& v_label,
      Filter<EXPR, SELECTOR...>&& filter) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertex(
        graph, v_label, std::move(filter));

    return Context<COL_T, -1, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  // Scan vertices with multiple labels, more than two labels
  template <AppendOpt append_opt, size_t num_labels, typename EXPR,
            typename... SELECTOR,
            typename std::enable_if<(append_opt == AppendOpt::Persist &&
                                     num_labels != 2)>::type* = nullptr,
            typename COL_T =
                GeneralVertexSet<vertex_id_t, label_id_t, grape::EmptyType>>
  static Context<COL_T, 0, 0, grape::EmptyType> ScanVertex(
      const GRAPH_INTERFACE& graph,
      std::array<label_id_t, num_labels>&& v_labels,
      Filter<EXPR, SELECTOR...>&& filter) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanMultiLabelVertex(
        graph, v_labels, std::move(filter));

    return Context<COL_T, 0, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  // Scan vertices with multiple labels, more than two labels, temporally stored
  template <AppendOpt append_opt, size_t num_labels, typename EXPR,
            typename... SELECTOR,
            typename std::enable_if<(append_opt == AppendOpt::Temp &&
                                     num_labels != 2)>::type* = nullptr,
            typename COL_T =
                GeneralVertexSet<vertex_id_t, label_id_t, grape::EmptyType>>
  static Context<COL_T, 0, 0, grape::EmptyType> ScanVertex(
      const GRAPH_INTERFACE& graph,
      std::array<label_id_t, num_labels>&& v_labels,
      Filter<EXPR, SELECTOR...>&& filter) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanMultiLabelVertex(
        graph, v_labels, std::move(filter));

    return Context<COL_T, -1, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  /// @brief Scan vertices with two labels
  /// @tparam FUNC
  /// @tparam COL_T
  /// @tparam res_alias
  /// @param time_stamp
  /// @param graph
  /// @param v_label
  /// @param func
  /// @return
  template <AppendOpt append_opt, size_t num_labels, typename EXPR,
            typename... SELECTOR,
            typename std::enable_if<(append_opt == AppendOpt::Persist &&
                                     num_labels == 2)>::type* = nullptr,
            typename COL_T = two_label_set_t>
  static Context<COL_T, 0, 0, grape::EmptyType> ScanVertex(
      const GRAPH_INTERFACE& graph,
      std::array<label_id_t, num_labels>&& v_labels,
      Filter<EXPR, SELECTOR...>&& filter) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertex(
        graph, std::move(v_labels), std::move(filter));

    return Context<COL_T, 0, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  template <AppendOpt append_opt, size_t num_labels, typename EXPR,
            typename... SELECTOR,
            typename std::enable_if<(append_opt == AppendOpt::Temp &&
                                     num_labels == 2)>::type* = nullptr,
            typename COL_T = two_label_set_t>
  static Context<COL_T, -1, 0, grape::EmptyType> ScanVertex(
      const GRAPH_INTERFACE& graph,
      std::array<label_id_t, num_labels>&& v_labels,
      Filter<EXPR, SELECTOR...>&& filter) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertex(
        graph, std::move(v_labels), std::move(filter));

    return Context<COL_T, -1, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  template <AppendOpt append_opt, typename OID_T, typename LabelT,
            typename std::enable_if<(append_opt == AppendOpt::Persist)>::type* =
                nullptr,
            typename COL_T = default_vertex_set_t>
  static Context<COL_T, 0, 0, grape::EmptyType> ScanVertexWithOid(
      const GRAPH_INTERFACE& graph, LabelT v_label, OID_T oid) {
    auto v_set_tuple =
        Scan<GRAPH_INTERFACE>::ScanVertexWithOid(graph, v_label, oid);

    return Context<COL_T, 0, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  template <
      AppendOpt append_opt, typename OID_T, typename LabelT,
      typename std::enable_if<(append_opt == AppendOpt::Temp)>::type* = nullptr,
      typename COL_T = default_vertex_set_t>
  static Context<COL_T, -1, 0, grape::EmptyType> ScanVertexWithOid(
      const GRAPH_INTERFACE& graph, LabelT v_label, OID_T oid) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertexWithOid<OID_T>(
        graph, v_label, oid);

    return Context<COL_T, -1, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  template <
      AppendOpt append_opt, typename OID_T, typename LabelT, size_t num_labels,
      typename std::enable_if<(append_opt == AppendOpt::Persist)>::type* =
          nullptr,
      typename COL_T = GeneralVertexSet<vertex_id_t, LabelT, grape::EmptyType>>
  static Context<COL_T, 0, 0, grape::EmptyType> ScanVertexWithOid(
      const GRAPH_INTERFACE& graph, std::array<LabelT, num_labels> v_labels,
      OID_T oid) {
    auto v_set_tuple =
        Scan<GRAPH_INTERFACE>::template ScanVertexWithOid<OID_T, num_labels>(
            graph, v_labels, oid);

    return Context<COL_T, 0, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  template <
      AppendOpt append_opt, typename LabelT, size_t num_labels,
      typename std::enable_if<(append_opt == AppendOpt::Temp)>::type* = nullptr,
      typename COL_T = GeneralVertexSet<vertex_id_t, LabelT, grape::EmptyType>>
  static Context<COL_T, -1, 0, grape::EmptyType> ScanVertexWithOid(
      const GRAPH_INTERFACE& graph, std::array<LabelT, num_labels> v_labels,
      int64_t oid) {
    auto v_set_tuple =
        Scan<GRAPH_INTERFACE>::ScanVertexWithOid(graph, v_labels, oid);

    return Context<COL_T, -1, 0, grape::EmptyType>(std::move(v_set_tuple));
  }

  //////////////////////////EdgeExpand////////////////////////////

  ////Edge ExpandE with multiple edge label triplets. (src, dst, edge)
  template <AppendOpt append_opt, int input_col_id, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, size_t num_pairs,
            typename FILTER_T, typename... PropTuple>
  static auto EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandMultiEOpt<num_pairs, label_id_t, FILTER_T, PropTuple...>&&
          edge_expand_opt,
      size_t limit = INT_MAX) {
    auto& select_node = gs::Get<input_col_id>(ctx);

    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandEMultiTriplet<
        num_pairs, PropTuple...>(
        graph, select_node, edge_expand_opt.dir_,
        edge_expand_opt.edge_label_triplets_, edge_expand_opt.prop_names_,
        std::move(edge_expand_opt.edge_filter_), limit);
    return ctx.template AddNode<append_opt>(
        std::move(pair.first), std::move(pair.second), input_col_id);
  }

  /// @brief //////// Edge Expand to vertex, the output is vertices with out any
  /// property!
  /// According to whether the alias_to_use is the head node, we shall have two
  /// kind
  /// of implementation
  /// 0. If start from tag, all good.
  /// 1. If from a previous alias, a additional repeat array should be provided,
  /// to make sure the output vertices is aligned with the current head alias.
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @tparam
  /// @tparam N
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <AppendOpt append_opt, int input_col_id, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV,
            typename EDGE_FILTER_T, typename... SELECTOR,
            typename RES_T = typename ResultContextT<
                append_opt, default_vertex_set_t, cur_alias, CTX_HEAD_T,
                base_tag, CTX_PREV...>::result_t>
  static RES_T EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandOpt<label_id_t, EDGE_FILTER_T, SELECTOR...>&& edge_expand_opt,
      size_t limit = INT_MAX) {
    auto& select_node = gs::Get<input_col_id>(ctx);

    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandV(
        graph, select_node, edge_expand_opt.dir_, edge_expand_opt.edge_label_,
        edge_expand_opt.other_label_, std::move(edge_expand_opt.edge_filter_),
        limit);
    return ctx.template AddNode<append_opt>(
        std::move(pair.first), std::move(pair.second), input_col_id);
  }

  // EdgeExpand via multiple edge triplet, got vertices with different labels.
  template <AppendOpt append_opt, int input_col_id, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, size_t num_pairs,
            typename EDGE_FILTER_T, typename... PropTuple>
  static auto EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandMultiEOpt<num_pairs, label_id_t, EDGE_FILTER_T, PropTuple...>&&
          edge_expand_opt,
      size_t limit = INT_MAX) {
    auto& select_node = gs::Get<input_col_id>(ctx);

    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandVMultiTriplet<
        num_pairs, PropTuple...>(
        graph, select_node, edge_expand_opt.dir_,
        edge_expand_opt.edge_label_triplets_, edge_expand_opt.prop_names_,
        std::move(edge_expand_opt.edge_filter_), limit);
    return ctx.template AddNode<append_opt>(
        std::move(pair.first), std::move(pair.second), input_col_id);
  }

  /// @brief //////// Edge Expand to vertex, the output is vertices with out any
  /// property!
  /// @tparam EDATA_T
  /// @tparam VERTEX_SET_T
  /// @tparam
  /// @tparam N
  /// @param frag
  /// @param v_sets
  /// @param edge_expand_opt
  /// @return
  template <AppendOpt append_opt, int alias_to_use, typename... T,
            typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename LabelT, typename EDGE_FILTER_T,
            typename... SELECTORS>
  static auto EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandEOpt<LabelT, EDGE_FILTER_T, std::tuple<SELECTORS...>, T...>&&
          edge_expand_opt,
      size_t limit = INT_MAX) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modify offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandE<T...>(
        graph, select_node, edge_expand_opt.dir_, edge_expand_opt.edge_label_,
        edge_expand_opt.other_label_, edge_expand_opt.edge_filter_,
        edge_expand_opt.prop_names_, limit);
    // create new context node, update offsets.
    return ctx.template AddNode<append_opt>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abandoned here.
  }

  /// @brief //////// Edge Expand to Edge, with multiple dst vertex labels.
  /// @tparam ...T
  /// @tparam CTX_HEAD_T
  /// @tparam ...CTX_PREV
  /// @tparam LabelT
  /// @tparam EDGE_FILTER_T
  /// @tparam res_alias
  /// @tparam alias_to_use
  /// @tparam cur_alias
  /// @tparam base_tag
  /// @param time_stamp
  /// @param graph
  /// @param ctx
  /// @param edge_expand_opt
  /// @param limit
  /// @return
  template <AppendOpt append_opt, int alias_to_use, typename... T,
            typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename LabelT, size_t num_labels,
            typename EDGE_FILTER_T>
  static auto EdgeExpandE(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandEMultiLabelOpt<num_labels, LabelT, EDGE_FILTER_T, T...>&&
          edge_expand_opt,
      size_t limit = INT_MAX) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modify offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandE<T...>(
        graph, select_node, edge_expand_opt.dir_, edge_expand_opt.edge_label_,
        edge_expand_opt.other_label_, edge_expand_opt.edge_filter_,
        edge_expand_opt.prop_names_, limit);
    // create new context node, update offsets.
    return ctx.template AddNode<append_opt>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abandoned here.
  }

  template <AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            size_t num_labels, typename EDGE_FILTER_T>
  static auto EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandOptMultiLabel<LabelT, num_labels, EDGE_FILTER_T>&&
          edge_expand_opt) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modify offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::EdgeExpandV(
        graph, select_node, edge_expand_opt.direction_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_labels_,
        std::move(edge_expand_opt.edge_filter_),
        std::make_index_sequence<num_labels>());
    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandoned here.
  }

  template <AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T>
  static auto EdgeExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modify offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::EdgeExpandV(
        graph, select_node, edge_expand_opt.direction_,
        edge_expand_opt.edge_label_triplets_,
        std::move(edge_expand_opt.edge_filter_));
    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandoned here.
  }

  //////////////////////////////////////Path Expand/////////////////////////
  // Path Expand to vertices with columns
  template <AppendOpt opt, int alias_to_use, typename VERTEX_FILTER_T,
            typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename LabelT, typename EDGE_FILTER_T,
            typename... T, typename RES_SET_T = vertex_set_t<dist_t, T...>,
            typename RES_T =
                typename ResultContextT<opt, RES_SET_T, cur_alias, CTX_HEAD_T,
                                        base_tag, CTX_PREV...>::result_t>
  static RES_T PathExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandVOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T, T...>&&
          path_expand_opt) {
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandV(
        graph, select_node, std::move(path_expand_opt));

    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandon here.
  }

  template <AppendOpt opt, int alias_to_use, typename VERTEX_FILTER_T,
            typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename LabelT, size_t num_labels,
            typename EDGE_FILTER_T, size_t get_v_num_labels, typename... T>
  static auto PathExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandVMultiDstOpt<LabelT, num_labels, EDGE_FILTER_T,
                             get_v_num_labels, VERTEX_FILTER_T, T...>&&
          path_expand_opt) {
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandV(
        graph, select_node, std::move(path_expand_opt));

    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandon here.
  }

  template <AppendOpt opt, int alias_to_use, typename VERTEX_FILTER_T,
            typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename LabelT, typename EDGE_FILTER_T,
            size_t get_v_num_labels, typename... T>
  static auto PathExpandV(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T, get_v_num_labels,
                                 VERTEX_FILTER_T, T...>&& path_expand_opt) {
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandVMultiTriplet(
        graph, select_node, std::move(path_expand_opt));

    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandon here.
  }

  /// Expand to Path
  template <AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T, typename VERTEX_FILTER_T>
  static auto PathExpandP(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandPOpt<LabelT, EDGE_FILTER_T, VERTEX_FILTER_T>&&
          path_expand_opt) {
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandP(
        graph, select_node, std::move(path_expand_opt));

    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandon here.
  }

  /// Expand to Path
  template <AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T, size_t get_v_num_labels,
            typename VERTEX_FILTER_T>
  static auto PathExpandP(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandVMultiTripletOpt<LabelT, EDGE_FILTER_T, get_v_num_labels,
                                 VERTEX_FILTER_T>&& path_expand_opt) {
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandP(
        graph, select_node, std::move(path_expand_opt));

    // create new context node, update offsets.
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
    // old context will be abandon here.
  }

  // get no props, just filter
  template <
      AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... SELECTOR, typename... T,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_vertex_set &&
                               sizeof...(T) == 0)>::type* = nullptr,
      typename NEW_HEAD_T = old_node_t,
      typename RES_T =
          typename ResultContextT<opt, NEW_HEAD_T, cur_alias, CTX_HEAD_T,
                                  base_tag, CTX_PREV...>::result_t>
  static RES_T GetV(const GRAPH_INTERFACE& frag,
                    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                    GetVOpt<LabelT, num_labels, Filter<EXPRESSION, SELECTOR...>,
                            T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetNoPropV(frag, select, get_v_opt);
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
  }

  // get vertex from edge set
  template <
      AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... SELECTOR, typename... T,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_edge_set &&
                               sizeof...(T) == 0)>::type* = nullptr>
  static auto GetV(const GRAPH_INTERFACE& graph,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   GetVOpt<LabelT, num_labels, Filter<EXPRESSION, SELECTOR...>,
                           T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetNoPropVFromEdgeSet(
        graph, select, std::move(get_v_opt));
    VLOG(10) << "new node's size: " << pair.first.Size();
    //  << ", offset: " << gs::to_string(pair.second);
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
  }

  // get vertex from path set
  template <
      AppendOpt opt, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... SELECTOR, typename... T,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_path_set &&
                               sizeof...(T) == 0)>::type* = nullptr>
  static auto GetV(const GRAPH_INTERFACE& graph,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   GetVOpt<LabelT, num_labels, Filter<EXPRESSION, SELECTOR...>,
                           T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetNoPropVFromPathSet(
        graph, select, std::move(get_v_opt));
    VLOG(10) << "new node's size: " << pair.first.Size();
    //  << ", offset: " << gs::to_string(pair.second);
    return ctx.template AddNode<opt>(std::move(pair.first),
                                     std::move(pair.second), alias_to_use);
  }

  //////////////////////////////////////Project/////////////////////////
  // Project current relations to new columns, append or not.
  // TODO: add type inference back:
  //      typename RES_T = typename ProjectResT<
  // is_append, Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
  // PROJECT_OPT>::result_t
  template <bool is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... ProjMapper>
  static auto Project(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<ProjMapper...>&& proj_mappers) {
    VLOG(10) << "[Project] with project opt size: " << sizeof...(ProjMapper);
    return ProjectOp<GRAPH_INTERFACE>::template ProjectImpl<is_append>(
        graph, std::move(ctx), std::move(proj_mappers));
  }

  //////////////////////////////////////Sort/Order/////////////////////////
  // From current context, do the sort.
  // After sort, the corresponding order maintained by csr offsets will be
  // cleaned. We need to flat current context to new context. Each node will
  // be replaced by the flat one. The alignment between nodes will be 1-1.
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... ORDER_PAIRS>
  static auto Sort(const GRAPH_INTERFACE& graph,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   Range&& limit_range,
                   std::tuple<ORDER_PAIRS...>&& ordering_pairs) {
    if (limit_range.start_ != 0) {
      LOG(FATAL) << "Current only support topk";
    }
    if (limit_range.limit_ == 0) {
      LOG(FATAL) << "Current only support non-empty range";
    }

    VLOG(10) << "[Sort: ] Sort with " << sizeof...(ORDER_PAIRS) << " keys";
    return SortOp<GRAPH_INTERFACE>::SortTopK(
        graph, std::move(ctx), std::move(ordering_pairs), limit_range.limit_);
  }

  //////////////////////////////////////Select/Filter/////////////////////////
  // Select with head node. The type doesn't change
  // select only head node.
  template <
      int in_col_id, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename EXPR, typename... Selector,
      typename std::enable_if<CTX_HEAD_T::is_two_label_set &&
                              (in_col_id == -1 ||
                               in_col_id == cur_alias)>::type* = nullptr,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Select(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      Filter<EXPR, Selector...>&& filter) {
    VLOG(10) << "[Select]";
    auto expr = filter.expr_;
    auto selectors = filter.selectors_;

    auto& head = ctx.GetMutableHead();
    auto labels = head.GetLabels();
    auto prop_getter_tuple =
        get_prop_getters_from_selectors(graph, labels, selectors);
    SelectTwoLabelSetImpl(ctx, head, prop_getter_tuple, expr);

    return std::move(ctx);
  }

  template <typename CTX_T, typename HEAD_T, typename EXPR,
            typename PROP_GETTER_T, size_t... Is>
  static void SelectTwoLabelSetImpl(
      CTX_T&& ctx, HEAD_T& head,
      const std::array<PROP_GETTER_T, 2>& prop_getter_tuple, const EXPR& expr) {
    auto& bitset = head.GetMutableBitset();
    auto& vertices = head.GetMutableVertices();
    size_t cur = 0;
    static_assert(HEAD_T::num_props == 0);
    auto& last_offset = ctx.GetMutableOffset(-1);
    double t0 = -grape::GetCurrentTime();
    grape::Bitset new_bitset;
    new_bitset.init(vertices.size());
    size_t cur_begin = last_offset[0];
    for (size_t i = 0; i < last_offset.size() - 1; ++i) {
      auto limit = last_offset[i + 1];
      for (auto j = cur_begin; j < limit; ++j) {
        auto vid = vertices[j];
        if (bitset.get_bit(j)) {
          if (std::apply(expr, prop_getter_tuple[0].get_view(vid))) {
            new_bitset.set_bit(cur);
            if (cur < j) {
              vertices[cur++] = vid;
            } else {
              cur++;
            }
          }
        } else {
          if (std::apply(expr, prop_getter_tuple[1].get_view(vid))) {
            if (cur < j) {
              vertices[cur++] = vid;
            } else {
              cur++;
            }
          }
        }
      }
      cur_begin = last_offset[i + 1];
      last_offset[i + 1] = cur;
    }
    vertices.resize(cur);
    bitset.swap(new_bitset);
    t0 += grape::GetCurrentTime();
    VLOG(10) << "after filter: " << vertices.size() << ", time: " << t0;
  }

  // Select from row vertex set.
  // only the case of select head node is supported.
  template <
      int in_col_id, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename EXPR, typename... SELECTOR,
      typename std::enable_if<CTX_HEAD_T::is_row_vertex_set &&
                              (in_col_id == -1 ||
                               in_col_id == cur_alias)>::type* = nullptr,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Select(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      Filter<EXPR, SELECTOR...>&& filter) {
    VLOG(10) << "[Select]";

    // Currently only support select with head node.
    auto expr = filter.expr_;
    auto selectors = filter.selectors_;

    auto& head = ctx.GetMutableHead();
    auto label = head.GetLabel();
    auto prop_getter_tuple =
        std::array{get_prop_getter_from_selectors(graph, label, selectors)};
    // TODO: implement
    SelectRowVertexSetImpl(ctx, head, prop_getter_tuple, expr,
                           std::make_index_sequence<sizeof...(SELECTOR)>());

    return std::move(ctx);
  }

  template <typename CTX_T, typename HEAD_T, typename EXPR,
            typename PROP_GETTER_T, size_t... Is>
  static void SelectRowVertexSetImpl(
      CTX_T& ctx, HEAD_T& head,
      const std::array<PROP_GETTER_T, 1>& prop_getters, const EXPR& expr,
      std::index_sequence<Is...>) {
    double t0 = -grape::GetCurrentTime();
    size_t cur = 0;
    auto& vertices = head.GetMutableVertices();
    auto& prop_getter = prop_getters[0];
    if constexpr (CTX_T::prev_alias_num == 0) {
      for (size_t i = 0; i < vertices.size(); ++i) {
        auto vid = vertices[i];
        if (std::apply(expr, prop_getter.get_view(vid))) {
          if (cur < i) {
            vertices[cur++] = vid;
          } else {
            cur++;
          }
        }
      }
    } else {
      auto& last_offset = ctx.GetMutableOffset(-1);

      size_t cur_begin = last_offset[0];
      for (size_t i = 0; i + 1 < last_offset.size(); ++i) {
        auto limit = last_offset[i + 1];
        for (auto j = cur_begin; j < limit; ++j) {
          auto vid = vertices[j];
          if (std::apply(expr, prop_getter.get_view(vid))) {
            if (cur < j) {
              vertices[cur++] = vid;
            } else {
              cur++;
            }
          }
        }
        cur_begin = last_offset[i + 1];
        last_offset[i + 1] = cur;
      }
    }
    vertices.resize(cur);
    t0 += grape::GetCurrentTime();
    VLOG(10) << "after filter: " << vertices.size() << ", time: " << t0;
  }

  //////////////////////////////////////Select/Filter/////////////////////////
  // Select with head node. The type doesn't change
  // select can possibly applied on multiple tags
  // (!CTX_HEAD_T::is_row_vertex_set) && (!CTX_HEAD_T::is_two_label_set) &&
  template <
      int... in_col_id, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename EXPR, typename... SELECTOR,
      typename std::enable_if<((sizeof...(in_col_id) >= 1) &&
                               (sizeof...(in_col_id) ==
                                sizeof...(SELECTOR)))>::type* = nullptr,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Select(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      Filter<EXPR, SELECTOR...>&& filter) {
    VLOG(10) << "[Context]: Select in place";
    auto expr = filter.expr_;
    auto selectors = filter.selectors_;

    std::vector<offset_t> new_offsets;
    std::vector<offset_t> select_indices;
    new_offsets.emplace_back(0);
    offset_t cur_offset = 0;
    offset_t cur_ind = 0;
    auto& cur_ = ctx.GetMutableHead();
    select_indices.reserve(cur_.Size());
    // create prop_desc from in_col_id and selectors
    auto prop_descs = create_prop_descs_from_selectors<in_col_id...>(selectors);
    auto prop_getters_tuple =
        create_prop_getters_from_prop_desc(graph, ctx, prop_descs);
    for (auto iter : ctx) {
      auto eles = iter.GetAllElement();
      // if (expr(eles)) {
      // if (std::apply(expr, props)) {
      if (run_expr_filter(expr, prop_getters_tuple, eles)) {
        select_indices.emplace_back(cur_ind);
        cur_offset += 1;
      }
      cur_ind += 1;
      new_offsets.emplace_back(cur_offset);
    }
    VLOG(10) << "Select " << select_indices.size() << ", out of " << cur_ind
             << " records"
             << ", head size: " << cur_.Size();

    cur_.SubSetWithIndices(select_indices);
    ctx.merge_offset_with_back(new_offsets);
    return std::move(ctx);
  }

  template <typename EXPR, typename... PROP_GETTER, typename... ELE>
  static inline bool run_expr_filter(
      const EXPR& expr, std::tuple<PROP_GETTER...>& prop_getter_tuple,
      std::tuple<ELE...>& eles) {
    return run_expr_filter_impl(
        expr, prop_getter_tuple, eles,
        std::make_index_sequence<sizeof...(PROP_GETTER)>());
  }

  template <typename EXPR, typename... PROP_GETTER, typename... ELE,
            size_t... Is>
  static inline bool run_expr_filter_impl(
      const EXPR& expr, std::tuple<PROP_GETTER...>& prop_getter_tuple,
      std::tuple<ELE...>& eles, std::index_sequence<Is...>) {
    return expr(std::get<Is>(prop_getter_tuple).get_from_all_element(eles)...);
  }

  //////////////////////////////////////Group/////////////////////////
  // We currently support group with one key, and possibly multiple values.
  // create a brand new context type.
  // group count is included in this implementation.
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... GROUP_KEY, typename... AGG_FUNC,
            typename RES_T = typename GroupResT<
                Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                std::tuple<GROUP_KEY...>, std::tuple<AGG_FUNC...>>::result_t>
  static RES_T GroupBy(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<GROUP_KEY...>&& group_key,
      std::tuple<AGG_FUNC...>&& agg_func) {
    VLOG(10) << "[Group] with group opt";
    return GroupByOp<GRAPH_INTERFACE>::GroupByImpl(
        graph, std::move(ctx), std::move(group_key), std::move(agg_func));
  }

  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... AGG_T>
  static auto GroupByWithoutKey(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<AGG_T...>&& fold_opt) {
    VLOG(10) << "[Group] with fold opt";
    return GroupByOp<GRAPH_INTERFACE>::GroupByWithoutKeyImpl(
        graph, std::move(ctx), std::move(fold_opt));
  }

  //--------- Sink the context to output--------
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV>
  static auto Sink(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
      std::array<int32_t,
                 Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>::col_num>
          tag_ids) {
    return SinkOp<GRAPH_INTERFACE>::Sink(graph, ctx, tag_ids);
  }

  //---------------Sink without giving the tags explicitly.----------------
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV>
  static auto Sink(const GRAPH_INTERFACE& graph,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx) {
    std::array<int32_t,
               Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>::col_num>
        tag_ids;
    for (size_t i = 0; i < tag_ids.size(); i++) {
      tag_ids[i] = i;
    }
    return Sink(graph, ctx, tag_ids);
  }

  //////////////////////////////////////Shortest Path/////////////////////////
  // Return the path.
  template <AppendOpt opt, int alias_to_use, typename EXPR, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T, typename UNTIL_CONDITION, typename... T,
            typename RES_SET_T = PathSet<vertex_id_t, LabelT>,
            typename RES_T =
                typename ResultContextT<opt, RES_SET_T, cur_alias, CTX_HEAD_T,
                                        base_tag, CTX_PREV...>::result_t>
  static RES_T ShortestPath(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      ShortestPathOpt<LabelT, EXPR, EDGE_FILTER_T, UNTIL_CONDITION, T...>&&
          shortest_path_opt) {
    static_assert(alias_to_use == -1 || alias_to_use == cur_alias);
    if (shortest_path_opt.path_opt_ != PathOpt::Simple) {
      LOG(FATAL) << "Only support Simple path now";
    }
    if (shortest_path_opt.result_opt_ != ResultOpt::AllV) {
      LOG(FATAL) << "Only support AllV now";
    }

    auto& set = ctx.template GetNode<alias_to_use>();
    auto path_set_and_offset = ShortestPathOp<GRAPH_INTERFACE>::ShortestPath(
        graph, set, std::move(shortest_path_opt));
    return ctx.template AddNode<opt>(std::move(path_set_and_offset.first),
                                     std::move(path_set_and_offset.second));
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_SYNC_ENGINE_H_
