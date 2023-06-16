#ifndef GRAPHSCOPE_ENGINE_SYNC_ENGINE_H_
#define GRAPHSCOPE_ENGINE_SYNC_ENGINE_H_

#include <climits>

#include "flex/engines/hqps/ds/multi_vertex_set/multi_label_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/row_vertex_set.h"
// #include "flex/engines/hqps/ds/unkeyed_vertex_set.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/ds/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps/ds/path.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/engines/hqps/engine/params.h"

#include "flex/storages/mutable_csr/property/column.h"
#include "flex/storages/mutable_csr/property/types.h"
#include "flex/storages/mutable_csr/types.h"
#include "flex/engines/hqps/engine/utils/bitset.h"

#include "flex/engines/hqps/engine/operator/edge_expand.h"
#include "flex/engines/hqps/engine/operator/fused_operator.h"
#include "flex/engines/hqps/engine/operator/get_v.h"
#include "flex/engines/hqps/engine/operator/group_by.h"
#include "flex/engines/hqps/engine/operator/path_expand.h"
#include "flex/engines/hqps/engine/operator/scan.h"
#include "flex/engines/hqps/engine/operator/shorest_path.h"
#include "flex/engines/hqps/engine/operator/sink.h"

#include "flex/engines/hqps/engine/base_engine.h"
#include "flex/engines/hqps/engine/operator//prop_utils.h"

// include boost headers, for apps use.
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

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
  template <int res_alias, typename FUNC, typename COL_T = default_vertex_set_t>
  static Context<COL_T, res_alias, 0, grape::EmptyType> ScanVertex(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      const label_id_t& v_label, FUNC&& func) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertex(
        time_stamp, graph, v_label, std::move(func));

    return Context<COL_T, res_alias, 0, grape::EmptyType>(
        std::move(v_set_tuple));
  }

  /// @brief Scan vertices with multiple labels
  /// @tparam FUNC
  /// @tparam COL_T
  /// @tparam res_alias
  /// @param time_stamp
  /// @param graph
  /// @param v_label
  /// @param func
  /// @return
  template <int res_alias, size_t num_labels, typename FUNC,
            typename COL_T = two_label_set_t>
  static Context<COL_T, res_alias, 0, grape::EmptyType> ScanVertex(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      std::array<label_id_t, num_labels>&& v_labels, FUNC&& func) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::template ScanVertex(
        time_stamp, graph, std::move(v_labels), std::move(func));

    return Context<COL_T, res_alias, 0, grape::EmptyType>(
        std::move(v_set_tuple));
  }

  template <int res_alias, typename LabelT,
            typename COL_T = default_vertex_set_t>
  static Context<COL_T, res_alias, 0, grape::EmptyType> ScanVertexWithOid(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, LabelT v_label,
      int64_t oid) {
    auto v_set_tuple = Scan<GRAPH_INTERFACE>::ScanVertexWithOid(
        time_stamp, graph, v_label, oid);

    return Context<COL_T, res_alias, 0, grape::EmptyType>(
        std::move(v_set_tuple));
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
  template <int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T,
            typename std::enable_if<(alias_to_use == -1)>::type* = nullptr,
            typename RES_T = typename ResultContextT<
                res_alias, default_vertex_set_t, cur_alias, CTX_HEAD_T,
                base_tag, CTX_PREV...>::result_t>
  static RES_T EdgeExpandV(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
      size_t limit = INT_MAX) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modifiy offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandV(
        time_stamp, graph, select_node, edge_expand_opt.dir_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_label_,
        std::move(edge_expand_opt.edge_filter_), limit);
    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
  }

  // Specialization for the case where start expand from some internal alias.
  template <int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T,
            typename std::enable_if<(alias_to_use != -1)>::type* = nullptr,
            typename RES_T = typename ResultContextT<
                res_alias, default_vertex_set_t, cur_alias, CTX_HEAD_T,
                base_tag, CTX_PREV...>::result_t>
  static RES_T EdgeExpandV(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandOpt<LabelT, EDGE_FILTER_T>&& edge_expand_opt,
      size_t limit = INT_MAX) {
    LOG(INFO) << "[EdgeExpandV] from tag: " << alias_to_use
              << ", which is not head";
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandV(
        time_stamp, graph, select_node, edge_expand_opt.dir_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_label_,
        std::move(edge_expand_opt.edge_filter_), limit);
    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
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
  template <int res_alias, int alias_to_use, typename... T, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T>
  static auto EdgeExpandE(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandEOpt<LabelT, EDGE_FILTER_T, T...>&& edge_expand_opt,
      size_t limit = INT_MAX) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modifiy offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandE<T...>(
        time_stamp, graph, select_node, edge_expand_opt.dir_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_label_,
        edge_expand_opt.edge_filter_, edge_expand_opt.prop_names_, limit);
    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
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
  template <int res_alias, int alias_to_use, typename... T, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, typename LabelT,
            size_t num_labels, typename EDGE_FILTER_T>
  static auto EdgeExpandE(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandEMultiLabelOpt<num_labels, LabelT, EDGE_FILTER_T, T...>&&
          edge_expand_opt,
      size_t limit = INT_MAX) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modifiy offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::template EdgeExpandE<T...>(
        time_stamp, graph, select_node, edge_expand_opt.dir_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_label_,
        edge_expand_opt.edge_filter_, edge_expand_opt.prop_names_, limit);
    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
  }

  template <int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            size_t num_labels, typename EDGE_FILTER_T>
  static auto EdgeExpandVMultiLabel(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandOptMultiLabel<LabelT, num_labels, EDGE_FILTER_T>&&
          edge_expand_opt) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // Modifiy offsets.
    // pass select node by reference.
    auto pair = EdgeExpand<GRAPH_INTERFACE>::EdgeExpandVMultiLabel(
        time_stamp, graph, select_node, edge_expand_opt.direction_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_labels_,
        std::move(edge_expand_opt.edge_filter_),
        std::make_index_sequence<num_labels>());
    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
  }

  // EdgeExpandV with filter on vertices
  template <
      int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EDGE_FILTER_T, typename GET_V_EXPR, typename... GET_V_PROP,
      typename std::enable_if<(!IsTruePredicate<GET_V_EXPR>::value &&
                               IsTruePredicate<EDGE_FILTER_T>::value &&
                               sizeof...(GET_V_PROP) == 0)>::type* = nullptr>
  static auto EdgeExpandVMultiLabelWithFilter(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EdgeExpandOptMultiLabel<LabelT, num_labels, EDGE_FILTER_T>&&
          edge_expand_opt,
      GET_V_EXPR&& get_v_expr) {
    // Unwrap params here.
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // TODO: we currently assume select all labels.

    auto pair = EdgeExpand<GRAPH_INTERFACE>::EdgeExpandVMultiLabelWithFilter(
        time_stamp, graph, select_node, edge_expand_opt.direction_,
        edge_expand_opt.edge_label_, edge_expand_opt.other_labels_,
        std::move(edge_expand_opt.edge_filter_), get_v_expr,
        std::make_index_sequence<num_labels>());
    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
  }

  //////////////////////////////////////Path Expand/////////////////////////
  // Path Expand to vertices with columns
  template <int res_alias, int alias_to_use, typename EXPR, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T, typename... T,
            typename RES_SET_T = vertex_set_t<dist_t, T...>,
            typename RES_T = typename ResultContextT<
                res_alias, RES_SET_T, cur_alias, CTX_HEAD_T, base_tag,
                CTX_PREV...>::result_t>
  static RES_T PathExpandV(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T, T...>&& path_expand_opt) {
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandV(
        time_stamp, graph, select_node, std::move(path_expand_opt));

    // create new context node, update offsets.
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
    // old context will be abondon here.
  }

  //   //////////////////////////////////////Path
  //   Expand/////////////////////////
  //   // Path Expand to vertices without columns
  //   template <int res_alias, int alias_to_use, typename EXPR, typename
  //   CTX_HEAD_T,
  //             int cur_alias, int base_tag, typename... CTX_PREV, typename
  //             LabelT, typename EDGE_FILTER_T, typename RES_SET_T =
  //             vertex_set_t<dist_t>, typename RES_T = typename ResultContextT<
  //                 res_alias, RES_SET_T, cur_alias, CTX_HEAD_T, base_tag,
  //                 CTX_PREV...>::result_t>
  //   static RES_T PathExpandV(
  //       int64_t time_stamp, const GRAPH_INTERFACE& graph,
  //       Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
  //       PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T>&& path_expand_opt) {
  //     auto& select_node = gs::Get<alias_to_use>(ctx);
  //     auto pair = PathExpand<GRAPH_INTERFACE>::PathExpandV(
  //         time_stamp, graph, select_node, std::move(path_expand_opt));
  //     // create new context node, update offsets.
  //     return ctx.template AddNode<res_alias>(
  //         std::move(pair.first), std::move(pair.second), alias_to_use);
  //     // old context will be abondon here.
  //   }

  /////////////////////GetV, output vertices with columns //////////////////////
  // res_alias: the alias of output
  // alias_to_use: the alias of col of current ctx we use as input.
  // cur_alias: the  alias of current head node.
  // num_properties: the properties num to get from vertex. should eq
  // sizeof...(COL_T)
  template <int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            size_t num_labels, typename EXPRESSION, typename... T,
            typename std::enable_if<(num_labels > 1 &&
                                     sizeof...(T) >= 1)>::type* = nullptr>
  static auto GetV(int64_t time_stamp, const GRAPH_INTERFACE& frag,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetPropertyV(
        time_stamp, frag, select, std::move(get_v_opt));
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
  }

  template <int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
            int base_tag, typename... CTX_PREV, typename LabelT,
            size_t num_labels, typename EXPRESSION, typename... T,
            typename std::enable_if<(num_labels == 1 &&
                                     sizeof...(T) >= 1)>::type* = nullptr>
  static auto GetV(int64_t time_stamp, const GRAPH_INTERFACE& frag,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetPropertyV(
        time_stamp, frag, select, std::move(get_v_opt));
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
  }

  // get no props, just filter
  template <
      int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... T,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_vertex_set &&
                               sizeof...(T) == 0)>::type* = nullptr,
      typename NEW_HEAD_T = old_node_t,
      typename RES_T =
          typename ResultContextT<res_alias, NEW_HEAD_T, cur_alias, CTX_HEAD_T,
                                  base_tag, CTX_PREV...>::result_t>
  static RES_T GetV(int64_t time_stamp, const GRAPH_INTERFACE& frag,
                    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                    GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetNoPropV(time_stamp, frag, select,
                                                       get_v_opt);
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
  }

  // get vertex from edge set
  template <
      int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... T,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_edge_set &&
                               sizeof...(T) == 0)>::type* = nullptr>
  static auto GetV(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    auto& select = gs::Get<alias_to_use>(ctx);
    auto pair = GetVertex<GRAPH_INTERFACE>::GetNoPropVFromEdgeSet(
        time_stamp, graph, select, std::move(get_v_opt));
    VLOG(10) << "new node's size: " << pair.first.Size();
    //  << ", offset: " << gs::to_string(pair.second);
    return ctx.template AddNode<res_alias>(
        std::move(pair.first), std::move(pair.second), alias_to_use);
  }

  //////////////////////////////////////Project/////////////////////////
  // Project current relations to new columns, append or not.
  // TODO: add type infere back:
  //      typename RES_T = typename ProjectResT<
  // is_append, Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
  // PROJECT_OPT>::result_t
  template <bool is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename PROJECT_OPT>
  static auto Project(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PROJECT_OPT&& project_opt) {
    VLOG(10) << "[Project] with project opt size: "
             << PROJECT_OPT::num_proj_cols;
    return ProjectOp<GRAPH_INTERFACE>::template ProjectImpl<is_append>(
        time_stamp, graph, std::move(ctx), std::move(project_opt));
  }

  //////////////////////////////////////Sort/Order/////////////////////////
  // From current context, do the sort.
  // After sort, the corresponding order maintained by csr offsets will be
  // cleaned. We need to flat current context to new context. Each node will
  // be replaced by the flat one. The alignment between nodes will be 1-1.
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... ORDER_PAIRS>
  static auto Sort(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                   Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                   SortOrderOpt<ORDER_PAIRS...>&& sort_opt) {
    auto& range = sort_opt.range_;
    if (range.start_ != 0) {
      LOG(FATAL) << "Current only support topk";
    }
    if (range.limit_ == 0) {
      LOG(FATAL) << "Current only support empty range";
    }

    VLOG(10) << "[Sort: ] Sort with " << sizeof...(ORDER_PAIRS) << " keys";
    return SortOp<GRAPH_INTERFACE>::SortTopK(
        time_stamp, graph, std::move(ctx), std::move(sort_opt.ordering_pairs_),
        range.limit_);
  }

  //////////////////////////////////////Select/Filter/////////////////////////
  // Select with head node. The type doesn't change
  // select only head node.
  template <
      typename CTX_HEAD_T, int cur_alias, int base_tag, typename... CTX_PREV,
      typename EXPR,
      typename std::enable_if<
          CTX_HEAD_T::is_two_label_set &&
          (std::tuple_size_v<typename EXPR::tag_prop_t> == 1) &&
          (std::tuple_element_t<0, std::remove_reference_t<decltype(
                                       std::declval<EXPR>().Properties())>>::
               tag_id == -1)>::type* = nullptr,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Select(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EXPR&& expr) {
    VLOG(10) << "[Select]";
    // Currently only support select with head node.

    auto named_prop = expr.Properties();

    auto& head = ctx.GetMutableHead();
    auto labels = head.GetLabels();
    auto prop_getter_tuple = get_prop_getters_from_named_property(
        time_stamp, graph, labels, named_prop);  // std::array<,2>
    // TODO: implement
    SelectTwoLabelSetImpl(time_stamp, ctx, head, prop_getter_tuple, expr);

    return std::move(ctx);
  }

  template <typename CTX_T, typename HEAD_T, typename EXPR,
            typename PROP_GETTER_T, size_t... Is>
  static void SelectTwoLabelSetImpl(
      int64_t time_stamp, CTX_T&& ctx, HEAD_T& head,
      const std::array<PROP_GETTER_T, 2>& prop_getter_tuple, const EXPR& expr) {
    auto& bitset = head.GetMutableBitset();
    auto& vertices = head.GetMutableVertices();
    size_t cur = 0;
    static_assert(HEAD_T::num_props == 0);
    auto& last_offset = ctx.GetMutableOffset(-1);
    double t0 = -grape::GetCurrentTime();
    Bitset new_bitset;
    new_bitset.init(vertices.size());
    size_t cur_begin = last_offset[0];
    for (auto i = 0; i < last_offset.size() - 1; ++i) {
      auto limit = last_offset[i + 1];
      for (auto j = cur_begin; j < limit; ++j) {
        auto vid = vertices[j];
        if (bitset.get_bit(j)) {
          // auto prop = single_prop_getter[0].get_view(vid);
          // if (expr(prop)) {
          // if (expr(prop_getter_tuple)[0].get_view(vid)...) {
          if (std::apply(expr, prop_getter_tuple[0].get_view(vid))) {
            new_bitset.set_bit(cur);
            if (cur < j) {
              vertices[cur++] = vid;
            } else {
              cur++;
            }
          }
        } else {
          // auto prop = single_prop_getter[1].get_view(vid);
          // if (expr(prop)) {
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
    LOG(INFO) << "after filter: " << vertices.size() << ", time: " << t0;
  }

  // Select from row vertex set.
  // only the case of select head node is supported.
  template <
      typename CTX_HEAD_T, int cur_alias, int base_tag, typename... CTX_PREV,
      typename EXPR,
      typename std::enable_if<
          CTX_HEAD_T::is_row_vertex_set &&
          (std::tuple_size_v<typename EXPR::tag_prop_t> == 1) &&
          (std::tuple_element_t<0, std::remove_reference_t<decltype(
                                       std::declval<EXPR>().Properties())>>::
               tag_id == -1)>::type* = nullptr,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Select(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EXPR&& expr) {
    VLOG(10) << "[Select]";
    using ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
    // Currently only support select with head node.

    auto named_prop = expr.Properties();
    // named_prop is a tuple
    // TODO: check prop's id is -1

    auto& head = ctx.GetMutableHead();
    auto label = head.GetLabel();
    auto prop_getter_tuple = get_prop_getters_from_named_property(
        time_stamp, graph, label, named_prop);  // std::array<,2>
    // TODO: implement
    SelectRowVertexSetImpl(ctx, head, prop_getter_tuple, expr,
                           std::make_index_sequence<
                               std::tuple_size_v<typename EXPR::tag_prop_t>>());

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
      for (auto i = 0; i < vertices.size(); ++i) {
        auto vid = vertices[i];
        // auto prop = single_prop_getter.get_view(vid);
        // if (expr(std::get<Is>(prop_getters).get_view(vid)...)) {
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
      for (auto i = 0; i < last_offset.size() - 1; ++i) {
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
    LOG(INFO) << "after filter: " << vertices.size() << ", time: " << t0;
  }

  //////////////////////////////////////Select/Filter/////////////////////////
  // Select with head node. The type doesn't change
  // select can possiblely applied on multiple tags
  // (!CTX_HEAD_T::is_row_vertex_set) && (!CTX_HEAD_T::is_two_label_set) &&
  template <
      typename CTX_HEAD_T, int cur_alias, int base_tag, typename... CTX_PREV,
      typename EXPR,
      typename std::enable_if<((std::tuple_size_v<typename EXPR::tag_prop_t>) >
                               1)>::type* = nullptr,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Select(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      EXPR&& expr) {
    VLOG(10) << "[Context]: Select in place";
    auto prop_desc = expr.Properties();
    std::vector<offset_t> new_offsets;
    std::vector<offset_t> select_indices;
    new_offsets.emplace_back(0);
    offset_t cur_offset = 0;
    offset_t cur_ind = 0;
    auto& cur_ = ctx.GetHead();
    select_indices.reserve(cur_.Size());
    auto prop_getters_tuple =
        create_prop_getters_from_prop_desc(time_stamp, graph, ctx, prop_desc);
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
  // We currently support group with one key, and possiblely multiple values.
  // create a brand new context type.
  // group count is included in this implementation.
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename KEY_ALIAS, typename... AGG,
            typename RES_T = typename GroupResT<
                Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                GroupOpt<KEY_ALIAS, AGG...>>::result_t>
  static RES_T GroupBy(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      GroupOpt<KEY_ALIAS, AGG...>&& group_opt) {
    VLOG(10) << "[Group] with with group opt";
    return GroupByOp<GRAPH_INTERFACE>::GroupByImpl(
        time_stamp, graph, std::move(ctx), std::move(group_opt));
  }

  /// @brief Group by two key_alias
  /// @tparam CTX_HEAD_T
  /// @tparam ...CTX_PREV
  /// @tparam _GROUP_OPT
  /// @tparam RES_T
  /// @tparam cur_alias
  /// @tparam base_tag
  /// @param time_stamp
  /// @param graph
  /// @param ctx
  /// @param group_opt
  /// @return
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename KEY_ALIAS0, typename KEY_ALIAS1,
            typename... AGG,
            typename RES_T = typename GroupResT<
                Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                GroupOpt2<KEY_ALIAS0, KEY_ALIAS1, AGG...>>::result_t>
  static RES_T GroupBy(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      GroupOpt2<KEY_ALIAS0, KEY_ALIAS1, AGG...>&& group_opt) {
    VLOG(10) << "[Group] with with group opt, two key_alias";
    return GroupByOp<GRAPH_INTERFACE>::GroupByImpl(
        time_stamp, graph, std::move(ctx), std::move(group_opt));
  }

  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename FOLD_OPT>
  static auto GroupByWithoutKey(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      FOLD_OPT&& fold_opt) {
    VLOG(10) << "[Group] with fold opt";
    return GroupByOp<GRAPH_INTERFACE>::GroupByWithoutKeyImpl(
        time_stamp, graph, std::move(ctx), std::move(fold_opt));
  }

  //////////////////////////////////////Shortest Path/////////////////////////
  // Return the path.
  template <int res_alias, int alias_to_use, typename EXPR, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, typename LabelT,
            typename EDGE_FILTER_T, typename UNTIL_CONDITION, typename... T,
            typename RES_SET_T = PathSet<vertex_id_t, LabelT>,
            typename RES_T = typename ResultContextT<
                res_alias, RES_SET_T, cur_alias, CTX_HEAD_T, base_tag,
                CTX_PREV...>::result_t>
  static RES_T ShortestPath(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
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
        time_stamp, graph, set, std::move(shortest_path_opt));
    return ctx.template AddNode<res_alias>(
        std::move(path_set_and_offset.first),
        std::move(path_set_and_offset.second));
  }

  //////////////////////////////////////Shortest Path/////////////////////////
  // Fused operator
  // 0. path Expand and filter with predicate, finally sort by property
  template <
      int alias_to_use, int res_alias, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename EXPR, typename LabelT,
      typename EDGE_FILTER_T, typename... EDGE_T, size_t num_labels,
      typename GET_V_EXPR, typename... VERTEX_T, typename... ORDER_PAIRS,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_vertex_set &&
                               (sizeof...(EDGE_T) == 0) &&
                               (sizeof...(VERTEX_T) == 0))>::type* = nullptr>
  static auto PathExpandVAndFilterAndSort(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T, EDGE_T...>&& path_expand_opt,
      GetVOpt<LabelT, num_labels, GET_V_EXPR, VERTEX_T...>&& get_v_opt,
      SortOrderOpt<ORDER_PAIRS...>&& sort_opt) {
    LOG(INFO) << "Fused operator: PathExpand+GetV+Sort";
    if (path_expand_opt.path_opt_ != PathOpt::Arbitrary) {
      LOG(FATAL) << "Only support Arbitrary path now";
    }
    if (path_expand_opt.result_opt_ != ResultOpt::EndV) {
      LOG(FATAL) << "Only support EndV now";
    }
    return FusedOperator<GRAPH_INTERFACE>::
        template PathExpandVNoPropsAndFilterVAndSort<alias_to_use, res_alias>(
            time_stamp, graph, std::move(ctx), std::move(path_expand_opt),
            std::move(get_v_opt), std::move(sort_opt));
  }

  template <
      int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... GetVProp, typename... ORDER_PAIRS,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_two_label_set &&
                               sizeof...(GetVProp) == 0)>::type* = nullptr>
  static auto GetVAndSort(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      GetVOpt<LabelT, num_labels, EXPRESSION, GetVProp...>&& get_v_opt,
      SortOrderOpt<ORDER_PAIRS...>&& sort_opt) {
    return FusedOperator<GRAPH_INTERFACE>::template GetVAndSort<res_alias,
                                                                alias_to_use>(
        time_stamp, graph, std::move(ctx), std::move(get_v_opt),
        std::move(sort_opt));
  }
};
}  // namespace gs

#endif  // GRAPHSCOPE_ENGINE_SYNC_ENGINE_H_
