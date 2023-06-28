/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_OPERATOR_GET_V_H_
#define GRAPHSCOPE_OPERATOR_GET_V_H_

#include <string>
#include <vector>

#include "flex/engines/hqps/ds/multi_vertex_set/multi_label_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/two_label_vertex_set.h"
// #include "flex/engines/hqps/ds/unkeyed_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/utils/bitset.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

template <typename GRAPH_INTERFACE>
class GetVertex {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using default_vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;

  template <typename... T>
  using vertex_set_t = RowVertexSet<label_id_t, vertex_id_t, T...>;

  template <typename SET_T, typename LabelT, size_t num_labels,
            typename EXPRESSION,
            typename std::enable_if<(SET_T::is_vertex_set)>::type* = nullptr,
            typename RES_T = std::pair<SET_T, std::vector<offset_t>>>
  static RES_T GetNoPropV(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                          const SET_T& set,
                          GetVOpt<LabelT, num_labels, EXPRESSION>& get_v_opt) {
    // VLOG(10) << "[Get no PropertyV from vertex set]" << set.Size();
    return GetNoPropVSetFromVertexSet<RES_T>(time_stamp, graph, set, get_v_opt);
  }

  // get no propv from common edge set.
  template <
      typename SET_T, typename LabelT, size_t num_labels, typename EXPRESSION,
      typename std::enable_if<(SET_T::is_edge_set &&
                               !SET_T::is_multi_dst_label)>::type* = nullptr,
      typename RES_T = std::pair<default_vertex_set_t, std::vector<offset_t>>>
  static RES_T GetNoPropVFromEdgeSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    VLOG(10) << "[Get no PropertyV from edge set]" << set.Size();
    return GetNoPropVSetFromSingleDstEdgeSet<RES_T>(time_stamp, graph, set,
                                                    std::move(get_v_opt));
  }

  // get no propv from two label dst edge set
  template <typename SET_T, typename LabelT, size_t num_labels,
            typename EXPRESSION,
            typename std::enable_if<(SET_T::is_edge_set &&
                                     SET_T::is_multi_dst_label &&
                                     num_labels == 2)>::type* = nullptr,
            typename RES_T = std::pair<
                TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType>,
                std::vector<offset_t>>>
  static RES_T GetNoPropVFromEdgeSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    VLOG(10) << "[Get no PropertyV from mutlti dst edge set]" << set.Size();
    return GetNoPropVSetFromMutliDstEdgeSet<RES_T>(time_stamp, graph, set,
                                                   std::move(get_v_opt));
  }

  // Result is multilabelVertexset.
  template <typename SET_T, typename LabelT, typename... T, size_t num_labels,
            typename EXPRESSION,
            typename std::enable_if<(SET_T::is_vertex_set &&
                                     !SET_T::is_two_label_set &&
                                     num_labels > 1)>::type* = nullptr,
            typename RES_T =
                std::pair<MultiLabelVertexSet<vertex_set_t<T...>, num_labels>,
                          std::vector<offset_t>>>
  static RES_T GetPropertyV(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    VLOG(10) << "[Get PropertyV from vertex set]" << set.Size();
    return GetMultiPropertyVSetFromVertexSet<RES_T>(time_stamp, graph, set,
                                                    std::move(get_v_opt));
  }

  /// Get vertex with properties from two label vertex set.
  template <typename LabelT, typename SET_T, typename... T, size_t num_labels,
            typename EXPRESSION>
  static auto GetPropertyVFromTwoLabelSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      const GetVOpt<LabelT, num_labels, EXPRESSION, T...>& get_v_opt) {
    auto v_opt = get_v_opt.v_opt_;
    CHECK(v_opt == VOpt::Itself)
        << "Can only get v from vertex set with v_opt == vopt::Itself";
    auto v_labels = get_v_opt.v_labels_;
    auto props = get_v_opt.props_;
    auto expr = get_v_opt.expr_;
    // first extract properties, and create new properties,
    // We assume the expr.props <= props.
    double t0 = -grape::GetCurrentTime();
    auto property_tuples =
        get_property_tuple_two_label(time_stamp, graph, set, props);
    auto set_with_tuple =
        set.WithData(std::move(property_tuples), std::move(props));
    t0 += grape::GetCurrentTime();
    LOG(INFO) << "Get property tuple for two label set of size: " << set.Size()
              << " cost: " << t0;
    double t1 = -grape::GetCurrentTime();
    auto res = set_with_tuple.project_vertices_internal(v_labels, expr);
    t1 += grape::GetCurrentTime();
    LOG(INFO) << "Filter cost: " << t1;
    return res;
    //.project_vertices.
    // create new vertices with indices and property vector.
  }

  // specialization for two label vertex set, return two label vertex set
  // with
  // labels.
  template <
      typename SET_T, typename LabelT, typename... T, size_t num_labels,
      typename EXPRESSION,
      typename std::enable_if<(SET_T::is_vertex_set &&
                               SET_T::is_two_label_set &&
                               num_labels > 1)>::type* = nullptr,
      typename RES_T = std::pair<TwoLabelVertexSet<vertex_id_t, LabelT, T...>,
                                 std::vector<offset_t>>>
  static RES_T GetPropertyV(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    VLOG(10) << "[Get PropertyV from vertex set]" << set.Size();
    return GetPropertyVFromTwoLabelSet(time_stamp, graph, set, get_v_opt);
  }

  /// Get vertex with properties from vertex set.
  template <typename RES_T, typename LabelT, typename SET_T, typename... T,
            size_t num_labels, typename EXPRESSION>
  static RES_T GetMultiPropertyVSetFromVertexSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    static_assert(SET_T::is_multi_label);
    auto v_opt = get_v_opt.v_opt_;
    auto v_labels = get_v_opt.v_labels_;
    auto props = get_v_opt.props_;
    auto expr = get_v_opt.expr_;

    auto result_vertex_and_offset =
        do_project(time_stamp, graph, v_labels, expr, set);
    /// Then combine columns.
    // TODO: Shrink for vector-based columns.
    // auto col_tuple = GetColTuples(graph result_vertex_and_offset.first,
    // props);

    static constexpr size_t multi_set_size = SET_T::num_labels;
    auto array = get_multi_label_set_properties<T...>(
        time_stamp, graph, std::move(result_vertex_and_offset.first), props,
        std::make_index_sequence<multi_set_size>());
    typename RES_T::first_type multi_v_set(std::move(std::get<0>(array)),
                                           std::move(std::get<1>(array)));
    return std::make_pair(std::move(multi_v_set),
                          std::move(result_vertex_and_offset.second));
  }

  template <typename... T, typename SET_T, size_t... Is>
  static auto get_multi_label_set_properties(int64_t time_stamp,
                                             const GRAPH_INTERFACE& graph,
                                             SET_T&& multi_set,
                                             PropNameArray<T...>& props,
                                             std::index_sequence<Is...>) {
    using res_set_t = vertex_set_t<T...>;
    static constexpr size_t num_labels = SET_T::num_labels;
    std::array<std::vector<std::tuple<T...>>, num_labels> res_data_tuples;
    for (auto i = 0; i < num_labels; ++i) {
      auto& cur_set = multi_set.GetSet(i);
      VLOG(10) << "set: " << i << ", size: " << cur_set.Size();
      res_data_tuples[i] = graph.template GetVertexPropsFromVid<T...>(
          time_stamp, cur_set.GetLabel(), cur_set.GetVertices(), props);
    }
    VLOG(10) << "Finish get data tuples";
    auto set_array = std::array<res_set_t, num_labels>{make_row_vertex_set(
        std::move(multi_set.template GetSet<Is>().MoveVertices()),
        multi_set.template GetSet<Is>().GetLabel(),
        std::move(res_data_tuples[Is]), props)...};
    auto offset_array = std::array<std::vector<offset_t>, num_labels>{
        std::move(multi_set.template GetOffset<Is>())...};
    return std::make_pair(std::move(set_array), std::move(offset_array));
  }

  template <typename RES_T, typename LabelT, size_t num_labels, typename SET_T,
            typename EXPRESSION>
  static RES_T GetNoPropVSetFromVertexSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>& get_v_opt) {
    auto v_opt = get_v_opt.v_opt_;
    auto expr = get_v_opt.expr_;
    auto props = get_v_opt.props_;
    return do_project(time_stamp, graph, get_v_opt.v_labels_, expr, set);
  }

  // get single label from single dst edge label.
  template <typename RES_T, typename LabelT, size_t num_labels, typename SET_T,
            typename EXPRESSION>
  static RES_T GetNoPropVSetFromSingleDstEdgeSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    auto v_opt = get_v_opt.v_opt_;
    auto v_label = get_v_opt.v_labels_[0];
    auto expr = get_v_opt.expr_;
    auto props = get_v_opt.props_;
    return set.GetVertices(get_v_opt.v_opt_, get_v_opt.v_labels_, expr);
  }

  // get multiple label dst edge label. returns two label set.
  template <typename RES_T, typename LabelT, size_t num_labels,
            typename EXPRESSION,
            typename std::enable_if<num_labels == 2>::type* = nullptr>
  static RES_T GetNoPropVSetFromMutliDstEdgeSet(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      const MultiLabelDstEdgeSet<num_labels, GRAPH_INTERFACE, grape::EmptyType>&
          set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    auto v_opt = get_v_opt.v_opt_;
    auto expr = get_v_opt.expr_;
    auto props = get_v_opt.props_;
    return set.GetVertices(get_v_opt.v_opt_, get_v_opt.v_labels_, expr);
  }

 private:
  // udf exp with generalset
  template <
      typename LabelT, size_t num_labels, typename EXPRESSION, typename SET_T,
      typename std::enable_if<!std::is_same_v<EXPRESSION, TruePredicate> &&
                              SET_T::is_general_set>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, const SET_T& set) {
    // we need to fetch the properties that will be used for filtering.
    // TODO: implement general set.
    auto named_prop = expr.Properties();
    double t0 = -grape::GetCurrentTime();
    // array size : num_labels
    auto property_getters_array = get_prop_getters_from_named_property(
        time_stamp, graph, set.GetLabels(), named_prop);
    t0 += grape::GetCurrentTime();
    LOG(INFO) << "Get property tuple for general set of size: " << set.Size()
              << " cost: " << t0;
    return set.project_vertices(labels, expr, property_getters_array);
  }
  // udf exp with two_label set
  template <
      typename LabelT, size_t num_labels, typename EXPRESSION, typename SET_T,
      typename std::enable_if<!std::is_same_v<EXPRESSION, TruePredicate> &&
                              SET_T::is_two_label_set>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, const SET_T& set) {
    // we need to fetch the properties that will be used for filtering.
    // TODO: implement general set.
    auto named_prop = expr.Properties();
    double t0 = -grape::GetCurrentTime();
    auto property_getters_array = get_prop_getters_from_named_property(
        time_stamp, graph, set.GetLabels(), named_prop);
    t0 += grape::GetCurrentTime();
    LOG(INFO) << "Get property tuple for two label set of size: " << set.Size()
              << " cost: " << t0;
    return set.project_vertices(labels, expr, property_getters_array);
  }

  // udf expression with single label.
  template <typename LabelT, size_t num_labels, typename EXPRESSION,
            typename SET_T,
            typename std::enable_if<
                !std::is_same_v<EXPRESSION, TruePredicate> &&
                !SET_T::is_multi_label && !SET_T::is_two_label_set &&
                SET_T::is_row_vertex_set>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, SET_T& set) {
    // we need to fetch the properties that will be used for filtering.
    auto named_props = expr.Properties();
    auto property_getters_array = get_prop_getters_from_named_property(
        time_stamp, graph, set.GetLabel(), named_props);
    return set.project_vertices(labels, expr, property_getters_array);
  }

  // true predicate and single label.
  template <typename LabelT, size_t num_labels, typename EXPRESSION,
            typename SET_T,
            typename std::enable_if<std::is_same_v<EXPRESSION, TruePredicate> &&
                                    !SET_T::is_multi_label &&
                                    SET_T::is_row_vertex_set>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, SET_T& set) {
    // since expression always returns true, we provide set with a
    // always-return-true prop getter.
    return set.project_vertices(labels);
  }

  // True predicate and multi label
  template <typename LabelT, size_t num_labels, typename EXPRESSION,
            typename SET_T,
            typename std::enable_if<std::is_same_v<EXPRESSION, TruePredicate> &&
                                    SET_T::is_multi_label>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, SET_T& set) {
    // we need to fetch the properties that will be used for filtering.
    std::vector<std::vector<std::tuple<>>> property_tuples(set.Size());
    return set.project_vertices(labels, expr, property_tuples);
  }

  // true predicate and general set
  template <typename LabelT, size_t num_labels, typename EXPRESSION,
            typename SET_T,
            typename std::enable_if<std::is_same_v<EXPRESSION, TruePredicate> &&
                                    SET_T::is_general_set>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, SET_T& set) {
    // we need to fetch the properties that will be used for filtering.
    // TODO: implement general set.
    std::vector<std::vector<std::tuple<>>> property_tuples(set.Size());
    return set.project_vertices(labels, expr, property_tuples);
  }

  // true predicate and two label set
  template <typename LabelT, size_t num_labels, typename EXPRESSION,
            typename SET_T,
            typename std::enable_if<std::is_same_v<EXPRESSION, TruePredicate> &&
                                    SET_T::is_two_label_set>::type* = nullptr>
  static auto do_project(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, SET_T& set) {
    // we need to fetch the properties that will be used for filtering.
    // TODO: implement general set.
    std::vector<std::vector<std::tuple<>>> property_tuples(set.Size());
    return set.project_vertices(labels, expr, property_tuples);
  }
};
}  // namespace gs

#endif  // GRAPHSCOPE_OPERATOR_GET_V_H_
