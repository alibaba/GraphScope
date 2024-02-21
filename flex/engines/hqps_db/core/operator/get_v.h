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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_GET_V_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_GET_V_H_

#include <string>
#include <vector>

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/multi_label_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"
#include "flex/storages/rt_mutable_graph/types.h"

#include "grape/utils/bitset.h"

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
            typename EXPRESSION, typename... SELECTOR,
            typename std::enable_if<(SET_T::is_vertex_set)>::type* = nullptr,
            typename RES_T = std::pair<SET_T, std::vector<offset_t>>>
  static RES_T GetNoPropV(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, Filter<EXPRESSION, SELECTOR...>>& get_v_opt) {
    // VLOG(10) << "[Get no PropertyV from vertex set]" << set.Size();
    return GetNoPropVSetFromVertexSet<RES_T>(graph, set, get_v_opt);
  }

  // get no propv from common edge set.
  template <
      typename SET_T, typename LabelT, size_t num_labels, typename EXPRESSION,
      typename std::enable_if<(SET_T::is_edge_set &&
                               !SET_T::is_multi_dst_label)>::type* = nullptr>
  static auto GetNoPropVFromEdgeSet(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    VLOG(10) << "[Get no PropertyV from edge set]" << set.Size();
    return GetNoPropVSetFromSingleDstEdgeSet(graph, set, std::move(get_v_opt));
  }

  // get no prop v from PathSet.
  template <typename SET_T, typename LabelT, size_t num_labels,
            typename EXPRESSION,
            typename std::enable_if<(SET_T::is_path_set)>::type* = nullptr>
  static auto GetNoPropVFromPathSet(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    VLOG(10) << "Get no PropertyV from path set, size: " << set.Size();
    return GetNoPropVFromPathSetImpl(graph, set, get_v_opt.v_opt_,
                                     get_v_opt.v_labels_, get_v_opt.filter_);
  }

  // get no prop v from untyped edge set, with no predicate.
  template <typename LabelT, size_t num_labels>
  static auto GetNoPropVFromEdgeSet(
      const GRAPH_INTERFACE& graph,
      const UnTypedEdgeSet<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                           typename GRAPH_INTERFACE::sub_graph_t>& set,
      GetVOpt<LabelT, num_labels, Filter<TruePredicate>>&& get_v_opt) {
    VLOG(10) << "[Get no PropertyV from unkeyed dst edge set]" << set.Size();
    return set.GetVertices(get_v_opt);
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
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    VLOG(10) << "[Get PropertyV from vertex set]" << set.Size();
    return GetMultiPropertyVSetFromVertexSet<RES_T>(graph, set,
                                                    std::move(get_v_opt));
  }

  /// Get vertex with properties from two label vertex set.
  template <typename LabelT, typename SET_T, typename... T, size_t num_labels,
            typename EXPRESSION>
  static auto GetPropertyVFromTwoLabelSet(
      const GRAPH_INTERFACE& graph, const SET_T& set,
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
    auto property_tuples = get_property_tuple_two_label(graph, set, props);
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
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    VLOG(10) << "[Get PropertyV from vertex set]" << set.Size();
    return GetPropertyVFromTwoLabelSet(graph, set, get_v_opt);
  }

  /// Get vertex with properties from vertex set.
  template <typename RES_T, typename LabelT, typename SET_T, typename... T,
            size_t num_labels, typename EXPRESSION>
  static RES_T GetMultiPropertyVSetFromVertexSet(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION, T...>&& get_v_opt) {
    static_assert(SET_T::is_multi_label);
    auto v_opt = get_v_opt.v_opt_;
    auto v_labels = get_v_opt.v_labels_;
    auto props = get_v_opt.props_;
    auto expr = get_v_opt.expr_;

    auto result_vertex_and_offset = do_project(graph, v_labels, expr, set);
    /// Then combine columns.
    // TODO: Shrink for vector-based columns.
    // auto col_tuple = GetColTuples(graph result_vertex_and_offset.first,
    // props);

    static constexpr size_t multi_set_size = SET_T::num_labels;
    auto array = get_multi_label_set_properties<T...>(
        graph, std::move(result_vertex_and_offset.first), props,
        std::make_index_sequence<multi_set_size>());
    typename RES_T::first_type multi_v_set(std::move(std::get<0>(array)),
                                           std::move(std::get<1>(array)));
    return std::make_pair(std::move(multi_v_set),
                          std::move(result_vertex_and_offset.second));
  }

  template <typename... T, typename SET_T, size_t... Is>
  static auto get_multi_label_set_properties(const GRAPH_INTERFACE& graph,
                                             SET_T&& multi_set,
                                             PropNameArray<T...>& props,
                                             std::index_sequence<Is...>) {
    using res_set_t = vertex_set_t<T...>;
    static constexpr size_t num_labels = SET_T::num_labels;
    std::array<std::vector<std::tuple<T...>>, num_labels> res_data_tuples;
    for (size_t i = 0; i < num_labels; ++i) {
      auto& cur_set = multi_set.GetSet(i);
      VLOG(10) << "set: " << i << ", size: " << cur_set.Size();
      res_data_tuples[i] = graph.template GetVertexPropsFromVid<T...>(
          cur_set.GetLabel(), cur_set.GetVertices(), props);
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
            typename EXPRESSION, typename... SELECTOR>
  static RES_T GetNoPropVSetFromVertexSet(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, Filter<EXPRESSION, SELECTOR...>>& get_v_opt) {
    auto filter = get_v_opt.filter_;
    return do_project(graph, get_v_opt.v_labels_, filter, set);
  }

  // get single label from single dst edge label.
  template <typename LabelT, size_t num_labels, typename SET_T,
            typename EXPRESSION>
  static auto GetNoPropVSetFromSingleDstEdgeSet(
      const GRAPH_INTERFACE& graph, const SET_T& set,
      GetVOpt<LabelT, num_labels, EXPRESSION>&& get_v_opt) {
    auto expr = get_v_opt.filter_.expr_;
    return set.GetVertices(get_v_opt.v_opt_, get_v_opt.v_labels_, expr);
  }

 private:
  // get no prop v from path set.
  template <size_t num_labels, typename EXPRESSION, typename... SELECTOR>
  static auto GetNoPropVFromPathSetImpl(
      const GRAPH_INTERFACE& graph,
      const CompressedPathSet<vertex_id_t, label_id_t>& set, VOpt v_opt,
      std::array<label_id_t, num_labels>& req_labels,
      Filter<EXPRESSION, SELECTOR...>& filter) {
    auto req_label_vec = array_to_vec(req_labels);
    std::vector<label_id_t> labels = set.GetLabels(v_opt);

    // remove duplicate from labels
    std::sort(labels.begin(), labels.end());
    labels.erase(std::unique(labels.begin(), labels.end()), labels.end());
    // Can only be one label.
    CHECK(labels.size() == 1);
    // if req_labels is empty, then use the label from path set.
    if (req_label_vec.empty()) {
      req_label_vec.push_back(labels[0]);
    }
    // check if the label is in v_labels.
    auto label = labels[0];
    auto it = std::find(labels.begin(), labels.end(), label);
    if (it == labels.end()) {
      LOG(WARNING) << "Label: " << label << " is not in path labels";
      // create an empty row vertex set.
      auto res_set =
          make_default_row_vertex_set<vertex_id_t, label_id_t>({}, label);
      // create offsets.
      auto offsets = std::vector<offset_t>(set.Size() + 1);
      for (size_t i = 0; i < offsets.size(); ++i) {
        offsets[i] = 0;
      }
      return std::make_pair(std::move(res_set), std::move(offsets));
    }
    auto property_getters_array =
        get_prop_getters_from_selectors(graph, labels, filter.selectors_);
    return set.GetVertices(v_opt, filter.expr_, property_getters_array);
  }

  // get no prop v from path set.
  template <size_t num_labels, typename EXPRESSION, typename... SELECTOR>
  static auto GetNoPropVFromPathSetImpl(
      const GRAPH_INTERFACE& graph, const PathSet<vertex_id_t, label_id_t>& set,
      VOpt v_opt, std::array<label_id_t, num_labels>& req_labels,
      Filter<EXPRESSION, SELECTOR...>& filter) {
    auto req_label_vec = array_to_vec(req_labels);
    return set.GetVertices(v_opt, req_label_vec);
  }

  // User-defined expression
  // for vertex set with multiple labels, i.e. two_label or general vertex set.
  // do project.
  template <
      typename LabelT, size_t num_labels, typename EXPRESSION,
      typename... SELECTOR, typename SET_T,
      typename std::enable_if<!std::is_same_v<EXPRESSION, TruePredicate> &&
                              (SET_T::is_general_set ||
                               SET_T::is_two_label_set)>::type* = nullptr>
  static auto do_project(const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         Filter<EXPRESSION, SELECTOR...>& filter,
                         const SET_T& set) {
    double t0 = -grape::GetCurrentTime();
    // array size : num_labels
    auto property_getters_array = get_prop_getters_from_selectors(
        graph, set.GetLabels(), filter.selectors_);
    t0 += grape::GetCurrentTime();
    LOG(INFO) << "Get property tuple for general set of size: " << set.Size()
              << " cost: " << t0;
    return set.project_vertices(labels, filter.expr_, property_getters_array);
  }

  // udf expression with single label.
  template <typename LabelT, size_t num_labels, typename EXPRESSION,
            typename... SELECTOR, typename... V_SET_T>
  static auto do_project(
      const GRAPH_INTERFACE& graph, std::array<LabelT, num_labels>& labels,
      Filter<EXPRESSION, SELECTOR...>& filter,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& set) {
    // TODO: support for multiple selectors
    auto property_getters_array = std::array{get_prop_getter_from_selectors(
        graph, set.GetLabel(), filter.selectors_)};
    return set.project_vertices(labels, filter.expr_, property_getters_array);
  }

  // true predicate and single label.
  template <typename LabelT, size_t num_labels, typename... SELECTOR,
            typename... V_SET_T>
  static auto do_project(
      const GRAPH_INTERFACE& graph, std::array<LabelT, num_labels>& labels,
      Filter<TruePredicate>& filter,
      const RowVertexSet<LabelT, vertex_id_t, V_SET_T...>& set) {
    // since expression always returns true, we provide set with a
    // always-return-true prop getter.
    return set.project_vertices(labels);
  }

  // True predicate and multi label
  template <typename LabelT, size_t num_labels, typename SET_T,
            typename std::enable_if<SET_T::is_two_label_set ||
                                    SET_T::is_general_set>::type* = nullptr>
  static auto do_project(const GRAPH_INTERFACE& graph,
                         std::array<LabelT, num_labels>& labels,
                         Filter<TruePredicate>& filter, const SET_T& set) {
    return set.project_vertices(labels);
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_GET_V_H_
