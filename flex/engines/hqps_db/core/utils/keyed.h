
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
#ifndef ENGINES_HQPS_ENGINE_KEYED_UTILS_H_
#define ENGINES_HQPS_ENGINE_KEYED_UTILS_H_

#include "flex/engines/hqps_db/core/utils/props.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/engines/hqps_db/structures/collection.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/adj_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/untyped_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/keyed_row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"
#include "flex/engines/hqps_db/structures/path.h"

namespace gs {

template <typename SET_T>
struct AggFirst;

template <typename T>
struct AggFirst<Collection<T>> {
  using result_t = Collection<T>;
};

template <typename LabelT, typename VID_T, typename... T>
struct AggFirst<RowVertexSetImpl<LabelT, VID_T, T...>> {
  using result_t = RowVertexSetImpl<LabelT, VID_T, T...>;
};

template <typename VID_T, typename LabelT, typename... T>
struct AggFirst<TwoLabelVertexSetImpl<VID_T, LabelT, T...>> {
  using result_t = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;
};

/// @brief Helper to get keyed set type
/// @tparam T
/// @tparam ValueT Keyed prop type
template <typename T, typename KEY_ALIAS_T>
struct KeyedT;

// group by the vertex set itself
template <typename LabelT, typename VID_T, typename... T>
struct KeyedT<RowVertexSet<LabelT, VID_T, T...>,
              PropertySelector<grape::EmptyType>> {
  using keyed_set_t = KeyedRowVertexSet<LabelT, VID_T, VID_T, T...>;
  // // The builder type.
  using keyed_builder_t = KeyedRowVertexSetBuilder<LabelT, VID_T, VID_T, T...>;
  using unkeyed_builder_t = RowVertexSetBuilder<LabelT, VID_T, T...>;

  static keyed_builder_t create_keyed_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return keyed_builder_t(set);
  }

  static unkeyed_builder_t create_unkeyed_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return set.CreateBuilder();
  }
};

// Group By TwoLabelVertexSet's internal id.
template <typename LabelT, typename VID_T, typename... T>
struct KeyedT<TwoLabelVertexSet<VID_T, LabelT, T...>,
              PropertySelector<grape::EmptyType>> {
  using keyed_set_t = TwoLabelVertexSet<VID_T, LabelT, T...>;
  // // The builder type.
  using keyed_builder_t = TwoLabelVertexSetImplBuilder<VID_T, LabelT, T...>;
  using unkeyed_builder_t = TwoLabelVertexSetImplBuilder<LabelT, VID_T, T...>;

  static keyed_builder_t create_keyed_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return keyed_builder_t(set);
  }

  static unkeyed_builder_t create_unkeyed_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return set.CreateBuilder();
  }
};

// Group By TwoLabelVertexSet's other properties
template <typename LabelT, typename VID_T, typename... T, typename PropT>
struct KeyedT<TwoLabelVertexSet<VID_T, LabelT, T...>, PropertySelector<PropT>> {
  using keyed_set_t = Collection<PropT>;
  // // The builder type.
  using keyed_builder_t = KeyedCollectionBuilder<PropT>;
  using unkeyed_builder_t = CollectionBuilder<PropT>;

  static keyed_builder_t create_keyed_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set,
      const PropertySelector<PropT>& selector) {
    return keyed_builder_t();
  }

  static unkeyed_builder_t create_unkeyed_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return unkeyed_builder_t();
  }
};

// group by the vertex set' property
template <typename LabelT, typename VID_T, typename... T, typename PropT>
struct KeyedT<RowVertexSet<LabelT, VID_T, T...>, PropertySelector<PropT>> {
  using keyed_set_t = Collection<PropT>;
  // // The builder type.
  using keyed_builder_t = KeyedCollectionBuilder<PropT>;
  using unkeyed_builder_t = CollectionBuilder<PropT>;

  static keyed_builder_t create_keyed_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set,
      const PropertySelector<PropT>& selector) {
    return keyed_builder_t(set);
  }

  static unkeyed_builder_t create_unkeyed_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set,
      const PropertySelector<PropT>& selector) {
    return unkeyed_builder_t();
  }
};

// key on a keyed row vertex get us a unkeyed set.
template <typename LabelT, typename KEY_T, typename VID_T, typename... SET_T>
struct KeyedT<KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, SET_T...>,
              PropertySelector<grape::EmptyType>> {
  using keyed_set_t = KeyedRowVertexSetImpl<LabelT, VID_T, SET_T...>;
  // // The builder type.
  using keyed_builder_t =
      KeyedRowVertexSetBuilder<LabelT, KEY_T, VID_T, SET_T...>;
  using unkeyed_builder_t =
      typename KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, SET_T...>::builder_t;
  static keyed_builder_t create_keyed_builder(
      const KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, SET_T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return builder_t(set);
  }
  static unkeyed_builder_t create_unkeyedkeyed_builder(
      const KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, SET_T...>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return set.CreateBuilder();
  }
};

// group by vertex set' id, for generate vertex set.
// template <typename VID_T, typename LabelT, size_t N>
// struct KeyedT<GeneralVertexSet<VID_T, LabelT, N>,
//               PropertySelector<grape::EmptyType>> {
//   using keyed_set_t = KeyedRowVertexSet<LabelT, VID_T, VID_T,
//   grape::EmptyType>;
//   // // The builder type.
//   using builder_t =
//       KeyedRowVertexSetBuilder<LabelT, VID_T, VID_T, grape::EmptyType>;
// };

template <typename T>
struct KeyedT<Collection<T>, PropertySelector<grape::EmptyType>> {
  using keyed_set_t = Collection<T>;
  // // The builder type.
  using keyed_builder_t = KeyedCollectionBuilder<T>;
  using unkeyed_builder_t = CollectionBuilder<T>;

  static keyed_builder_t create_keyed_builder(
      const Collection<T>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return builder_t(set);
  }
  static unkeyed_builder_t create_unkeyed_builder(
      const Collection<T>& set,
      const PropertySelector<grape::EmptyType>& selector) {
    return unkeyed_builder_t();
  }
};

// when keyed with aggregation function, (which we currently only support
// collection)

/// @brief Helper to get keyed set type with aggregation func
/// @tparam T
/// @tparam ValueT Keyed prop type
template <typename GI, typename T, AggFunc agg_func, typename Props,
          typename Tags>
struct KeyedAggT;

/// @brief Helper to get keyed set type with aggregation func, which is applied
/// on multiple column
/// @tparam T
/// @tparam ValueT Keyed prop type
template <typename GI, typename SET_TUPLE_T, AggFunc agg_func, typename Props,
          typename Tags>
struct KeyedAggMultiColT;

template <typename GI, typename LabelT, typename VID_T, typename... T,
          typename PropT, int tag_id>
struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::COUNT,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using index_ele_t =
      typename RowVertexSet<LabelT, VID_T, T...>::index_ele_tuple_t;
  using prop_getter_t = RowVertexSetPropGetter<
      tag_id, gs::mutable_csr_graph_impl::SinglePropGetter<PropT>, index_ele_t>;
  // build a counter array.
  using aggregate_res_builder_t = PropCountBuilder<tag_id, prop_getter_t>;

  static aggregate_res_builder_t create_agg_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selector) {
    auto prop_getter = create_prop_getter_impl<tag_id, PropT>(
        set, graph, std::get<0>(selector).prop_name_);
    return aggregate_res_builder_t(std::move(prop_getter));
  }
};

template <typename GI, typename LabelT, typename VID_T, typename... T,
          int tag_id>
struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selector) {
    return CountBuilder<tag_id>();
  }
};

// aggregate count_dist
template <typename GI, typename LabelT, typename VID_T, typename... T,
          int tag_id>
struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::COUNT_DISTINCT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using vertex_set_t = RowVertexSet<LabelT, VID_T, T...>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, vertex_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set.GetVertices());
  }
};

template <typename GI, typename VID_T, typename LabelT, typename... T,
          typename PropT, int tag_id>
struct KeyedAggT<GI, TwoLabelVertexSet<VID_T, LabelT, T...>, AggFunc::COUNT,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using index_ele_t =
      typename TwoLabelVertexSet<VID_T, LabelT, T...>::index_ele_tuple_t;
  // build a counter array.
  using prop_getter_t = TwoLabelVertexSetImplPropGetter<
      tag_id, gs::mutable_csr_graph_impl::SinglePropGetter<PropT>, index_ele_t>;
  using aggregate_res_builder_t = PropCountBuilder<tag_id, prop_getter_t>;

  static aggregate_res_builder_t create_agg_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    auto prop_getter = create_prop_getter_impl<tag_id, PropT>(
        set, graph, std::get<0>(selectors).prop_name_);
    return aggregate_res_builder_t(std::move(prop_getter));
  }
};

template <typename GI, typename VID_T, typename LabelT, typename... T,
          int tag_id>
struct KeyedAggT<GI, TwoLabelVertexSet<VID_T, LabelT, T...>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

// count distinct for two_label set.
template <typename GI, typename VID_T, typename LabelT, typename... T,
          int tag_id>
struct KeyedAggT<GI, TwoLabelVertexSet<VID_T, LabelT, T...>,
                 AggFunc::COUNT_DISTINCT, std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using vertex_set_t = TwoLabelVertexSet<VID_T, LabelT, T...>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, vertex_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set.GetBitset(), set.GetVertices());
  }
};

// general vertex set to_count
template <typename GI, typename VID_T, typename LabelT, typename... SET_T,
          typename PropT, int tag_id>
struct KeyedAggT<GI, GeneralVertexSet<VID_T, LabelT, SET_T...>, AggFunc::COUNT,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using index_ele_t =
      typename GeneralVertexSet<VID_T, LabelT, SET_T...>::index_ele_tuple_t;
  using prop_getter_t = GeneralVertexSetPropGetter<
      tag_id, gs::mutable_csr_graph_impl::SinglePropGetter<PropT>, index_ele_t>;

  // build a counter array.
  using aggregate_res_builder_t = PropCountBuilder<tag_id, prop_getter_t>;

  static aggregate_res_builder_t create_agg_builder(
      const GeneralVertexSet<VID_T, LabelT, SET_T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    auto prop_getter = create_prop_getter_impl<tag_id, PropT>(
        set, graph, std::get<0>(selectors).prop_name_);
    return aggregate_res_builder_t(std::move(prop_getter));
  }
};

// count internal for general vertex set.
template <typename GI, typename VID_T, typename LabelT, typename... SET_T,
          int tag_id>
struct KeyedAggT<GI, GeneralVertexSet<VID_T, LabelT, SET_T...>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const GeneralVertexSet<VID_T, LabelT, SET_T...>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

template <typename GI, typename T, int tag_id>
struct KeyedAggT<GI, Collection<T>, AggFunc::SUM, std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<T>;
  // build a counter array.
  using aggregate_res_builder_t = SumBuilder<T, tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const Collection<T>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t();
  }
};

template <typename GI, typename LabelT, typename VID_T, typename... T,
          typename PropT, int tag_id>
struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::TO_SET,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = CollectionOfVec<PropT>;
  using aggregate_res_builder_t =
      CollectionOfSetBuilder<PropT, GI, RowVertexSet<LabelT, VID_T, T...>,
                             tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    return CollectionOfSetBuilder<PropT, GI, RowVertexSet<LabelT, VID_T, T...>,
                                  tag_id>(
        set, graph, std::array{std::get<0>(selectors).prop_name_});
  }
};

// to_vector
template <typename GI, typename T, typename PropT, int tag_id>
struct KeyedAggT<GI, Collection<T>, AggFunc::TO_LIST, std::tuple<PropT>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = CollectionOfVec<PropT>;
  using aggregate_res_builder_t =
      CollectionOfVecBuilder<T, GI, Collection<T>, tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const Collection<T>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    return aggregate_res_builder_t(
        graph, set, std::array{std::get<0>(selectors).prop_name_});
  }
};

template <typename GI, typename LabelT, typename VID_T, typename... T,
          typename PropT, int tag_id>
struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::TO_LIST,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  static_assert(!std::is_same_v<PropT, grape::EmptyType>,
                "Aggregate to_list for vertex set it self is not allowed");
  using agg_res_t = CollectionOfVec<PropT>;
  using aggregate_res_builder_t =
      CollectionOfVecBuilder<PropT, GI, RowVertexSet<LabelT, VID_T, T...>,
                             tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    return aggregate_res_builder_t(set, graph,
                                   {std::get<0>(selectors).prop_name_});
  }
};

// get min
template <typename GI, typename T, typename PropT, int tag_id>
struct KeyedAggT<GI, Collection<T>, AggFunc::MIN, std::tuple<PropT>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<T>;
  using aggregate_res_builder_t = MinBuilder<GI, T, tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const Collection<T>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    return aggregate_res_builder_t(set, graph,
                                   {std::get<0>(selectors).prop_name_});
  }
};

// get max
template <typename GI, typename T, typename PropT, int tag_id>
struct KeyedAggT<GI, Collection<T>, AggFunc::MAX, std::tuple<PropT>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<T>;
  using aggregate_res_builder_t = MaxBuilder<GI, T, tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const Collection<T>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    return aggregate_res_builder_t(
        set, graph, std::array{std::get<0>(selectors).prop_name_});
  }
};

template <typename GI, typename T, typename PropT, int tag_id>
struct KeyedAggT<GI, Collection<T>, AggFunc::FIRST, std::tuple<PropT>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<T>;
  using aggregate_res_builder_t =
      FirstBuilder<GI, Collection<T>, PropT, tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const Collection<T>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
    return aggregate_res_builder_t(
        set, graph, std::array{std::get<0>(selectors).prop_name_});
  }
};

// Aggregate first for twolabel vertex set
template <typename GI, typename VID_T, typename LabelT, typename... T,
          int tag_id>
struct KeyedAggT<GI, TwoLabelVertexSetImpl<VID_T, LabelT, T...>, AggFunc::FIRST,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;
  using old_set_t = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;
  using aggregate_res_builder_t =
      FirstBuilder<GI, TwoLabelVertexSetImpl<VID_T, LabelT, T...>,
                   grape::EmptyType, tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const old_set_t& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    auto labels = set.GetLabels();
    return aggregate_res_builder_t(
        set, graph, std::array{std::get<0>(selectors).prop_name_});
  }
};

template <typename GI, typename VID_T, typename LabelT, int tag_id>
struct KeyedAggT<GI, UnTypedEdgeSet<VID_T, LabelT, typename GI::sub_graph_t>,
                 AggFunc::COUNT, std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const UnTypedEdgeSet<VID_T, LabelT, typename GI::sub_graph_t>& set,
      const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

template <typename GI, typename VID_T, typename LabelT, typename SET_T,
          int tag_id>
struct KeyedAggT<GI, SingleLabelEdgeSet<VID_T, LabelT, SET_T>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const SingleLabelEdgeSet<VID_T, LabelT, SET_T>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

template <typename GI, typename VID_T, typename LabelT, typename EDATA_T,
          int tag_id>
struct KeyedAggT<GI, FlatEdgeSet<VID_T, LabelT, EDATA_T>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const FlatEdgeSet<VID_T, LabelT, EDATA_T>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

template <typename GI, typename VID_T, typename LabelT, int tag_id>
struct KeyedAggT<GI, CompressedPathSet<VID_T, LabelT>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const CompressedPathSet<VID_T, LabelT>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

template <typename GI, typename VID_T, typename LabelT, int tag_id>
struct KeyedAggT<GI, PathSet<VID_T, LabelT>, AggFunc::COUNT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const PathSet<VID_T, LabelT>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return CountBuilder<tag_id>();
  }
};

// COUNT DISTINCT for EdgeSets.
template <typename GI, typename VID_T, typename LabelT, int tag_id>
struct KeyedAggT<GI, UnTypedEdgeSet<VID_T, LabelT, typename GI::sub_graph_t>,
                 AggFunc::COUNT_DISTINCT, std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using edge_set_t = UnTypedEdgeSet<VID_T, LabelT, typename GI::sub_graph_t>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, edge_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const UnTypedEdgeSet<VID_T, LabelT, typename GI::sub_graph_t>& set,
      const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set);
  }
};

template <typename GI, typename VID_T, typename LabelT, typename SET_T,
          int tag_id>
struct KeyedAggT<GI, SingleLabelEdgeSet<VID_T, LabelT, SET_T>,
                 AggFunc::COUNT_DISTINCT, std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using edge_set_t = SingleLabelEdgeSet<VID_T, LabelT, SET_T>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, edge_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const SingleLabelEdgeSet<VID_T, LabelT, SET_T>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set);
  }
};

template <typename GI, typename VID_T, typename LabelT, typename EDATA_T,
          int tag_id>
struct KeyedAggT<GI, FlatEdgeSet<VID_T, LabelT, EDATA_T>,
                 AggFunc::COUNT_DISTINCT, std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using edge_set_t = FlatEdgeSet<VID_T, LabelT, EDATA_T>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, edge_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const FlatEdgeSet<VID_T, LabelT, EDATA_T>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set);
  }
};

template <typename GI, typename VID_T, typename LabelT, int tag_id>
struct KeyedAggT<GI, CompressedPathSet<VID_T, LabelT>, AggFunc::COUNT_DISTINCT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using path_set_t = CompressedPathSet<VID_T, LabelT>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, path_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const path_set_t& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set);
  }
};

template <typename GI, typename VID_T, typename LabelT, int tag_id>
struct KeyedAggT<GI, PathSet<VID_T, LabelT>, AggFunc::COUNT_DISTINCT,
                 std::tuple<grape::EmptyType>,
                 std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  using path_set_t = PathSet<VID_T, LabelT>;
  using aggregate_res_builder_t = DistinctCountBuilder<tag_id, path_set_t>;

  static aggregate_res_builder_t create_agg_builder(
      const path_set_t& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set);
  }
};

template <typename GI, typename... SET_T, typename... PropSelectorT,
          int... tag_ids>
struct KeyedAggMultiColT<GI, std::tuple<SET_T...>, AggFunc::COUNT_DISTINCT,
                         std::tuple<PropSelectorT...>,
                         std::integer_sequence<int32_t, tag_ids...>> {
  using agg_res_t = Collection<size_t>;
  // get the tuple of sets from the tuple of tags.
  using aggregate_res_builder_t =
      MultiColDistinctCountBuilder<std::tuple<SET_T...>, tag_ids...>;

  static aggregate_res_builder_t create_agg_builder(
      const std::tuple<SET_T...>& set, const GI& graph,
      std::tuple<PropSelectorT...>& selectors) {
    return aggregate_res_builder_t();
  }
};

template <typename GI, typename... SET_T, typename... PropSelectorT,
          int... tag_ids>
struct KeyedAggMultiColT<GI, std::tuple<SET_T...>, AggFunc::COUNT,
                         std::tuple<PropSelectorT...>,
                         std::integer_sequence<int32_t, tag_ids...>> {
  using agg_res_t = Collection<size_t>;
  // get the tuple of sets from the tuple of tags.
  using aggregate_res_builder_t = MultiColCountBuilder<tag_ids...>;

  static aggregate_res_builder_t create_agg_builder(
      const std::tuple<SET_T...>& set, const GI& graph,
      std::tuple<PropSelectorT...>& selectors) {
    return aggregate_res_builder_t();
  }
};

template <typename LabelT, typename KEY_T, typename VID_T, typename... T,
          typename ELE, typename DATA>
static inline auto insert_into_builder_v2_impl(
    KeyedRowVertexSetBuilderImpl<LabelT, KEY_T, VID_T, T...>& builder,
    const ELE& ele, const DATA& data) {
  return builder.insert(ele, data);
}

template <typename LabelT, typename KEY_T, typename VID_T, typename ELE,
          typename DATA>
static inline auto insert_into_builder_v2_impl(
    KeyedRowVertexSetBuilderImpl<LabelT, KEY_T, VID_T, grape::EmptyType>&
        builder,
    const ELE& ele, const DATA& data) {
  return builder.Insert(ele);
}

// insert_into_bulder_v2_impl
template <
    typename BuilderT, typename ELE, typename DATA,
    typename std::enable_if<
        (BuilderT::is_row_vertex_set_builder &&
         std::is_same_v<DATA, std::tuple<grape::EmptyType>>)>::type* = nullptr>
static inline auto insert_into_builder_v2_impl(BuilderT& builder,
                                               const ELE& ele,
                                               const DATA& data) {
  return builder.Insert(ele);
}

// insert_into_bulder_v2_impl
template <
    typename BuilderT, typename ELE, typename DATA,
    typename std::enable_if<
        (BuilderT::is_row_vertex_set_builder &&
         !std::is_same_v<DATA, std::tuple<grape::EmptyType>>)>::type* = nullptr>
static inline auto insert_into_builder_v2_impl(BuilderT& builder,
                                               const ELE& ele,
                                               const DATA& data) {
  return builder.Insert(ele, data);
}

template <typename VID_T, typename LabelT, typename EDATA_T, typename ELE,
          typename DATA>
static inline auto insert_into_builder_v2_impl(
    FlatEdgeSetBuilder<VID_T, LabelT, EDATA_T>& builder, const ELE& ele,
    const DATA& data) {
  return builder.Insert(ele);
}

template <typename BuilderT, typename ELE, typename DATA,
          typename std::enable_if<
              (BuilderT::is_general_edge_set_builder)>::type* = nullptr>
static inline auto insert_into_builder_v2_impl(BuilderT& builder,
                                               const ELE& ele,
                                               const DATA& data) {
  return builder.Insert(ele);
}

template <
    typename BuilderT, typename ELE, typename DATA,
    typename std::enable_if<
        (BuilderT::is_two_label_set_builder &&
         std::is_same_v<DATA, std::tuple<grape::EmptyType>>)>::type* = nullptr>
static inline auto insert_into_builder_v2_impl(BuilderT& builder,
                                               const ELE& ele,
                                               const DATA& data) {
  return builder.Insert(ele);
}

template <
    typename BuilderT, typename ELE, typename DATA,
    typename std::enable_if<
        (BuilderT::is_two_label_set_builder &&
         !std::is_same_v<DATA, std::tuple<grape::EmptyType>>)>::type* = nullptr>
static inline auto insert_into_builder_v2_impl(BuilderT& builder,
                                               const ELE& ele,
                                               const DATA& data) {
  return builder.Insert(ele, data);
}

// insert for collectionBuilder
template <
    typename BuilderT, typename ELE, typename DATA,
    typename std::enable_if<(BuilderT::is_collection_builder)>::type* = nullptr>
static inline auto insert_into_builder_v2_impl(BuilderT& builder,
                                               const ELE& ele,
                                               const DATA& data) {
  return builder.Insert(ele);
}

// insert for adjEdgeSetBuilder
template <typename ELE, typename DATA, typename GI, typename LabelT,
          typename VID_T, typename... EDATA_T>
static inline auto insert_into_builder_v2_impl(
    AdjEdgeSetBuilder<GI, LabelT, VID_T, EDATA_T...>& builder, const ELE& ele,
    const DATA& data) {
  return builder.Insert(ele);
}

template <typename ELE, typename DATA, typename GI, typename LabelT,
          typename VID_T>
static inline auto insert_into_builder_v2_impl(
    AdjEdgeSetBuilder<GI, LabelT, VID_T, grape::EmptyType>& builder,
    const ELE& ele, const DATA& data) {
  return builder.Insert(ele);
}

// insert to single label edge label.
template <typename ELE, typename DATA, typename LabelT, typename VID_T,
          typename EDATA_T>
static inline auto insert_into_builder_v2_impl(
    SingleLabelEdgeSetBuilder<VID_T, LabelT, EDATA_T>& builder, const ELE& ele,
    const DATA& data) {
  return builder.Insert(ele);
}

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_KEYED_UTILS_H_
