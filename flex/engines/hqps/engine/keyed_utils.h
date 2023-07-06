
#ifndef GRAPHSCOPE_ENGINE_KEYED_UTILS_H_
#define GRAPHSCOPE_ENGINE_KEYED_UTILS_H_

#include "flex/engines/hqps/ds/collection.h"
#include "flex/engines/hqps/ds/multi_edge_set/adj_edge_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/keyed_row_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/two_label_vertex_set.h"
#include "flex/engines/hqps/engine/operator//prop_utils.h"

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

/*
template <typename UNKEYED_SET_T>
struct ToKeyedUtils{
using keyed_vertex_set_t = UNKEYED_SET_T;
};

template <>
struct ToKeyedUtils<DefaultVertexSet> {
using keyed_vertex_set_t = DefaultKeyedVertexSet;
static keyed_vertex_set_t toKeyed(const DefaultVertexSet& unkeyed) {
auto vec_copied = unkeyed.vids_;
return keyed_vertex_set_t(std::move(unkeyed.GenerateKeys()),
                      std::move(vec_copied), unkeyed.v_label_);
}
};

template <typename... COL_T>
struct ToKeyedUtils<UnkeyedVertexSet<COL_T...>> {
using keyed_vertex_set_t = KeyedVertexSet<COL_T...>;
static keyed_vertex_set_t toKeyed(const UnkeyedVertexSet<COL_T...>& unkeyed) {
auto vec_copied = unkeyed.vids_;
auto cols_copied = unkeyed.cols_;
return keyed_vertex_set_t(std::move(unkeyed.GenerateKeys()),
                      std::move(vec_copied), unkeyed.v_label_,
                      std::move(cols_copied));
}
};

*/
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
  using builder_t = KeyedRowVertexSetBuilder<LabelT, VID_T, VID_T, T...>;
};

// group by the vertex set itself
template <typename LabelT, typename VID_T>
struct KeyedT<RowVertexSet<LabelT, VID_T, grape::EmptyType>,
              PropertySelector<grape::EmptyType>> {
  using keyed_set_t = KeyedRowVertexSet<LabelT, VID_T, VID_T, grape::EmptyType>;
  // // The builder type.
  using builder_t =
      KeyedRowVertexSetBuilder<LabelT, VID_T, VID_T, grape::EmptyType>;
};

// group by the vertex set' property
template <typename LabelT, typename VID_T, typename... T, typename PropT>
struct KeyedT<RowVertexSet<LabelT, VID_T, T...>, PropertySelector<PropT>> {
  using keyed_set_t = Collection<PropT>;
  // // The builder type.
  using builder_t = KeyedCollectionBuilder<PropT>;
};

// key on a keyed row vertex get us a unkeyed set.
template <typename LabelT, typename KEY_T, typename VID_T, typename... SET_T>
struct KeyedT<KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, SET_T...>,
              PropertySelector<grape::EmptyType>> {
  using keyed_set_t = KeyedRowVertexSetImpl<LabelT, VID_T, SET_T...>;
  // // The builder type.
  using builder_t = KeyedRowVertexSetBuilder<LabelT, VID_T, SET_T...>;
};

// group by vertex set' id, for generate vertex set.
template <typename VID_T, typename LabelT, size_t N>
struct KeyedT<GeneralVertexSet<VID_T, LabelT, N>,
              PropertySelector<grape::EmptyType>> {
  using keyed_set_t = KeyedRowVertexSet<LabelT, VID_T, VID_T, grape::EmptyType>;
  // // The builder type.
  using builder_t =
      KeyedRowVertexSetBuilder<LabelT, VID_T, VID_T, grape::EmptyType>;
};

template <typename T>
struct KeyedT<Collection<T>, PropertySelector<grape::EmptyType>> {
  using keyed_set_t = Collection<T>;
  // // The builder type.
  using builder_t = KeyedCollectionBuilder<T>;
};

// when keyed with aggregation function, (which we currently only support
// collection)

/// @brief Helper to get keyed set type with aggregation fnc
/// @tparam T
/// @tparam ValueT Keyed prop type
template <typename GI, typename T, AggFunc agg_func, typename Props,
          typename Tags>
struct KeyedAggT;

template <typename GI, typename LabelT, typename VID_T, typename... T,
          typename PropT, int tag_id>
struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::COUNT,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const RowVertexSet<LabelT, VID_T, T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selector) {
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
  using aggregate_res_builder_t = DistinctCountBuilder<1, tag_id, VID_T>;

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
  // build a counter array.
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
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
  using aggregate_res_builder_t = DistinctCountBuilder<2, tag_id, VID_T>;

  static aggregate_res_builder_t create_agg_builder(
      const TwoLabelVertexSet<VID_T, LabelT, T...>& set, const GI& graph,
      std::tuple<PropertySelector<grape::EmptyType>>& selectors) {
    return aggregate_res_builder_t(set.GetBitset(), set.GetVertices());
  }
};

// general vertex set to_count
template <typename GI, typename VID_T, typename LabelT, size_t N,
          typename PropT, int tag_id>
struct KeyedAggT<GI, GeneralVertexSet<VID_T, LabelT, N>, AggFunc::COUNT,
                 std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
  using agg_res_t = Collection<size_t>;
  // build a counter array.
  using aggregate_res_builder_t = CountBuilder<tag_id>;

  static aggregate_res_builder_t create_agg_builder(
      const GeneralVertexSet<VID_T, LabelT, N>& set, const GI& graph,
      std::tuple<PropertySelector<PropT>>& selectors) {
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

// // get max of vertex set's property
// template <typename GI, typename LabelT, typename VID_T, typename... T,
//           typename PropT, int tag_id>
// struct KeyedAggT<GI, RowVertexSet<LabelT, VID_T, T...>, AggFunc::MAX,
//                  std::tuple<PropT>, std::integer_sequence<int32_t, tag_id>> {
//   using agg_res_t = Collection<PropT>;
//   using aggregate_res_builder_t = MaxBuilder<GI, PropT, tag_id>;

//   static aggregate_res_builder_t create_agg_builder(
//        const RowVertexSet<LabelT, VID_T, T...>& set,
//       const GI& graph, PropNameArray<PropT>& prop_names) {
//     return aggregate_res_builder_t(time_stamp, set, graph, prop_names);
//   }
// };

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

template <typename VID_T, typename LabelT, size_t N, typename... EDATA_T,
          typename ELE, typename DATA>
static inline auto insert_into_builder_v2_impl(
    FlatEdgeSetBuilder<VID_T, LabelT, N, EDATA_T...>& builder, const ELE& ele,
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

}  // namespace gs

#endif  // GRAPHSCOPE_ENGINE_KEYED_UTILS_H_
