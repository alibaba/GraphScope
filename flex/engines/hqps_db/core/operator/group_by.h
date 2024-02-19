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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_GROUP_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_GROUP_H_

#include <tuple>
#include <unordered_map>
#include <vector>

#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/keyed.h"
#include "flex/engines/hqps_db/core/utils/props.h"
#include "flex/engines/hqps_db/structures/collection.h"

namespace gs {

// For each aggregator, return the type of applying aggregate on the desired col.
// with possible aggregate func.

template <typename CTX_T, typename GROUP_KEY>
struct CommonBuilderT;

template <typename CTX_T, int col_id>
struct CommonBuilderT<CTX_T, GroupKey<col_id, grape::EmptyType>> {
  using set_t = std::remove_const_t<std::remove_reference_t<decltype(
      std::declval<CTX_T>().template GetNode<col_id>())>>;
  using builder_t = typename set_t::builder_t;
  using result_t = typename builder_t::result_t;
  using result_ele_t = typename result_t::element_type;
};

template <typename CTX_T, int col_id, typename T>
struct CommonBuilderT<CTX_T, GroupKey<col_id, T>> {
  using set_t = std::remove_const_t<std::remove_reference_t<decltype(
      std::declval<CTX_T>().template GetNode<col_id>())>>;
  using builder_t = CollectionBuilder<T>;
  using result_t = typename builder_t::result_t;
  using result_ele_t = typename result_t::element_type;
};

template <typename CTX_T, typename GROUP_KEY>
struct GroupKeyResT;

template <typename CTX_T, int col_id, typename T>
struct GroupKeyResT<CTX_T, GroupKey<col_id, T>> {
  using set_t = std::remove_const_t<std::remove_reference_t<decltype(
      std::declval<CTX_T>().template GetNode<col_id>())>>;
  using result_t = typename KeyedT<set_t, PropertySelector<T>>::keyed_set_t;
};

template <typename CTX_T, typename AGG_T, typename Enable = void>
struct GroupValueResT;

// The SET_T could b a single set or a tuple of sets.
template <typename SET_T, AggFunc agg_func, typename SELECTOR_TUPLE>
struct GroupValueResTImpl;

// Specialize for single set
template <typename CTX_T, AggFunc agg_func, typename... SELECTOR, int... Is>
struct GroupValueResT<CTX_T,
                      AggregateProp<agg_func, std::tuple<SELECTOR...>,
                                    std::integer_sequence<int, Is...>>,
                      typename std::enable_if<(sizeof...(Is) == 1)>::type> {
  using old_set_t = std::remove_const_t<std::remove_reference_t<decltype(
      std::declval<CTX_T>().template GetNode<FirstElement<Is...>::value>())>>;
  using result_t =
      typename GroupValueResTImpl<old_set_t, agg_func,
                                  std::tuple<SELECTOR...>>::result_t;
};

// Specialize for multiple sets
template <typename CTX_T, AggFunc agg_func, typename... SELECTOR, int... Is>
struct GroupValueResT<CTX_T,
                      AggregateProp<agg_func, std::tuple<SELECTOR...>,
                                    std::integer_sequence<int, Is...>>,
                      typename std::enable_if<(sizeof...(Is) > 1)>::type> {
  using old_set_tuple_t =
      std::tuple<std::remove_const_t<std::remove_reference_t<decltype(
          std::declval<CTX_T>().template GetNode<Is>())>>...>;
  using result_t =
      typename GroupValueResTImpl<old_set_tuple_t, agg_func,
                                  std::tuple<SELECTOR...>>::result_t;
};

// specialization for count for single tag
// TODO: count for pairs.
template <typename SET_T>
struct GroupValueResTImpl<SET_T, AggFunc::COUNT,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  using result_t = Collection<size_t>;
};

template <typename SET_T>
struct GroupValueResTImpl<SET_T, AggFunc::COUNT_DISTINCT,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  using result_t = Collection<size_t>;
};

// PropSelectorTuple doesn't effect the result type.
template <typename SET_TUPLE_T, typename PropSelectorTuple>
struct GroupValueResTImpl<SET_TUPLE_T, AggFunc::COUNT_DISTINCT,
                          PropSelectorTuple> {
  using result_t = Collection<size_t>;
};

template <typename SET_TUPLE_T, typename PropSelectorTuple>
struct GroupValueResTImpl<SET_TUPLE_T, AggFunc::COUNT, PropSelectorTuple> {
  using result_t = Collection<size_t>;
};

template <typename T>
struct GroupValueResTImpl<Collection<T>, AggFunc::SUM,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  using result_t = Collection<T>;
};

// specialization for to_set
// TODO: to set for pairs.
template <typename T>
struct GroupValueResTImpl<Collection<T>, AggFunc::TO_SET,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  using result_t = Collection<std::vector<T>>;
};

template <typename LabelT, typename VID_T, typename... SET_T, typename PropT>
struct GroupValueResTImpl<RowVertexSet<LabelT, VID_T, SET_T...>,
                          AggFunc::TO_SET,
                          std::tuple<PropertySelector<PropT>>> {
  using result_t = Collection<std::vector<PropT>>;
};

// specialization for to_list
// TODO: to set for pairs.
template <typename T>
struct GroupValueResTImpl<Collection<T>, AggFunc::TO_LIST,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  using result_t = Collection<std::vector<T>>;
};

// get the vertex's certain properties as list
template <typename LabelT, typename VID_T, typename... SET_T, typename PropT>
struct GroupValueResTImpl<RowVertexSet<LabelT, VID_T, SET_T...>,
                          AggFunc::TO_LIST,
                          std::tuple<PropertySelector<PropT>>> {
  // using old_set_t = std::remove_const_t<std::remove_reference_t<decltype(
  //     std::declval<CTX_T>().template GetNode<Is>())>>;
  using result_t = Collection<std::vector<PropT>>;
};

// get min value
template <typename T>
struct GroupValueResTImpl<Collection<T>, AggFunc::MIN,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  using result_t = Collection<T>;
};

// support get max of vertexset's id
template <typename LabelT, typename VID_T, typename... SET_T, typename T>
struct GroupValueResTImpl<RowVertexSet<LabelT, VID_T, SET_T...>, AggFunc::MAX,
                          std::tuple<PropertySelector<T>>> {
  using result_t = Collection<T>;
};

// support get first from vertexset
template <typename LabelT, typename VID_T, typename... SET_T, typename T>
struct GroupValueResTImpl<RowVertexSet<LabelT, VID_T, SET_T...>, AggFunc::FIRST,
                          std::tuple<PropertySelector<T>>> {
  // the old_set_t is vertex_set or collection
  using result_t =
      typename AggFirst<RowVertexSet<LabelT, VID_T, SET_T...>>::result_t;
};

// support get first from two label vertex set
template <typename VID_T, typename LabelT, typename... SET_T>
struct GroupValueResTImpl<TwoLabelVertexSet<VID_T, LabelT, SET_T...>,
                          AggFunc::FIRST,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  // the old_set_t is vertex_set or collection
  using result_t =
      typename AggFirst<TwoLabelVertexSet<VID_T, LabelT, SET_T...>>::result_t;
};

// get first from collection
template <typename T>
struct GroupValueResTImpl<Collection<T>, AggFunc::FIRST,
                          std::tuple<PropertySelector<grape::EmptyType>>> {
  // the old_set_t is vertex_set or collection
  using result_t = typename AggFirst<Collection<T>>::result_t;
};

template <typename Head, int new_head_tag, int base_tag, typename PREV>
struct UnWrapTuple;

template <typename Head, int new_head_tag, int base_tag, typename... T>
struct UnWrapTuple<Head, new_head_tag, base_tag, std::tuple<T...>> {
  using context_t = Context<Head, new_head_tag, base_tag, T...>;
};

template <int new_head_tag, int base_tag, typename... Nodes>
struct Rearrange {
  using head_t =
      std::tuple_element_t<sizeof...(Nodes) - 1, std::tuple<Nodes...>>;
  using prev_t =
      typename first_n<sizeof...(Nodes) - 1, std::tuple<Nodes...>>::type;
  using context_t =
      typename UnWrapTuple<head_t, new_head_tag, base_tag, prev_t>::context_t;
};

// only two nodes
// template <int new_head_tag, int base_tag, typename First, typename Node>
// struct Rearrange<new_head_tag, base_tag, First, Node> {
//   using context_t = Context<Node, new_head_tag, base_tag, First>;
// };

// only one nodes
template <int new_head_tag, int base_tag, typename First>
struct Rearrange<new_head_tag, base_tag, First> {
  using context_t = Context<First, new_head_tag, base_tag, grape::EmptyType>;
};

template <typename CTX_T, typename GROUP_KEYs, typename AGG_FUNCs>
struct GroupResT;

// We will return a brand new context.

// after groupby, we will get a brand new context, and the tag_ids will start
// from 0.
template <typename CTX_T, typename GROUP_KEY, typename... AGG_T>
struct GroupResT<CTX_T, std::tuple<GROUP_KEY>, std::tuple<AGG_T...>> {
  static constexpr int new_cur_alias = +sizeof...(AGG_T);
  // result ctx type
  using result_t = typename Rearrange<
      new_cur_alias, 0, typename GroupKeyResT<CTX_T, GROUP_KEY>::result_t,
      typename GroupValueResT<CTX_T, AGG_T>::result_t...>::context_t;
};

// keyed by two sets
template <typename CTX_T, typename... GROUP_KEY, typename... AGG_T>
struct GroupResT<CTX_T, std::tuple<GROUP_KEY...>, std::tuple<AGG_T...>> {
  static constexpr int new_cur_alias =
      sizeof...(GROUP_KEY) + sizeof...(AGG_T) - 1;
  // result ctx type
  using result_t = typename Rearrange<
      new_cur_alias, 0, typename CommonBuilderT<CTX_T, GROUP_KEY>::result_t...,
      typename GroupValueResT<CTX_T, AGG_T>::result_t...>::context_t;
};

template <typename CTX_T, typename GROUP_OPT>
struct FoldResT;

// We will return a brand new context.
template <typename CTX_T, typename... AGG_T>
struct FoldResT<CTX_T, std::tuple<AGG_T...>> {
  // take the largest alias in current context as base_tag.
  static constexpr int base_tag = CTX_T::max_tag_id + 1;
  static constexpr int new_head_tag = base_tag + sizeof...(AGG_T) - 1;

  // result ctx type
  using result_t = typename Rearrange<
      new_head_tag, base_tag,
      typename GroupValueResT<CTX_T, AGG_T>::result_t...>::context_t;
};

template <typename GRAPH_INTERFACE>
class GroupByOp {
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;

 public:
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... FOLD_OPT,
            typename RES_T = typename FoldResT<
                Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                std::tuple<FOLD_OPT...>>::result_t>
  static RES_T GroupByWithoutKeyImpl(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<FOLD_OPT...>&& group_opt) {
    VLOG(10) << "new result_t, base tag: " << RES_T::base_tag_id;
    // Currently we only support to to_count;
    using agg_tuple_t = std::tuple<FOLD_OPT...>;

    static constexpr size_t agg_num = std::tuple_size_v<agg_tuple_t>;
    static constexpr size_t grouped_value_num = std::tuple_size_v<agg_tuple_t>;
    // the result context must be one-to-one mapping.

    int start_tag = 0;
    VLOG(10) << "start tag: " << start_tag;
    auto& agg_tuple = group_opt;

    auto value_set_builder_tuple = create_keyed_value_set_builder_tuple(
        graph, ctx, agg_tuple, std::make_index_sequence<grouped_value_num>());
    VLOG(10) << "Create value set builders";

    // if ctx has only element, and is COUNT, just return the size;
    if constexpr (agg_num == 1 &&
                  std::is_same_v<
                      std::tuple_element_t<0, std::tuple<FOLD_OPT...>>,
                      AggregateProp<
                          AggFunc::COUNT,
                          std::tuple<PropertySelector<grape::EmptyType>>,
                          std::integer_sequence<int32_t, 0>>>) {
      auto& builder = std::get<0>(value_set_builder_tuple);
      auto size = ctx.GetHead().Size();
      std::tuple<std::tuple<grape::EmptyType>> empty_tuple;
      for (size_t i = 0; i < size; ++i) {
        builder.insert(0, empty_tuple, empty_tuple);
      }
    } else {
      for (auto iter : ctx) {
        auto ele_tuple = iter.GetAllIndexElement();
        auto data_tuple = iter.GetAllData();
        size_t start_tag_ind = 0;

        // indicate at which index the start_tag element is in.
        insert_to_value_set_builder(value_set_builder_tuple, ele_tuple,
                                    data_tuple, start_tag_ind);
      }
    }
    auto value_set_built =
        build_value_set_tuple(std::move(value_set_builder_tuple),
                              std::make_index_sequence<grouped_value_num>());
    return RES_T(std::move(std::get<0>(value_set_built)),
                 ctx.get_sub_task_start_tag());

    // // create offset array with one-one mapping.
    // if (grouped_value_num == 1) {
    // } else {
    //   auto offset_vec = make_offset_vector(
    //       grouped_value_num - 1, std::get<0>(value_set_built).size() + 1);
    //   VLOG(10) << "after group by, the set size: " << keyed_set_built.Size();
    //   VLOG(10) << "offset vec: " << offset_vec.size();
    //   VLOG(10) << "," << offset_vec[0].size();

    //   RES_T res(std::move(std::get<grouped_value_num - 1>(value_set_built)),
    //             std::move(gs::tuple_slice<0, grouped_value_num - 1>(
    //                 std::move(value_set_built))),
    //             std::move(offset_vec));
    //   return res;
    // }
  }

  // group by only one key_alias
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename GROUP_KEY, typename... AGG_T,
            typename RES_T = typename GroupResT<
                Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                std::tuple<GROUP_KEY>, std::tuple<AGG_T...>>::result_t>
  static RES_T GroupByImpl(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<GROUP_KEY>&& group_keys, std::tuple<AGG_T...>&& agg_tuple) {
    VLOG(10) << "new result_t, base tag: " << RES_T::base_tag_id;
    // Currently we only support to to_count;
    using agg_tuple_t = std::tuple<AGG_T...>;
    using key_alias_t = typename GROUP_KEY::selector_t;
    static constexpr size_t grouped_value_num = std::tuple_size_v<agg_tuple_t>;
    static constexpr int keyed_tag_id = GROUP_KEY::col_id;
    // the result context must be one-to-one mapping.

    auto& old_key_set = gs::Get<keyed_tag_id>(ctx);
    using old_key_set_t = typename std::remove_const_t<
        std::remove_reference_t<decltype(old_key_set)>>;
    using keyed_set_builder_t =
        typename KeyedT<old_key_set_t, key_alias_t>::keyed_builder_t;

    // create a keyed set from the old key set.
    keyed_set_builder_t keyed_set_builder =
        KeyedT<old_key_set_t, key_alias_t>::create_keyed_builder(
            old_key_set, std::get<0>(group_keys).selector_);

    // VLOG(10) << "Create keyed set builder";
    auto value_set_builder_tuple = create_keyed_value_set_builder_tuple(
        graph, ctx, agg_tuple, std::make_index_sequence<grouped_value_num>());

    // if group_key use property, we need property getter
    // else we just insert into key_set
    if constexpr (group_key_on_property<key_alias_t>::value) {
      auto named_property = create_prop_desc_from_selector<GROUP_KEY::col_id>(
          std::get<0>(group_keys).selector_);
      auto prop_getter =
          create_prop_getter_from_prop_desc(graph, ctx, named_property);
      for (auto iter : ctx) {
        auto ele_tuple = iter.GetAllIndexElement();
        auto data_tuple = iter.GetAllData();

        auto key_ele = gs::get_from_tuple<GROUP_KEY::col_id>(ele_tuple);
        size_t ind = insert_to_keyed_set_with_prop_getter(keyed_set_builder,
                                                          prop_getter, key_ele);

        insert_to_value_set_builder(value_set_builder_tuple, ele_tuple,
                                    data_tuple, ind);
      }
    } else {
      for (auto iter : ctx) {
        auto ele_tuple = iter.GetAllIndexElement();
        auto data_tuple = iter.GetAllData();

        auto key_ele = gs::get_from_tuple<key_alias_t::tag_id>(ele_tuple);
        auto data_ele = gs::get_from_tuple<key_alias_t::tag_id>(data_tuple);
        size_t ind = insert_to_keyed_set(keyed_set_builder, key_ele, data_ele);
        insert_to_value_set_builder(value_set_builder_tuple, ele_tuple,
                                    data_tuple, ind);
      }
    }

    auto keyed_set_built = keyed_set_builder.Build();

    auto value_set_built =
        build_value_set_tuple(std::move(value_set_builder_tuple),
                              std::make_index_sequence<grouped_value_num>());

    // create offset array with one-one mapping.
    auto offset_vec =
        make_offset_vector(grouped_value_num, keyed_set_built.Size());

    auto new_tuple = std::tuple_cat(std::move(std::make_tuple(keyed_set_built)),
                                    std::move(value_set_built));

    RES_T res(
        std::move(std::get<grouped_value_num>(new_tuple)),
        std::move(gs::tuple_slice<0, grouped_value_num>(std::move(new_tuple))),
        std::move(offset_vec));

    return res;
  }

  // group by multiple key_alias
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... KEY_ALIAS, typename... AGG,
            typename RES_T = typename GroupResT<
                Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                std::tuple<KEY_ALIAS...>, std::tuple<AGG...>>::result_t>
  static RES_T GroupByImpl(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<KEY_ALIAS...> group_keys, std::tuple<AGG...>&& aggs) {
    // Currently we only support to to_count;
    using agg_tuple_t = std::tuple<AGG...>;
    using alias_tuple_t = std::tuple<KEY_ALIAS...>;

    static constexpr size_t grouped_value_num = std::tuple_size_v<agg_tuple_t>;
    static constexpr size_t group_key_num = std::tuple_size_v<alias_tuple_t>;

    // the result context must be one-to-one mapping.
    auto key_set_ref_tuple = std::tie(gs::Get<KEY_ALIAS::col_id>(ctx)...);

    auto value_set_builder_tuple = create_keyed_value_set_builder_tuple(
        graph, ctx, aggs, std::make_index_sequence<grouped_value_num>());
    VLOG(10) << "Create value set builders";

    // create keyed_set_builder_tuple
    auto keyed_set_builder_tuple = create_unkeyed_set_builder_tuple(
        graph, ctx.GetPrevCols(), ctx.GetHead(), group_keys,
        std::make_index_sequence<group_key_num>());

    // the type of selected tuple.
    using con_key_ele_t = std::tuple<typename CommonBuilderT<
        Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
        KEY_ALIAS>::result_ele_t...>;
    std::unordered_map<con_key_ele_t, int, boost::hash<con_key_ele_t>>
        key_tuple_set;

    auto named_properties = create_prop_descs_from_group_keys(group_keys);
    auto prop_getters =
        create_prop_getters_from_prop_desc(graph, ctx, named_properties);
    size_t cur_ind = 0;
    for (auto iter : ctx) {
      auto ele_tuple = iter.GetAllElement();
      auto ind_ele_tuple = iter.GetAllIndexElement();
      auto data_tuple = iter.GetAllData();
      auto key_data_tuple =
          std::make_tuple(gs::get_from_tuple<KEY_ALIAS::col_id>(data_tuple)...);
      auto key_tuple = create_key_tuple_ele(ele_tuple, prop_getters);
      size_t ind = 0;
      if (key_tuple_set.find(key_tuple) != key_tuple_set.end()) {
        // already exist
        ind = key_tuple_set[key_tuple];
      } else {
        // not exist
        ind = cur_ind++;
        insert_into_comment_builder_tuple<0>(
            keyed_set_builder_tuple, group_keys, key_tuple, key_data_tuple);
        key_tuple_set[key_tuple] = ind;
      }
      // CHECK insert key.
      insert_to_value_set_builder(value_set_builder_tuple, ind_ele_tuple,
                                  data_tuple, ind);
    }

    // get the result tuple of applying build on keyed_set_builder_tuple.
    auto key_built_tuple = std::apply(
        [](auto&&... args) { return std::make_tuple(args.Build()...); },
        std::move(keyed_set_builder_tuple));

    auto value_set_built =
        build_value_set_tuple(std::move(value_set_builder_tuple),
                              std::make_index_sequence<grouped_value_num>());
    // create offset array with one-one mapping.
    auto offset_vec = make_offset_vector(grouped_value_num + group_key_num - 1,
                                         std::get<0>(key_built_tuple).Size());

    auto new_tuple =
        std::tuple_cat(std::move(key_built_tuple), std::move(value_set_built));

    RES_T res(
        std::move(std::get<grouped_value_num + group_key_num - 1>(new_tuple)),
        std::move(gs::tuple_slice<0, grouped_value_num + group_key_num - 1>(
            std::move(new_tuple))),
        std::move(offset_vec));

    return res;
  }

  // ind is the index of the key in the key set
  template <size_t Is = 0, typename ele_tuple_t, typename data_tuple_t,
            typename... SET_T>
  static void insert_to_value_set_builder(
      std::tuple<SET_T...>& value_set_builder, const ele_tuple_t& ele_tuple,
      const data_tuple_t& data_tuple, size_t ind) {
    std::get<Is>(value_set_builder).insert(ind, ele_tuple, data_tuple);
    if constexpr (Is + 1 < sizeof...(SET_T)) {
      insert_to_value_set_builder<Is + 1>(value_set_builder, ele_tuple,
                                          data_tuple, ind);
    }
  }

  template <typename... BUILDER_T, size_t... Is>
  static auto build_value_set_tuple(std::tuple<BUILDER_T...>&& builder_tuple,
                                    std::index_sequence<Is...>) {
    return std::make_tuple(std::get<Is>(builder_tuple).Build()...);
  }

  // Create value set builder from previous context.
  template <typename... CTX_PREV, typename HEAD_T, int cur_alias, int base_tag,
            typename... AGG_T, size_t... Is>
  static auto create_keyed_value_set_builder_tuple(
      const GRAPH_INTERFACE& graph,
      const Context<HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
      std::tuple<AGG_T...>& agg_tuple, std::index_sequence<Is...>) {
    return std::make_tuple(create_keyed_value_set_builder(
        graph, ctx.GetPrevCols(), ctx.GetHead(), std::get<Is>(agg_tuple))...);
  }

  // create for ctx with only one column
  template <typename HEAD_T, int cur_alias, int base_tag, typename... AGG_T,
            size_t... Is>
  static auto create_keyed_value_set_builder_tuple(
      const GRAPH_INTERFACE& graph,
      Context<HEAD_T, cur_alias, base_tag, grape::EmptyType>& ctx,
      std::tuple<AGG_T...>& agg_tuple, std::index_sequence<Is...>) {
    return std::make_tuple(create_keyed_value_set_builder(
        graph, ctx.GetHead(), std::get<Is>(agg_tuple))...);
  }

  // create tuple of keyed builders.
  template <typename... SET_T, typename HEAD_T, typename... KEY_ALIAS,
            size_t... Is>
  static auto create_unkeyed_set_builder_tuple(
      const GRAPH_INTERFACE& graph, const std::tuple<SET_T...>& prev,
      const HEAD_T& head, std::tuple<KEY_ALIAS...>& group_keys,
      std::index_sequence<Is...>) {
    return std::make_tuple(create_unkeyed_set_builder(
        graph, prev, head, std::get<Is>(group_keys))...);
  }

  template <typename... SET_T, typename HEAD_T, AggFunc _agg_func, typename T,
            int tag_id>
  static auto create_keyed_value_set_builder_single_tag(
      const GRAPH_INTERFACE& graph, const std::tuple<SET_T...>& tuple,
      const HEAD_T& head,
      AggregateProp<_agg_func, std::tuple<PropertySelector<T>>,
                    std::integer_sequence<int32_t, tag_id>>& agg) {
    if constexpr (tag_id < sizeof...(SET_T)) {
      auto old_set = gs::get_from_tuple<tag_id>(tuple);
      using old_set_t = typename std::remove_const_t<
          std::remove_reference_t<decltype(old_set)>>;

      return KeyedAggT<GRAPH_INTERFACE, old_set_t, _agg_func, std::tuple<T>,
                       std::integer_sequence<int32_t, tag_id>>::
          create_agg_builder(old_set, graph, agg.selectors_);
    } else {
      return KeyedAggT<GRAPH_INTERFACE, HEAD_T, _agg_func, std::tuple<T>,
                       std::integer_sequence<int32_t, tag_id>>::
          create_agg_builder(head, graph, agg.selectors_);
    }
  }

  // For aggregate on multiple tags, we currently only support count distinct
  // and count.
  template <typename... SET_T, typename HEAD_T, AggFunc _agg_func,
            typename PROP_TUPLE_T, int... tag_id>
  static auto create_keyed_value_set_builder_multi_tag(
      const GRAPH_INTERFACE& graph, const std::tuple<SET_T...>& tuple,
      const HEAD_T& head,
      AggregateProp<_agg_func, PROP_TUPLE_T,
                    std::integer_sequence<int32_t, tag_id...>>& agg) {
    // create const ref tuple from tuple and head using std::cref
    // construct a const ref tuple from tuple
    auto const_ref_tuple = make_tuple_of_const_refs(tuple);

    auto old_set = std::tuple_cat(const_ref_tuple, std::tie(head));
    // get the tuple from old_set, with tag_ids
    auto old_set_tuple = std::tuple{gs::get_from_tuple<tag_id>(old_set)...};

    return KeyedAggMultiColT<GRAPH_INTERFACE, decltype(old_set_tuple),
                             _agg_func, PROP_TUPLE_T,
                             std::integer_sequence<int32_t, tag_id...>>::
        create_agg_builder(old_set_tuple, graph, agg.selectors_);
  }

  template <typename... SET_T, typename HEAD_T, AggFunc _agg_func,
            typename PROP_SELECTOR_TUPLE, int... tag_ids>
  static auto create_keyed_value_set_builder(
      const GRAPH_INTERFACE& graph, const std::tuple<SET_T...>& tuple,
      const HEAD_T& head,
      AggregateProp<_agg_func, PROP_SELECTOR_TUPLE,
                    std::integer_sequence<int32_t, tag_ids...>>& agg) {
    if constexpr (sizeof...(tag_ids) == 1) {
      return create_keyed_value_set_builder_single_tag(graph, tuple, head, agg);
    } else {
      return create_keyed_value_set_builder_multi_tag(graph, tuple, head, agg);
    }
  }

  // create builder for single key_alias
  template <typename... SET_T, typename HEAD_T, int col_id, typename KEY_PROP>
  static auto create_unkeyed_set_builder(
      const GRAPH_INTERFACE& graph, const std::tuple<SET_T...>& tuple,
      const HEAD_T& head, const GroupKey<col_id, KEY_PROP>& key_alias) {
    if constexpr (col_id < sizeof...(SET_T)) {
      auto old_set = gs::get_from_tuple<col_id>(tuple);
      using old_set_t = typename std::remove_const_t<
          std::remove_reference_t<decltype(old_set)>>;

      return KeyedT<old_set_t, PropertySelector<KEY_PROP>>::
          create_unkeyed_builder(old_set, key_alias.selector_);
    } else {
      return KeyedT<HEAD_T, PropertySelector<KEY_PROP>>::create_unkeyed_builder(
          head, key_alias.selector_);
    }
  }

  template <typename... SET_T, typename HEAD_T, AggFunc _agg_func, typename T,
            int tag_id>
  static auto create_keyed_value_set_builder(
      const GRAPH_INTERFACE& graph, const HEAD_T& head,
      AggregateProp<_agg_func, std::tuple<PropertySelector<T>>,
                    std::integer_sequence<int32_t, tag_id>>& agg) {
    static_assert(tag_id == 0 || tag_id == -1);
    return KeyedAggT<GRAPH_INTERFACE, HEAD_T, _agg_func, std::tuple<T>,
                     std::integer_sequence<int32_t, tag_id>>::
        create_agg_builder(head, graph, agg.selectors_);
  }

  // insert_to_key_set with respect to property type
  template <typename BuilderT, typename PROP_GETTER, typename ELE>
  static inline auto insert_to_keyed_set_with_prop_getter(
      BuilderT& builder, const PROP_GETTER& prop_getter, const ELE& ele) {
    return builder.insert(prop_getter.get_view(ele));
  }

  // insert_into_bulder_v2_impl
  template <typename BuilderT, typename ELE, typename DATA,
            typename std::enable_if<std::is_same_v<
                DATA, std::tuple<grape::EmptyType>>>::type* = nullptr>
  static inline auto insert_to_keyed_set(BuilderT& builder, const ELE& ele,
                                         const DATA& data) {
    return builder.insert(ele);
  }

  // insert_into_bulder_v2_impl
  template <typename BuilderT, typename ELE, typename DATA,
            typename std::enable_if<!std::is_same_v<
                DATA, std::tuple<grape::EmptyType>>>::type* = nullptr>
  static inline auto insert_to_keyed_set(BuilderT& builder, const ELE& ele,
                                         const DATA& data) {
    return builder.insert(ele, data);
  }

  template <typename... ELE_T, typename... PROP_GETTER>
  static inline auto create_key_tuple_ele(
      const std::tuple<ELE_T...>& eles,
      const std::tuple<PROP_GETTER...>& getters) {
    return create_key_tuple_ele_impl(eles, getters,
                                     std::index_sequence_for<PROP_GETTER...>{});
  }

  template <typename... ELE_T, typename... PROP_GETTER, size_t... Is>
  static inline auto create_key_tuple_ele_impl(
      const std::tuple<ELE_T...>& eles,
      const std::tuple<PROP_GETTER...>& getters, std::index_sequence<Is...>) {
    return std::make_tuple(std::get<Is>(getters).get_from_all_element(eles)...);
  }

  // insert into common builders.
  template <size_t Ind, typename... BuilderT, typename... GROUP_KEY,
            typename... ELE, typename... DATA>
  static inline void insert_into_comment_builder_tuple(
      std::tuple<BuilderT...>& builders, const std::tuple<GROUP_KEY...>& keys,
      const std::tuple<ELE...>& eles, const std::tuple<DATA...>& data) {
    auto& builder = std::get<Ind>(builders);
    auto& group_key = std::get<Ind>(keys);
    auto& ele = std::get<Ind>(eles);
    auto& d = std::get<Ind>(data);
    insert_to_keyed_set_with_group_key(builder, group_key, ele, d);

    if constexpr (Ind + 1 < sizeof...(BuilderT)) {
      insert_into_comment_builder_tuple<Ind + 1>(builders, keys, eles, data);
    }
  }

  template <typename BuilderT, int col_id, typename T, typename ELE,
            typename DATA>
  static inline void insert_to_keyed_set_with_group_key(
      BuilderT& builder, const GroupKey<col_id, T>& group_key, const ELE& ele,
      const DATA& data) {
    builder.Insert(ele);
  }

  template <typename BuilderT, int col_id, typename ELE, typename DATA>
  static inline void insert_to_keyed_set_with_group_key(
      BuilderT& builder, const GroupKey<col_id, grape::EmptyType>& group_key,
      const ELE& ele, const DATA& data) {
    insert_into_builder_v2_impl(builder, ele, data);
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_GROUP_H_
