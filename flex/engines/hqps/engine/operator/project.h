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

#ifndef GRAPHSCOPE_OPERATOR_PROJECT_H_
#define GRAPHSCOPE_OPERATOR_PROJECT_H_

#include <tuple>
#include <unordered_map>
#include <vector>

#include "flex/engines/hqps/ds/collection.h"
#include "flex/engines/hqps/ds/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps/ds/multi_vertex_set/two_label_vertex_set.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/keyed_utils.h"
#include "flex/engines/hqps/engine/params.h"

namespace gs {

template <typename CTX_T, typename KEY_ALIAS>
struct ResultOfContextKeyAlias;

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int tag_id, int res_alias, int... Is>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    KeyAlias<tag_id, res_alias, Is...>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using ctx_node_t = std::remove_reference_t<
      decltype(std::declval<context_t>().template GetNode<tag_id>())>;
  using ctx_node_data_tuple = typename gs::TupleCatT<
      typename ctx_node_t::data_tuple_t,
      std::tuple<typename ctx_node_t::EntityValueType>>::tuple_cat_t;
  using result_t = Collection<
      std::tuple<typename gs::tuple_element<Is, ctx_node_data_tuple>::type...>>;
};

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int tag_id, int res_alias, typename... T>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    AliasTagProp<tag_id, res_alias, T...>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using ctx_node_t = std::remove_reference_t<
      decltype(std::declval<context_t>().template GetNode<tag_id>())>;
  // using ctx_node_data_tuple = typename gs::TupleCatT<
  //     typename ctx_node_t::data_tuple_t,
  //     std::tuple<typename ctx_node_t::EntityValueType>>::tuple_cat_t;
  using result_t = Collection<std::tuple<T...>>;
};

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int res_alias, typename... TAG_PROP>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    MultiKeyAliasProp<res_alias, TAG_PROP...>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using res_tuple_t = gs::tuple_cat_t<typename TAG_PROP::prop_tuple_t...>;
  // using ctx_node_data_tuple = typename gs::TupleCatT<
  //     typename ctx_node_t::data_tuple_t,
  //     std::tuple<typename ctx_node_t::EntityValueType>>::tuple_cat_t;
  using result_t = Collection<res_tuple_t>;
};

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int tag_id, int res_alias>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    ProjectSelf<tag_id, res_alias>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using ctx_node_t = std::remove_reference_t<
      decltype(std::declval<context_t>().template GetNode<tag_id>())>;
  using result_t = ctx_node_t;
};

// infer the output type of project expr by compiler
template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int res_alias, typename RES_T, typename EXPR>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    ProjectExpr<res_alias, RES_T, EXPR>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  // must project to collection
  // using result_type =
  // std::remove_reference_t<decltype(std::declval<EXPR>()())>;
  using result_type = RES_T;
  using result_t = Collection<result_type>;
};

template <int new_head_alias, typename new_head_t, int cur_alias,
          typename old_head_t, int base_tag, typename tuple>
struct ResultContextTWithPrevTuple;

template <int new_head_alias, typename new_head_t, int cur_alias,
          typename old_head_t, int base_tag, typename... T>
struct ResultContextTWithPrevTuple<new_head_alias, new_head_t, cur_alias,
                                   old_head_t, base_tag, std::tuple<T...>> {
  using result_t =
      typename ResultContextT<new_head_alias, new_head_t, cur_alias, old_head_t,
                              base_tag, T...>::result_t;
};

template <bool is_append, typename CTX_T, typename PROJECT_OPT>
struct ProjectResT;

// We will return a brand new context.
// template <typename CTX_HEAD_T, int cur_alias, int base_tag,
//           typename... CTX_PREV, typename KEY_ALIAS_T>
// struct ProjectResT<true, Context<CTX_HEAD_T, cur_alias, base_tag,
// CTX_PREV...>,
//                    ProjectOpt<KEY_ALIAS_T>> {
//   using project_opt_t = ProjectOpt<KEY_ALIAS_T>;
//   using new_head_t = typename ResultOfContextKeyAlias<
//       Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
//       KEY_ALIAS_T>::result_t;
//   static constexpr int new_head_alias = KEY_ALIAS_T::res_alias;
//   using result_t =
//       typename ResultContextT<new_head_alias, new_head_t, cur_alias,
//       CTX_HEAD_T,
//                               base_tag, CTX_PREV...>;
// };

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, typename... KEY_ALIAS_T>
struct ProjectResT<true, Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                   ProjectOpt<KEY_ALIAS_T...>> {
  using old_ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using project_opt_t = ProjectOpt<KEY_ALIAS_T...>;
  static constexpr size_t num_key_alias = sizeof...(KEY_ALIAS_T);
  using last_key_alias_t =
      std::tuple_element_t<num_key_alias - 1, std::tuple<KEY_ALIAS_T...>>;

  using new_head_t =
      typename ResultOfContextKeyAlias<old_ctx_t, last_key_alias_t>::result_t;
  static constexpr int new_head_alias = last_key_alias_t::res_alias;
  using proj_res_tuple_t = std::tuple<
      typename ResultOfContextKeyAlias<old_ctx_t, KEY_ALIAS_T>::result_t...>;
  using first_n_of_key_alias_tuple =
      typename first_n<sizeof...(KEY_ALIAS_T) - 1, proj_res_tuple_t>::type;
  using result_t = typename ResultContextTWithPrevTuple<
      new_head_alias, new_head_t, cur_alias, CTX_HEAD_T, base_tag,
      typename TupleCatT<std::tuple<CTX_PREV...>,
                         first_n_of_key_alias_tuple>::tuple_cat_t>::result_t;
};

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, typename... KEY_ALIAS_T>
struct ProjectResT<false, Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
                   ProjectOpt<KEY_ALIAS_T...>> {
  using old_ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using project_opt_t = ProjectOpt<KEY_ALIAS_T...>;
  static constexpr size_t num_key_alias = sizeof...(KEY_ALIAS_T);
  using last_key_alias_t =
      std::tuple_element_t<num_key_alias - 1, std::tuple<KEY_ALIAS_T...>>;

  using new_head_t =
      typename ResultOfContextKeyAlias<old_ctx_t, last_key_alias_t>::result_t;
  static constexpr int new_head_alias = last_key_alias_t::res_alias;
  using proj_res_tuple_t = std::tuple<
      typename ResultOfContextKeyAlias<old_ctx_t, KEY_ALIAS_T>::result_t...>;
  using first_n_of_key_alias_tuple =
      typename first_n<sizeof...(KEY_ALIAS_T) - 1, proj_res_tuple_t>::type;
  using result_t = typename ResultContextTWithPrevTuple<
      new_head_alias, new_head_t, cur_alias, CTX_HEAD_T, base_tag,
      typename TupleCatT<std::tuple<CTX_PREV...>,
                         first_n_of_key_alias_tuple>::tuple_cat_t>::result_t;
};

template <typename GRAPH_INTERFACE>
class ProjectOp {
 public:
  // specialized to append
  // Project a previous tag and append to traversal.
  template <bool is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename PROJECT_OPT,
            typename std::enable_if<is_append>::type* = nullptr>
  static auto ProjectImpl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PROJECT_OPT&& project_opt) {
    auto node_size = gs::Get<-1>(ctx).Size();
    VLOG(10) << "Current head size: " << node_size;

    std::vector<offset_t> offsets(node_size + 1, 0);
    for (auto i = 1; i <= node_size; ++i) {
      offsets[i] = i;
    }
    // VLOG(10) << "finish set offset: " << gs::to_string(offsets);

    return apply_projects<0>(time_stamp, graph, std::move(ctx),
                             project_opt.key_alias_tuple_, offsets);
  }

  // implementation for project is false, only proj one column
  template <bool is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... PROJ_PROPS,
            typename std::enable_if<!is_append && (sizeof...(PROJ_PROPS) ==
                                                   1)>::type* = nullptr>
  static auto ProjectImpl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      ProjectOpt<PROJ_PROPS...>&& project_opt) {
    auto node_size = gs::Get<-1>(ctx).Size();
    VLOG(10) << "Current head size: " << node_size;

    auto head = apply_single_project(time_stamp, graph, ctx,
                                     std::get<0>(project_opt.key_alias_tuple_));
    using new_head_t =
        std::remove_const_t<std::remove_reference_t<decltype(head)>>;
    using first_alias_t = std::tuple_element_t<0, std::tuple<PROJ_PROPS...>>;
    return Context<new_head_t, first_alias_t::res_alias,
                   first_alias_t::res_alias, grape::EmptyType>(std::move(head));
  }

  // implementation for project is false. project multiple columns
  template <bool is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV, typename... PROJ_PROPS,
            typename std::enable_if<!is_append && (sizeof...(PROJ_PROPS) >
                                                   1)>::type* = nullptr>
  static auto ProjectImpl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      ProjectOpt<PROJ_PROPS...>&& project_opt) {
    static constexpr size_t proj_num = sizeof...(PROJ_PROPS);
    auto node_size = gs::Get<-1>(ctx).Size();
    // VLOG(10) << "Current head size: " << node_size;
    std::vector<offset_t> offset(node_size + 1, 0);
    for (auto i = 1; i <= node_size; ++i) {
      offset[i] = i;
    }
    std::vector<std::vector<offset_t>> offsets;
    offsets.reserve(proj_num - 1);
    for (auto i = 0; i < proj_num - 1; ++i) {
      offsets.push_back(offset);
    }

    // LOG(INFO) << "Projecting columns: " << proj_num;
    auto head = apply_single_project(
        time_stamp, graph, ctx,
        std::get<proj_num - 1>(project_opt.key_alias_tuple_));
    auto prev_tuple = apply_single_project_until<proj_num - 1>(
        time_stamp, graph, ctx, project_opt.key_alias_tuple_,
        std::make_index_sequence<proj_num - 1>{});
    using new_head_t =
        std::remove_const_t<std::remove_reference_t<decltype(head)>>;
    using first_alias_t = std::tuple_element_t<0, std::tuple<PROJ_PROPS...>>;
    using last_alias_t =
        std::tuple_element_t<proj_num - 1, std::tuple<PROJ_PROPS...>>;
    return make_context<first_alias_t::res_alias, last_alias_t::res_alias>(
        std::move(prev_tuple), std::move(head), std::move(offsets));
  }

  template <size_t Is, typename CTX_T, typename... KEY_ALIAS_PROP,
            typename std::enable_if<(Is < sizeof...(KEY_ALIAS_PROP) -
                                              1)>::type* = nullptr>
  static auto apply_projects(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                             CTX_T&& ctx,
                             std::tuple<KEY_ALIAS_PROP...>& key_alias,
                             std::vector<offset_t>& offsets) {
    static constexpr int res_alias_tag =
        std::tuple_element_t<Is, std::tuple<KEY_ALIAS_PROP...>>::res_alias;
    auto new_node =
        apply_single_project(time_stamp, graph, ctx, std::get<Is>(key_alias));
    std::vector<offset_t> res_offsets(offsets);
    auto res = ctx.template AddNode<res_alias_tag>(std::move(new_node),
                                                   std::move(res_offsets));
    return apply_projects<Is + 1>(time_stamp, graph, std::move(res), key_alias,
                                  offsets);
  }

  // For the last element, return the result
  template <size_t Is, typename CTX_T, typename... KEY_ALIAS_PROP,
            typename std::enable_if<(Is == sizeof...(KEY_ALIAS_PROP) -
                                               1)>::type* = nullptr>
  static auto apply_projects(int64_t time_stamp, const GRAPH_INTERFACE& graph,
                             CTX_T&& ctx,
                             std::tuple<KEY_ALIAS_PROP...>& key_alias,
                             std::vector<offset_t>& offsets) {
    static constexpr int res_alias_tag =
        std::tuple_element_t<Is, std::tuple<KEY_ALIAS_PROP...>>::res_alias;
    auto new_node =
        apply_single_project(time_stamp, graph, ctx, std::get<Is>(key_alias));
    std::vector<offset_t> res_offsets(offsets);
    return ctx.template AddNode<res_alias_tag>(std::move(new_node),
                                               std::move(res_offsets));
  }

  // Apply single project on old context's node until the indicated index of
  // project opts
  template <size_t limit, typename CTX_T, typename... PROJ_PROP, size_t... Is>
  static auto apply_single_project_until(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      std::tuple<PROJ_PROP...>& proj_prop_tuple, std::index_sequence<Is...>) {
    static_assert(limit < sizeof...(PROJ_PROP));
    return std::make_tuple(apply_single_project(
        time_stamp, graph, ctx, std::get<Is>(proj_prop_tuple))...);
  }

  // Apply project with multiple key alias prop.
  template <typename CTX_T, int res_alias, typename... TAG_PROP>
  static auto apply_single_project(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      MultiKeyAliasProp<res_alias, TAG_PROP...>& key_alias_prop) {
    // static_assert(res_alias > CTX_T::max_tag_id);
    static constexpr size_t num_tag = sizeof...(TAG_PROP);
    std::array<int, num_tag> tags{TAG_PROP::tag_id...};

    std::vector<std::vector<offset_t>> repeat_array_vec =
        get_repeat_array_vec_for_tags(ctx, tags);
    // Create a empty copy.
    // A col describe what content is used to project
    return apply_single_project_on_multi_tags_impl(
        time_stamp, graph, ctx, key_alias_prop, repeat_array_vec,
        std::make_index_sequence<sizeof...(TAG_PROP)>{});
  }

  template <typename CTX_T, size_t N>
  static auto get_repeat_array_vec_for_tags(
      const CTX_T& ctx, const std::array<int32_t, N>& tags) {
    std::vector<std::vector<offset_t>> res_vec;
    for (auto i = 0; i < N; ++i) {
      int32_t cur_tag = tags[i];
      auto offset_array = ctx.ObtainOffsetFromTag(cur_tag);
      auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
      VLOG(10) << "repeat array: " << gs::to_string(repeat_array);
      res_vec.emplace_back(std::move(repeat_array));
    }
    return res_vec;
  }

  // Apply single project with AliasTagProp.
  template <typename CTX_T, int tag_id, int res_alias, typename... T>
  static auto apply_single_project(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      AliasTagProp<tag_id, res_alias, T...>& key_alias_prop) {
    auto& node = ctx.template GetNode<tag_id>();
    // Create a empty copy.
    // VLOG(10) << "begin obtaining offset from tag";
    auto offset_array = ctx.ObtainOffsetFromTag(tag_id);
    // VLOG(10) << "Obtains offset to head from tag: " << tag_id
    //  << ", size: " << offset_array.size();
    // We need to repeat the selected node with respect to offset_array.
    // Imitate the iteration result.
    // convert offset array to repeat times array
    auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
    // VLOG(10) << "repeat array: " << gs::to_string(repeat_array);
    // check whether we get the right array;
    // auto& head_node = ctx.GetHead();

    // A col describe what content is used to project
    return apply_single_project_impl(time_stamp, graph, node,
                                     key_alias_prop.tag_prop_, repeat_array);
  }

  // Project self.
  template <typename CTX_T, int tag_id, int res_alias, typename... T>
  static auto apply_single_project(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      ProjectSelf<tag_id, res_alias>& key_alias_prop) {
    auto& node = ctx.template GetNode<tag_id>();
    // Create a empty copy.
    auto offset_array = ctx.ObtainOffsetFromTag(tag_id);
    // VLOG(10) << "Obtains offset to head from tag: " << tag_id
    //  << gs::to_string(offset_array);
    // We need to repeat the selected node with respect to offset_array.
    // Imitate the iteration result.
    // convert offset array to repeat times array
    auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
    // VLOG(10) << "repeat array: " << gs::to_string(repeat_array);
    // check whether we get the right array;
    // auto& head_node = ctx.GetHead();

    // A col describe what content is used to project
    // using cur_node_entity_value_t =
    //     typename std::remove_reference_t<decltype(node)>::EntityValueType;
    KeyAlias<tag_id, res_alias, -1> key_alias;
    return node.ProjectWithRepeatArray(std::move(repeat_array), key_alias);
  }

  // Project with expression.
  template <typename CTX_T, int res_alias, typename RES_T, typename EXPR>
  static auto apply_single_project(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      ProjectExpr<res_alias, RES_T, EXPR>& proj_expr) {
    // using result_type =
    //     std::remove_reference_t<decltype(std::declval<EXPR>()())>;
    // using result_type = std::result_of_t<EXPR(void)>;
    // get the return type of EXPR()
    using result_type = RES_T;
    std::vector<result_type> res_vec;
    res_vec.reserve(ctx.GetHead().Size());
    auto expr = proj_expr.expr_;
    auto tag_props = proj_expr.expr_.Properties();
    auto prop_getters =
        create_prop_getters_from_prop_desc(time_stamp, graph, ctx, tag_props);
    LOG(INFO) << "In project with expression, successfully got prop getters";
    for (auto iter : ctx) {
      auto ele_tuple = iter.GetAllElement();
      LOG(INFO) << gs::to_string(ele_tuple);
      res_vec.emplace_back(evaluate_proj_expr(expr, ele_tuple, prop_getters));
    }
    return Collection<result_type>(std::move(res_vec));
  }

  // single label vertex set.
  template <typename LabelT, typename VID_T, typename... SET_T, int _tag_id,
            typename... T>
  static auto apply_single_project_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      RowVertexSetImpl<LabelT, VID_T, SET_T...>& node,
      const TagProp<_tag_id, T...>& tag_prop,
      const std::vector<size_t>& repeat_array) {
    // LOG(INFO) << "[Single project on RowVertexSet:]" << node.GetLabel()
    // << ", prop: " << gs::to_string(tag_prop.prop_names_);
    if constexpr (sizeof...(SET_T) > 0 &&
                  !std::is_same_v<std::tuple_element_t<0, std::tuple<SET_T...>>,
                                  grape::EmptyType>) {
      // LOG(INFO) << ", my props: " << gs::to_string(node.GetPropNames());
    }

    // VLOG(10) << "start fetching properties";
    // Get property from storage.
    auto prop_tuple_vec = graph.template GetVertexPropsFromVid<T...>(
        time_stamp, node.GetLabel(), node.GetVertices(), tag_prop.prop_names_);
    // VLOG(10) << "Finish fetching properties";
    std::vector<std::tuple<T...>> res_prop_vec;
    for (auto i = 0; i < repeat_array.size(); ++i) {
      for (auto j = 0; j < repeat_array[i]; ++j) {
        res_prop_vec.push_back(prop_tuple_vec[i]);
      }
    }
    // check builtin properties.
    // Found if there is any builtin properties need.
    node.fillBuiltinProps(res_prop_vec, tag_prop.prop_names_, repeat_array);
    return Collection<std::tuple<T...>>(std::move(res_prop_vec));
  }

  // single keyed label vertex set.
  template <typename LabelT, typename KEY_T, typename VID_T, typename... SET_T,
            int _tag_id, typename... T>
  static auto apply_single_project_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, SET_T...>& node,
      const TagProp<_tag_id, T...>& tag_prop,
      const std::vector<size_t>& repeat_array) {
    LOG(INFO) << "[Single project on KeyedRowVertexSet:]" << node.GetLabel();
    // VLOG(10) << "start fetching properties";
    // Get property from storage.
    auto prop_tuple_vec = graph.template GetVertexPropsFromVid<T...>(
        time_stamp, node.GetLabel(), node.GetVertices(), tag_prop.prop_names_);
    // VLOG(10) << "Finish fetching properties";
    std::vector<std::tuple<T...>> res_prop_vec;
    for (auto i = 0; i < repeat_array.size(); ++i) {
      for (auto j = 0; j < repeat_array[i]; ++j) {
        res_prop_vec.push_back(prop_tuple_vec[i]);
      }
    }
    // check builtin properties.
    // Found if there is any builtin properties need.
    node.fillBuiltinProps(res_prop_vec, tag_prop.prop_names_, repeat_array);
    return Collection<std::tuple<T...>>(std::move(res_prop_vec));
  }

  // project for two label vertex set.
  template <typename NODE_T, int _tag_id, typename... T,
            typename std::enable_if<
                !NODE_T::is_multi_label && !NODE_T::is_collection &&
                NODE_T::is_vertex_set && NODE_T::is_two_label_set &&
                !NODE_T::is_general_set>::type* = nullptr>
  static auto apply_single_project_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, NODE_T& node,
      const TagProp<_tag_id, T...>& tag_prop,
      const std::vector<size_t>& repeat_array) {
    auto tmp_prop_vec = get_property_tuple_two_label<T...>(
        time_stamp, graph, node, tag_prop.prop_names_);

    // make_repeat;
    size_t sum = 0;
    bool flag = true;
    for (auto i = 0; i < repeat_array.size(); ++i) {
      if (repeat_array[i] != 1) {
        flag = false;
      }
      sum += repeat_array[i];
    }
    if (flag) {
      return Collection<std::tuple<T...>>(std::move(tmp_prop_vec));
    } else {
      std::vector<std::tuple<T...>> res_prop_vec;
      res_prop_vec.reserve(sum);
      for (auto i = 0; i < repeat_array.size(); ++i) {
        for (auto j = 0; j < repeat_array[i]; ++j) {
          res_prop_vec.push_back(tmp_prop_vec[i]);
        }
      }
      return Collection<std::tuple<T...>>(std::move(res_prop_vec));
    }
  }

  // multi label vertex set.
  template <
      typename NODE_T, int _tag_id, typename... T,
      typename std::enable_if<NODE_T::is_multi_label &&
                              !NODE_T::is_collection && NODE_T::is_vertex_set &&
                              !NODE_T::is_general_set>::type* = nullptr>
  static auto apply_single_project_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, NODE_T& multi_set,
      const TagProp<_tag_id, T...>& tag_prop,
      const std::vector<size_t>& repeat_array) {
    VLOG(10) << "start fetching properties";
    // Get property from storage.
    static constexpr size_t N = NODE_T::num_labels;
    std::array<std::vector<std::tuple<T...>>, N> tuples;
    for (auto i = 0; i < N; ++i) {
      auto& node = multi_set.GetSet(i);
      VLOG(10) << "start fetch properties for " << node.GetLabel()
               << " size: " << node.GetVertices().size();
      tuples[i] = graph.template GetVertexPropsFromVid<T...>(
          time_stamp, node.GetLabel(), node.GetVertices(),
          tag_prop.prop_names_);
      VLOG(10) << "Finish fetching properties";
    }
    std::vector<std::tuple<T...>> res_prop_vec;

    size_t cur_ind = 0;
    for (auto iter : multi_set) {
      CHECK(cur_ind <= repeat_array.size());
      auto cur_label = iter.GetCurInd();
      auto inner_ind = iter.GetCurSetInnerInd();
      // VLOG(10) << "cur: " << cur_label << ", " << inner_ind << ", " <<
      // cur_ind;
      for (auto j = 0; j < repeat_array[cur_ind]; ++j) {
        res_prop_vec.push_back(tuples[cur_label][inner_ind]);
      }
      cur_ind += 1;
    }
    VLOG(10) << "res prop vec size: " << res_prop_vec.size();

    // TODO: support builtin property for multi vertex set
    return Collection<std::tuple<T...>>(std::move(res_prop_vec));
  }

  // general vertex set.
  template <
      typename NODE_T, int _tag_id, typename... T,
      typename std::enable_if<NODE_T::is_general_set &&
                              !NODE_T::is_collection && NODE_T::is_vertex_set &&
                              !NODE_T::is_multi_label>::type* = nullptr>
  static auto apply_single_project_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, NODE_T& multi_set,
      const TagProp<_tag_id, T...>& tag_prop,
      const std::vector<size_t>& repeat_array) {
    VLOG(10) << "start fetching properties";
    auto tmp_prop_vec = get_property_tuple_general<T...>(
        time_stamp, graph, multi_set, tag_prop.prop_names_);

    // make_repeat;
    size_t sum = 0;
    bool flag = true;
    for (auto i = 0; i < repeat_array.size(); ++i) {
      if (repeat_array[i] != 1) {
        flag = false;
      }
      sum += repeat_array[i];
    }
    if (flag) {
      return Collection<std::tuple<T...>>(std::move(tmp_prop_vec));
    } else {
      std::vector<std::tuple<T...>> res_prop_vec;
      res_prop_vec.reserve(sum);
      for (auto i = 0; i < repeat_array.size(); ++i) {
        for (auto j = 0; j < repeat_array[i]; ++j) {
          res_prop_vec.push_back(tmp_prop_vec[i]);
        }
      }
      return Collection<std::tuple<T...>>(std::move(res_prop_vec));
    }
  }

  // single label edge set
  template <typename NODE_T, int _tag_id, typename... T,
            typename std::enable_if<NODE_T::is_edge_set>::type* = nullptr>
  static auto apply_single_project_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, NODE_T& node,
      const TagProp<_tag_id, T...>& tag_prop,
      const std::vector<size_t>& repeat_array) {
    // VLOG(10) << "start fetching properties";
    // Get property from storage.
    // auto prop_tuple_vec = graph.template GetVertexPropsFromVid<T...>(
    //     time_stamp, node.GetLabel(), node.GetVertices(),
    //     key_alias_prop.prop_names_);
    VLOG(10) << "Finish fetching properties";
    std::vector<std::tuple<T...>> res_prop_vec;
    {
      size_t sum = 0;
      for (auto v : repeat_array) {
        sum += v;
      }
      res_prop_vec.resize(sum);
    }
    // We assume edge properties are already got in getEdges.
    node.fillBuiltinProps(res_prop_vec, tag_prop.prop_names_, repeat_array);

    return Collection<std::tuple<T...>>(std::move(res_prop_vec));
  }

  // apply
  template <typename CTX_T, int res_alias, typename... TAG_PROP, size_t... Is>
  static auto apply_single_project_on_multi_tags_impl(
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      MultiKeyAliasProp<res_alias, TAG_PROP...>& key_alias_prop,
      const std::vector<std::vector<size_t>>& repeat_array_vec,
      std::index_sequence<Is...>) {
    auto collection_tuple = std::make_tuple(apply_single_project_impl(
        time_stamp, graph, ctx.template GetNode<TAG_PROP::tag_id>(),
        std::get<Is>(key_alias_prop.tag_props_), repeat_array_vec[Is])...);

    VLOG(10) << "Got collection tuple";
    // check length;
    size_t length = get_length<0>(collection_tuple);
    VLOG(10) << "Finish length check: " << length;
    // cat tuple
    // using res_tuple_t =
    // typename MultiKeyAliasProp<res_alias, TAG_PROP...>::res_prop_tuple_t;
    using res_tuple_t = gs::tuple_cat_t<typename TAG_PROP::prop_tuple_t...>;
    std::vector<res_tuple_t> res_vec;
    res_vec.reserve(length);

    for (auto i = 0; i < length; ++i) {
      res_vec.emplace_back(
          std::tuple_cat(std::get<Is>(collection_tuple).Get(i)...));
    }
    return Collection<res_tuple_t>(std::move(res_vec));
  }

  template <size_t Is, typename... T,
            typename std::enable_if<(Is == sizeof...(T) - 1)>::type* = nullptr>
  static size_t get_length(size_t length, std::tuple<T...>& tuple) {
    if (length == std::get<Is>(tuple).Size()) {
      return length;
    } else {
      LOG(FATAL) << "Check length fail at ind: " << Is;
      return 0;
    }
  }

  template <size_t Is, typename... T,
            typename std::enable_if<(Is < sizeof...(T) - 1)>::type* = nullptr>
  static size_t get_length(size_t length, std::tuple<T...>& tuple) {
    if (length == std::get<Is>(tuple).Size()) {
      return get_length<Is + 1>(length, tuple);
    } else {
      LOG(FATAL) << "Check length fail at ind: " << Is;
      return 0;
    }
  }

  template <size_t Is, typename... T,
            typename std::enable_if<(Is == 0)>::type* = nullptr>
  static size_t get_length(std::tuple<T...>& tuple) {
    size_t length = std::get<Is>(tuple).Size();
    return get_length<Is + 1>(length, tuple);
  }

  // evaluate expression in project op
  template <typename EXPR, typename... ELE, typename... PROP_GETTER>
  static inline auto evaluate_proj_expr(
      const EXPR& expr, std::tuple<ELE...>& eles,
      std::tuple<PROP_GETTER...>& prop_getter_tuple) {
    return evaluate_proj_expr_impl(
        expr, eles, prop_getter_tuple,
        std::make_index_sequence<sizeof...(PROP_GETTER)>());
  }

  template <typename EXPR, typename... ELE, typename... PROP_GETTER,
            size_t... Is>
  static inline auto evaluate_proj_expr_impl(
      const EXPR& expr, std::tuple<ELE...>& eles,
      std::tuple<PROP_GETTER...>& prop_getter_tuple,
      std::index_sequence<Is...>) {
    return expr(std::get<Is>(prop_getter_tuple).get_from_all_element(eles)...);
  }
};
}  // namespace gs

#endif  // GRAPHSCOPE_OPERATOR_GROUP_H_
