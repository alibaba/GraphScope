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

#ifndef ENGINES_HQPS_ENGINE_OPERATOR_PROJECT_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_PROJECT_H_

#include <tuple>
#include <unordered_map>
#include <vector>

#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/keyed.h"

#include "flex/engines/hqps_db/structures/collection.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/untyped_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"
#include "flex/engines/hqps_db/structures/path.h"

namespace gs {

template <typename CTX_T, typename KEY_ALIAS>
struct ResultOfContextKeyAlias;

// project one single property
template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int in_col_id, typename T>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    IdentityMapper<in_col_id, PropertySelector<T>>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using ctx_node_t = std::remove_reference_t<decltype(
      std::declval<context_t>().template GetNode<in_col_id>())>;
  using result_t = Collection<T>;
};

// project the tag itself.
template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int in_col_id>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    IdentityMapper<in_col_id, PropertySelector<grape::EmptyType>>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using ctx_node_t = std::remove_reference_t<decltype(
      std::declval<context_t>().template GetNode<in_col_id>())>;
  using result_t = ctx_node_t;
};

template <typename CTX_HEAD_T, int cur_alias, int base_tag,
          typename... CTX_PREV, int in_col_id>
struct ResultOfContextKeyAlias<
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
    IdentityMapper<in_col_id, PropertySelector<GlobalId>>> {
  using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
  using result_t = Collection<typename GlobalId::gid_t>;
};

// Mapping a node to
// template <typename CTX_HEAD_T, int cur_alias, int base_tag,
//           typename... CTX_PREV, typename... Mapper>
// struct ResultOfContextKeyAlias<
//     Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
//     KeyValueMappers<Mapper...>> {
//   using context_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
//   using ctx_node_t = std::remove_reference_t<decltype(
//       std::declval<context_t>().template GetNode<in_col_id>())>;
//   using result_t = Collection
// };

template <int new_head_alias, typename new_head_t, int cur_alias,
          typename old_head_t, int base_tag, typename tuple>
struct ResultContextTWithPrevTuple;

template <int new_head_alias, typename new_head_t, int cur_alias,
          typename old_head_t, int base_tag, typename... T>
struct ResultContextTWithPrevTuple<new_head_alias, new_head_t, cur_alias,
                                   old_head_t, base_tag, std::tuple<T...>> {
  // FIXME: use correct append_opt
  using result_t =
      typename ResultContextT<AppendOpt::Persist, new_head_t, cur_alias,
                              old_head_t, base_tag, T...>::result_t;
};

template <typename GRAPH_INTERFACE>
class ProjectOp {
 public:
  // specialized to append
  // Project a previous tag and append to traversal.
  template <
      ProjectDesc is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename... ProjectMapper,
      typename std::enable_if<(is_append != ProjectDesc::New)>::type* = nullptr>
  static auto ProjectImpl(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<ProjectMapper...>&& mappers) {
    auto node_size = gs::Get<-1>(ctx).Size();
    VLOG(10) << "Current head size: " << node_size;

    std::vector<offset_t> offsets(node_size + 1, 0);
    for (size_t i = 1; i <= node_size; ++i) {
      offsets[i] = i;
    }

    return apply_projects_append<0, is_append>(graph, std::move(ctx), mappers,
                                               offsets);
  }

  // implementation for project is false, only proj one column
  template <
      ProjectDesc is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename... ProjectMapper,
      typename std::enable_if<(is_append == ProjectDesc::New) &&
                              (sizeof...(ProjectMapper) == 1)>::type* = nullptr>
  static auto ProjectImpl(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<ProjectMapper...>&& mappers) {
    auto node_size = gs::Get<-1>(ctx).Size();
    LOG(INFO) << "Project with: " << demangle(std::get<0>(mappers));
    VLOG(10) << "Current head size: " << node_size;

    auto head = apply_single_project(graph, ctx, std::get<0>(mappers));
    using new_head_t =
        std::remove_const_t<std::remove_reference_t<decltype(head)>>;
    return Context<new_head_t, 0, 0, grape::EmptyType>(std::move(head));
  }

  // implementation for project is false. project multiple columns
  template <
      ProjectDesc is_append, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename... ProjectMapper,
      typename std::enable_if<(is_append == ProjectDesc::New) &&
                              (sizeof...(ProjectMapper) > 1)>::type* = nullptr>
  static auto ProjectImpl(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      std::tuple<ProjectMapper...>&& mappers) {
    static constexpr size_t proj_num = sizeof...(ProjectMapper);
    auto node_size = gs::Get<-1>(ctx).Size();
    // VLOG(10) << "Current head size: " << node_size;
    std::vector<offset_t> offset(node_size + 1, 0);
    for (size_t i = 1; i <= node_size; ++i) {
      offset[i] = i;
    }
    std::vector<std::vector<offset_t>> offsets;
    offsets.reserve(proj_num - 1);
    for (size_t i = 0; i + 1 < proj_num; ++i) {
      offsets.push_back(offset);
    }

    // LOG(INFO) << "Projecting columns: " << proj_num;
    auto head =
        apply_single_project(graph, ctx, std::get<proj_num - 1>(mappers));
    auto prev_tuple = apply_single_project_until<proj_num - 1>(
        graph, ctx, mappers, std::make_index_sequence<proj_num - 1>{});
    return make_context<0, proj_num - 1>(std::move(prev_tuple), std::move(head),
                                         std::move(offsets));
  }

  template <size_t Is, ProjectDesc append_opt, typename CTX_T,
            typename... ProjectMapper,
            typename std::enable_if<(Is < sizeof...(ProjectMapper) -
                                              1)>::type* = nullptr>
  static auto apply_projects_append(const GRAPH_INTERFACE& graph, CTX_T&& ctx,
                                    std::tuple<ProjectMapper...>& key_alias,
                                    std::vector<offset_t>& offsets) {
    auto new_node = apply_single_project(graph, ctx, std::get<Is>(key_alias));
    std::vector<offset_t> res_offsets(offsets);
    if constexpr (append_opt == ProjectDesc::AppendTemp) {
      auto res = ctx.template AddNode<AppendOpt::Temp>(std::move(new_node),
                                                       std::move(res_offsets));
      return apply_projects_append<Is + 1, append_opt>(graph, std::move(res),
                                                       key_alias, offsets);
    } else {
      auto res = ctx.template AddNode<AppendOpt::Persist>(
          std::move(new_node), std::move(res_offsets));
      return apply_projects_append<Is + 1, append_opt>(graph, std::move(res),
                                                       key_alias, offsets);
    }
  }

  // For the last element, return the result
  template <size_t Is, ProjectDesc append_opt, typename CTX_T,
            typename... ProjectMapper,
            typename std::enable_if<(Is == sizeof...(ProjectMapper) -
                                               1)>::type* = nullptr>
  static auto apply_projects_append(const GRAPH_INTERFACE& graph, CTX_T&& ctx,
                                    std::tuple<ProjectMapper...>& key_alias,
                                    std::vector<offset_t>& offsets) {
    auto new_node = apply_single_project(graph, ctx, std::get<Is>(key_alias));
    std::vector<offset_t> res_offsets(offsets);

    if constexpr (append_opt == ProjectDesc::AppendTemp) {
      return ctx.template AddNode<AppendOpt::Temp>(std::move(new_node),
                                                   std::move(res_offsets));
    } else {
      return ctx.template AddNode<AppendOpt::Persist>(std::move(new_node),
                                                      std::move(res_offsets));
    }
  }

  // Apply single project on old context's node until the indicated index of
  // project opts
  template <size_t limit, typename CTX_T, typename... PROJ_PROP, size_t... Is>
  static auto apply_single_project_until(
      const GRAPH_INTERFACE& graph, CTX_T& ctx,
      std::tuple<PROJ_PROP...>& proj_prop_tuple, std::index_sequence<Is...>) {
    static_assert(limit < sizeof...(PROJ_PROP));
    return std::make_tuple(
        apply_single_project(graph, ctx, std::get<Is>(proj_prop_tuple))...);
  }

  // Apply single project with IdentityMapper.
  template <typename CTX_T, int in_col_id, typename SelectorValueType,
            typename std::enable_if<(
                !std::is_same_v<grape::EmptyType, SelectorValueType> &&
                !std::is_same_v<GlobalId, SelectorValueType>)>::type* = nullptr>
  static auto apply_single_project(
      const GRAPH_INTERFACE& graph, CTX_T& ctx,
      IdentityMapper<in_col_id, PropertySelector<SelectorValueType>>& mapper) {
    auto& node = ctx.template GetNode<in_col_id>();
    // Create a empty copy.
    auto offset_array = ctx.ObtainOffsetFromTag(in_col_id);
    auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
    // A col describe what content is used to project
    return apply_single_project_impl<SelectorValueType>(
        graph, node, mapper.selector_.prop_name_, repeat_array);
  }

  // Project self.
  // Selector with GlobalId or grape::EmptyType is different
  template <typename CTX_T, int in_col_id, typename SelectorValueType,
            typename std::enable_if<
                (std::is_same_v<grape::EmptyType, SelectorValueType>)>::type* =
                nullptr>
  static auto apply_single_project(
      const GRAPH_INTERFACE& graph, CTX_T& ctx,
      IdentityMapper<in_col_id, PropertySelector<SelectorValueType>>& mapper) {
    auto& node = ctx.template GetNode<in_col_id>();
    // Create a empty copy.
    auto offset_array = ctx.ObtainOffsetFromTag(in_col_id);
    auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
    KeyAlias<in_col_id, -1> key_alias;
    return node.ProjectWithRepeatArray(repeat_array, key_alias);
  }

  // Project to GlobalId
  template <typename CTX_T, int in_col_id, typename SelectorValueType,
            typename std::enable_if<
                (std::is_same_v<GlobalId, SelectorValueType>)>::type* = nullptr>
  static auto apply_single_project(
      const GRAPH_INTERFACE& graph, CTX_T& ctx,
      IdentityMapper<in_col_id, PropertySelector<SelectorValueType>>& mapper) {
    auto& node = ctx.template GetNode<in_col_id>();
    static_assert(std::remove_reference_t<decltype(
                      node)>::is_vertex_set);  // edge_set not supported
    // Create a empty copy.
    auto offset_array = ctx.ObtainOffsetFromTag(in_col_id);
    auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
    auto prop_getter = create_global_id_prop_getter_from_prop_desc(
        graph, node, GlobalIdProperty<in_col_id>{});
    std::vector<typename GlobalId::gid_t> res_prop_vec;
    // iterate over node with ind_ele
    auto iter = node.begin();
    auto end = node.end();
    size_t i = 0;
    CHECK(repeat_array.size() == node.Size());
    for (; iter != end; ++iter) {
      auto ele_tuple = iter.GetIndexElement();
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        res_prop_vec.push_back(prop_getter.get_view(ele_tuple).global_id);
      }
      ++i;
    }
    return Collection<typename GlobalId::gid_t>(std::move(res_prop_vec));
  }

  // Project with  single mapper
  template <typename CTX_T, typename EXPR, typename... SELECTOR,
            int32_t... in_col_id>
  static auto apply_single_project(
      const GRAPH_INTERFACE& graph, CTX_T& ctx,
      MultiMapper<EXPR, std::tuple<SELECTOR...>, in_col_id...>& mapper) {
    // using expr_trait = gs::function_traits<decltype(std::declval<EXPR>())>;

    // using expr_result_t = typename expr_trait::result_type;
    using expr_result_t = typename EXPR::result_t;
    std::vector<expr_result_t> res_vec;
    res_vec.reserve(ctx.GetHead().Size());
    auto expr = mapper.expr_;
    auto prop_desc =
        create_prop_descs_from_selectors<in_col_id...>(mapper.selectors_);
    auto prop_getters =
        create_prop_getters_from_prop_desc(graph, ctx, prop_desc);
    LOG(INFO) << "In project with expression, successfully got prop getters";
    for (auto iter : ctx) {
      auto ele_tuple = iter.GetAllIndexElement();
      res_vec.emplace_back(evaluate_proj_expr(expr, ele_tuple, prop_getters));
    }
    return Collection<expr_result_t>(std::move(res_vec));
  }

  ///////////////////Project implementation for all data structures.

  /// Special case for project for labelKey
  template <
      typename T, typename NODE_T,
      typename std::enable_if<std::is_same_v<T, LabelKey>>::type* = nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph, NODE_T& node, const std::string& prop_name,
      const std::vector<size_t>& repeat_array) {
    LOG(INFO) << "[Single project on labelKey]" << demangle(node);
    auto label_vec = node.GetLabelVec();
    std::vector<T> res_prop_vec;
    CHECK(label_vec.size() == repeat_array.size())
        << "label size: " << label_vec.size()
        << " repeat size: " << repeat_array.size();
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        res_prop_vec.emplace_back(label_vec[i]);
      }
    }
    return Collection<T>(std::move(res_prop_vec));
  }

  // single label vertex set.
  template <
      typename T, typename NODE_T,
      typename std::enable_if<(!std::is_same_v<T, LabelKey> &&
                               NODE_T::is_row_vertex_set)>::type* = nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph, NODE_T& node, const std::string& prop_name,
      const std::vector<size_t>& repeat_array) {
    // Get property from storage.
    auto prop_tuple_vec = graph.template GetVertexPropsFromVid<T>(
        node.GetLabel(), node.GetVertices(), {prop_name});
    // VLOG(10) << "Finish fetching properties";
    node.fillBuiltinProps(prop_tuple_vec, {prop_name}, repeat_array);
    std::vector<T> res_prop_vec;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        res_prop_vec.push_back(std::get<0>(prop_tuple_vec[i]));
      }
    }
    // check builtin properties.
    // Found if there is any builtin properties need.

    return Collection<T>(std::move(res_prop_vec));
  }

  // project for two label vertex set.
  template <
      typename T, typename VID_T, typename LabelT, typename... SET_T,
      typename std::enable_if<(!std::is_same_v<T, LabelKey>)>::type* = nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph,
      TwoLabelVertexSetImpl<VID_T, LabelT, SET_T...>& node,
      const std::string& prop_name, const std::vector<size_t>& repeat_array) {
    auto tmp_prop_vec =
        get_property_tuple_two_label<T>(graph, node, {prop_name});

    // make_repeat;
    size_t sum = 0;
    bool flag = true;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      if (repeat_array[i] != 1) {
        flag = false;
      }
      sum += repeat_array[i];
    }
    std::vector<T> res_prop_vec;
    if (flag) {
      {
        // convert tuple to vector.
        res_prop_vec.reserve(tmp_prop_vec.size());
        for (auto& ele : tmp_prop_vec) {
          res_prop_vec.emplace_back(std::get<0>(ele));
        }
      }
      return Collection<T>(std::move(res_prop_vec));
    } else {
      res_prop_vec.reserve(sum);
      for (size_t i = 0; i < repeat_array.size(); ++i) {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          res_prop_vec.emplace_back(std::get<0>(tmp_prop_vec[i]));
        }
      }
      return Collection<T>(std::move(res_prop_vec));
    }
  }

  // general vertex set.
  template <
      typename T, typename VID_T, typename LabelT, typename... SET_T,
      typename std::enable_if<(!std::is_same_v<T, LabelKey>)>::type* = nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph,
      GeneralVertexSet<VID_T, LabelT, SET_T...>& node,
      const std::string& prop_name_, const std::vector<size_t>& repeat_array) {
    VLOG(10) << "start fetching properties";
    auto tmp_prop_vec = get_property_tuple_general<T>(
        graph, node, std::array<std::string, 1>{prop_name_});
    VLOG(10) << "Got properties for general vertex set: "
             << gs::to_string(tmp_prop_vec);
    std::vector<T> res_prop_vec;
    // make_repeat;
    size_t sum = 0;
    bool flag = true;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      if (repeat_array[i] != 1) {
        flag = false;
      }
      sum += repeat_array[i];
    }
    if (flag) {
      {
        // convert tmp_prop_vec to vector.
        res_prop_vec.reserve(tmp_prop_vec.size());
        for (auto& ele : tmp_prop_vec) {
          res_prop_vec.push_back(std::get<0>(ele));
        }
      }
      return Collection<T>(std::move(res_prop_vec));
    } else {
      res_prop_vec.reserve(sum);
      for (size_t i = 0; i < repeat_array.size(); ++i) {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          res_prop_vec.push_back(std::get<0>(tmp_prop_vec[i]));
        }
      }
      return Collection<T>(std::move(res_prop_vec));
    }
  }

  // single label edge set
  template <
      typename T, typename NODE_T,
      typename std::enable_if<NODE_T::is_edge_set &&
                              (!std::is_same_v<T, LabelKey>)>::type* = nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph, NODE_T& node, const std::string& prop_name,
      const std::vector<size_t>& repeat_array) {
    VLOG(10) << "Finish fetching properties";
    std::vector<std::tuple<T>> tmp_prop_vec;
    {
      size_t sum = 0;
      for (auto v : repeat_array) {
        sum += v;
      }
      tmp_prop_vec.resize(sum);
    }
    // We assume edge properties are already got in getEdges.
    node.fillBuiltinProps(tmp_prop_vec, {prop_name}, repeat_array);

    std::vector<T> res_prop_vec;
    {
      // convert tmp_prop_vec to vector.
      res_prop_vec.reserve(tmp_prop_vec.size());
      for (auto& ele : tmp_prop_vec) {
        res_prop_vec.push_back(std::get<0>(ele));
      }
    }

    return Collection<T>(std::move(res_prop_vec));
  }

  /// Apply project on untyped edge set.
  template <
      typename T, typename VID_T, typename LabelT, typename SUB_GRAPH_T,
      typename std::enable_if<(!std::is_same_v<T, LabelKey>)>::type* = nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph,
      UnTypedEdgeSet<VID_T, LabelT, SUB_GRAPH_T>& node,
      const std::string& prop_name, const std::vector<size_t>& repeat_array) {
    VLOG(10) << "Finish fetching properties";

    // We assume edge properties are already got in getEdges.
    std::array<std::string, 1> prop_array{prop_name};
    std::vector<T> tmp_prop_vec =
        node.template getProperties<T>(prop_array, repeat_array);

    return Collection<T>(std::move(tmp_prop_vec));
  }

  // apply project on path set，the type must be lengthKey
  template <typename PROP_T, typename VID_T, typename LabelT,
            typename std::enable_if<std::is_same_v<PROP_T, LengthKey>>::type* =
                nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph, CompressedPathSet<VID_T, LabelT>& node,
      const std::string& prop_name, const std::vector<size_t>& repeat_array) {
    VLOG(10) << "Finish fetching properties";

    std::vector<typename LengthKey::length_data_type> lengths_vec;
    auto path_vec = node.get_all_valid_paths();
    CHECK(path_vec.size() == repeat_array.size());
    lengths_vec.reserve(path_vec.size());
    for (size_t i = 0; i < path_vec.size(); ++i) {
      if (repeat_array[i] > 0) {
        auto length = path_vec[i].length();
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          lengths_vec.push_back(length);
        }
      }
    }

    return Collection<typename LengthKey::length_data_type>(
        std::move(lengths_vec));
  }

  // apply project on path set，the type must be lengthKey
  template <typename PROP_T, typename VID_T, typename LabelT,
            typename std::enable_if<std::is_same_v<PROP_T, LengthKey>>::type* =
                nullptr>
  static auto apply_single_project_impl(
      const GRAPH_INTERFACE& graph, PathSet<VID_T, LabelT>& node,
      const std::string& prop_name, const std::vector<size_t>& repeat_array) {
    VLOG(10) << "Finish fetching properties";

    std::vector<typename LengthKey::length_data_type> lengths_vec;
    for (size_t i = 0; i < node.Size(); ++i) {
      const auto& path = node.get(i);
      if (repeat_array[i] > 0) {
        auto length = path.length();
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          lengths_vec.push_back(length);
        }
      }
    }

    return Collection<typename LengthKey::length_data_type>(
        std::move(lengths_vec));
  }

  ///////////////////Apply KeyValueMapper to all data structures.
  template <typename CTX_T, typename... MAPPER>
  static auto apply_single_project(
      const GRAPH_INTERFACE& graph, CTX_T& ctx,
      KeyValueMappers<MAPPER...>& key_value_mappers) {
    LOG(INFO) << "Project KeyValueMapper: " << demangle(key_value_mappers);
    // the result is collection<VariableKeyValue>
    std::vector<VariableKeyValue> res_vec;
    res_vec.reserve(ctx.GetHead().Size());
    auto prop_desc =
        create_prop_descs_from_mappers<MAPPER...>(key_value_mappers);
    LOG(INFO) << "Prop Desc: " << demangle(prop_desc);
    auto prop_getters =
        create_prop_getters_from_prop_desc(graph, ctx, prop_desc);
    for (auto iter : ctx) {
      auto ele_tuple = iter.GetAllIndexElement();
      res_vec.emplace_back(
          evaluate_kv_mapper(ele_tuple, prop_getters, key_value_mappers));
    }
    return Collection<VariableKeyValue>(std::move(res_vec));
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
    return expr(
        std::get<Is>(prop_getter_tuple).get_from_all_index_element(eles)...);
  }

  // create prop desc from mappers
  template <typename... MAPPER>
  static inline auto create_prop_descs_from_mappers(
      KeyValueMappers<MAPPER...>& key_value_mappers) {
    return create_prop_descs_from_mappers_impl(
        key_value_mappers, std::make_index_sequence<sizeof...(MAPPER)>());
  }

  template <int32_t... in_col_id, typename... T, size_t... Is>
  static inline auto create_prop_descs_from_mappers_impl(
      KeyValueMappers<KeyValueMapper<in_col_id, PropertySelector<T>>...>&
          key_value_mappers,
      std::index_sequence<Is...>) {
    return std::make_tuple(create_prop_desc_from_mapper(
        std::get<Is>(key_value_mappers.mappers_))...);
  }

  // create prop desc from mapper
  template <int32_t in_col_id, typename T>
  static inline auto create_prop_desc_from_mapper(
      KeyValueMapper<in_col_id, PropertySelector<T>>& mapper) {
    return create_prop_desc_from_selector<in_col_id>(mapper.value_selector_);
  }

  // evaluate_kv_mapper
  template <typename... ELE, typename... PROP_GETTER, typename... Mapper>
  static inline auto evaluate_kv_mapper(
      std::tuple<ELE...>& eles, std::tuple<PROP_GETTER...>& prop_getter_tuple,
      const KeyValueMappers<Mapper...>& key_value_mappers) {
    return evaluate_kv_mapper_impl(
        eles, prop_getter_tuple, key_value_mappers,
        std::make_index_sequence<sizeof...(PROP_GETTER)>());
  }

  template <typename... ELE, typename... PROP_GETTER, typename... Mapper,
            size_t... Is>
  static inline auto evaluate_kv_mapper_impl(
      std::tuple<ELE...>& eles, std::tuple<PROP_GETTER...>& prop_getter_tuple,
      const KeyValueMappers<Mapper...>& key_value_mappers,
      std::index_sequence<Is...>) {
    LOG(INFO) << "Prop Getters: " << demangle(prop_getter_tuple);
    static_assert(sizeof...(PROP_GETTER) == sizeof...(Mapper));
    VariableKeyValue res;

    emplace_key_value_mapper<0>(res, eles, prop_getter_tuple,
                                key_value_mappers);
    // (res.emplace(
    //      std::get<Is>(key_value_mappers.mappers_).key_,
    //      std::get<Is>(prop_getter_tuple)
    //          .get_view(gs::get_from_tuple<std::tuple_element_t<
    //                        Is, std::tuple<Mapper...>>::in_col_id>(eles))),
    //  ...);
    for (auto iter : res) {
      LOG(INFO) << "Key: " << iter.first
                << " Value: " << iter.second.to_string();
    }
    return res;
  }

  template <size_t Is, typename... ELE, typename... PROP_GETTER,
            typename... Mapper>
  static inline void emplace_key_value_mapper(
      VariableKeyValue& res, std::tuple<ELE...>& eles,
      std::tuple<PROP_GETTER...>& prop_getter_tuple,
      const KeyValueMappers<Mapper...>& key_value_mappers) {
    if constexpr (Is < sizeof...(Mapper)) {
      auto cur_value =
          std::get<Is>(prop_getter_tuple)
              .get_view(gs::get_from_tuple<std::tuple_element_t<
                            Is, std::tuple<Mapper...>>::in_col_id>(eles));
      if (!IsNull(cur_value)) {
        res.emplace_back(std::get<Is>(key_value_mappers.mappers_).key_,
                         cur_value);
      } else {
        LOG(INFO) << "cur value is null: " << gs::to_string(cur_value);
        res.emplace_back(std::get<Is>(key_value_mappers.mappers_).key_,
                         Any::From(grape::EmptyType()));
      }
      emplace_key_value_mapper<Is + 1>(res, eles, prop_getter_tuple,
                                       key_value_mappers);
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_PROJECT_H_
