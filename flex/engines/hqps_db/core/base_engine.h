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
#ifndef ENGINES_HQPS_ENGINE_BASE_ENGINE_H_
#define ENGINES_HQPS_ENGINE_BASE_ENGINE_H_

#include <algorithm>
#include <atomic>
#include <memory>
#include <queue>
#include <sstream>
#include <string>

#include <boost/functional/hash.hpp>

#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"

#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

#include "flex/engines/hqps_db/structures/multi_vertex_set/multi_label_vertex_set.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/column.h"
#include "grape/utils/bitset.h"

#include "flex/engines/hqps_db/core/operator/group_by.h"
#include "flex/engines/hqps_db/core/operator/limit.h"
#include "flex/engines/hqps_db/core/operator/project.h"
#include "flex/engines/hqps_db/core/operator/sink.h"
#include "flex/engines/hqps_db/core/operator/sort.h"

#include "flex/engines/hqps_db/core/null_record.h"

#include "glog/logging.h"

namespace gs {

template <typename T>
struct BuilderTuple;

template <typename... T>
struct BuilderTuple<std::tuple<T...>> {
  using type = std::tuple<typename T::builder_t...>;
};

class BaseEngine {
 public:
  template <int res_alias, int prev_alias, int base_tag, typename HEAD_T,
            typename... PREV,
            typename RES_T = Context<HEAD_T, res_alias, base_tag, PREV...>>
  static RES_T Alias(Context<HEAD_T, prev_alias, base_tag, PREV...>&& prev) {
    return prev.template Alias<res_alias>();
  }

  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV>
  static auto Limit(Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                    int32_t lower, int32_t upper) {
    return LimitOp::Limit(std::move(ctx), lower, upper);
  }

  //////////////////////////////////////Dedup/////////////////////////
  // Only can dedup head node.
  template <
      int alias_to_use, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static RES_T Dedup(
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx) {
    if constexpr (alias_to_use != cur_alias) {
      // When we dedup a intermediate node, we need to
      // 1) first dedup current node, no duplicate in us.
      // 2) then iterate whole context, for later nodes, we only preserve the
      // first element met.
      //
      // the result context type should be same with previous.
      // 1 -> (2, 3)
      // 2 -> (4, 5), 3 -> (6, 7);
      //
      // dedup on col 2, then we 1 -> (2, 3), 2 -> 4, 3 -> 6;

      // first remove all possible duplication introduced by later csr.
      ctx.template Dedup<alias_to_use>();
    }
    auto& select_node = gs::Get<alias_to_use>(ctx);
    // dedup inplace, and return the offset_array to old node.
    auto offset_to_old_node = select_node.Dedup();
    // The offset need to be changed.
    ctx.template UpdateChildNode<alias_to_use>(std::move(offset_to_old_node));
    return ctx;
  }

  /// @brief /////////////Dedup on multiple keys////////////////
  /// @param ctx
  /// @return
  template <
      int... alias_to_use, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV,
      typename RES_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename std::enable_if<(sizeof...(alias_to_use) > 1)>::type* = nullptr>
  static RES_T Dedup(
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx) {
    // get all ele_t of context
    using CTX_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>;
    using ctx_iter_t = typename CTX_T::iterator;
    using ctx_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_iter_t>().GetAllElement())>;
    using dedup_tuple_t =
        std::tuple<std::tuple_element_t<alias_to_use, ctx_all_ele_t>...>;
    std::unordered_set<dedup_tuple_t, boost::hash<dedup_tuple_t>> dedup_set;
    std::vector<size_t> active_indices;
    std::vector<size_t> new_offset;
    auto& cur_ = ctx.GetMutableHead();
    new_offset.reserve(cur_.Size());
    new_offset.emplace_back(0);
    size_t cnt = 0;
    for (auto iter : ctx) {
      auto eles = iter.GetAllElement();
      dedup_tuple_t dedup_tuple =
          std::make_tuple(std::get<alias_to_use>(eles)...);
      if (dedup_set.find(dedup_tuple) == dedup_set.end()) {
        dedup_set.insert(dedup_tuple);
        active_indices.emplace_back(cnt);
      }
      cnt += 1;
      new_offset.emplace_back(active_indices.size());
    }

    cur_.SubSetWithIndices(active_indices);
    ctx.merge_offset_with_back(new_offset);
    return ctx;
  }

  /////////////////////////Apply///////////////////////////////
  /// With a apply function, we get the result, and join with current node.
  //  append the result data to current traversal.
  template <
      JoinKind join_kind, typename CTX_HEAD_T, int cur_alias, int base_tag,
      typename... CTX_PREV, typename FUNC_T,
      typename std::enable_if<join_kind == JoinKind::AntiJoin>::type* = nullptr>
  static auto Apply(Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                    FUNC_T&& func) {
    VLOG(10) << "[Apply AntiJoin]: ";
    static constexpr size_t start_tag =
        (cur_alias == -1 ? base_tag + sizeof...(CTX_PREV) : cur_alias);
    // create a copied  ctx
    auto copied_ctx(ctx);
    copied_ctx.set_sub_task_start_tag(start_tag);

    auto inner_ctx = func(std::move(copied_ctx));
    // We shall obtain the active indices in res_ctx via csr offset
    // arrays.

    std::vector<offset_t> tmp_vec = inner_ctx.ObtainOffsetFromTag(start_tag);
    // Filter the total context with keys.
    // The res_alias is currently not used.
    ctx.FilterWithOffsets(tmp_vec, join_kind);
    return ctx;
  }
  /////////////////////////OuterJoin///////////////////////////////
  // Join on two context, and append the result data to current traversal.
  template <int alias_x, int alias_y, JoinKind join_kind, typename CTX_X,
            typename CTX_Y,
            typename std::enable_if<join_kind ==
                                    JoinKind::LeftOuterJoin>::type* = nullptr>
  static auto Join(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    LOG(INFO) << "[LeftOuterJoin] with left ele: " << ctx_x.GetHead().Size()
              << ", right: " << ctx_y.GetHead().Size();
    // get all tuples from two context.
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    using ctx_y_all_ind_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllIndexElement())>;
    using ctx_y_all_data_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllData())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr int x_base_tag = CTX_X::base_tag_id;
    static constexpr int y_base_tag = CTX_Y::base_tag_id;
    LOG(INFO) << "x ele: " << x_ele_num << ", y ele num: " << y_ele_num;

    static constexpr size_t real_x_ind =
        alias_x == -1 ? x_ele_num - 1 : alias_x - x_base_tag;
    static constexpr size_t real_y_ind =
        alias_y == -1 ? y_ele_num - 1 : alias_y - y_base_tag;
    using ctx_x_ele_t = std::tuple_element_t<real_x_ind, ctx_x_all_ele_t>;
    using ctx_y_ele_t = std::tuple_element_t<real_y_ind, ctx_y_all_ele_t>;
    using ctx_y_res_ele_t =
        typename gs::remove_ith_type<real_y_ind, ctx_y_all_ind_ele_t>::type;
    using ctx_y_res_data_t =
        typename gs::remove_ith_type<real_y_ind, ctx_y_all_data_t>::type;
    static_assert(std::is_same_v<ctx_x_ele_t, ctx_y_ele_t>,
                  "Join on different type is not supported.");
    // We shall preserve the records on the left, and append the right
    // context' columns(which is not in ctx_x) to ctx_x For CodegenBuilder,
    // the mapping from tagId to tag_ind should be updated.
    auto y_builder_tuple_init = ctx_y.CreateSetBuilder();
    auto x_builder_tuple_init = ctx_x.CreateSetBuilder();
    auto y_builder_tuple = remove_nth_element<real_y_ind>(y_builder_tuple_init);
    // auto x_builder_tuple =
    // remove_nth_element<real_x_ind>(x_builder_tuple_init);
    auto builder_tuple = std::tuple_cat(x_builder_tuple_init, y_builder_tuple);
    static_assert(is_tuple<ctx_y_res_ele_t>::value);
    static_assert(is_tuple<ctx_y_res_data_t>::value);
    std::unordered_map<
        ctx_y_ele_t, std::vector<std::pair<ctx_y_res_ele_t, ctx_y_res_data_t>>,
        boost::hash<ctx_y_ele_t>>
        y_ele_to_ind;
    {
      // fill in ele_to_ind
      for (auto iter : ctx_y) {
        auto ele = iter.GetAllElement();
        auto index_ele = iter.GetAllIndexElement();
        auto data = iter.GetAllData();
        auto y_ele = std::get<real_y_ind>(ele);
        y_ele_to_ind[y_ele].emplace_back(
            std::make_pair(remove_nth_element<real_y_ind>(index_ele),
                           remove_nth_element<real_y_ind>(data)));
      }
    }

    {
      double t0 = -grape::GetCurrentTime();
      for (auto iter_x : ctx_x) {
        auto ele = iter_x.GetAllElement();
        auto ind_ele = iter_x.GetAllIndexElement();
        auto data_tuple = iter_x.GetAllData();
        auto x_ele = std::get<real_x_ind>(ele);
        // auto x_ele_to_insert = remove_nth_element<real_x_ind>(ele);
        auto iter = y_ele_to_ind.find(x_ele);
        if (iter != y_ele_to_ind.end()) {
          for (auto& y_ele_data : iter->second) {
            auto copied_ele = y_ele_data.first;
            auto new_ele =
                std::tuple_cat(std::move(ind_ele), std::move(copied_ele));
            auto new_data = std::tuple_cat(std::move(data_tuple),
                                           std::move(y_ele_data.second));
            insert_into_builder_v2(builder_tuple, new_ele, new_data);
          }
        } else {
          LOG(INFO) << "no y ele found";
          auto new_ele =
              std::tuple_cat(std::move(ind_ele),
                             NullRecordCreator<ctx_y_res_ele_t>::GetNull());
          LOG(INFO) << "new ele: " << gs::to_string(new_ele);
          auto new_data =
              std::tuple_cat(std::move(data_tuple),
                             NullRecordCreator<ctx_y_res_data_t>::GetNull());
          LOG(INFO) << "new data: " << gs::to_string(new_data);
          insert_into_builder_v2(builder_tuple, new_ele, new_data);
        }
      }
      LOG(INFO) << "here";
      t0 += grape::GetCurrentTime();
      LOG(INFO) << "Join cost: " << t0;
    }
    static constexpr size_t final_col_num = x_ele_num + y_ele_num - 1;
    auto built_tuple = builder_finish(
        builder_tuple, std::make_index_sequence<final_col_num>{});
    LOG(INFO) << "after build, size: " << std::get<0>(built_tuple).Size();
    auto offset_vec =
        make_offset_vector(final_col_num - 1, std::get<0>(built_tuple).Size());
    VLOG(10) << "offset vec size:  " << offset_vec.size();
    auto prev_tuple = gs::remove_nth_element<final_col_num - 1>(built_tuple);
    auto head_tuple = std::get<final_col_num - 1>(built_tuple);

    return make_context<0, final_col_num - 1>(
        std::move(prev_tuple), std::move(head_tuple), std::move(offset_vec));
  }

  // Join on two context, and append the result data to current traversal.
  template <int alias_x0, int alias_x1, int alias_y0, int alias_y1,
            JoinKind join_kind, typename CTX_X, typename CTX_Y,
            typename std::enable_if<join_kind ==
                                    JoinKind::LeftOuterJoin>::type* = nullptr>
  static auto Join(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    LOG(INFO) << "[LeftOuterJoin] with ";
    // get all tuples from two context.
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    using ctx_y_all_ind_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllIndexElement())>;
    using ctx_y_all_data_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllData())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr int x_base_tag = CTX_X::base_tag_id;
    static constexpr int y_base_tag = CTX_Y::base_tag_id;
    LOG(INFO) << "x ele: " << x_ele_num << ", y ele num: " << y_ele_num;

    static constexpr size_t real_x_ind0 =
        alias_x0 == -1 ? x_ele_num - 1 : alias_x0 - x_base_tag;
    static constexpr size_t real_x_ind1 =
        alias_x1 == -1 ? x_ele_num - 1 : alias_x1 - x_base_tag;
    static constexpr size_t real_y_ind0 =
        alias_y0 == -1 ? y_ele_num - 1 : alias_y0 - y_base_tag;
    static constexpr size_t real_y_ind1 =
        alias_y1 == -1 ? y_ele_num - 1 : alias_y1 - y_base_tag;
    using ctx_x_ele_t =
        std::pair<std::tuple_element_t<real_x_ind0, ctx_x_all_ele_t>,
                  std::tuple_element_t<real_x_ind1, ctx_x_all_ele_t>>;
    using ctx_y_ele_t =
        std::pair<std::tuple_element_t<real_y_ind0, ctx_y_all_ele_t>,
                  std::tuple_element_t<real_y_ind1, ctx_y_all_ele_t>>;

    using ctx_y_res_ele_t =
        typename gs::remove_ith_jth_type<real_y_ind0, real_y_ind1,
                                         ctx_y_all_ind_ele_t>::type;
    using ctx_y_res_data_t =
        typename gs::remove_ith_jth_type<real_y_ind0, real_y_ind1,
                                         ctx_y_all_data_t>::type;
    static_assert(std::is_same_v<ctx_x_ele_t, ctx_y_ele_t>,
                  "Join on different type is not supported.");
    // if
    //   contexpr(y_ele_num == 2) {}
    // We shall preserve the records on the left, and append the right
    // context' columns(which is not in ctx_x) to ctx_x For CodegenBuilder,
    // the mapping from tagId to tag_ind should be updated.
    auto y_builder_tuple_init = ctx_y.CreateSetBuilder();
    auto x_builder_tuple_init = ctx_x.CreateSetBuilder();
    auto y_builder_tuple =
        remove_ith_jth_element<real_y_ind0, real_y_ind1>(y_builder_tuple_init);
    // auto x_builder_tuple =
    // remove_nth_element<real_x_ind>(x_builder_tuple_init);
    auto builder_tuple = std::tuple_cat(std::move(x_builder_tuple_init),
                                        std::move(y_builder_tuple));
    static_assert(is_tuple<ctx_y_res_ele_t>::value);
    static_assert(is_tuple<ctx_y_res_data_t>::value);
    std::unordered_map<
        ctx_y_ele_t, std::vector<std::pair<ctx_y_res_ele_t, ctx_y_res_data_t>>,
        boost::hash<ctx_y_ele_t>>
        y_ele_to_ind;

    LOG(INFO) << "ctx x: " << ctx_x.GetHead().Size()
              << ", ctx y:" << ctx_y.GetHead().Size();
    {
      auto t0 = -grape::GetCurrentTime();
      // fill in ele_to_ind
      for (auto iter : ctx_y) {
        auto ele = iter.GetAllElement();
        auto index_ele = iter.GetAllIndexElement();
        auto data = iter.GetAllData();
        auto y_ele = std::make_pair(std::get<real_y_ind0>(ele),
                                    std::get<real_y_ind1>(ele));
        y_ele_to_ind[y_ele].emplace_back(std::make_pair(
            remove_ith_jth_element<real_y_ind0, real_y_ind1>(index_ele),
            remove_ith_jth_element<real_y_ind0, real_y_ind1>(data)));
      }
      t0 += grape::GetCurrentTime();
      LOG(INFO) << "fill in ele_to_ind takes " << t0 << "s";
    }

    {
      double t0 = -grape::GetCurrentTime();
      for (auto iter_x : ctx_x) {
        auto ele = iter_x.GetAllElement();
        auto ind_ele = iter_x.GetAllIndexElement();
        auto data_tuple = iter_x.GetAllData();
        auto x_ele = std::make_pair(std::get<real_x_ind0>(ele),
                                    std::get<real_x_ind1>(ele));
        // auto x_ele_to_insert = remove_nth_element<real_x_ind>(ele);
        auto iter = y_ele_to_ind.find(x_ele);
        if (iter != y_ele_to_ind.end()) {
          for (auto& y_ele_data : iter->second) {
            auto copied_ele = y_ele_data.first;
            auto new_ele =
                std::tuple_cat(std::move(ind_ele), std::move(copied_ele));
            auto new_data = std::tuple_cat(std::move(data_tuple),
                                           std::move(y_ele_data.second));
            insert_into_builder_v2(builder_tuple, new_ele, new_data);
          }
        } else {
          auto new_ele =
              std::tuple_cat(std::move(ind_ele),
                             NullRecordCreator<ctx_y_res_ele_t>::GetNull());
          auto new_data =
              std::tuple_cat(std::move(data_tuple),
                             NullRecordCreator<ctx_y_res_data_t>::GetNull());
          insert_into_builder_v2(builder_tuple, new_ele, new_data);
        }
      }
      t0 += grape::GetCurrentTime();
      LOG(INFO) << "Join cost: " << t0;
    }
    static constexpr size_t final_col_num = x_ele_num + y_ele_num - 2;
    auto head_index_seq =
        gs::make_index_range<final_col_num - 1, final_col_num>{};
    auto other_index_seq = gs::make_index_range<0, final_col_num - 1>{};
    auto head_tuple = builder_finish(builder_tuple, head_index_seq);
    auto prev_tuple = builder_finish(builder_tuple, other_index_seq);
    LOG(INFO) << "after build, size: " << std::get<0>(head_tuple).Size();
    auto offset_vec =
        make_offset_vector(final_col_num - 1, std::get<0>(head_tuple).Size());
    VLOG(10) << "offset vec size:  " << offset_vec.size();
    // TODO: avoid copy here.

    return make_context<0, final_col_num - 1>(
        std::move(prev_tuple), std::move(std::get<0>(head_tuple)),
        std::move(offset_vec));
  }

  /////////////////////////Apply///////////////////////////////
  template <AppendOpt append_opt, JoinKind join_kind, typename CTX_HEAD_T,
            int cur_alias, int base_tag, typename... CTX_PREV, typename FUNC_T,
            typename std::enable_if<join_kind == JoinKind::InnerJoin>::type* =
                nullptr>
  static auto Apply(Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
                    FUNC_T&& func) {
    VLOG(10) << "[Apply Innerjoin]: ";
    static constexpr size_t start_tag =
        (cur_alias == -1 ? base_tag + sizeof...(CTX_PREV) : cur_alias);

    // create a copied  ctx
    auto copied_ctx(ctx);
    copied_ctx.set_sub_task_start_tag(start_tag);

    auto inner_ctx = func(std::move(copied_ctx));
    // append the record appears in copied_ctx(only last col) to current ctx.

    VLOG(10) << "After sub plan, try to obtain offset vec from: " << start_tag;
    // Obtain the mapping/offset vector between subtask'result's head and old
    // ctx's head.
    std::vector<offset_t> tmp_vec = inner_ctx.ObtainOffsetFromSubTaskStart();
    // NOTE: With fold op considered, we may lost recording in start_tag, when
    // they are filtered in sub plan.
    // We need to add them back to form a complete result.
    auto& inner_ctx_head = inner_ctx.GetMutableHead();
    if (ctx.template GetNode<start_tag>().Size() > inner_ctx_head.Size()) {
      VLOG(10) << "Make up empty entries filtered in subplan"
               << ctx.template GetNode<start_tag>().Size() << ", "
               << inner_ctx_head.Size();
      size_t old_size = inner_ctx_head.Size();
      inner_ctx_head.MakeUpTo(ctx.template GetNode<start_tag>().Size());
      // extend tmp_vec;
      size_t new_size = inner_ctx_head.Size();
      VLOG(10) << "old size: " << old_size << ", new size: " << new_size;
      for (auto i = old_size; i < new_size; ++i) {
        tmp_vec.emplace_back(i + 1);
      }
    }

    VLOG(10) << "head node size: " << inner_ctx_head.Size();
    VLOG(10) << "Obtain tmp_vec, size:" << tmp_vec.size();
    return ctx.template ApplyNode<append_opt>(std::move(inner_ctx_head),
                                              std::move(tmp_vec));
  }

  template <size_t real_x_ind, size_t real_y_ind, typename... BuilderX,
            typename... BuilderY>
  static auto BuilderConcatenate(const std::tuple<BuilderX...>& x_builders,
                             const std::tuple<BuilderY...>& y_builders) {
    auto remove_x_th_col = remove_nth_element<real_x_ind>(x_builders);
    auto remove_y_th_col = remove_nth_element<real_y_ind>(y_builders);
    return std::tuple_cat(std::move(remove_x_th_col),
                          std::move(remove_y_th_col));
  }

  template <size_t real_x_ind, size_t real_y_ind, typename CTX_HEAD_T_X,
            int cur_alias_x, int base_tag_x, typename... CTX_PREV_X,
            typename CTX_HEAD_T_Y, int cur_alias_y, int base_tag_y,
            typename... CTX_PREV_Y>
  static auto create_builder_tuple(
      const Context<CTX_HEAD_T_X, cur_alias_x, base_tag_x, CTX_PREV_X...>&
          ctx_x,
      const Context<CTX_HEAD_T_Y, cur_alias_y, base_tag_y, CTX_PREV_Y...>&
          ctx_y) {
    auto ctx_x_builder_tuple = ctx_x.CreateSetBuilder();
    auto ctx_y_builder_tuple = ctx_y.CreateSetBuilder();
    auto concatenated_builder_tuple = BuilderConcatenate<real_x_ind, real_y_ind>(
        ctx_x_builder_tuple, ctx_y_builder_tuple);
    return std::make_pair(concatenated_builder_tuple,
                          std::get<real_x_ind>(ctx_x_builder_tuple));
  }

  template <size_t real_x_ind, typename CTX_HEAD_T_X, int cur_alias_x,
            int base_tag_x, typename... CTX_PREV_X>
  static auto create_builder_tuple(
      const Context<CTX_HEAD_T_X, cur_alias_x, base_tag_x, CTX_PREV_X...>&
          ctx_x) {
    auto ctx_x_builder_tuple = ctx_x.CreateSetBuilder();
    return std::make_pair(remove_nth_element<real_x_ind>(ctx_x_builder_tuple),
                          std::get<real_x_ind>(ctx_x_builder_tuple));
  }

  template <size_t real_x_ind0, size_t real_x_ind1, size_t real_y_ind0,
            size_t real_y_ind1, typename CTX_HEAD_T_X, int cur_alias_x,
            int base_tag_x, typename... CTX_PREV_X, typename CTX_HEAD_T_Y,
            int cur_alias_y, int base_tag_y, typename... CTX_PREV_Y>
  static auto create_builder_tuple_for_join_pair(
      const Context<CTX_HEAD_T_X, cur_alias_x, base_tag_x, CTX_PREV_X...>&
          ctx_x,
      const Context<CTX_HEAD_T_Y, cur_alias_y, base_tag_y, CTX_PREV_Y...>&
          ctx_y) {
    static_assert(sizeof...(CTX_PREV_Y) ==
                  1);  // expect ctx_y has only two columns

    auto ctx_x_builder_tuple = ctx_x.CreateSetBuilder();
    return ctx_x_builder_tuple;
  }

  // InnerJoin
  // for example, join (a,b,c) with (b,c,d) we got (a,b,c,d);
  // prob: the mapping of tag_id to tag_inds may change.
  // prob: building new columns.
  template <int alias_x, int alias_y, JoinKind join_kind, typename CTX_X,
            typename CTX_Y,
            typename std::enable_if<join_kind == JoinKind::InnerJoin>::type* =
                nullptr>
  static auto Join(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    // static_assert(alias_x == alias_y);
    LOG(INFO) << "Join context with :" << gs::to_string(join_kind);
    // get all tuples from two context.
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    using ctx_y_all_data_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllData())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr int x_base_tag = CTX_X::base_tag_id;
    static constexpr int y_base_tag = CTX_Y::base_tag_id;
    LOG(INFO) << "x ele: " << x_ele_num << ", y ele num: " << y_ele_num;

    static constexpr size_t real_x_ind =
        alias_x == -1 ? x_ele_num - 1 : alias_x - x_base_tag;
    static constexpr size_t real_y_ind =
        alias_y == -1 ? y_ele_num - 1 : alias_y - y_base_tag;
    using ctx_x_ele_t = std::tuple_element_t<real_x_ind, ctx_x_all_ele_t>;
    using ctx_y_ele_t = std::tuple_element_t<real_y_ind, ctx_y_all_ele_t>;
    using ctx_y_res_ele_t =
        typename gs::remove_ith_type<real_y_ind, ctx_y_all_ele_t>::type;
    using ctx_y_res_data_t =
        typename gs::remove_ith_type<real_y_ind, ctx_y_all_data_t>::type;
    static_assert(std::is_same_v<ctx_x_ele_t, ctx_y_ele_t>,
                  "Join on different type is not supported.");

    auto x_builder_tuple_init = ctx_x.CreateSetBuilder();
    auto y_builder_tuple_init = ctx_y.CreateSetBuilder();
    auto y_builder_tuple = remove_nth_element<real_y_ind>(y_builder_tuple_init);
    auto all_builder = std::tuple_cat(x_builder_tuple_init, y_builder_tuple);

    std::unordered_map<
        ctx_x_ele_t, std::vector<std::tuple<ctx_y_res_ele_t, ctx_y_res_data_t>>>
        join_key_map;
    {
      for (auto iter : ctx_y) {
        auto y_ele = iter.GetAllElement();
        auto y_data = iter.GetAllData();
        auto y_key = std::get<real_y_ind>(y_ele);
        if (join_key_map.find(y_key) == join_key_map.end()) {
          join_key_map[y_key] =
              std::vector<std::tuple<ctx_y_res_ele_t, ctx_y_res_data_t>>();
        }
        auto y_res_ele = remove_nth_element<real_y_ind>(y_ele);
        auto y_res_data = remove_nth_element<real_y_ind>(y_data);
        join_key_map[y_key].emplace_back(
            std::make_tuple(y_res_ele, y_res_data));
      }
    }
    LOG(INFO) << " key map valid num: " << join_key_map.size();

    for (auto x_iter : ctx_x) {
      auto ele = x_iter.GetAllElement();
      auto data = x_iter.GetAllData();
      // the sequence of x_tuple shall not change
      auto x_key = std::get<real_x_ind>(ele);
      if (join_key_map.find(x_key) != join_key_map.end()) {
        for (auto y_res : join_key_map[x_key]) {
          auto y_res_ele = std::get<0>(y_res);
          auto y_res_data = std::get<1>(y_res);
          auto res_ele = std::tuple_cat(ele, y_res_ele);
          auto res_data = std::tuple_cat(data, y_res_data);
          insert_into_builder_v2(all_builder, res_ele, res_data);
        }
      }
    }

    auto built_tuple = builder_finish(
        all_builder, std::make_index_sequence<x_ele_num + y_ele_num - 1>{});
    LOG(INFO) << "after build, size: " << std::get<0>(built_tuple).Size();
    auto offset_vec = make_offset_vector(x_ele_num + y_ele_num - 2,
                                         std::get<0>(built_tuple).Size());
    VLOG(10) << "offset vec size:  " << offset_vec.size();
    auto prev_tuple =
        gs::remove_nth_element<x_ele_num + y_ele_num - 2>(built_tuple);
    auto head_tuple = std::get<x_ele_num + y_ele_num - 2>(built_tuple);

    return make_context<0, x_ele_num + y_ele_num - 2>(
        std::move(prev_tuple), std::move(head_tuple), std::move(offset_vec));
  }

  // We assume ctx_x and ctx_y doesn't contains duplicates.
  // Can only join on last tag.
  // join on alias_x0 == alias_y0, alias_x1 == alias_y1.
  // the resulted context will contains the ctx x, we assume ctx_y contains no
  // additional ele.
  template <int alias_x0, int alias_x1, int alias_y0, int alias_y1,
            JoinKind join_kind, typename CTX_X, typename CTX_Y,
            typename std::enable_if<join_kind == JoinKind::InnerJoin>::type* =
                nullptr>
  static auto Join(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    LOG(INFO) << "Join context with :" << gs::to_string(join_kind);

    // get all tuples from two context.
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr int x_base_tag = CTX_X::base_tag_id;
    static constexpr int y_base_tag = CTX_Y::base_tag_id;
    LOG(INFO) << "x ele: " << x_ele_num << ", y ele num: " << y_ele_num;

    static constexpr size_t real_x_ind0 =
        alias_x0 == -1 ? x_ele_num - 1 : alias_x0 - x_base_tag;
    static constexpr size_t real_x_ind1 =
        alias_x1 == -1 ? x_ele_num - 1 : alias_x1 - x_base_tag;
    static constexpr size_t real_y_ind0 =
        alias_y0 == -1 ? y_ele_num - 1 : alias_y0 - y_base_tag;
    static constexpr size_t real_y_ind1 =
        alias_y1 == -1 ? y_ele_num - 1 : alias_y1 - y_base_tag;
    using ctx_x_ele_t0 = std::tuple_element_t<real_x_ind0, ctx_x_all_ele_t>;
    using ctx_x_ele_t1 = std::tuple_element_t<real_x_ind1, ctx_x_all_ele_t>;
    using ctx_y_ele_t0 = std::tuple_element_t<real_y_ind0, ctx_y_all_ele_t>;
    using ctx_y_ele_t1 = std::tuple_element_t<real_y_ind1, ctx_y_all_ele_t>;
    using ctx_x_ele_t = std::pair<ctx_x_ele_t0, ctx_x_ele_t1>;
    using ctx_y_ele_t = std::pair<ctx_y_ele_t0, ctx_y_ele_t1>;
    static_assert(std::is_same_v<ctx_x_ele_t, ctx_y_ele_t>,
                  "Join on different type is not supported.");

    auto builder_tuple =
        create_builder_tuple_for_join_pair<real_x_ind0, real_x_ind1,
                                           real_y_ind0, real_y_ind1>(ctx_x,
                                                                     ctx_y);

    std::unordered_map<ctx_x_ele_t, int, boost::hash<ctx_x_ele_t>> join_key_map;
    {
      for (auto iter : ctx_x) {
        auto x_ele = iter.GetAllElement();
        auto pair = std::make_pair(std::get<real_x_ind0>(x_ele),
                                   std::get<real_x_ind1>(x_ele));
        if (join_key_map.find(pair) == join_key_map.end()) {
          join_key_map[pair] = 1;
        }
      }
    }
    {
      for (auto iter : ctx_y) {
        auto y_ele = iter.GetAllElement();
        auto pair = std::make_pair(std::get<real_y_ind0>(y_ele),
                                   std::get<real_y_ind1>(y_ele));
        if (join_key_map.find(pair) != join_key_map.end()) {
          join_key_map[pair] += 1;
        }
      }
    }
    LOG(INFO) << "total entry size in map: " << join_key_map.size();

    for (auto iter : ctx_x) {
      auto eles = iter.GetAllElement();
      auto datas = iter.GetAllData();
      auto pair = std::make_pair(std::get<real_x_ind0>(eles),
                                 std::get<real_x_ind1>(eles));
      if (join_key_map.find(pair) != join_key_map.end() &&
          join_key_map[pair] == 2) {
        // join.
        insert_into_builder_v2(builder_tuple, eles, datas);
      }
    }

    auto built_tuple =
        builder_finish(builder_tuple, std::make_index_sequence<x_ele_num>{});
    LOG(INFO) << "after build, size: " << std::get<0>(built_tuple).Size();
    auto offset_vec =
        make_offset_vector(x_ele_num - 1, std::get<0>(built_tuple).Size());
    VLOG(10) << "offset vec size:  " << offset_vec.size();
    auto prev_tuple = gs::remove_nth_element<x_ele_num - 1>(built_tuple);
    auto head_tuple = std::get<x_ele_num - 1>(built_tuple);

    return make_context<0, x_ele_num - 1>(
        std::move(prev_tuple), std::move(head_tuple), std::move(offset_vec));
  }

  // We assume ctx_x and ctx_y doesn't contains duplicates. ????
  // After antijoin, we will only preserve elements in left ctx.
  // 1. put all ctx_y eles into hash_set
  // 2. iterate ctx_x, building a subset_indices array.
  // 3. subset the head node and merge_offset with back
  template <
      int alias_x0, int alias_x1, int alias_y0, int alias_y1,
      JoinKind join_kind, typename CTX_X, typename CTX_Y,
      typename std::enable_if<join_kind == JoinKind::AntiJoin>::type* = nullptr>
  static auto Join(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    LOG(INFO) << "Anti Join context with :" << gs::to_string(join_kind);

    // get all tuples from two context.
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr int x_base_tag = CTX_X::base_tag_id;
    static constexpr int y_base_tag = CTX_Y::base_tag_id;
    LOG(INFO) << "x ele: " << x_ele_num << ", y ele num: " << y_ele_num;

    static constexpr size_t real_x_ind0 =
        alias_x0 == -1 ? x_ele_num - 1 : alias_x0 - x_base_tag;
    static constexpr size_t real_x_ind1 =
        alias_x1 == -1 ? x_ele_num - 1 : alias_x1 - x_base_tag;
    static constexpr size_t real_y_ind0 =
        alias_y0 == -1 ? y_ele_num - 1 : alias_y0 - y_base_tag;
    static constexpr size_t real_y_ind1 =
        alias_y1 == -1 ? y_ele_num - 1 : alias_y1 - y_base_tag;
    using ctx_x_ele_t0 = std::tuple_element_t<real_x_ind0, ctx_x_all_ele_t>;
    using ctx_x_ele_t1 = std::tuple_element_t<real_x_ind1, ctx_x_all_ele_t>;
    using ctx_y_ele_t0 = std::tuple_element_t<real_y_ind0, ctx_y_all_ele_t>;
    using ctx_y_ele_t1 = std::tuple_element_t<real_y_ind1, ctx_y_all_ele_t>;
    using ctx_x_ele_t = std::pair<ctx_x_ele_t0, ctx_x_ele_t1>;
    using ctx_y_ele_t = std::pair<ctx_y_ele_t0, ctx_y_ele_t1>;
    static_assert(std::is_same_v<ctx_x_ele_t, ctx_y_ele_t>,
                  "Join on different type is not supported.");

    double t0 = -grape::GetCurrentTime();
    std::unordered_set<ctx_x_ele_t, boost::hash<ctx_x_ele_t>> join_key_set;
    {
      for (auto iter : ctx_y) {
        auto y_ele = iter.GetAllElement();
        auto pair = std::make_pair(std::get<real_y_ind0>(y_ele),
                                   std::get<real_y_ind1>(y_ele));

        join_key_set.insert(pair);
      }
    }
    auto& cur_ = ctx_x.GetMutableHead();
    LOG(INFO) << "total entry size in set: " << join_key_set.size()
              << ", ctx x size: " << cur_.Size();
    {
      std::stringstream ss;
      for (auto iter : join_key_set) {
        ss << gs::to_string(iter) << ", ";
      }
      LOG(INFO) << "join key set: " << ss.str();
    }

    std::vector<size_t> active_indices;
    std::vector<size_t> new_offsets;
    new_offsets.reserve(cur_.Size() + 1);
    new_offsets.emplace_back(0);
    {
      size_t cur_ind = 0;
      ctx_x_ele_t prev_tuple;
      bool prev_res = false;
      for (auto iter : ctx_x) {
        auto x_ele = iter.GetAllElement();
        auto pair = std::make_pair(std::get<real_x_ind0>(x_ele),
                                   std::get<real_x_ind1>(x_ele));
        LOG(INFO) << "pair: " << pair.first << ", " << pair.second;
        if (cur_ind != 0) {
          if (prev_tuple == pair && prev_res) {
            LOG(INFO) << gs::to_string(prev_tuple)
                      << " == " << gs::to_string(pair);
            active_indices.emplace_back(cur_ind);
            new_offsets.emplace_back(active_indices.size());
            cur_ind += 1;
            continue;
          }
        }
        if (join_key_set.find(pair) == join_key_set.end()) {
          active_indices.emplace_back(cur_ind);
          prev_res = true;
        } else {
          prev_res = false;
        }
        prev_tuple = pair;
        cur_ind += 1;
        new_offsets.emplace_back(active_indices.size());
      }
    }
    LOG(INFO) << "active indices size: " << active_indices.size();

    t0 += grape::GetCurrentTime();
    LOG(INFO) << "filter time: " << t0;

    cur_.SubSetWithIndices(active_indices);
    ctx_x.merge_offset_with_back(new_offsets);
    return ctx_x;
  }

  // We assume ctx_x and ctx_y doesn't contains duplicates.
  // filter ctx_x with ctx_y;
  template <
      int alias_x, int alias_y, JoinKind join_kind, typename CTX_X,
      typename CTX_Y,
      typename std::enable_if<join_kind == JoinKind::AntiJoin>::type* = nullptr>
  static auto Join(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    LOG(INFO) << "Join context with :" << gs::to_string(join_kind);

    // get all tuples from two context.
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t real_x_ind =
        alias_x == -1 ? x_ele_num - 1 : alias_x - CTX_X::base_tag_id;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr size_t real_y_ind =
        alias_y == -1 ? y_ele_num - 1 : alias_y - CTX_Y::base_tag_id;
    using ctx_x_join_key_t =
        typename std::tuple_element<real_x_ind, ctx_x_all_ele_t>::type;
    using ctx_y_join_key_t =
        typename gs::tuple_element<real_y_ind, ctx_y_all_ele_t>::type;
    static_assert(std::is_same_v<ctx_x_join_key_t, ctx_y_join_key_t>);

    std::unordered_set<ctx_y_join_key_t> key_set;
    for (auto iter : ctx_y) {
      auto ele = iter.GetAllElement();
      key_set.insert(gs::get_from_tuple<alias_y>(ele));
    }
    std::vector<size_t> active_indices;
    std::vector<size_t> new_offsets;
    auto& x_head = ctx_x.GetMutableHead();
    new_offsets.reserve(x_head.Size() + 1);
    new_offsets.emplace_back(0);
    size_t cur_ind = 0;
    for (auto iter : ctx_x) {
      auto ele = iter.GetAllElement();
      auto x_key = gs::get_from_tuple<real_x_ind>(ele);
      if (key_set.find(x_key) == key_set.end()) {
        active_indices.emplace_back(cur_ind);
      }
      cur_ind += 1;
      new_offsets.emplace_back(active_indices.size());
    }
    x_head.SubSetWithIndices(active_indices);
    ctx_x.merge_offset_with_back(new_offsets);
    return std::move(ctx_x);
  }

  // intersect two context on the specified key, it is expected that two
  // context only differs at the last column
  template <int alias_x, int alias_y, typename CTX_X, typename CTX_Y,
            typename std::enable_if<std::is_same<
                typename CTX_X::prev_tuple_t,
                typename CTX_Y::prev_tuple_t>::value>::type* = nullptr>
  static auto Intersect(CTX_X&& ctx_x, CTX_Y&& ctx_y) {
    using ctx_x_iter_t = typename CTX_X::iterator;
    using ctx_y_iter_t = typename CTX_Y::iterator;
    // the prev column (the last column in prev_tuple should be the same.)
    using ctx_x_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_x_iter_t>().GetAllElement())>;
    using ctx_y_all_ele_t = std::remove_reference_t<decltype(
        std::declval<ctx_y_iter_t>().GetAllElement())>;
    static constexpr size_t x_ele_num = std::tuple_size_v<ctx_x_all_ele_t>;
    static constexpr size_t y_ele_num = std::tuple_size_v<ctx_y_all_ele_t>;
    static constexpr int x_base_tag = CTX_X::base_tag_id;
    static constexpr int y_base_tag = CTX_Y::base_tag_id;
    static constexpr size_t real_alias_x =
        alias_x == -1 ? x_ele_num - 1 : alias_x - x_base_tag;
    static constexpr size_t real_alias_y =
        alias_y == -1 ? y_ele_num - 1 : alias_y - y_base_tag;
    static_assert(real_alias_x > 0 && real_alias_y > 0);
    static_assert(real_alias_x == real_alias_y);
    static_assert(real_alias_x == x_ele_num - 1);

    auto& head_x = ctx_x.GetMutableHead();
    auto& head_y = ctx_y.GetMutableHead();
    auto left_repeat_array = ctx_x.ObtainOffsetFromTag(real_alias_x - 1);
    auto right_repeat_array = ctx_y.ObtainOffsetFromTag(real_alias_y - 1);
    CHECK(left_repeat_array.size() == right_repeat_array.size())
        << "left size " << left_repeat_array.size() << " right size "
        << right_repeat_array.size();

    std::vector<size_t> active_indices, new_offsets;
    std::tie(active_indices, new_offsets) =
        intersect_impl(head_x, head_y, left_repeat_array, right_repeat_array);
    head_x.SubSetWithIndices(active_indices);
    ctx_x.merge_offset_with_back(new_offsets);
    return ctx_x;
  }

  // intersect for rowVertexSet and twoLabelVertexSet
  template <typename LabelT, typename VID_T, typename... X_T, typename... Y_T>
  static std::pair<std::vector<size_t>, std::vector<size_t>> intersect_impl(
      const RowVertexSet<LabelT, VID_T, X_T...>& head_x,
      const RowVertexSet<LabelT, VID_T, Y_T...>& head_y,
      const std::vector<size_t>& left_repeat_array,
      const std::vector<size_t>& right_repeat_array) {
    std::vector<offset_t>
        active_indices;  // got a active_indices array to filter ctx_x.

    std::vector<offset_t> new_offsets;
    new_offsets.emplace_back(0);

    double t0 = -grape::GetCurrentTime();
    auto builder = head_x.CreateBuilder();
    auto x_vec = head_x.GetVertices();
    auto y_vec = head_y.GetVertices();
    active_indices.reserve(std::min(x_vec.size(), y_vec.size()));
    VID_T max_vid = 0;
    for (auto vid : x_vec) {
      max_vid = std::max(max_vid, vid);
    }
    for (auto vid : y_vec) {
      max_vid = std::max(max_vid, vid);
    }
    grape::Bitset bitset;
    bitset.init(max_vid + 1);
    CHECK(left_repeat_array.size() == right_repeat_array.size());
    for (size_t i = 0; i + 1 < left_repeat_array.size(); ++i) {
      auto x_start = left_repeat_array[i];
      auto x_end = left_repeat_array[i + 1];
      auto y_start = right_repeat_array[i];
      auto y_end = right_repeat_array[i + 1];
      if (x_start == x_end || y_start == y_end) {
        for (auto i = x_start; i < x_end; ++i) {
          new_offsets.emplace_back(active_indices.size());
        }
        continue;
      } else {
        for (auto i = y_start; i < y_end; ++i) {
          bitset.set_bit(y_vec[i]);
        }
        for (auto i = x_start; i < x_end; ++i) {
          if (bitset.get_bit(x_vec[i])) {
            active_indices.emplace_back(i);
          }
          new_offsets.emplace_back(active_indices.size());
        }
        bitset.clear();
      }
    }

    t0 += grape::GetCurrentTime();
    LOG(INFO) << "Intersect cost: " << t0;
    return std::make_pair(std::move(active_indices), std::move(new_offsets));
  }

  // intersect for row set and two label set.
  template <typename LabelT, typename VID_T, typename... X_T, typename... Y_T>
  static std::pair<std::vector<size_t>, std::vector<size_t>> intersect_impl(
      const RowVertexSet<LabelT, VID_T, X_T...>& head_x,
      const TwoLabelVertexSet<VID_T, LabelT, Y_T...>& head_y,
      const std::vector<size_t>& left_repeat_array,
      const std::vector<size_t>& right_repeat_array) {
    std::vector<offset_t>
        active_indices;  // got a active_indices array to filter ctx_x.
    std::vector<offset_t> new_offsets;
    new_offsets.emplace_back(0);

    size_t ind_x = 0;
    auto x_iter = head_x.begin();
    auto x_end = head_x.end();
    auto y_iter = head_y.begin();
    auto y_end = head_y.end();
    // check whether there is same label in head_y
    auto& y_labels = head_y.GetLabels();
    int valid_label_ind = -1;
    if (y_labels[0] == head_x.GetLabel()) {
      valid_label_ind = 0;
    } else if (y_labels[1] == head_x.GetLabel()) {
      valid_label_ind = 1;
    }
    if (valid_label_ind == -1) {
      while (x_iter != x_end) {
        new_offsets.emplace_back(active_indices.size());
        ind_x += 1;
        ++x_iter;
      }
      return std::make_pair(std::move(active_indices), std::move(new_offsets));
    } else {
      auto& vertices = head_y.GetVertices();
      auto& bitset = head_y.GetBitset();
      for (size_t i = 0; i + 1 < left_repeat_array.size(); ++i) {
        auto left_min = left_repeat_array[i];
        auto left_max = left_repeat_array[i + 1];
        auto right_min = right_repeat_array[i];
        auto right_max = right_repeat_array[i + 1];
        if (left_min == left_max || right_min == right_max) {
          // skip
          for (auto tmp = left_min; tmp < left_max; ++tmp) {
            new_offsets.emplace_back(active_indices.size());
            ind_x += 1;
            ++x_iter;
          }
          for (auto tmp = right_min; tmp < right_max; ++tmp) {
            ++y_iter;
          }
        } else {
          // intersect
          std::unordered_set<VID_T> set;
          for (auto tmp = right_min; tmp < right_max; ++tmp) {
            auto ele = y_iter.GetElement();
            if (ele.first == valid_label_ind) {
              set.insert(ele.second);
            }
            ++y_iter;
          }
          for (auto tmp = left_min; tmp < left_max; ++tmp) {
            auto ele = x_iter.GetElement();
            if (set.find(ele) != set.end()) {
              active_indices.emplace_back(ind_x);
            }
            ind_x += 1;
            ++x_iter;
            new_offsets.emplace_back(active_indices.size());
          }
        }
      }
      return std::make_pair(std::move(active_indices), std::move(new_offsets));
    }
  }

  template <typename... BuilderT, size_t... Is>
  static auto builder_finish(std::tuple<BuilderT...>& builder_tuple,
                             std::index_sequence<Is...>) {
    return std::make_tuple(std::get<Is>(builder_tuple).Build()...);
  }

  template <size_t Start, typename... BuilderT, size_t... Is>
  static auto builder_finish_right_impl(std::tuple<BuilderT...>& builder_tuple,
                                        std::index_sequence<Is...>) {
    return std::make_tuple(std::get<Is + Start>(builder_tuple).Build()...);
  }

  template <size_t S, size_t E, typename... BuilderT>
  static auto builder_finish_right(std::tuple<BuilderT...>& builder_tuple) {
    auto ind_seq = std::make_index_sequence<E - S>{};
    return builder_finish_right_impl<S>(builder_tuple, ind_seq);
  }

  // if Is < Ind
  template <size_t Is, size_t Ind, typename... SetBuilder,
            typename KeySetBuilder,
            typename std::enable_if<(Is < Ind)>::type* = nullptr>
  static auto builder_finish_left_impl(std::tuple<SetBuilder...>& set_builder,
                                       KeySetBuilder& key_builder) {
    return std::get<Is>(set_builder).Build();
  }
  // if Is == Ind
  template <size_t Is, size_t Ind, typename... SetBuilder,
            typename KeySetBuilder,
            typename std::enable_if<(Is == Ind)>::type* = nullptr>
  static auto builder_finish_left_impl(std::tuple<SetBuilder...>& set_builder,
                                       KeySetBuilder& key_builder) {
    return key_builder.Build();
  }

  // if Is > Ind
  template <size_t Is, size_t Ind, typename... SetBuilder,
            typename KeySetBuilder,
            typename std::enable_if<(Is > Ind)>::type* = nullptr>
  static auto builder_finish_left_impl(std::tuple<SetBuilder...>& set_builder,
                                       KeySetBuilder& key_builder) {
    return std::get<Is - 1>(set_builder).Build();
  }

  template <size_t Ind, typename... BuilderT, typename KeyBuilder, size_t... Is>
  static auto builder_finish_left_impl(std::tuple<BuilderT...>& builder_tuple,
                                       KeyBuilder& key_builder,
                                       std::index_sequence<Is...>) {
    return std::make_tuple(
        builder_finish_left_impl<Is, Ind>(builder_tuple, key_builder)...);
  }

  template <size_t Ind, size_t X_Builder_num, typename... BuilderT,
            typename KeyBuilder>
  static auto builder_finish_left(std::tuple<BuilderT...>& builder_tuple,
                                  KeyBuilder& key_builder) {
    auto ind_seq = std::make_index_sequence<X_Builder_num + 1>{};
    return builder_finish_left_impl<Ind>(builder_tuple, key_builder, ind_seq);
  }

  template <typename... BuilderT, typename... ELE, typename... DATA>
  static void insert_into_builder_v2(std::tuple<BuilderT...>& builder_tuple,
                                     const std::tuple<ELE...>& ele,
                                     const std::tuple<DATA...>& data) {
    static_assert(sizeof...(BuilderT) == sizeof...(ELE),
                  "Builder number and element number not match");
    static_assert(sizeof...(BuilderT) == sizeof...(DATA),
                  "Builder number and data number not match");
    insert_into_builder_v2(builder_tuple, ele, data,
                           std::make_index_sequence<sizeof...(ELE)>{});
  }

  template <typename... BuilderT, typename... ELE, typename... DATA,
            size_t... Is>
  static void insert_into_builder_v2(std::tuple<BuilderT...>& builder_tuple,
                                     const std::tuple<ELE...>& ele,
                                     const std::tuple<DATA...>& data,
                                     std::index_sequence<Is...>) {
    // (std::get<Is>(builder_tuple).Insert(std::get<Is>(ele)), ...);
    (insert_into_builder_v2_impl(std::get<Is>(builder_tuple), std::get<Is>(ele),
                                 std::get<Is>(data)),
     ...);
  }

  template <size_t base_tag, size_t real_ind, typename... BuilderT,
            typename... ELE, size_t... Is>
  static void insert_into_builder(std::tuple<BuilderT...>& builder_tuple,
                                  const std::tuple<ELE...>& ele,
                                  std::index_sequence<Is...>) {
    (insert_into_builder<base_tag, real_ind, Is>(builder_tuple, ele), ...);
  }

  // if cur_ind == real_ind, skip inserting.
  template <size_t base_tag, size_t real_ind, size_t cur_ind,
            typename... BuilderT, typename... ELE,
            typename std::enable_if<real_ind == cur_ind>::type* = nullptr>
  static inline void insert_into_builder(std::tuple<BuilderT...>& builder_tuple,
                                         const std::tuple<ELE...>& ele) {}

  // if cur_ind > real_ind, insert
  template <size_t base_tag, size_t real_ind, size_t cur_ind,
            typename... BuilderT, typename... ELE,
            typename std::enable_if<(real_ind < cur_ind)>::type* = nullptr>
  static inline void insert_into_builder(std::tuple<BuilderT...>& builder_tuple,
                                         const std::tuple<ELE...>& ele) {
    static constexpr size_t new_ind = (cur_ind - 1);
    std::get<base_tag + new_ind>(builder_tuple).Insert(std::get<cur_ind>(ele));
  }

  // if cur_ind < real_ind, insert
  template <size_t base_tag, size_t real_ind, size_t cur_ind,
            typename... BuilderT, typename... ELE,
            typename std::enable_if<(real_ind > cur_ind)>::type* = nullptr>
  static inline void insert_into_builder(std::tuple<BuilderT...>& builder_tuple,
                                         const std::tuple<ELE...>& ele) {
    std::get<base_tag + cur_ind>(builder_tuple).Insert(std::get<cur_ind>(ele));
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_BASE_ENGINE_H_
