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

#ifndef GRAPHSCOPE_OPERATOR_FUSED_OPERATOR_H_
#define GRAPHSCOPE_OPERATOR_FUSED_OPERATOR_H_

#include "flex/engines/hqps/engine/context.h"

#include "flex/engines/hqps/ds/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/engines/hqps/engine/params.h"

#include "flex/engines/hqps/engine/operator/get_v.h"
#include "flex/engines/hqps/engine/operator/sort.h"

#include "flat_hash_map/flat_hash_map.hpp"
namespace gs {

template <typename... ORDER_PAIRS>
struct FusedSorter {
  FusedSorter(TupleComparator<ORDER_PAIRS...>& tuple_comparator)
      : comparator_(tuple_comparator) {}

  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return comparator_(std::get<1>(lhs), std::get<1>(rhs));
  }
  TupleComparator<ORDER_PAIRS...> comparator_;
};

template <typename GRAPH_INTERFACE>
class FusedOperator {
 public:
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  using vertex_set_t = DefaultRowVertexSet<label_id_t, vertex_id_t>;

  template <
      int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename EXPR, typename LabelT,
      typename EDGE_FILTER_T, typename... EDGE_T, size_t num_labels,
      typename GET_V_EXPR, typename... VERTEX_T, typename... ORDER_PAIRS,
      typename CTX_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>>
  static auto PathExpandVNoPropsAndFilterVAndSort(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      PathExpandOpt<LabelT, EXPR, EDGE_FILTER_T, EDGE_T...>&& path_expand_opt,
      GetVOpt<LabelT, num_labels, GET_V_EXPR, VERTEX_T...>&& get_v_opt,
      SortOrderOpt<ORDER_PAIRS...>&& sort_opt) {
    auto& vertex_set = gs::Get<alias_to_use>(ctx);

    // about path expand
    auto cur_label = vertex_set.GetLabel();
    auto& range = path_expand_opt.range_;
    auto& edge_expand_opt = path_expand_opt.edge_expand_opt_;
    auto& path_opt_get_v_opt = path_expand_opt.get_v_opt_;  // not used.

    // about sort;
    auto& sort_pairs = sort_opt.ordering_pairs_;
    auto sort_limit = sort_opt.range_.limit_;
    // FIXME: only support sort the last col of vertices.
    using sort_tuple_t = std::tuple<typename ORDER_PAIRS::prop_t...>;
    using full_ele_tuple_t =
        std::tuple<size_t, sort_tuple_t,
                   vertex_id_t>;  // src_ind, cur_vid, prop_tuple

    // In general we need first extract properties for prev sets.
    // but since in current cases, we only need the props from pathExpandV, we
    // skip this.
    TupleComparator<ORDER_PAIRS...> tuple_comparator(sort_pairs);
    FusedSorter sorter(tuple_comparator);
    std::queue<std::pair<size_t, vertex_id_t>>
        q;  // the ind of parent and the vertex_id.
            // std::unordered_map<vertex_id_t, uint8_t> distance;
    ska::flat_hash_map<vertex_id_t, uint8_t> distance;
    std::priority_queue<full_ele_tuple_t, std::vector<full_ele_tuple_t>,
                        FusedSorter<ORDER_PAIRS...>>
        pq(sorter);

    // init start vertices;
    sort_tuple_t empty_tuple;
    sort_tuple_t& top_tuple = empty_tuple;

    bool first = true;
    auto& src_vertices_vec = vertex_set.GetVertices();
    {
      for (auto v : src_vertices_vec) {
        distance[v] = 0;
      }
      for (auto i = 0; i < src_vertices_vec.size(); ++i) {
        q.push(std::make_pair(i, src_vertices_vec[i]));
      }
      if (range.start_ <= 0 && range.limit_ > 0) {
        std::vector<size_t> indices(src_vertices_vec.size());
        std::vector<size_t> offset(src_vertices_vec.size() + 1);
        for (auto i = 0; i < src_vertices_vec.size(); ++i) {
          indices[i] = i;
          offset[i] = i;
        }
        offset[src_vertices_vec.size()] = src_vertices_vec.size();
        try_emplace_vertices(pq, top_tuple, src_vertices_vec, indices, offset,
                             cur_label, time_stamp, graph, ctx, sort_pairs, 0,
                             sort_limit, first, tuple_comparator);
      }
    }
    std::vector<vertex_id_t> tmp_vec;
    std::vector<size_t> tmp_src_ind;
    tmp_vec.resize(1);
    tmp_src_ind.resize(1);
    uint8_t cur_dep = 0;
    bool flag = false;
    double get_v_time = 0.0;
    double filter_dist_time = 0.0;
    double filter_v_time = 0.0;
    double emplace_time = 0.0;
    while (!q.empty()) {
      // size_t tmp_size = q.size();
      // tmp_vec.reserve(tmp_size);
      // tmp_src_ind.reserve(tmp_size);
      // LOG(INFO) << "cur dep: " << (int) cur_dep;

      size_t ind;
      vertex_id_t poped;
      std::tie(ind, poped) = q.front();
      q.pop();
      if (cur_dep != distance[poped]) {
        // the depth has increased;
        if (pq.size() == sort_limit) {
          LOG(INFO) << "pq size reach limit: " << sort_limit
                    << ", and cur_dep neq new dep" << (int) cur_dep << ", "
                    << (int) distance[poped];
          break;
        }
        cur_dep = distance[poped];
      }
      tmp_src_ind[0] = ind;
      tmp_vec[0] = poped;

      // if (flag) {
      //   break;
      // }
      if (cur_dep + 1 >= range.limit_) {
        break;
      }
      // LOG(INFO) << "after filter,size: " << tmp_vec.size()
      //           << ", new dep: " << (int32_t) cur_dep;
      // get other vertices.
      double t0 = -grape::GetCurrentTime();
      auto vids_and_offset = graph.GetOtherVerticesV2(
          time_stamp, cur_label, edge_expand_opt.other_label_,
          edge_expand_opt.edge_label_, tmp_vec,
          gs::to_string(edge_expand_opt.dir_), INT_MAX);
      t0 += grape::GetCurrentTime();
      get_v_time += t0;

      double t1 = -grape::GetCurrentTime();
      auto& new_vertices = vids_and_offset.first;
      auto& new_v_offset = vids_and_offset.second;

      {
        size_t cur_valid = 0;
        for (auto i = 0; i < new_vertices.size(); ++i) {
          auto vid = new_vertices[i];
          if (distance.find(vid) == distance.end()) {
            distance[vid] = cur_dep + 1;
            q.emplace(std::make_pair(tmp_src_ind[0], vid));
            new_vertices[cur_valid++] = vid;
          }
        }
        new_vertices.resize(cur_valid);
        new_v_offset[1] = cur_valid;
      }
      t1 += grape::GetCurrentTime();
      filter_dist_time += t1;

      double t2 = -grape::GetCurrentTime();
      auto tmp_vertex_set =
          MakeDefaultRowVertexSet(std::move(new_vertices), cur_label);
      auto vertices_and_offset = GetVertex<GRAPH_INTERFACE>::GetNoPropV(
          time_stamp, graph, tmp_vertex_set, get_v_opt);
      t2 += grape::GetCurrentTime();
      filter_v_time += t2;
      //  LOG(INFO) << "getVertices cost: "<< t0 << ", filter distance cost: "
      //  << t1 <<", get v cost: " << t2;
      // LOG(INFO) << "After filter: old set size:
      //" << tmp_vertex_set.Size()
      // << ", to new set size: " << vertices_and_offset.first.Size();

      auto& filtered_vertices = vertices_and_offset.first;
      // get the src_ind of filtered vertices.

      {
        auto& offset = vertices_and_offset.second;
        CHECK(tmp_vertex_set.Size() + 1 == offset.size());
        for (auto i = 0; i < new_v_offset.size(); ++i) {
          new_v_offset[i] = offset[new_v_offset[i]];
        }
      }

      // must be in nrage.
      auto& filter_vec = filtered_vertices.GetVertices();
      double t3 = -grape::GetCurrentTime();
      try_emplace_vertices(pq, top_tuple, filter_vec, tmp_src_ind, new_v_offset,
                           cur_label, time_stamp, graph, ctx, sort_pairs,
                           cur_dep + 1, sort_limit, first, tuple_comparator);
      t3 += grape::GetCurrentTime();
      emplace_time += t3;

      // filter the already visited vertices.
      // tmp_vec.clear();
      // tmp_src_ind.clear();
    }
    LOG(INFO) << "After path expand : get vertices time: " << get_v_time
              << ", filter dist time: " << filter_dist_time
              << ", filter v time: " << filter_v_time
              << ", emplace time: " << emplace_time;
    std::vector<vertex_id_t> res_vids;
    std::vector<std::tuple<Dist>> res_dists;
    std::vector<offset_t> res_offsets;

    std::vector<typename CTX_T::index_ele_tuples_t> new_ctx_tuples;
    std::vector<full_ele_tuple_t> full_tuples;
    {
      full_tuples.reserve(pq.size());
      while (!pq.empty()) {
        auto tuple = pq.top();
        pq.pop();
        full_tuples.emplace_back(std::move(tuple));
      }
      std::reverse(full_tuples.begin(), full_tuples.end());
      // flat prev context, and addNode with new offsets, which is
      // [0,1,...]
      std::vector<typename CTX_T::index_ele_tuples_t> old_ctx_tuples;
      old_ctx_tuples.reserve(ctx.GetHead().Size());
      for (auto iter : ctx) {
        old_ctx_tuples.emplace_back(iter.GetAllIndexElement());
      }

      new_ctx_tuples.reserve(full_tuples.size());
      for (auto i = 0; i < full_tuples.size(); ++i) {
        auto& full_ele = full_tuples[i];
        auto src_ind = std::get<0>(full_ele);
        new_ctx_tuples.push_back(old_ctx_tuples[src_ind]);
      }
    }

    auto new_ctx = ctx.Flat(std::move(new_ctx_tuples));

    res_vids.reserve(full_tuples.size());
    res_dists.reserve(full_tuples.size());
    res_offsets.reserve(full_tuples.size());
    res_offsets.emplace_back(0);
    for (auto i = 0; i < full_tuples.size(); ++i) {
      auto vid = std::get<2>(full_tuples[i]);
      res_vids.emplace_back(vid);
      res_dists.emplace_back(std::make_tuple(distance[vid]));
      res_offsets.emplace_back(i + 1);
    }

    auto new_set = make_row_vertex_set(std::move(res_vids), cur_label,
                                       std::move(res_dists), {"dist"});

    return new_ctx.template AddNode<res_alias>(std::move(new_set),
                                               std::move(res_offsets));
  }

  template <
      typename sort_tuple_t, typename LabelT, typename CTX_T,
      typename... ORDER_PAIRS,
      typename full_ele_tuple_t = std::tuple<size_t, vertex_id_t, sort_tuple_t>>
  static void try_emplace_vertices(
      std::priority_queue<full_ele_tuple_t, std::vector<full_ele_tuple_t>,
                          FusedSorter<ORDER_PAIRS...>>& pq,
      sort_tuple_t& top_tuple,
      const std::vector<vertex_id_t>& vertices_to_insert,
      const std::vector<size_t>& src_indices,
      const std::vector<size_t>& new_v_offset, LabelT cur_label,
      int64_t time_stamp, const GRAPH_INTERFACE& graph, CTX_T& ctx,
      std::tuple<ORDER_PAIRS...>& order_pairs, uint8_t cur_dist,
      int64_t sort_limit, bool& first,
      TupleComparator<ORDER_PAIRS...>& tuple_comparator) {
    // LOG(INFO) << "cur dist: " << (int32_t) cur_dist;
    // CHECK(src_indices.size() == vertices_to_insert.size());
    CHECK(src_indices.size() == new_v_offset.size() - 1);
    if (vertices_to_insert.size() <= 0) {
      return;
    }
    auto prop_vec_tuple = get_prop_vec_for_row_vertices(
        vertices_to_insert, cur_dist, cur_label, time_stamp, graph, ctx,
        order_pairs, std::make_index_sequence<sizeof...(ORDER_PAIRS)>{});
    // LOG(INFO) << "Prop vec tuple of size: "
    // << std::get<0>(prop_vec_tuple).size();
    size_t start_ind = 0;
    auto index_seq = std::make_index_sequence<sizeof...(ORDER_PAIRS)>{};
    size_t src_indices_ind = 0;
    if (first) {
      auto sort_tuple =
          get_sort_tuple_from_prop_vec_tuple(prop_vec_tuple, 0, index_seq);
      while (src_indices_ind < src_indices.size() &&
             new_v_offset[src_indices_ind + 1] <= 0) {
        src_indices_ind += 1;
      }
      CHECK(src_indices_ind < new_v_offset.size() - 1);
      pq.emplace(std::tuple{src_indices[src_indices_ind], sort_tuple,
                            vertices_to_insert[0]});
      top_tuple = std::get<1>(pq.top());
      start_ind = 1;
      first = false;
    }
    for (auto i = start_ind; i < vertices_to_insert.size(); ++i) {
      // LOG(INFO) << "i: " << i << ", start_ind: " << start_ind
      //           << ", size: " << vertices_to_insert.size();
      auto sort_tuple =
          get_sort_tuple_from_prop_vec_tuple(prop_vec_tuple, i, index_seq);
      // LOG(INFO) << "cur tuple: " << gs::to_string(sort_tuple) << ", top
      // tuple
      // "
      // << gs::to_string(top_tuple);
      if (pq.size() == sort_limit) {
        if (tuple_comparator(sort_tuple, top_tuple)) {
          pq.pop();
          while (src_indices_ind < src_indices.size() &&
                 new_v_offset[src_indices_ind + 1] <= i) {
            src_indices_ind += 1;
          }
          CHECK(src_indices_ind < src_indices.size());
          pq.emplace(std::tuple{src_indices[src_indices_ind], sort_tuple,
                                vertices_to_insert[i]});
          top_tuple = std::get<1>(pq.top());
        }
      } else {
        while (src_indices_ind < src_indices.size() &&
               new_v_offset[src_indices_ind + 1] <= i) {
          src_indices_ind += 1;
        }
        CHECK(src_indices_ind < src_indices.size());
        pq.emplace(std::tuple{src_indices[src_indices_ind], sort_tuple,
                              vertices_to_insert[i]});
        top_tuple = std::get<1>(pq.top());
      }
    }
  }

  template <typename... ORDER_PAIR, typename LabelT, typename CTX_T,
            size_t... Is>
  static auto get_prop_vec_for_row_vertices(
      const std::vector<vertex_id_t>& vid_vec, uint8_t cur_dist,
      LabelT cur_label, int64_t time_stamp, const GRAPH_INTERFACE& graph,
      CTX_T& ctx, std::tuple<ORDER_PAIR...>& order_pairs,
      std::index_sequence<Is...>) {
    return std::tuple{get_prop_vec_for_row_vertices_each(
        vid_vec, cur_dist, cur_label, time_stamp, graph, ctx,
        std::get<Is>(order_pairs))...};
  }

  // don't use -1 in order_pair opt.
  template <typename ORDER_PAIR, typename LabelT, typename CTX_T,
            typename std::enable_if<(ORDER_PAIR::tag_id <=
                                     CTX_T::max_tag_id)>::type* = nullptr>
  static auto get_prop_vec_for_row_vertices_each(
      const std::vector<vertex_id_t>& vid_vec, uint8_t cur_dist,
      LabelT curLabel, int64_t time_stamp, const GRAPH_INTERFACE& graph,
      CTX_T& ctx, ORDER_PAIR& order_pair) {
    LOG(FATAL) << "Not supported";
    auto& set = ctx.template GetNode<ORDER_PAIR::tag_id>();
    PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    auto props =
        graph.template GetVertexPropsFromVid<typename ORDER_PAIR::prop_t>(
            time_stamp, set.GetLabel(), set.GetVertices(), names);
    // fill builtin props;
    set.fillBuiltinProps(props, names);
    return props;
  }

  // don't use -1 in order_pair opt.
  template <typename ORDER_PAIR, typename LabelT, typename CTX_T,
            typename std::enable_if<
                ((ORDER_PAIR::tag_id > CTX_T::max_tag_id) &&
                 (std::is_same_v<typename ORDER_PAIR::prop_t, Dist>) )>::type* =
                nullptr>
  static auto get_prop_vec_for_row_vertices_each(
      const std::vector<vertex_id_t>& vid_vec, uint8_t cur_dist,
      LabelT cur_label, int64_t time_stamp, const GRAPH_INTERFACE& graph,
      CTX_T& ctx, ORDER_PAIR& order_pair) {
    // const std::vector<vertex_id_t>& vec = set.GetVertices();
    PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    auto props =
        graph.template GetVertexPropsFromVid<typename ORDER_PAIR::prop_t>(
            time_stamp, cur_label, vid_vec, names);

    // fill dist property.
    if (order_pair.name == "dist") {
      // LOG(INFO) << "Filling dist prop of size: " << vid_vec.size();
      props.resize(vid_vec.size());
      for (auto i = 0; i < vid_vec.size(); ++i) {
        std::get<0>(props[i]) = (int32_t) cur_dist;
      }
    }

    return props;
  }

  // don't use -1 in order_pair opt.
  template <typename ORDER_PAIR, typename LabelT, typename CTX_T,
            typename std::enable_if<(
                (ORDER_PAIR::tag_id > CTX_T::max_tag_id) &&
                !(std::is_same_v<typename ORDER_PAIR::prop_t, Dist>) )>::type* =
                nullptr>
  static auto get_prop_vec_for_row_vertices_each(
      const std::vector<vertex_id_t>& vid_vec, uint8_t cur_dist,
      LabelT cur_label, int64_t time_stamp, const GRAPH_INTERFACE& graph,
      CTX_T& ctx, ORDER_PAIR& order_pair) {
    // const std::vector<vertex_id_t>& vec = set.GetVertices();
    PropNameArray<typename ORDER_PAIR::prop_t> names{order_pair.name};
    auto props =
        graph.template GetVertexPropsFromVid<typename ORDER_PAIR::prop_t>(
            time_stamp, cur_label, vid_vec, names);

    return props;
  }

  template <typename... VEC_T, size_t... Is>
  static auto get_sort_tuple_from_prop_vec_tuple(
      std::tuple<VEC_T...>& prop_vec_tuple, size_t ind,
      std::index_sequence<Is...>) {
    return std::tuple_cat(std::get<Is>(prop_vec_tuple)[ind]...);
  }

  //////////////////////////////////Filter and sort/////////////////////////
  // filter and sort by last column.
  template <
      int res_alias, int alias_to_use, typename CTX_HEAD_T, int cur_alias,
      int base_tag, typename... CTX_PREV, typename LabelT, size_t num_labels,
      typename EXPRESSION, typename... GetVProp, typename SORT_FUNC,
      typename... ORDER_PAIRS,
      typename ctx_t = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>,
      typename old_node_t = std::remove_reference_t<
          decltype(std::declval<ctx_t>().template GetNode<alias_to_use>())>,
      typename std::enable_if<(old_node_t::is_two_label_set &&
                               sizeof...(GetVProp) == 0)>::type* = nullptr>
  static auto GetVAndSort(
      int64_t time_stamp, const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>&& ctx,
      GetVOpt<LabelT, num_labels, EXPRESSION, GetVProp...>&& get_v_opt,
      SortOrderOpt<SORT_FUNC, ORDER_PAIRS...>&& sort_opt) {
    static_assert(alias_to_use == -1 || alias_to_use == ctx_t::max_tag_id);
    LOG(INFO) << "GetV and Sort";
    auto& range = sort_opt.range_;
    auto& order_pairs = sort_opt.ordering_pairs_;
    auto& sort_func = sort_opt.sort_func_;
    auto sort_limit = range.limit_;

    if (range.start_ != 0) {
      LOG(FATAL) << "Current only support topk";
    }
    if (range.limit_ == 0) {
      LOG(FATAL) << "Current only support empty range";
    }

    auto v_opt = get_v_opt.v_opt_;
    auto& expr = get_v_opt.expr_;
    auto& get_v_labels = get_v_opt.v_labels_;
    auto named_prop = expr.Properties();
    CHECK(v_opt == VOpt::Itself) << "Can only get v from vertex set "
                                    "with v_opt == vopt::Itself";

    auto& vertex_set = gs::Get<alias_to_use>(ctx);
    auto& old_vids = vertex_set.GetVertices();
    auto& vset_v_labels = vertex_set.GetLabels();
    auto& old_bitset = vertex_set.GetBitset();
    std::vector<bool> valid_label(2, false);
    for (auto i = 0; i < 2; ++i) {
      for (auto j = 0; j < get_v_labels.size(); ++j) {
        if (vset_v_labels[i] == get_v_labels[j]) {
          valid_label[i] = true;
        }
      }
    }
    auto filter_prop_getter_vec =
        create_prop_getter(named_prop, vset_v_labels, graph, time_stamp);
    auto sort_prop_getter_vec =
        create_prop_getter(order_pairs, vset_v_labels, graph, time_stamp);
    LOG(INFO) << "Create filter property getter and sort prop_getter : ";

    using sort_tuple_t = std::tuple<typename ORDER_PAIRS::prop_t...>;
    using full_ele_tuple_t =
        std::pair<sort_tuple_t, size_t>;  //(old_index, tuple_value)
    std::priority_queue<full_ele_tuple_t, std::vector<full_ele_tuple_t>,
                        SORT_FUNC>
        pq(sort_func);

    sort_tuple_t empty_tuple;
    sort_tuple_t& top_tuple = empty_tuple;
    double t0 = -grape::GetCurrentTime();
    bool init = false;
    size_t cnt = 0;

    // label: 0
    CHECK(valid_label[0]);
    auto& cur_getter = sort_prop_getter_vec[0];
    auto& filter_getter = filter_prop_getter_vec[0];
    for (auto i = 0; i < vertex_set.Size(); ++i) {
      if (!old_bitset.get_bit(i)) {
        continue;
      }
      auto vid = old_vids[i];

      auto filter_ele = filter_getter.get_view(vid);
      if (expr(filter_ele)) {
        cnt += 1;
        if (pq.size() < sort_limit) {
          // emplace ele.
          pq.emplace(std::make_pair(
              get_sort_tuple_from_getter_tuple(vid, cur_getter), i));
          // init top_tuple;
          top_tuple = pq.top().first;
        } else if (pq.size() == sort_limit) {
          // compare to top.
          if (sort_func.eval(vid, cur_getter, top_tuple)) {
            pq.pop();
            pq.emplace(std::make_pair(
                get_sort_tuple_from_getter_tuple(vid, cur_getter), i));
            top_tuple = pq.top().first;
          }
        }
      }
    }

    CHECK(valid_label[1]);
    cur_getter = sort_prop_getter_vec[1];
    filter_getter = filter_prop_getter_vec[1];
    for (auto i = 0; i < vertex_set.Size(); ++i) {
      if (old_bitset.get_bit(i)) {
        continue;
      }
      auto vid = old_vids[i];

      auto filter_ele = filter_getter.get_view(vid);
      if (expr(filter_ele)) {
        cnt += 1;
        if (pq.size() < sort_limit) {
          // emplace ele.
          pq.emplace(std::make_pair(
              get_sort_tuple_from_getter_tuple(vid, cur_getter), i));
          // init top_tuple;
          top_tuple = pq.top().first;
        } else if (sort_func.eval(vid, cur_getter, top_tuple)) {
          pq.pop();
          pq.emplace(std::make_pair(
              get_sort_tuple_from_getter_tuple(vid, cur_getter), i));
          top_tuple = pq.top().first;
        }
      }
    }
    t0 += grape::GetCurrentTime();
    LOG(INFO) << cnt << "/" << vertex_set.Size() << " vertices are selected"
              << ", pq size: " << pq.size();

    double t1 = -grape::GetCurrentTime();
    std::vector<std::pair<size_t, size_t>> inds;
    inds.reserve(pq.size());
    std::vector<vertex_id_t> new_vids;
    Bitset new_bitset;
    new_bitset.init(pq.size());
    new_vids.reserve(pq.size());
    cnt = 0;
    while (!pq.empty()) {
      auto top_ind = pq.top().second;
      // LOG(INFO) << "top ind: " << top_ind
      // << ", tuple: " << gs::to_string(pq.top().second);
      inds.emplace_back(std::make_pair(cnt, top_ind));
      new_vids.emplace_back(old_vids[top_ind]);
      pq.pop();
      cnt += 1;
    }

    for (int32_t i = inds.size() - 1; i >= 0; --i) {
      auto set_ind = inds[i].second;
      auto vid = new_vids[i];
      if (old_bitset.get_bit(set_ind)) {
        new_bitset.set_bit(inds.size() - i - 1);
      }
    }
    std::reverse(new_vids.begin(), new_vids.end());

    sort(inds.begin(), inds.end(),
         [](const auto& a, const auto& b) { return a.second < b.second; });
    using index_ele_tuple_t = typename ctx_t::index_ele_tuples_t;
    std::vector<index_ele_tuple_t> index_eles;
    index_eles.resize(inds.size());
    auto iter2 = ctx.begin();
    cnt = 0;
    size_t inds_ind = 0;
    while (inds_ind < inds.size()) {
      auto pair = inds[inds_ind];
      while (cnt < pair.second) {
        ++iter2;
        ++cnt;
      }
      index_eles[pair.first] = iter2.GetAllIndexElement();
      inds_ind += 1;
    }
    LOG(INFO) << "Finish index ele sorting";
    std::reverse(index_eles.begin(), index_eles.end());
    t1 += grape::GetCurrentTime();

    auto flated_ctx = ctx.Flat(std::move(index_eles));
    LOG(INFO) << "Finish extract top k result, sort tuple time: " << t0
              << ", prepare index ele: " << t1;

    auto two_label_set = make_two_label_set(std::move(new_vids), vset_v_labels,
                                            std::move(new_bitset));
    CHECK(flated_ctx.GetHead().Size() == two_label_set.Size());
    std::vector<offset_t> offset;
    auto prev_size = flated_ctx.GetHead().Size();
    offset.reserve(prev_size + 1);
    for (auto i = 0; i <= prev_size; ++i) {
      offset.emplace_back(i);
    }
    return flated_ctx.template AddNode<res_alias>(
        std::move(two_label_set), std::move(offset), alias_to_use);
  }

  template <typename T>
  static auto create_prop_getter(const NamedProperty<T>& named_prop,
                                 const std::array<label_id_t, 2>& labels,
                                 const GRAPH_INTERFACE& graph,
                                 int64_t time_stamp) {
    const std::array<std::string, 1>& prop_names = named_prop.names;
    std::array<typename GRAPH_INTERFACE::template single_prop_getter_t<T>, 2>
        prop_getter;
    for (auto i = 0; i < 2; ++i) {
      prop_getter[i] = graph.template GetSinglePropGetter<T>(
          time_stamp, labels[i], prop_names);
    }
    return prop_getter;
  }

  // returns a tuple of single prop getter.
  // template <typename... ORDER_PAIR>
  // static auto create_prop_getter(const std::tuple<ORDER_PAIR...>&
  // ordering_pair,
  //                                const std::array<label_id_t, 2>& labels,
  //                                const GRAPH_INTERFACE& graph,
  //                                int64_t time_stamp) {
  //   using prop_getter_tuple_t =
  //       std::tuple<typename GRAPH_INTERFACE::template single_prop_getter_t<
  //           typename ORDER_PAIR::prop_t>...>;
  //   std::array<prop_getter_tuple_t, 2> prop_getter;
  //   std::array<std::string, sizeof...(ORDER_PAIR)> prop_names;
  //   set_prop_names(prop_names, ordering_pair);
  //   for (auto i = 0; i < 2; ++i) {
  //     prop_getter[i] =
  //         graph.template GetPropGetterTuple<typename ORDER_PAIR::prop_t...>(
  //             time_stamp, labels[i], prop_names);
  //   }
  //   return prop_getter;
  // }

  template <size_t Is = 0, typename... PAIR>
  static void set_prop_names(std::array<std::string, sizeof...(PAIR)>& names,
                             const std::tuple<PAIR...>& tuple) {
    names[Is] = std::get<Is>(tuple).name;
    if constexpr (Is + 1 < sizeof...(PAIR)) {
      set_prop_names<Is + 1>(names, tuple);
    }
  }

  template <typename... PROP_GETTER>
  static inline auto get_sort_tuple_from_getter_tuple(
      vertex_id_t vid, const std::tuple<PROP_GETTER...>& prop_getter_tuple) {
    static constexpr auto ind_seq =
        std::make_index_sequence<sizeof...(PROP_GETTER)>();
    return get_sort_tuple_from_getter_tuple_impl(vid, prop_getter_tuple,
                                                 ind_seq);
  }

  template <typename... PROP_GETTER, size_t... Is>
  static inline auto get_sort_tuple_from_getter_tuple_impl(
      vertex_id_t vid, const std::tuple<PROP_GETTER...>& prop_getter_tuple,
      std::index_sequence<Is...>) {
    return std::tuple{std::get<Is>(prop_getter_tuple).get_view(vid)...};
  }
};  // namespace gs
}  // namespace gs

#endif  // GRAPHSCOPE_OPERATOR_FUSED_OPERATOR_H_
