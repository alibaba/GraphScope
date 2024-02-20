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

#ifndef ENGINES_HQPS_ENGINE_CONTEXT_H_
#define ENGINES_HQPS_ENGINE_CONTEXT_H_

#include <type_traits>
#include <vector>

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "grape/types.h"

namespace gs {

static constexpr int INVALID_TAG = -2;

// Convert a offset array to repeat array.
std::vector<size_t> offset_array_to_repeat_array(
    std::vector<size_t>&& offset_array) {
  std::vector<size_t> repeat_array(offset_array.size() - 1);
  for (size_t i = 0; i < repeat_array.size(); ++i) {
    repeat_array[i] = offset_array[i + 1] - offset_array[i];
  }
  return repeat_array;
}

/**
 * @brief The iterator for context.
 *
 * @tparam SET_TS The sets we current holds, include head node.
 * @tparam Enable For template specialization.
 */
template <int base_tag, typename SET_TS>
class ContextIter;

//-------------------------Definition for context iter--------------

// 0. specialization for only one vertex set
template <int base_tag, typename SET_T>
class ContextIter<base_tag, std::tuple<SET_T>> {
 public:
  using tuple_t = typename SET_T::data_tuple_t;
  using self_type_t = ContextIter<base_tag, std::tuple<SET_T>>;
  using head_iter = typename SET_T::iterator;

  ContextIter(head_iter&& iter) : iter_(std::move(iter)) {}

  // GetVertex for Vertex set and GetEdge for Edge set.
  auto GetElement() const { return iter_.GetElement(); }

  // template <typename... ORDER_PAIR>
  auto GetAllIndexDataEle() const {
    return std::make_tuple(GetAllData(), GetAllIndexElement());
  }

  auto GetAllElement() const { return std::make_tuple(GetElement()); }

  auto GetAllIndexElement() const {
    return std::make_tuple(iter_.GetIndexElement());
  }

  auto GetData() const { return iter_.GetData(); }

  auto GetAllData() const { return std::make_tuple(GetData()); }

  inline self_type_t& operator++() {
    ++iter_;
    return *this;
  };
  inline bool operator==(const self_type_t& rhs) const {
    return iter_ == rhs.iter_;
  }
  inline bool operator!=(const self_type_t& rhs) const {
    return iter_ != rhs.iter_;
  }
  inline bool operator<(const self_type_t& rhs) const {
    return iter_ < rhs.iter_;
  }
  inline const self_type_t& operator*() const { return *this; }
  inline const self_type_t* operator->() const { return this; }

 private:
  typename SET_T::iterator iter_;
};

// 2. specialization for head node is vertex_set
template <int base_tag, typename SET_T, typename... PREV_SETS>
class ContextIter<base_tag, std::tuple<SET_T, PREV_SETS...>> {
 public:
  static constexpr int num_others = sizeof...(PREV_SETS);
  static constexpr auto index_seq =
      std::make_integer_sequence<int, num_others>{};
  using others_tuple_t = std::tuple<PREV_SETS...>;
  using others_iter_tuple_t = std::tuple<IterOf<PREV_SETS>...>;
  using head_iter_t = typename SET_T::iterator;
  using tuple_t =
      std::tuple<typename SET_T::data_tuple_t, DataTupleT<PREV_SETS>...>;
  using self_type_t = ContextIter<base_tag, std::tuple<SET_T, PREV_SETS...>>;

  ContextIter(head_iter_t&& cur_iter, others_iter_tuple_t&& others_iter_tuple,
              const std::vector<std::vector<offset_t>>& offsets)
      : cur_iter_(cur_iter),
        offsets_arrays_(offsets),
        others_iter_tuple_(std::move(others_iter_tuple)),
        cur_offset_(0) {
    others_offset_.fill(0);
    init_iter_tuple();  // init iterator tuple to a valid position.
  }

  // GetVertex for Vertex set and GetEdge for Edge set, for head node.
  auto GetElement() const { return cur_iter_.GetElement(); }

  auto GetData() const { return cur_iter_.GetData(); }

  auto GetAllElement() const { return get_element_tuple_impl(index_seq); }

  template <int... Is>
  auto get_element_tuple_impl(std::integer_sequence<int, Is...>) const {
    return std::make_tuple(std::get<Is>(others_iter_tuple_).GetElement()...,
                           cur_iter_.GetElement());
  }

  size_t GetTagOffset(int tag) const {
    size_t real_tag = tag == -1 ? num_others : tag;
    real_tag -= base_tag;
    CHECK(real_tag <= others_offset_.size());
    return others_offset_[real_tag];
  }

  auto GetAllIndexElement() const {
    return get_index_ele_tuple_impl(index_seq);
  }

  template <int... Is>
  auto get_index_ele_tuple_impl(std::integer_sequence<int, Is...>) const {
    return std::make_tuple(
        std::get<Is>(others_iter_tuple_).GetIndexElement()...,
        cur_iter_.GetIndexElement());
  }

  auto GetAllIndexDataEle() const {
    return std::make_tuple(GetAllData(), GetAllIndexElement());
  }

  auto GetAllData() const { return get_data_tuple_impl(index_seq); }

  template <int... Is>
  auto get_data_tuple_impl(std::integer_sequence<int, Is...>) const {
    return std::make_tuple(std::get<Is>(others_iter_tuple_).GetData()...,
                           cur_iter_.GetData());
  }

  inline self_type_t& operator++() {
    cur_offset_ += 1;
    // VLOG(10) << "cur offset:" << cur_offset_;
    ++cur_iter_;
    // VLOG(10) << "inc iter";
    // update with reverse index seq.
    update_other_iter<num_others - 1>();
    return *this;
  }

  // General implementation for cols
  template <int Is>
  inline typename std::enable_if<(Is > -1 && Is < num_others - 1)>::type
  update_other_iter() {
    // VLOG(10) << "updating " << Is;
    auto child_cur_ind = others_offset_[Is + 1];
    bool flag = false;
    auto& cur_off_array = offsets_arrays_[Is];

    // quick check
    if (child_cur_ind < other_offset_limit_[Is]) {
      return;
    }
    // cache the upper bound to avoid repeated computation
    while (others_offset_[Is] + 1 < cur_off_array.size() &&
           child_cur_ind >= cur_off_array[others_offset_[Is] + 1]) {
      ++std::get<Is>(others_iter_tuple_);
      ++others_offset_[Is];
      flag = true;
    }
    if (flag) {
      other_offset_limit_[Is] = cur_off_array[others_offset_[Is] + 1];
      // propagate to iterator with smaller index
      update_other_iter<Is - 1>();
    }
  }

  // Specialization for -1 col.
  template <int Is>
  typename std::enable_if<(Is == -1)>::type update_other_iter() {
    return;
  }

  // Specialization for the first col.
  template <int Is>
  inline typename std::enable_if<(Is == num_others - 1)>::type
  update_other_iter() {
    // VLOG(10) << "updating " << Is;
    auto& my_cur_ind = others_offset_[Is];
    auto& cur_offset_array_ = offsets_arrays_[Is];
    bool flag = false;

    if (cur_offset_ < other_offset_limit_[Is]) {
      return;
    }

    while (my_cur_ind + 1 < cur_offset_array_.size() &&
           cur_offset_ >= cur_offset_array_[my_cur_ind + 1]) {
      ++std::get<Is>(others_iter_tuple_);
      others_offset_[Is] += 1;
      flag = true;
    }
    if (flag) {
      other_offset_limit_[Is] = cur_offset_array_[my_cur_ind + 1];
      // propagate to iterator with smaller index
      update_other_iter<num_others - 2>();
    }
  }

  inline bool operator==(const self_type_t& rhs) const {
    return cur_iter_ == rhs.cur_iter_;
  }
  inline bool operator!=(const self_type_t& rhs) const {
    return cur_iter_ != rhs.cur_iter_;
  }
  inline bool operator<(const self_type_t& rhs) const {
    return cur_iter_ < rhs.cur_iter_;
  }
  inline const self_type_t& operator*() const { return *this; }
  inline const self_type_t* operator->() const { return this; }

 private:
  // Init iter tuple recursively.
  // a = [1,2], b = [3,4,5,6], c = [7,8,9,10]
  // offset_arrays = [[0,2,4], [0,0,1,3,4]]
  // init : others_offset = [0,0];
  // after init_iter_tuple_impl<1>: [0,1]
  // after init_iter_tuple_impl<0>: [0,1]
  void init_iter_tuple() { init_iter_tuple_impl<num_others - 1>(); }

  template <int Is>
  typename std::enable_if<(Is == -1)>::type init_iter_tuple_impl() {
    return;
  }

  template <int Is>
  typename std::enable_if<(Is > -1 && Is < num_others - 1)>::type
  init_iter_tuple_impl() {
    size_t limit = offsets_arrays_[Is].size();
    size_t this_offset = 0;
    size_t child_offset = others_offset_[Is + 1];
    while (this_offset + 1 < limit &&
           offsets_arrays_[Is][this_offset + 1] <= child_offset) {
      this_offset += 1;
      ++std::get<Is>(others_iter_tuple_);
    }
    others_offset_[Is] = this_offset;
    other_offset_limit_[Is] = offsets_arrays_[Is][this_offset + 1];
    init_iter_tuple_impl<Is - 1>();
  }

  template <int Is>
  typename std::enable_if<(Is == num_others - 1)>::type init_iter_tuple_impl() {
    size_t limit = offsets_arrays_[num_others - 1].size();
    size_t this_offset = 0;
    while (this_offset + 1 < limit &&
           offsets_arrays_[num_others - 1][this_offset + 1] <=
               cur_offset_) {  // cur_offset_ == 0
      this_offset += 1;
      ++std::get<Is>(others_iter_tuple_);
    }
    others_offset_[num_others - 1] = this_offset;
    other_offset_limit_[Is] = offsets_arrays_[num_others - 1][this_offset + 1];
    init_iter_tuple_impl<num_others - 2>();
  }
  ///////////////////////////////////////////////////////////////////////////////

  typename SET_T::iterator cur_iter_;
  const std::vector<std::vector<offset_t>>& offsets_arrays_;
  others_iter_tuple_t others_iter_tuple_;
  size_t cur_offset_;
  std::array<size_t, num_others> others_offset_;
  std::array<size_t, num_others> other_offset_limit_;
};

/**
 * @brief A data structure holding all the data we have in query.
 *
 * @tparam HEAD_T The current head node.
 * @tparam base_tag The base tag based on which the tag id increases. Default
 * 0, set to non-zero for grouped sets.
 * @tparam cur_alias To which col_id it is aliased.
 * @tparam ALIAS_COL The saved obj in query up till now.
 */
template <typename HEAD_T, int cur_alias, int base_tag, typename... ALIAS_SETS>
class Context;
template <typename... T>
using FirstEntityType = std::tuple_element_t<0, std::tuple<T...>>;

template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct Dummy;

template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename CTX_PREV,
          typename Enable = void>
struct ResultContextTImpl;

// Get the correct result context type for returning.
template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct ResultContextTImpl<
    new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
    std::tuple<CTX_PREV...>,
    typename std::enable_if<
        old_alias != -1 && new_alias != old_alias &&
            !std::is_same<std::tuple_element_t<0, std::tuple<CTX_PREV...>>,
                          grape::EmptyType>::value,
        Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
              CTX_PREV...>>::type> {
  using result_t =
      Context<NEW_HEAD_T, new_alias, base_tag, CTX_PREV..., OLD_HEAD_T>;
};

template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct ResultContextTImpl<
    new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
    std::tuple<CTX_PREV...>,
    typename std::enable_if<
        old_alias != -1 && new_alias != old_alias &&
            std::is_same<std::tuple_element_t<0, std::tuple<CTX_PREV...>>,
                         grape::EmptyType>::value,
        Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
              CTX_PREV...>>::type> {
  using result_t = Context<NEW_HEAD_T, new_alias, base_tag, OLD_HEAD_T>;
};

template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct ResultContextTImpl<
    new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
    std::tuple<CTX_PREV...>,
    typename std::enable_if<new_alias == old_alias,
                            Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T,
                                  base_tag, CTX_PREV...>>::type> {
  using result_t = Context<NEW_HEAD_T, new_alias, base_tag, CTX_PREV...>;
};

template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct ResultContextTImpl<
    new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
    std::tuple<CTX_PREV...>,
    typename std::enable_if<
        old_alias == -1 && new_alias != old_alias &&
            std::is_same<std::tuple_element_t<0, std::tuple<CTX_PREV...>>,
                         grape::EmptyType>::value,
        Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
              CTX_PREV...>>::type> {
  using result_t = Context<NEW_HEAD_T, new_alias, base_tag, grape::EmptyType>;
};

template <int new_alias, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct ResultContextTImpl<
    new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
    std::tuple<CTX_PREV...>,
    typename std::enable_if<
        old_alias == -1 && new_alias != old_alias &&
            !std::is_same<std::tuple_element_t<0, std::tuple<CTX_PREV...>>,
                          grape::EmptyType>::value,
        Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
              CTX_PREV...>>::type> {
  using result_t = Context<NEW_HEAD_T, new_alias, base_tag, CTX_PREV...>;
};

template <AppendOpt append_opt, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag, typename... CTX_PREV>
struct ResultContextT {
  static constexpr int32_t new_alias =
      ResultColId<append_opt, old_alias, CTX_PREV...>::res_alias;
  using result_t = typename ResultContextTImpl<
      new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
      std::tuple<CTX_PREV...>,
      Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
            CTX_PREV...>>::result_t;
};

template <AppendOpt append_opt, typename NEW_HEAD_T, int old_alias,
          typename OLD_HEAD_T, int base_tag>
struct ResultContextT<append_opt, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
                      grape::EmptyType> {
  static constexpr int32_t new_alias =
      ResultColId<append_opt, old_alias, grape::EmptyType>::res_alias;
  using result_t = typename ResultContextTImpl<
      new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
      std::tuple<grape::EmptyType>,
      Dummy<new_alias, NEW_HEAD_T, old_alias, OLD_HEAD_T, base_tag,
            grape::EmptyType>>::result_t;
};

std::vector<offset_t> obtain_offset_between_tags_impl(
    const std::vector<std::vector<offset_t>>& offsets, int dst_tag) {
  CHECK((int32_t) offsets.size() > dst_tag)
      << "offset size" << offsets.size() << ", dst tag" << dst_tag;
  std::vector<offset_t> res = offsets[dst_tag];
  // VLOG(10) << "init offset: " << gs::to_string(res);
  for (size_t i = dst_tag + 1; i < offsets.size(); ++i) {
    for (size_t j = 0; j < res.size(); ++j) {
      res[j] = offsets[i][res[j]];
    }
  }
  return res;
}

// specialization for Context with aliased at least 1 alias_col.
template <typename HEAD_T, int cur_alias, int base_tag, typename... ALIAS_COL>
class Context {
 public:
  using head_t = HEAD_T;
  static constexpr int prev_alias_num = sizeof...(ALIAS_COL);
  static constexpr int cur_col_id = cur_alias;
  static_assert(cur_alias == -1 || cur_alias == prev_alias_num + base_tag);
  // alias_num equals to the count of aliases, not total alias num, not included
  // ones below base_tag.
  static constexpr size_t alias_num =
      cur_alias == -1 ? prev_alias_num : prev_alias_num + 1;
  static constexpr size_t col_num = sizeof...(ALIAS_COL) + 1;
  // max tag id appeared in current context.
  // static constexpr size_t max_tag_id = alias_num + base_tag;
  static constexpr int max_tag_id =
      cur_alias == -1 ? base_tag + prev_alias_num - 1 : cur_alias;
  static constexpr int base_tag_id = base_tag;

  static constexpr auto index_seq =
      std::make_integer_sequence<int, prev_alias_num>{};
  using iterator = ContextIter<base_tag, std::tuple<HEAD_T, ALIAS_COL...>>;
  using others_iter_tuple_t = std::tuple<IterOf<ALIAS_COL>...>;
  using self_type_t = Context<HEAD_T, cur_alias, base_tag, ALIAS_COL...>;
  using index_ele_tuples_t =
      std::tuple<typename ALIAS_COL::index_ele_tuple_t...,
                 typename HEAD_T::index_ele_tuple_t>;
  using prev_tuple_t = std::tuple<ALIAS_COL...>;

  template <size_t ind>
  using nth_node_t =
      std::tuple_element_t<ind, std::tuple<ALIAS_COL..., HEAD_T>>;

  // Move constructor, passing the member not the context object
  Context(HEAD_T&& head, std::tuple<ALIAS_COL...>&& old_cols,
          std::vector<std::vector<offset_t>>&& offset,
          int sub_task_start_tag = INVALID_TAG)
      : cur_(std::move(head)),
        prev_(std::move(old_cols)),
        offsets_arrays_(std::move(offset)),
        sub_task_start_tag_(sub_task_start_tag) {}

  Context(Context<HEAD_T, cur_alias, base_tag, ALIAS_COL...>&& other) noexcept
      : cur_(std::move(other.cur_)),
        prev_(std::move(other.prev_)),
        offsets_arrays_(std::move(other.offsets_arrays_)),
        sub_task_start_tag_(other.sub_task_start_tag_) {}

  // copy constructor
  Context(Context<HEAD_T, cur_alias, base_tag, ALIAS_COL...>& other)
      : cur_(other.cur_),
        prev_(other.prev_),
        offsets_arrays_(other.offsets_arrays_),
        sub_task_start_tag_(other.sub_task_start_tag_) {}

  // Merge another node with a different head. We expect the other things, like
  // prev nodes, prev offset array, are the same. We will create a new Node,
  // UnionedNode, which contains two labels. <1,2,<>, 4> <1,2,3,<>>

  ~Context() {}

  const HEAD_T& GetHead() const { return cur_; }

  HEAD_T& GetMutableHead() { return cur_; }

  // we shall never change.
  const std::tuple<ALIAS_COL...>& GetPrevCols() const { return prev_; }

  auto CreateSetBuilder() const {
    return std::tuple_cat(
        create_prev_set_builder(std::make_index_sequence<prev_alias_num>{}),
        std::make_tuple(cur_.CreateBuilder()));
  }

  template <size_t... Is>
  auto create_prev_set_builder(std::index_sequence<Is...>) const {
    return std::make_tuple(std::get<Is>(prev_).CreateBuilder()...);
  }

  // for the passing offset array, check whether the corresponding data is
  // valid.
  // only filter, no data is append.
  void FilterWithOffsets(std::vector<size_t>& offset, JoinKind join_kind) {
    std::vector<size_t> active_indices;
    for (size_t i = 0; i + 1 < offset.size(); ++i) {
      if (offset[i] < offset[i + 1]) {
        active_indices.emplace_back(i);
      }
    }
    std::vector<offset_t> res_offset =
        cur_.FilterWithIndices(active_indices, join_kind);
    merge_offset(offsets_arrays_.back(), res_offset);
    // this->Alias<res_alias>();
  }

  std::vector<offset_t> ObtainOffsetFromTag(int dst_tag) const {
    CHECK(dst_tag > 0 || dst_tag <= prev_alias_num + base_tag);
    if (dst_tag == -1) {
      dst_tag = prev_alias_num + base_tag;
    }
    if (dst_tag < prev_alias_num + base_tag) {
      return obtain_offset_between_tags_impl(offsets_arrays_,
                                             dst_tag - base_tag);
    } else {
      std::vector<offset_t> res;
      res.reserve(cur_.Size() + 1);
      for (size_t i = 0; i <= cur_.Size(); ++i) {
        res.push_back(i);
      }
      return res;
    }
  }

  std::vector<offset_t> ObtainOffsetFromSubTaskStart() const {
    CHECK(sub_task_start_tag_ != INVALID_TAG);
    if (base_tag <= sub_task_start_tag_) {
      size_t dst_tag = sub_task_start_tag_ - base_tag;
      CHECK(offsets_arrays_.size() > dst_tag)
          << "offset size" << offsets_arrays_.size() << ", dst tag" << dst_tag;
      // VLOG(10) << "dst tag: " << dst_tag
      //  << ", offset size: " << offsets_arrays_.size();
      std::vector<offset_t> res = offsets_arrays_[dst_tag];
      for (size_t i = dst_tag + 1; i < offsets_arrays_.size(); ++i) {
        for (size_t j = 0; j < res.size(); ++j) {
          res[j] = offsets_arrays_[i][res[j]];
        }
      }
      return res;
      // return the offset between sub_task_start_tag to head_tag;
    } else {
      // If we abandon the context's history, we must be one-one mapping.
      // Only support fold, no
      std::vector<offset_t> res;
      res.reserve(cur_.Size() + 1);
      for (size_t i = 0; i <= cur_.Size(); ++i) {
        res.push_back(i);
      }
      return res;
    }
  }

  // Is is counted from zero, not from base_tag.
  template <int Is,
            typename std::enable_if<
                (Is >= 0 && Is < prev_alias_num + base_tag)>::type* = nullptr>
  auto& GetNode() {
    return std::get<Is - base_tag>(prev_);
  }

  template <int Is,
            typename std::enable_if<
                (Is == -1 || Is == prev_alias_num + base_tag)>::type* = nullptr>
  HEAD_T& GetNode() {
    return cur_;
  }

  template <int Is,
            typename std::enable_if<
                (Is >= 0 && Is < prev_alias_num + base_tag)>::type* = nullptr>
  auto& GetMutableNode() {
    return std::get<Is - base_tag>(prev_);
  }

  template <int Is,
            typename std::enable_if<
                (Is == -1 || Is == prev_alias_num + base_tag)>::type* = nullptr>
  HEAD_T& GetMutableNode() {
    return cur_;
  }

  const std::vector<offset_t>& GetOffset(int ind) const {
    if (ind == -1) {
      ind = offsets_arrays_.size() - 1;
    }
    CHECK(offsets_arrays_.size() > ind);
    return offsets_arrays_[ind];
  }

  std::vector<offset_t>& GetMutableOffset(int ind) {
    if (ind == -1) {
      ind = offsets_arrays_.size() - 1;
    }
    CHECK(offsets_arrays_.size() > ind);
    return offsets_arrays_[ind];
  }

  HEAD_T&& MoveHead() { return std::move(cur_); }

  size_t AliasNum() const { return alias_num; }

  template <int... Is>
  others_iter_tuple_t make_others_begin_iter_tuple(
      std::integer_sequence<int, Is...>) const {
    return std::make_tuple(std::get<Is>(prev_).begin()...);
  }

  iterator begin() const {
    auto others_iter_tuple = make_others_begin_iter_tuple(index_seq);
    return iterator(std::move(cur_.begin()), std::move(others_iter_tuple),
                    offsets_arrays_);
  }
  iterator end() const {
    auto others_iter_tuple = make_others_begin_iter_tuple(index_seq);
    return iterator(std::move(cur_.end()), std::move(others_iter_tuple),
                    offsets_arrays_);
  }

  // Alias head node with a alias value.
  template <int new_alias,
            typename RES_T = Context<HEAD_T, new_alias, base_tag, ALIAS_COL...>>
  RES_T Alias() {
    static_assert(cur_alias == -1 && new_alias == prev_alias_num + base_tag);
    return RES_T(std::move(cur_), std::move(prev_), std::move(offsets_arrays_),
                 sub_task_start_tag_);
  }

  self_type_t ReplaceHead(HEAD_T&& new_head, std::vector<offset_t>&& offset) {
    merge_offset(offsets_arrays_.back(), offset);
    return self_type_t(std::move(new_head), std::move(prev_),
                       std::move(offsets_arrays_), sub_task_start_tag_);
  }

  template <AppendOpt append_opt, typename NEW_HEAD_T,
            typename std::enable_if<NEW_HEAD_T::is_collection>::type* = nullptr>
  auto ApplyNode(NEW_HEAD_T&& new_head, std::vector<offset_t>&& offset) {
    // Collection
    auto new_set_and_offset = new_head.apply(offset);
    CHECK(std::get<1>(new_set_and_offset).size() == cur_.Size() + 1);
    return this->template AddNode<append_opt>(
        std::move(std::get<0>(new_set_and_offset)),
        std::move(std::get<1>(new_set_and_offset)));
  }

  // For non-collection apply result, just append.
  template <
      AppendOpt append_opt, typename NEW_HEAD_T,
      typename std::enable_if<!NEW_HEAD_T::is_collection>::type* = nullptr>
  auto ApplyNode(NEW_HEAD_T&& new_head, std::vector<offset_t>&& offset) {
    CHECK(offset.size() == cur_.Size() + 1);
    return this->template AddNode<append_opt>(std::move(new_head),
                                              std::move(offset));
  }

  // 0. add new node to obtain a new Context, if i'm not aliased
  // alias_to_use indicates which column the input offset array is aligned to.
  // we need to transform it to make it align with the ending column.
  template <
      AppendOpt opt, typename NEW_HEAD_T,
      typename RES_T = Context<
          NEW_HEAD_T, ResultColId<opt, cur_alias, ALIAS_COL...>::res_alias,
          base_tag, ALIAS_COL...>,
      typename std::enable_if<(cur_alias == -1), NEW_HEAD_T>::type* = nullptr>
  RES_T AddNode(NEW_HEAD_T&& new_node, std::vector<offset_t>&& offset,
                int alias_to_use = -1) {
    VLOG(10) << "Replace head with cur_alias == -1, offset array size:"
             << offsets_arrays_.size() << " alias to use: " << alias_to_use;
    if (offsets_arrays_.size() == 0) {
      offsets_arrays_.emplace_back(std::move(offset));
    } else {
      // Make input offset array align with the last set.
      auto new_offset = align_offset(new_node, std::move(offset),
                                     offsets_arrays_, alias_to_use);
      // Combine the input offset array with offset array of
      // offsets_arrays_[-1].
      merge_offset(offsets_arrays_.back(), new_offset);
    }
    return RES_T(std::move(new_node), std::move(prev_),
                 std::move(offsets_arrays_), sub_task_start_tag_);
  }

  // 1. res_alias eq cur_alias, we need to replace the current head node.
  template <
      AppendOpt opt, typename NEW_HEAD_T,
      typename RES_T = typename ResultContextT<
          opt, NEW_HEAD_T, cur_alias, HEAD_T, base_tag, ALIAS_COL...>::result_t,
      typename std::enable_if<(opt == AppendOpt::Replace), NEW_HEAD_T>::type* =
          nullptr>
  RES_T AddNode(NEW_HEAD_T&& new_node, std::vector<offset_t>&& offset,
                int alias_to_use = -1) {
    VLOG(10) << "Replace head with cur_alias" << cur_alias
             << ", append opt:" << gs::to_string(opt)
             << ",align to use:" << alias_to_use;
    // append the offset to the offset array.
    // Make input offset array align with the last set.
    auto new_offset = align_offset(new_node, std::move(offset), offsets_arrays_,
                                   alias_to_use);
    merge_offset(offsets_arrays_.back(), new_offset);
    return RES_T(std::move(new_node), std::move(prev_),
                 std::move(offsets_arrays_), sub_task_start_tag_);
  }

  // 2.
  // Replace current Head with new node, if i'm aliased to prev_alias_num.
  template <AppendOpt append_opt, typename NEW_HEAD_T,
            typename RES_T = typename ResultContextT<
                append_opt, NEW_HEAD_T, cur_alias, HEAD_T, base_tag,
                ALIAS_COL...>::result_t,
            typename std::enable_if<(append_opt != AppendOpt::Replace &&
                                     cur_alias != -1),
                                    NEW_HEAD_T>::type* = nullptr>
  RES_T AddNode(NEW_HEAD_T&& new_node, std::vector<offset_t>&& offset,
                int alias_to_use = -1) {
    VLOG(10) << "Replace head with cur_alias" << cur_alias
             << ", append opt:" << gs::to_string(append_opt) << ",align to use"
             << alias_to_use;
    // append the offset to the offset array.
    // Make input offset array align with the last set.
    {
      std::vector<size_t> offset_array_size;
      for (auto& off : offsets_arrays_) {
        offset_array_size.emplace_back(off.size());
      }
      LOG(INFO) << "Cur ctx offset array size: "
                << gs::to_string(offset_array_size);
      LOG(INFO) << "input offset size: " << offset.size();
    }
    auto new_offset = align_offset(new_node, std::move(offset), offsets_arrays_,
                                   alias_to_use);
    LOG(INFO) << "After alias " << new_offset.size();
    offsets_arrays_.emplace_back(std::move(new_offset));
    auto cated_tuple =
        std::tuple_cat(std::move(prev_), std::make_tuple(std::move(cur_)));
    return RES_T(std::move(new_node), std::move(cated_tuple),
                 std::move(offsets_arrays_), sub_task_start_tag_);
  }

  template <int Is>
  typename std::enable_if<(Is == -1 || Is == prev_alias_num + base_tag)>::type
  UpdateChildNode(std::vector<offset_t>&& offset) {
    static constexpr size_t act_Is = prev_alias_num;
    merge_offset(offsets_arrays_[act_Is - 1], offset);
  }

  // Do not use this to replace head.
  template <int Is>
  typename std::enable_if<(Is >= 0 && Is < prev_alias_num + base_tag)>::type
  UpdateChildNode(std::vector<offset_t>&& offset) {
    // The input offset is respect to the old node.
    // we need
    // 1) merge offset and offsets_array_[Is] to make sure (tag < Is) can be
    // correctly visited.
    // 2) propagate the change of offsets_array[Is] to later
    // (tag > Is) nodes.
    static constexpr int act_Is = Is - base_tag;
    // VLOG(10) << "use act_Is: " << act_Is << ", " << Is;
    if constexpr (act_Is > 0) {
      merge_offset(offsets_arrays_[act_Is - 1], offset);
    }

    auto new_size = std::get<act_Is>(prev_).Size();

    std::vector<offset_t> removed_indices;
    // CHECK(removed_indices.size() != old_size);
    for (size_t i = 0; i + 1 < offset.size(); ++i) {
      if (offset[i] == offset[i + 1]) {
        removed_indices.emplace_back(i);
      }
    }
    if (removed_indices.size() == 0) {
      VLOG(10) << "no ele is delete from tag: " << act_Is << ", return ";
      return;
    }
    VLOG(10) << "removed indices" << gs::to_string(removed_indices);
    CHECK(new_size == offsets_arrays_[act_Is - 1].back());
    // create a vector contains all indices before this update. so we can gather
    // the offset range vec.
    std::vector<offset_t> all_indices;
    for (size_t i = 0; i < offset.size(); ++i) {
      all_indices.emplace_back(i);
    }

    updateChildNodeAndOffset<Is + 1>(all_indices, removed_indices);
  }

  // For current tag, remove ele with respect to removed indices, and update the
  // offset array.
  template <size_t Is>
  typename std::enable_if<(Is < prev_alias_num + base_tag)>::type
  updateChildNodeAndOffset(std::vector<size_t>& all_indices,
                           std::vector<size_t>& removed_indices) {
    static constexpr size_t act_Is = Is - base_tag;
    for (size_t i = 0; i < all_indices.size(); ++i) {
      all_indices[i] = offsets_arrays_[act_Is - 1][all_indices[i]];
    }
    auto res_offset = std::get<act_Is>(prev_).SubSetWithRemovedIndices(
        removed_indices, all_indices);
    offsets_arrays_[act_Is - 1].swap(res_offset);

    // all_indices are changed after each run, while removed_indices never
    // changes.
    updateChildNodeAndOffset<Is + 1>(all_indices, removed_indices);
    return;
  }

  template <size_t Is>
  typename std::enable_if<(Is == prev_alias_num + base_tag)>::type
  updateChildNodeAndOffset(std::vector<size_t>& all_indices,
                           std::vector<size_t>& removed_indices) {
    static constexpr size_t act_Is = Is - base_tag;
    for (size_t i = 0; i < all_indices.size(); ++i) {
      all_indices[i] = offsets_arrays_[act_Is - 1][all_indices[i]];
    }
    auto res_offset =
        cur_.SubSetWithRemovedIndices(removed_indices, all_indices);
    offsets_arrays_[act_Is - 1].swap(res_offset);
    return;
  }

  template <typename... index_ele_tuple_t,
            typename RES_T = Context<typename HEAD_T::flat_t, cur_alias,
                                     base_tag, typename ALIAS_COL::flat_t...>>
  RES_T Flat(std::vector<std::tuple<index_ele_tuple_t...>>&& index_eles) {
    static_assert(std::tuple_size_v<std::tuple<index_ele_tuple_t...>> ==
                  1 + prev_alias_num);
    VLOG(10) << "Context: Flat";
    auto flat_head = cur_.template Flat<prev_alias_num>(index_eles);
    auto flat_prev = flat_prev_tuple(index_eles);
    // now all values are 1-to-1 mapping.
    std::vector<std::vector<offset_t>> new_offset_array;
    CHECK(offsets_arrays_.size() == prev_alias_num);
    new_offset_array.reserve(offsets_arrays_.size());
    size_t num_eles = index_eles.size();

    std::vector<offset_t> offset_vec(num_eles + 1, 0);
    for (size_t i = 0; i <= num_eles; ++i) {
      offset_vec[i] = i;
    }

    for (size_t i = 0; i < prev_alias_num; ++i) {
      new_offset_array.push_back(offset_vec);
    }
    VLOG(10) << "FInish flat";
    return RES_T(std::move(flat_head), std::move(flat_prev),
                 std::move(new_offset_array), sub_task_start_tag_);
  }

  template <int deduped_tag>
  typename std::enable_if<(deduped_tag == prev_alias_num + base_tag ||
                           deduped_tag == -1)>::type
  Dedup() {
    VLOG(10) << "Dedup on tag:" << deduped_tag << "  means nothing"
             << std::to_string(deduped_tag);
  }

  // This dedup doesn't clear duplication in individual set.!!!!!
  // start from alias_to_use, simplify all later csr.
  // no meaning to dedup with tag == 0;
  template <int raw_deduped_tag>
  typename std::enable_if<(raw_deduped_tag < prev_alias_num + base_tag &&
                           raw_deduped_tag >= 0)>::type
  Dedup() {
    // modify static variable?
    static constexpr int deduped_tag = raw_deduped_tag - base_tag;
    static_assert(deduped_tag >= 0);
    // do stuff.
    std::vector<size_t> indices;
    std::vector<size_t> offset_vec;
    // the offset vec with index deduped_tag - 1 's last elements indicates
    // how many elements in `deduped_tag`
    size_t num_deduped_ele = std::get<deduped_tag>(prev_).Size();
    VLOG(10) << "dedup at col:" << deduped_tag << ", with " << num_deduped_ele
             << " eles";
    {
      if constexpr (deduped_tag > 0) {
        auto& vec = offsets_arrays_[deduped_tag - 1];
        CHECK(num_deduped_ele == vec[vec.size() - 1]);
      }
    }
    {
      auto offset_vec_toward_head = ObtainOffsetFromTag(deduped_tag);
      offset_vec.reserve(offset_vec_toward_head.size());
      offset_vec.emplace_back(0);
      for (size_t i = 0; i < offset_vec_toward_head.size() - 1; ++i) {
        if (offset_vec_toward_head[i] < offset_vec_toward_head[i + 1]) {
          indices.emplace_back(i);
        }
        offset_vec.emplace_back(indices.size());
      }
    }
    std::vector<std::vector<size_t>> all_indices;
    all_indices.push_back(indices);
    for (int32_t i = deduped_tag; i < prev_alias_num; ++i) {
      std::vector<size_t> new_indices;
      auto& cur_offset_vec = offsets_arrays_[i];
      VLOG(10) << "tag: " << i << " indices: " << gs::to_string(indices);
      for (auto ind : indices) {
        // select the first ele.
        // if not element, skip.
        if (cur_offset_vec[ind] < cur_offset_vec[ind + 1]) {
          new_indices.emplace_back(cur_offset_vec[ind]);
        }
      }
      VLOG(10) << "for tag: " << i
               << ", new indices: " << gs::to_string(new_indices);
      all_indices.push_back(new_indices);
      indices.swap(new_indices);
    }
    // apply indices on all later sets
    // if constexpr (prev_alias_num > deduped_tag + 1) {

    // first subset deduped col, get offset, then apply to later ones.
    if constexpr (deduped_tag > 0) {
      merge_offset(offsets_arrays_[deduped_tag - 1], offset_vec);
    }

    static constexpr size_t num_prev_set_to_update =
        prev_alias_num - deduped_tag;
    if constexpr (num_prev_set_to_update > 0) {
      auto index_seq = std::make_index_sequence<num_prev_set_to_update>();
      CHECK(all_indices.size() == prev_alias_num - deduped_tag + 1);
      SubSetWithIndices<deduped_tag>(index_seq, all_indices);
      VLOG(10) << "Finish subseting";
    } else {
      VLOG(10) << "no prev set need subseting";
    }
    // }
    // if constexpr (prev_alias_num >= deduped_tag + 1) {
    VLOG(10) << "Subset current set";
    cur_.SubSetWithIndices(all_indices[prev_alias_num - deduped_tag]);
    // }
    // start from deduped_tag + 1,

    updateOffsetVec<raw_deduped_tag + 1>();
  }

  template <size_t start_tag = 0, size_t... Is>
  void SubSetWithIndices(std::index_sequence<Is...> index_seq,
                         std::vector<std::vector<size_t>>& new_indices) {
    VLOG(10) << "subset context from tag: " << start_tag
             << "with new_indices size:" << new_indices.size()
             << " index_seq size: " << sizeof...(Is)
             << ",start tag: " << start_tag;
    ((std::get<start_tag + Is>(prev_).SubSetWithIndices(new_indices[Is])), ...);
  }

  void set_sub_task_start_tag(int sub_task_start_tag) {
    if (sub_task_start_tag == sub_task_start_tag_) {
      LOG(WARNING) << "in sub task already set to " << sub_task_start_tag_;
    } else {
      sub_task_start_tag_ = sub_task_start_tag;
    }
  }

  int get_sub_task_start_tag() const { return sub_task_start_tag_; }

  void merge_offset_with_back(std::vector<offset_t>& new_offset_array) {
    merge_offset(offsets_arrays_.back(), new_offset_array);
  }

 private:
  template <size_t Is>
  typename std::enable_if<(Is <= prev_alias_num + base_tag)>::type
  updateOffsetVec() {
    static constexpr size_t act_Is = Is - base_tag;
    VLOG(10) << "updateOffsetVec: tag: " << Is << ", act: " << act_Is;

    size_t size = 0;
    if constexpr (act_Is < prev_alias_num) {
      size = std::get<act_Is>(prev_).Size();
    } else {
      size = cur_.Size();
    }
    VLOG(10) << "in updateOffsetVec: tag: " << act_Is << ",size: " << size;
    auto& offset_vec = offsets_arrays_[act_Is - 1];
    offset_vec.clear();
    offset_vec.reserve(size + 1);
    for (size_t i = 0; i <= size; ++i) {
      offset_vec.emplace_back(i);
    }
    updateOffsetVec<Is + 1>();
  }

  template <size_t Is>
  typename std::enable_if<(Is > prev_alias_num + base_tag)>::type
  updateOffsetVec() {}

  template <typename index_ele_tuple_t, size_t... Is>
  auto flat_prev_tuple_impl(std::vector<index_ele_tuple_t>& index_eles,
                            std::index_sequence<Is...>) {
    return std::make_tuple(
        std::move(std::get<Is>(prev_).template Flat<Is>(index_eles))...);
  }

  template <typename index_ele_tuple_t>
  auto flat_prev_tuple(std::vector<index_ele_tuple_t>& index_eles) {
    return flat_prev_tuple_impl(index_eles,
                                std::make_index_sequence<prev_alias_num>());
  }

  void merge_offset(std::vector<offset_t>& old_offset_array,
                    std::vector<offset_t>& new_offset_array) {
    VLOG(10) << "merging offset";
    CHECK(new_offset_array.size() == old_offset_array.back() + 1)
        << "new size " << new_offset_array.size() << ", old back"
        << old_offset_array.back();
    for (size_t i = 0; i < old_offset_array.size(); ++i) {
      old_offset_array[i] = new_offset_array[old_offset_array[i]];
    }
  }

  /// @brief Align input offset with previous offset. Finally, it is aligned
  /// with offset_array.back().
  /// @param offset
  /// @param offset_array
  /// @param from_ind count from zero, need to minus base_tag
  template <typename NODE_T>
  std::vector<offset_t> align_offset(
      NODE_T& new_node, std::vector<offset_t>&& offset,
      std::vector<std::vector<offset_t>>& offset_array, int from_ind) {
    if (from_ind != -1) {
      from_ind = from_ind - base_tag;
    }
    if (from_ind == -1 || from_ind == (int32_t) offset_array.size()) {
      VLOG(10) << "No need to align with backend " << from_ind
               << ", offsets size: " << offset_array.size();
      return std::move(offset);
    }
    // First got offset array which indicate the repeated times.
    CHECK(from_ind <= (int32_t) offset_array.size())
        << "out of range: " << from_ind << ", " << offset_array.size();
    std::vector<offset_t> copied = offset_array[from_ind];
    VLOG(10) << "copied: " << gs::to_string(copied);
    for (size_t i = from_ind + 1; i < offset_array.size(); ++i) {
      for (size_t j = 0; j < copied.size(); ++j) {
        copied[j] = offset_array[i][copied[j]];
      }
    }
    CHECK(copied.size() == offset.size());
    // indicate the value at ind repeat how many times due to our chain.
    VLOG(10) << "repeat array is :" << gs::to_string(copied);
    VLOG(10) << "current offset:" << gs::to_string(offset);
    new_node.Repeat(offset, copied);
    std::vector<offset_t> res_offset;
    {
      // apply repeat on offset array;
      size_t cur = 0;
      for (size_t i = 0; i + 1 < offset.size(); ++i) {
        if (copied[i] < copied[i + 1]) {
          int gap = offset[i + 1] - offset[i];
          size_t times_to_copy = copied[i + 1] - copied[i];

          for (size_t j = 0; j < times_to_copy; ++j) {
            res_offset.push_back(cur);
            cur += gap;
          }
        } else {
          // NO action if there is no child nodes for i.
          // res_offset.push_back(cur);
        }
      }
      res_offset.push_back(cur);
    }
    LOG(INFO) << "res_offset size: " << res_offset.size();
    VLOG(10) << "res offset: " << gs::to_string(res_offset);
    return res_offset;
  }
  HEAD_T cur_;
  std::tuple<ALIAS_COL...> prev_;
  std::vector<std::vector<offset_t>> offsets_arrays_;
  int sub_task_start_tag_;
};

// Specialization for Context with no alias
template <typename HEAD_T, int cur_alias, int base_tag>
class Context<HEAD_T, cur_alias, base_tag, grape::EmptyType> {
 public:
  using head_t = HEAD_T;
  using iterator = ContextIter<base_tag, std::tuple<HEAD_T>>;
  static constexpr size_t alias_num = cur_alias == -1 ? 0 : 1;
  static constexpr size_t col_num = 1;
  static constexpr int prev_alias_num = 0;
  static constexpr int max_tag_id = cur_alias;
  static constexpr int base_tag_id = base_tag;
  using self_type_t = Context<HEAD_T, cur_alias, base_tag, grape::EmptyType>;
  using prev_tuple_t = std::tuple<grape::EmptyType>;

  using index_ele_tuples_t = std::tuple<typename HEAD_T::index_ele_tuple_t>;

  template <int ind>
  using nth_node_t = std::tuple_element_t<ind, std::tuple<HEAD_T>>;

  Context(HEAD_T&& head, int sub_task_start_tag = INVALID_TAG)
      : cur_(std::move(head)), sub_task_start_tag_(sub_task_start_tag) {}

  Context(
      Context<HEAD_T, cur_alias, base_tag, grape::EmptyType>&& other) noexcept
      : cur_(std::move(other.cur_)),
        sub_task_start_tag_(other.sub_task_start_tag_) {}

  Context(const Context<HEAD_T, cur_alias, base_tag, grape::EmptyType>& other)
      : cur_(other.cur_), sub_task_start_tag_(other.sub_task_start_tag_) {}

  ~Context() {}

  template <typename EXPR>
  void SelectInPlace(EXPR& expr) {
    // The result context can be defined by the selected indices of the head
    // node. We can got the result context by applying selected indices.
    // auto new_head_and_offset = select_node.Filter(std::move(expr),
    // col_tuples);
    std::vector<offset_t> select_indices;
    offset_t cur_ind = 0;
    for (auto iter : *this) {
      auto ele_tuple = iter.GetAllElement();
      auto data_tuple = iter.GetAllData();
      if (expr(ele_tuple, data_tuple)) {
        select_indices.emplace_back(cur_ind);
      }
      cur_ind += 1;
    }
    // The offset need to be changed.
    // replace head in place
    cur_.SubSetWithIndices(select_indices);
  }

  size_t AliasNum() const { return alias_num; }

  const HEAD_T& GetHead() const { return cur_; }

  HEAD_T& GetMutableHead() { return cur_; }

  // This tag is the absolute tag, start from zero.
  template <int Is>
  HEAD_T& GetNode() {
    static_assert((Is - base_tag) == 0 || Is == -1);
    return cur_;
  }

  template <int Is>
  HEAD_T& GetMutableNode() {
    static_assert((Is - base_tag) == 0 || Is == -1);
    return cur_;
  }

  auto CreateSetBuilder() const {
    return std::make_tuple(cur_.CreateBuilder());
  }

  // Towards which tag we will align on.

  HEAD_T&& MoveHead() { return std::move(cur_); }

  iterator begin() const { return iterator(cur_.begin()); }
  iterator end() const { return iterator(cur_.end()); }

  // Alias a Context with only head node.
  template <int alias,
            typename RES_T = Context<HEAD_T, alias, base_tag, grape::EmptyType>>
  RES_T Alias() {
    static_assert(alias == base_tag && cur_alias == -1);
    return RES_T(std::move(cur_), sub_task_start_tag_);
  }

  // must return 1,1,1,.
  std::vector<offset_t> ObtainOffsetFromTag(int dst_tag) const {
    CHECK(dst_tag == cur_alias);
    auto size = cur_.Size();
    std::vector<offset_t> res;
    res.reserve(size + 1);
    for (size_t i = 0; i <= size; ++i) {
      res.push_back(i);
    }
    return res;
  }

  std::vector<offset_t> ObtainOffsetFromSubTaskStart() const {
    CHECK(sub_task_start_tag_ != INVALID_TAG);
    std::vector<offset_t> res;
    res.reserve(cur_.Size() + 1);
    for (size_t i = 0; i <= cur_.Size(); ++i) {
      res.push_back(i);
    }
    return res;
  }

  template <AppendOpt append_opt, typename NEW_HEAD_T,
            typename std::enable_if<NEW_HEAD_T::is_collection>::type* = nullptr>
  auto ApplyNode(NEW_HEAD_T&& new_head, std::vector<offset_t>&& offset) {
    // Collection
    auto new_set_and_offset = new_head.apply(offset);
    CHECK(std::get<1>(new_set_and_offset).size() == cur_.Size() + 1);
    return this->template AddNode<append_opt>(
        std::move(std::get<0>(new_set_and_offset)),
        std::move(std::get<1>(new_set_and_offset)));
  }

  // For non-collection apply result, just append.
  template <
      AppendOpt append_opt, typename NEW_HEAD_T,
      typename std::enable_if<!NEW_HEAD_T::is_collection>::type* = nullptr>
  auto ApplyNode(NEW_HEAD_T&& new_head, std::vector<offset_t>&& offset) {
    CHECK(offset.size() == cur_.Size() + 1);
    return this->template AddNode<append_opt>(std::move(new_head),
                                              std::move(offset));
  }

  // 0. Replace current HEAD to obtain a new Traversal, if i'm not aliased
  template <
      AppendOpt opt, typename NEW_HEAD_T,
      typename RES_T = Context<
          NEW_HEAD_T, ResultColId<opt, cur_alias, grape::EmptyType>::res_alias,
          base_tag, grape::EmptyType>,
      typename std::enable_if<(cur_alias == -1), NEW_HEAD_T>::type* = nullptr>
  RES_T AddNode(NEW_HEAD_T&& new_node, std::vector<offset_t>&& offset,
                int alias_to_use = -1) {  // offset vector and alias_to_use
                                          // is dummy in this case
    CHECK(alias_to_use == cur_alias || alias_to_use == -1);
    // VLOG(10) << "[AddNode:] offset size " << offset.size();
    return RES_T(std::move(new_node), sub_task_start_tag_);
  }

  // 1. Replace current Head with new node, if i'm aliased to 0.
  template <
      AppendOpt opt, typename NEW_HEAD_T,
      typename RES_T = Context<
          NEW_HEAD_T, ResultColId<opt, cur_alias, grape::EmptyType>::res_alias,
          base_tag, HEAD_T>,
      typename std::enable_if<(cur_alias != -1), NEW_HEAD_T>::type* = nullptr>
  RES_T AddNode(NEW_HEAD_T&& new_node, std::vector<offset_t>&& offset,
                int alias_to_use = -1) {
    CHECK(alias_to_use == cur_alias || alias_to_use == -1);
    std::vector<std::vector<offset_t>> offsets;
    offsets.emplace_back(std::move(offset));
    return RES_T(std::move(new_node), std::make_tuple(std::move(cur_)),
                 std::move(offsets), sub_task_start_tag_);
  }

  self_type_t ReplaceHead(HEAD_T&& new_head, std::vector<offset_t>&& offset) {
    return self_type_t(std::move(new_head));
  }

  template <typename index_ele_tuple_t,
            typename RES_T = Context<typename HEAD_T::flat_t, cur_alias,
                                     base_tag, grape::EmptyType>>
  RES_T Flat(std::vector<index_ele_tuple_t>&& index_eles) {
    static_assert(std::tuple_size_v<index_ele_tuple_t> == 1);
    return RES_T(std::move(cur_.template Flat<0>(index_eles)),
                 sub_task_start_tag_);
  }

  void merge_offset_with_back(std::vector<offset_t>& new_offset_array) {}

  template <int Is>
  typename std::enable_if<(Is == -1 || Is == 0)>::type UpdateChildNode(
      std::vector<offset_t>&& offset) {
    return;
  }

  void set_sub_task_start_tag(int sub_task_start_tag) {
    if (sub_task_start_tag == sub_task_start_tag_) {
      LOG(WARNING) << "in sub task already set to " << sub_task_start_tag_;
    } else {
      sub_task_start_tag_ = sub_task_start_tag;
    }
  }

  int get_sub_task_start_tag() const { return sub_task_start_tag_; }

  // for
  // template <int res_alias>
  void FilterWithOffsets(std::vector<size_t>& offset, JoinKind join_kind) {
    CHECK(join_kind == JoinKind::AntiJoin);
    std::vector<size_t> active_indices;
    for (size_t i = 0; i + 1 < offset.size(); ++i) {
      if (offset[i] < offset[i + 1]) {
        active_indices.emplace_back(i);
      }
    }
    VLOG(10) << "[Filter with offsets:], active indices: "
             << gs::to_string(active_indices)
             << " join kind: " << gs::to_string(join_kind);
    std::vector<offset_t> res_offset =
        cur_.FilterWithIndices(active_indices, join_kind);
  }

 private:
  HEAD_T cur_;
  int sub_task_start_tag_;
};
// deduped

template <int ind, typename HEAD_T, int cur_alias, int base_tag, typename... T,
          typename std::enable_if<(ind == -1)>::type* = nullptr>
auto& Get(Context<HEAD_T, cur_alias, base_tag, T...>& ctx) {
  return ctx.GetMutableHead();
}

template <int ind, typename HEAD_T, int cur_alias, int base_tag, typename... T,
          typename std::enable_if<
              (ind != -1 && ind == base_tag + sizeof...(T))>::type* = nullptr>
auto& Get(Context<HEAD_T, cur_alias, base_tag, T...>& ctx) {
  return ctx.GetMutableHead();
}

template <int ind, typename HEAD_T, int cur_alias, int base_tag, typename... T,
          typename std::enable_if<
              (ind != -1 && ind < base_tag + sizeof...(T))>::type* = nullptr>
auto& Get(Context<HEAD_T, cur_alias, base_tag, T...>& ctx) {
  return ctx.template GetNode<ind>();
}

template <int ind, typename HEAD_T, int cur_alias, int base_tag, typename... T,
          typename std::enable_if<(ind == -1)>::type* = nullptr>
auto&& Move(Context<HEAD_T, cur_alias, base_tag, T...>& ctx) {
  return ctx.MoveHead();
}
template <int ind, typename HEAD_T, int cur_alias, int base_tag, typename... T,
          typename std::enable_if<
              (ind != -1 && ind == base_tag + sizeof...(T))>::type* = nullptr>
auto&& Move(Context<HEAD_T, cur_alias, base_tag, T...>& ctx) {
  return ctx.MoveHead();
}

template <typename HEAD_T, int cur_alias, typename... PREV>
using DefaultContext = Context<HEAD_T, cur_alias, 0, PREV...>;
// For get inner nodes, not implemented yet.

template <size_t base_tag, size_t cur_alias, typename... Prev, typename HEAD_T,
          typename std::enable_if<(sizeof...(Prev) == 0)>::type* = nullptr>
static auto make_context(std::tuple<Prev...>&& prev_sets, HEAD_T&& head,
                         std::vector<std::vector<offset_t>>&& offsets) {
  return Context<HEAD_T, cur_alias, base_tag, grape::EmptyType>(
      std::move(head));
}

template <size_t base_tag, size_t cur_alias, typename... Prev, typename HEAD_T,
          typename std::enable_if<(sizeof...(Prev) > 0)>::type* = nullptr>
static auto make_context(std::tuple<Prev...>&& prev_sets, HEAD_T&& head,
                         std::vector<std::vector<offset_t>>&& offsets) {
  return Context<HEAD_T, cur_alias, base_tag, Prev...>(
      std::move(head), std::move(prev_sets), std::move(offsets));
}

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_CONTEXT_H_
