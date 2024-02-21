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
#ifndef ENGINES_DS_MULTI_VERTEX_SET_MULTI_LABEL_VERTEX_SET_H_
#define ENGINES_DS_MULTI_VERTEX_SET_MULTI_LABEL_VERTEX_SET_H_

#include <array>
#include <unordered_set>
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "grape/util.h"
#include "grape/utils/bitset.h"

namespace gs {

template <typename VERTEX_SET_T, size_t N>
class MultiLabelVertexSetIter {
 public:
  using lid_t = typename VERTEX_SET_T::lid_t;
  using self_type_t = MultiLabelVertexSetIter<VERTEX_SET_T, N>;
  using inner_iter_t = typename VERTEX_SET_T::iterator;
  using data_tuple_t = typename inner_iter_t::data_tuple_t;
  using index_ele_tuple_t =
      std::tuple<size_t, typename VERTEX_SET_T::index_ele_tuple_t>;

  MultiLabelVertexSetIter(
      const std::array<VERTEX_SET_T, N>& set_array,
      const std::array<std::vector<offset_t>, N>& offset_array,
      std::array<inner_iter_t, N>&& begin_iters,
      std::array<inner_iter_t, N>&& end_iters, size_t ind)
      : set_array_(set_array),
        offset_array_(offset_array),
        begin_iters_(std::move(begin_iters)),
        end_iters_(std::move(end_iters)),
        ind_(ind),
        cur_label_(0),
        limit_(offset_array_[0].size() - 1),
        safe_eles(0) {
    for (size_t i = 0; i < N; ++i) {
      local_ind_[i] = 0;
    }
    probe_for_next();
  }

  lid_t GetVertex() const { return begin_iters_[cur_label_].GetVertex(); }

  lid_t GetElement() const { return GetVertex(); }

  data_tuple_t GetData() const { return begin_iters_[cur_label_].GetData(); }

  // Get the current ind of which set we are using.
  size_t GetCurInd() const { return cur_label_; }

  size_t GetCurSetInnerInd() const { return local_ind_[cur_label_]; }

  inline const self_type_t& operator++() {
    ++begin_iters_[cur_label_];
    ++local_ind_[cur_label_];
    if (safe_eles > 0) {
      safe_eles -= 1;
    } else {
      cur_label_ = cur_label_ + 1;
      probe_for_next();
    }

    return *this;
  }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(
        cur_label_, std::make_tuple(local_ind_[cur_label_],
                                    begin_iters_[cur_label_].GetVertex()));
  }

  inline bool operator==(const self_type_t& rhs) const {
    return ind_ == rhs.ind_;
  }

  inline bool operator!=(const self_type_t& rhs) const {
    return ind_ != rhs.ind_;
  }

  inline bool operator<(const self_type_t& rhs) const {
    return ind_ < rhs.ind_;
  }

  inline const self_type_t& operator*() const { return *this; }

  inline const self_type_t* operator->() const { return this; }

  inline void probe_for_next() {
    while (ind_ < limit_) {
      while (cur_label_ < N &&
             local_ind_[cur_label_] >= offset_array_[cur_label_][ind_ + 1]) {
        cur_label_ += 1;
      }
      if (cur_label_ < N) {
        break;
      }
      cur_label_ = 0;
      ind_ += 1;
    }
    if (ind_ < limit_) {
      safe_eles = offset_array_[cur_label_][ind_ + 1] -
                  offset_array_[cur_label_][ind_] - 1;
    }
  }

 private:
  size_t ind_;
  size_t cur_label_;
  const std::array<VERTEX_SET_T, N>& set_array_;
  const std::array<std::vector<offset_t>, N> offset_array_;
  std::array<inner_iter_t, N> begin_iters_;
  std::array<inner_iter_t, N> end_iters_;
  std::array<size_t, N> local_ind_;
  size_t safe_eles;
  size_t limit_;
};

// The vertex sets can be of different labels.
// But share the same vertex set type.
template <typename VERTEX_SET_T, size_t N>
class MultiLabelVertexSet {
 public:
  using inner_iter = typename VERTEX_SET_T::iterator;
  using lid_t = typename VERTEX_SET_T::lid_t;
  using iterator = MultiLabelVertexSetIter<VERTEX_SET_T, N>;
  using data_tuple_t = typename VERTEX_SET_T::data_tuple_t;
  using index_ele_tuple_t =
      std::tuple<size_t, typename VERTEX_SET_T::index_ele_tuple_t>;
  using self_type_t = MultiLabelVertexSet<VERTEX_SET_T, N>;
  using flat_t = self_type_t;
  using EntityValueType = typename VERTEX_SET_T::EntityValueType;
  using label_id_t = typename VERTEX_SET_T::label_id_t;
  static constexpr auto ind_seq = std::make_index_sequence<N>{};
  static constexpr bool is_keyed = false;
  static constexpr bool is_vertex_set = true;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_edge_set = false;
  static constexpr bool is_multi_label = true;
  static constexpr bool is_collection = false;
  static constexpr bool is_general_set = false;
  static constexpr size_t num_labels = N;

  MultiLabelVertexSet(std::array<VERTEX_SET_T, N>&& set_array,
                      std::array<std::vector<offset_t>, N>&& offset_array)
      : set_array_(std::move(set_array)),
        offset_array_(std::move(offset_array)) {}

  iterator begin() const {
    auto begin_iters = create_begin_array(ind_seq);
    auto end_iters = create_end_array(ind_seq);
    return iterator(set_array_, offset_array_, std::move(begin_iters),
                    std::move(end_iters), 0);
  }

  iterator end() const {
    auto begin_iters = create_begin_array(ind_seq);
    auto end_iters = create_end_array(ind_seq);
    return iterator(set_array_, offset_array_, std::move(begin_iters),
                    std::move(end_iters),
                    std::get<0>(offset_array_).size() - 1);
  }

  template <size_t... Is>
  auto create_begin_array(std::index_sequence<Is...>) const {
    return gs::make_array<inner_iter>(set_array_[Is].begin()...);
  }

  template <size_t... Is>
  auto create_end_array(std::index_sequence<Is...>) const {
    return gs::make_array<inner_iter>(set_array_[Is].end()...);
  }

  size_t Size() const {
    size_t res = 0;
    for (auto s : set_array_) {
      res += s.Size();
    }
    return res;
  }

  std::array<label_id_t, N> GetLabels() const {
    std::array<label_id_t, N> labels;
    for (size_t i = 0; i < N; ++i) {
      labels[i] = set_array_[i].GetLabel();
    }
    return labels;
  }

  // subset inplace.
  void SubSetWithIndices(std::vector<offset_t>& select_indices) {
    std::vector<std::vector<offset_t>> indices_vec(N);
    std::vector<std::vector<offset_t>> local_offsets(N);
    size_t cur_cnt = 0;
    size_t select_indices_ind = 0;
    for (size_t i = 0; i < N; ++i) {
      local_offsets[i].emplace_back(0);
    }
    for (auto iter : *this) {
      auto set_ind = iter.GetCurInd();
      auto set_inner_ind = iter.GetCurSetInnerInd();
      // if (active_label.get_bit(array_[set_ind].first.GetLabel())) {
      while (select_indices_ind < select_indices.size() &&
             select_indices[select_indices_ind] < cur_cnt) {
        select_indices_ind++;
      }
      if (select_indices_ind >= select_indices.size()) {
        break;
      }
      if (select_indices[select_indices_ind] == cur_cnt) {
        indices_vec[set_ind].emplace_back(set_inner_ind);
      }
      local_offsets[set_ind].emplace_back(indices_vec[set_ind].size());
      cur_cnt += 1;
    }

    // for (size_t i = 0;i < N; ++i) {
    //   VLOG(10) << "sub set: " << i
    //            << ", offset: " << gs::to_string(local_offsets[i]);
    //   VLOG(10) << "sub set: " << i
    //            << ", indices: " << gs::to_string(indices_vec[i]);
    // }
    // regard offset array
    for (size_t i = 0; i < N; ++i) {
      for (size_t j = 0; j < offset_array_[i].size(); ++j) {
        local_offsets[i][j] = local_offsets[i][offset_array_[i][j]];
      }
    }
    // for (size_t i = 0;i < N; ++i) {
    //   VLOG(10) << "sub set: " << i
    //            << ", res offset: " << gs::to_string(local_offsets[i]);
    // }
    for (size_t i = 0; i < N; ++i) {
      set_array_[i].SubSetWithIndices(indices_vec[i]);
      offset_array_[i].swap(local_offsets[i]);
    }
  }

  // project self.
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<std::vector<offset_t>> indices_vec(N);
    std::array<std::vector<offset_t>, N> local_offsets;

    for (size_t i = 0; i < N; ++i) {
      local_offsets[i].emplace_back(0);
    }

    size_t cur_ind = 0;
    CHECK(Size() == repeat_array.size());
    for (auto iter : *this) {
      auto set_ind = iter.GetCurInd();
      auto set_inner_ind = iter.GetCurSetInnerInd();
      if (repeat_array[cur_ind] > 0) {
        for (size_t j = 0; j < repeat_array[cur_ind]; ++j) {
          indices_vec[set_ind].emplace_back(set_inner_ind);
          // local_offsets[set_ind].emplace_back(indices_vec[set_ind].size());
        }
        for (size_t j = 0; j < N; ++j) {
          local_offsets[j].emplace_back(indices_vec[j].size());
        }
      }
      cur_ind += 1;
    }

    auto res_set_array = make_set_offset_pair_array_pair(
        std::move(indices_vec), std::make_index_sequence<N>());
    MultiLabelVertexSet<VERTEX_SET_T, N> res_set(
        std::move(res_set_array), std::move(std::move(local_offsets)));
    return res_set;
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  self_type_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) {
    //<label_t, <ind_, vid_t>>
    std::array<std::vector<size_t>, N> indices;
    for (size_t i = 0; i < N; ++i) {
      offset_array_[i].clear();
    }

    // update offsets.
    std::array<size_t, N> local_ind;
    for (size_t i = 0; i < N; ++i) {
      local_ind[i] = 0;
      offset_array_[i].emplace_back(local_ind[i]);
    }
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_index_ele = std::get<col_ind>(index_ele_tuple[i]);
      VLOG(10) << "MultiLabel: got index ele: " << gs::to_string(cur_index_ele);
      auto label = std::get<0>(cur_index_ele);
      auto inner_ind = std::get<0>(std::get<1>(cur_index_ele));
      local_ind[label] += 1;

      indices[label].emplace_back(inner_ind);
      for (size_t i = 0; i < N; ++i) {
        offset_array_[i].emplace_back(local_ind[i]);
      }
    }
    for (size_t i = 0; i < N; ++i) {
      set_array_[i].SubSetWithIndices(indices[i]);
      // array_[i].second.emplace_back(local_ind[i]++);
      VLOG(10) << "offset for: " << i << ",is"
               << gs::to_string(offset_array_[i]);
    }

    for (size_t i = 0; i < N; ++i) {
      VLOG(10) << "Multi label finish flat: " << local_ind[i];
    }
    VLOG(10) << "size: " << Size();
    return std::move(*this);
  }

  // Filter vertex sets with expression and labels.
  template <typename LabelT, typename EXPRESSION, size_t num_labels,
            typename ELE_TUPLE,
            typename RES_SET_T = MultiLabelVertexSet<VERTEX_SET_T, N>,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr,
                         std::vector<std::vector<ELE_TUPLE>>& eles) const {
    // TODO: vector-based cols should be able to be selected with certain rows.

    std::unordered_set<LabelT> active_label;
    for (auto l : labels) {
      active_label.insert(l);
    }
    VLOG(10) << "finish set active label";

    std::vector<std::vector<offset_t>> indices_vec(N);
    std::vector<offset_t> global_offset;
    std::vector<std::vector<offset_t>> local_offsets(N);
    size_t cur_cnt = 0;
    global_offset.emplace_back(0);
    for (size_t i = 0; i < N; ++i) {
      local_offsets[i].emplace_back(0);
    }
    for (auto iter : *this) {
      auto set_ind = iter.GetCurInd();
      auto set_inner_ind = iter.GetCurSetInnerInd();
      if (active_label.find(set_array_[set_ind].GetLabel()) !=
          active_label.end()) {
        // check filter
        if (expr(eles[set_ind][set_inner_ind])) {
          indices_vec[set_ind].emplace_back(set_inner_ind);
          cur_cnt += 1;
        }
      }
      local_offsets[set_ind].emplace_back(indices_vec[set_ind].size());
      global_offset.emplace_back(cur_cnt);
    }
    // build global offset from local offset.

    std::array<std::vector<offset_t>, N> new_offset;
    for (size_t i = 0; i < N; ++i) {
      new_offset[i].reserve(offset_array_[i].size());
      for (size_t j = 0; j < offset_array_[i].size(); ++j) {
        new_offset[i].emplace_back(local_offsets[i][offset_array_[i][j]]);
        // local_offsets[i][j] = local_offsets[i][offset_array_[i][j]];
      }
    }

    auto res_set_array = make_set_offset_pair_array_pair(
        std::move(indices_vec), std::make_index_sequence<N>());
    MultiLabelVertexSet<VERTEX_SET_T, N> res_set(
        std::move(res_set_array), std::move(std::move(new_offset)));

    return std::make_pair(std::move(res_set), std::move(global_offset));
  }

  template <size_t... Is>
  auto make_set_offset_pair_array_pair(
      std::vector<std::vector<offset_t>>&& indices,
      std::index_sequence<Is...>) const {
    CHECK(indices.size() == N);
    return std::array<VERTEX_SET_T, N>{
        std::move(std::get<Is>(set_array_).WithIndices(indices[Is]))...};
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "Not implemented";
  }

  const VERTEX_SET_T& GetSet(size_t ind) const { return set_array_[ind]; }

  template <size_t Is>
  VERTEX_SET_T& GetSet() {
    return set_array_[Is];
  }

  template <size_t Is>
  std::vector<offset_t>& GetOffset() {
    return offset_array_[Is];
  }

  std::vector<offset_t>& GetOffset(size_t Is) { return offset_array_[Is]; }

  template <size_t Is, typename... PropT>
  void fillBuiltinPropsImpl(
      std::vector<std::vector<std::tuple<PropT...>>>& tuples,
      std::string& prop_name, std::vector<offset_t>& repeat_array) const {
    if constexpr (std::is_same_v<std::tuple_element_t<Is, std::tuple<PropT...>>,
                                 Dist>) {
      if (prop_name == "dist") {
        LOG(FATAL) << "Not supported";
      }
    }
  }

  template <typename... PropT, size_t... Is>
  void fillBuiltinPropsImpl(
      std::vector<std::vector<std::tuple<PropT...>>>& tuples,
      PropNameArray<PropT...>& prop_names, std::vector<offset_t>& repeat_array,
      std::index_sequence<Is...>) const {
    (fillBuiltinPropsImpl<Is, PropT...>(tuples, std::get<Is>(prop_names),
                                        repeat_array),
     ...);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::vector<std::tuple<PropT...>>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) const {
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  // No repeat array is not provided
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::vector<std::tuple<PropT...>>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    LOG(FATAL) << "not supported";
  }

 private:
  std::array<VERTEX_SET_T, N> set_array_;
  std::array<std::vector<offset_t>, N> offset_array_;
};
}  // namespace gs

#endif  // ENGINES_DS_MULTI_VERTEX_SET_MULTI_LABEL_VERTEX_SET_H_
