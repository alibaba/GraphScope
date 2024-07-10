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
#ifndef ENGINES_HQPS_DS_MULTI_VERTEX_SET_GENERAL_VERTEX_SET_H_
#define ENGINES_HQPS_DS_MULTI_VERTEX_SET_GENERAL_VERTEX_SET_H_

#include <array>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "grape/types.h"
#include "grape/util.h"
#include "grape/utils/bitset.h"

#include "flex/engines/hqps_db/core/null_record.h"
#include "flex/engines/hqps_db/core/utils/graph_utils.h"
#include "flex/engines/hqps_db/core/utils/hqps_type.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

namespace gs {

// return the old labels, that are active in filter.
template <typename VID_T, typename LabelT, typename EXPR,
          typename... PropGetterT, size_t filter_num_labels>
auto general_project_vertices_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<grape::Bitset>& old_bit_sets,
    const std::vector<LabelT>& old_labels,
    const std::array<LabelT, filter_num_labels>& filter_labels,
    const EXPR& expr,
    const std::vector<std::tuple<PropGetterT...>>& prop_getters) {
  std::vector<VID_T> res_vec;
  CHECK(old_bit_sets.size() == old_labels.size());
  CHECK(prop_getters.size() == old_labels.size());
  auto old_num_labels = old_bit_sets.size();
  std::vector<grape::Bitset> res_bitsets(old_num_labels);
  LOG(INFO) << "old labels: " << gs::to_string(old_labels)
            << ", filter labels: " << gs::to_string(filter_labels);
  // reserve enough size for bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].init(old_vec.size());
  }
  // std::vector<size_t> select_label_id;
  std::unordered_set<LabelT> label_ind_set;
  if constexpr (filter_num_labels == 0) {
    for (size_t i = 0; i < old_labels.size(); ++i) {
      label_ind_set.insert(i);
    }
  } else {
    for (size_t i = 0; i < old_labels.size(); ++i) {
      auto it =
          std::find(filter_labels.begin(), filter_labels.end(), old_labels[i]);
      if (it != filter_labels.end()) {
        label_ind_set.insert(i);
      }
    }
  }
  VLOG(10) << "selected label ids: " << label_ind_set.size()
           << ", out of size: " << old_labels.size();
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (size_t i = 0; i < old_vec.size(); ++i) {
    size_t j = 0;
    while (j < old_num_labels) {
      if (old_bit_sets[j].get_bit(i)) {
        break;
      }
      ++j;
    }
    CHECK(j < old_num_labels) << "no label is active at ind: " << i;
    if constexpr (sizeof...(PropGetterT) == 0) {
      if (expr()) {
        res_bitsets[j].set_bit(res_vec.size());
        res_vec.push_back(old_vec[i]);
      }
    } else {
      if (label_ind_set.find(j) != label_ind_set.end()) {
        if (std::apply(expr, get_view_from_prop_getters(prop_getters[j],
                                                        old_vec[i]))) {
          res_bitsets[j].set_bit(res_vec.size());
          res_vec.push_back(old_vec[i]);
        }
      }
    }
    offset.emplace_back(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  // resize bitset.
  LOG(INFO) << "res bitset size: " << res_bitsets.size();
  for (size_t i = 0; i < old_num_labels; ++i) {
    LOG(INFO) << "Shrink bitset: " << i
              << ", from size: " << res_bitsets[i].cardinality()
              << " to size: " << res_vec.size();
    res_bitsets[i].resize(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  return std::make_tuple(std::move(res_vec), std::move(res_bitsets),
                         std::move(offset));
}

template <typename VID_T, typename DATA_TUPLE, typename LabelT, typename EXPR,
          typename... PropGetterT, size_t filter_num_labels>
auto general_project_vertices_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<DATA_TUPLE>& old_data_vec,
    const std::vector<grape::Bitset>& old_bit_sets,
    const std::vector<LabelT>& old_labels,
    const std::array<LabelT, filter_num_labels>& filter_labels,
    const EXPR& expr,
    const std::vector<std::tuple<PropGetterT...>>& prop_getters) {
  std::vector<VID_T> res_vec;
  std::vector<DATA_TUPLE> res_data_vec;
  CHECK(old_bit_sets.size() == old_labels.size());
  CHECK(prop_getters.size() == old_labels.size());
  auto old_num_labels = old_bit_sets.size();
  std::vector<grape::Bitset> res_bitsets(old_num_labels);
  // reserve enough size for bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].init(old_vec.size());
  }
  std::vector<size_t> select_label_id;
  if constexpr (filter_num_labels == 0) {
    for (size_t i = 0; i < old_labels.size(); ++i) {
      select_label_id.emplace_back(i);
    }
  } else {
    std::unordered_set<LabelT> set;
    for (auto l : filter_labels) {
      set.insert(l);
    }
    for (size_t i = 0; i < old_labels.size(); ++i) {
      if (set.find(old_labels[i]) != set.end()) {
        select_label_id.emplace_back(i);
      }
    }
  }
  VLOG(10) << "selected label ids: " << gs::to_string(select_label_id)
           << ", out of size: " << old_labels.size();
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (size_t i = 0; i < old_vec.size(); ++i) {
    for (auto label_id : select_label_id) {
      if (old_bit_sets[label_id].get_bit(i)) {
        if constexpr (sizeof...(PropGetterT) == 0) {
          if (expr()) {
            res_bitsets[label_id].set_bit(res_vec.size());
            res_vec.push_back(old_vec[i]);
            res_data_vec.push_back(old_data_vec[i]);
            break;
          }
        } else {
          auto eles =
              get_view_from_prop_getters(prop_getters[label_id], old_vec[i]);
          if (std::apply(expr, eles)) {
            res_bitsets[label_id].set_bit(res_vec.size());
            res_vec.push_back(old_vec[i]);
            res_data_vec.push_back(old_data_vec[i]);
            break;
          }
        }
      }
    }
    offset.emplace_back(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  // resize bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].resize(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  return std::make_tuple(std::move(res_vec), std::move(res_data_vec),
                         std::move(res_bitsets), std::move(offset));
}

template <typename VID_T, typename LabelT, size_t filter_num_labels>
auto general_project_vertices_no_expr_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<grape::Bitset>& old_bit_sets,
    const std::vector<LabelT>& old_labels,
    const std::array<LabelT, filter_num_labels>& filter_labels) {
  auto old_num_labels = old_bit_sets.size();
  std::vector<VID_T> res_vec;
  std::vector<grape::Bitset> res_bitsets(old_num_labels);
  // reserve enough size for bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].init(old_vec.size());
  }
  std::vector<size_t> select_label_id;
  if constexpr (filter_num_labels == 0) {
    for (size_t i = 0; i < old_labels.size(); ++i) {
      select_label_id.emplace_back(i);
    }
  } else {
    std::unordered_set<LabelT> set;
    for (auto l : filter_labels) {
      set.insert(l);
    }
    for (size_t i = 0; i < old_labels.size(); ++i) {
      if (set.find(old_labels[i]) != set.end()) {
        select_label_id.emplace_back(i);
      }
    }
  }
  VLOG(10) << "selected label ids: " << gs::to_string(select_label_id)
           << ", out of size: " << old_labels.size();
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (size_t i = 0; i < old_vec.size(); ++i) {
    for (auto label_id : select_label_id) {
      if (old_bit_sets[label_id].get_bit(i)) {
        res_bitsets[label_id].set_bit(res_vec.size());
        res_vec.push_back(old_vec[i]);
        break;
      }
    }
    offset.emplace_back(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  // resize bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].resize(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  return std::make_tuple(std::move(res_vec), std::move(res_bitsets),
                         std::move(offset));
}

template <typename VID_T, typename DATA_TUPLE, typename LabelT,
          size_t filter_num_labels>
auto general_project_vertices_no_expr_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<DATA_TUPLE>& old_data_vec,
    const std::vector<grape::Bitset>& old_bit_sets,
    const std::vector<LabelT>& old_labels,
    const std::array<LabelT, filter_num_labels>& filter_labels) {
  auto old_num_labels = old_bit_sets.size();
  std::vector<VID_T> res_vec;
  std::vector<DATA_TUPLE> res_data_vec;
  std::vector<grape::Bitset> res_bitsets(old_num_labels);
  // reserve enough size for bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].init(old_vec.size());
  }
  std::vector<size_t> select_label_id;
  if constexpr (filter_num_labels == 0) {
    for (size_t i = 0; i < old_labels.size(); ++i) {
      select_label_id.emplace_back(i);
    }
  } else {
    std::unordered_set<LabelT> set;
    for (auto l : filter_labels) {
      set.insert(l);
    }
    for (size_t i = 0; i < old_labels.size(); ++i) {
      if (set.find(old_labels[i]) != set.end()) {
        select_label_id.emplace_back(i);
      }
    }
  }
  VLOG(10) << "selected label ids: " << gs::to_string(select_label_id)
           << ", out of size: " << old_labels.size();
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (size_t i = 0; i < old_vec.size(); ++i) {
    for (auto label_id : select_label_id) {
      if (old_bit_sets[label_id].get_bit(i)) {
        res_bitsets[label_id].set_bit(res_vec.size());
        res_vec.push_back(old_vec[i]);
        res_data_vec.push_back(old_data_vec[i]);
        break;
      }
    }
    offset.emplace_back(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  // resize bitset.
  for (size_t i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].resize(res_vec.size());
  }
  for (size_t i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (size_t j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  return std::make_tuple(std::move(res_vec), std::move(res_data_vec),
                         std::move(res_bitsets), std::move(offset));
}

template <int tag_id, int res_id, int... Is, typename lid_t>
auto general_project_with_repeat_array_impl(
    const KeyAlias<tag_id, res_id, Is...>& key_alias,
    const std::vector<size_t>& repeat_array,
    const std::vector<lid_t>& old_lids) {
  using res_t = std::vector<
      std::tuple<typename gs::tuple_element<Is, std::tuple<lid_t>>::type...>>;

  res_t res_vec;
  for (size_t i = 0; i < repeat_array.size(); ++i) {
    for (size_t j = 0; j < repeat_array[i]; ++j) {
      auto tuple = std::make_tuple(old_lids[i]);
      res_vec.emplace_back(std::make_tuple(gs::get_from_tuple<Is>(tuple)...));
    }
  }
  return res_vec;
}

template <size_t col_ind, typename... index_ele_tuple_t, typename DATA_TUPLE,
          typename lid_t>
auto generalSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    const std::vector<lid_t>& origin_vids,
    const std::vector<DATA_TUPLE>& origin_data,
    const std::vector<grape::Bitset>& origin_bitsets) {
  size_t dst_size = index_ele_tuples.size();
  std::vector<lid_t> res_vids;
  std::vector<DATA_TUPLE> res_data_vec;
  std::vector<grape::Bitset> res_bitsets(origin_bitsets.size());
  res_vids.reserve(dst_size);
  res_data_vec.reserve(dst_size);
  for (size_t i = 0; i < origin_bitsets.size(); ++i) {
    res_bitsets[i].init(dst_size);
  }
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());

    for (size_t i = 0; i < origin_bitsets.size(); ++i) {
      if (origin_bitsets[i].get_bit(ind)) {
        res_bitsets[i].set_bit(res_vids.size());
        break;
      }
    }
    res_vids.emplace_back(origin_vids[ind]);
    res_data_vec.emplace_back(origin_data[ind]);
  }
  return std::make_tuple(std::move(res_vids), std::move(res_data_vec),
                         std::move(res_bitsets));
}

template <size_t col_ind, typename... index_ele_tuple_t, typename lid_t>
auto generalSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    const std::vector<lid_t>& origin_vids,
    const std::vector<grape::Bitset>& origin_bitsets) {
  size_t dst_size = index_ele_tuples.size();
  std::vector<lid_t> res_vids;
  std::vector<grape::Bitset> res_bitsets(origin_bitsets.size());
  res_vids.reserve(dst_size);
  for (size_t i = 0; i < origin_bitsets.size(); ++i) {
    res_bitsets[i].init(dst_size);
  }
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());

    for (size_t i = 0; i < origin_bitsets.size(); ++i) {
      if (origin_bitsets[i].get_bit(ind)) {
        res_bitsets[i].set_bit(res_vids.size());
        break;
      }
    }
    res_vids.emplace_back(origin_vids[ind]);
  }
  return std::make_pair(std::move(res_vids), std::move(res_bitsets));
}

template <typename VID_T, typename LabelT, typename... T>
class GeneralVertexSet;

template <typename VID_T, typename LabelT, typename... T>
class GeneralVertexSetBuilder {
 public:
  using res_t = GeneralVertexSet<VID_T, LabelT, T...>;
  using ele_tuple_t = typename res_t::ele_tuple_t;
  using data_tuple_t = typename res_t::data_tuple_t;
  using index_ele_tuple_t = typename res_t::index_ele_tuple_t;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;
  static constexpr bool is_collection_builder = false;
  static constexpr bool is_general_vertex_set_builder = true;

  GeneralVertexSetBuilder(
      const std::vector<VID_T>& vec,
      const std::vector<std::tuple<T...>>& data_vec,
      const std::array<std::string, sizeof...(T)>& prop_names,
      const std::vector<LabelT>& labels,
      const std::vector<grape::Bitset>& bitsets)
      : labels_(labels), prop_names_(prop_names) {
    vec_.reserve(vec.size());
    data_vec_.reserve(data_vec.size());
    bitsets_.resize(bitsets.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets_[i].init(vec.size());
    }
  }

  void Insert(const index_ele_tuple_t& tuple, const data_tuple_t& data) {
    vec_.emplace_back(std::get<2>(tuple));
    data_vec_.emplace_back(data);
    CHECK(std::get<1>(tuple) < bitsets_.size());
    if (vec_.size() - 1 > bitsets_[std::get<1>(tuple)].cardinality()) {
      VLOG(10) << "vec size: " << vec_.size() << ", bitset size: "
               << bitsets_[std::get<1>(tuple)].cardinality();
      for (size_t i = 0; i < bitsets_.size(); ++i) {
        bitsets_[i].resize(2 * bitsets_[i].cardinality());
      }
    }
    bitsets_[std::get<1>(tuple)].set_bit(vec_.size() - 1);
  }

  res_t Build() {
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].resize(vec_.size());
    }
    return res_t(std::move(vec_), std::move(data_vec_), std::move(prop_names_),
                 std::move(labels_), std::move(bitsets_));
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::vector<std::tuple<T...>> data_vec_;
  std::array<std::string, sizeof...(T)> prop_names_;
  std::vector<LabelT> labels_;
  std::vector<grape::Bitset> bitsets_;
};

template <typename VID_T, typename LabelT, typename... T>
class GeneralVertexSetKeyedBuilder {
 public:
  using res_t = GeneralVertexSet<VID_T, LabelT, T...>;
  using ele_tuple_t = typename res_t::ele_tuple_t;
  using data_tuple_t = typename res_t::data_tuple_t;
  using index_ele_tuple_t = typename res_t::index_ele_tuple_t;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;
  static constexpr bool is_collection_builder = false;
  static constexpr bool is_general_vertex_set_builder = true;

  GeneralVertexSetKeyedBuilder(const res_t& from)
      : GeneralVertexSetKeyedBuilder(from.Size(), from.GetPropNames(),
                                     from.GetLabels()) {}

  GeneralVertexSetKeyedBuilder(
      size_t size, const std::array<std::string, sizeof...(T)>& prop_names,
      const std::vector<LabelT>& labels)
      : ind_(0), labels_(labels), prop_names_(prop_names) {
    vec_.reserve(size);
    data_vec_.reserve(size);
    bitsets_.resize(size);
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].init(size);
    }
  }

  int32_t insert(const index_ele_tuple_t& tuple, const data_tuple_t& data) {
    CHECK(std::get<1>(tuple) < bitsets_.size());
    auto global_id = GlobalId(std::get<1>(tuple), std::get<2>(tuple));
    if (IsNull(global_id)) {
      return -1;
    }
    if (global_id_map_.find(global_id) != global_id_map_.end()) {
      return global_id_map_[global_id];
    } else {
      global_id_map_[global_id] = ind_;
      data_vec_.emplace_back(data);
      vec_.emplace_back(std::get<2>(tuple));
      if (vec_.size() - 1 > bitsets_[std::get<1>(tuple)].cardinality()) {
        VLOG(10) << "vec size: " << vec_.size() << ", bitset size: "
                 << bitsets_[std::get<1>(tuple)].cardinality();
        for (size_t i = 0; i < bitsets_.size(); ++i) {
          bitsets_[i].resize(2 * bitsets_[i].cardinality());
        }
      }
      bitsets_[std::get<1>(tuple)].set_bit(vec_.size() - 1);
      return ind_++;
    }
  }

  res_t Build() {
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].resize(vec_.size());
    }
    return res_t(std::move(vec_), std::move(data_vec_), std::move(prop_names_),
                 std::move(labels_), std::move(bitsets_));
  }

  size_t Size() const { return vec_.size(); }

 private:
  size_t ind_;
  std::vector<VID_T> vec_;
  std::vector<std::tuple<T...>> data_vec_;
  std::array<std::string, sizeof...(T)> prop_names_;
  std::vector<LabelT> labels_;
  std::vector<grape::Bitset> bitsets_;
  std::unordered_map<GlobalId, size_t> global_id_map_;
};

// Specialize for grape::EmptyType
template <typename VID_T, typename LabelT>
class GeneralVertexSetBuilder<VID_T, LabelT, grape::EmptyType> {
 public:
  using res_t = GeneralVertexSet<VID_T, LabelT, grape::EmptyType>;
  using ele_tuple_t = typename res_t::ele_tuple_t;
  using data_tuple_t = typename res_t::data_tuple_t;
  using index_ele_tuple_t = typename res_t::index_ele_tuple_t;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;
  static constexpr bool is_collection_builder = false;
  static constexpr bool is_general_vertex_set_builder = true;

  GeneralVertexSetBuilder(const std::vector<VID_T>& vec,
                          const std::vector<LabelT>& labels,
                          const std::vector<grape::Bitset>& bitsets)
      : labels_(labels) {
    vec_.reserve(vec.size());
    bitsets_.resize(bitsets.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      CHECK(bitsets[i].cardinality() == vec.size());
      bitsets_[i].init(vec.size());
    }
  }

  GeneralVertexSetBuilder(size_t size, const std::vector<LabelT>& labels)
      : labels_(labels) {
    vec_.reserve(size);
    bitsets_.resize(labels.size());
    VLOG(10) << "Create general vertex builder : " << size
             << ", labels: " << labels.size();
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].init(size);
    }
  }

  void Insert(const index_ele_tuple_t& tuple, const data_tuple_t& data) {
    vec_.emplace_back(std::get<2>(tuple));
    CHECK(std::get<1>(tuple) < bitsets_.size());
    if (vec_.size() - 1 > bitsets_[std::get<1>(tuple)].cardinality()) {
      VLOG(10) << "vec size: " << vec_.size() << ", bitset size: "
               << bitsets_[std::get<1>(tuple)].cardinality();
      for (size_t i = 0; i < bitsets_.size(); ++i) {
        bitsets_[i].resize(2 * bitsets_[i].cardinality());
      }
    }
    bitsets_[std::get<1>(tuple)].set_bit(vec_.size() - 1);
  }

  res_t Build() {
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      LOG(INFO) << "Shrink bitset: " << i
                << ", from size: " << bitsets_[i].cardinality()
                << " to size: " << vec_.size();
      bitsets_[i].resize(vec_.size());
    }
    return res_t(std::move(vec_), std::move(labels_), std::move(bitsets_));
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::vector<LabelT> labels_;
  std::vector<grape::Bitset> bitsets_;
};

template <typename VID_T, typename LabelT>
class GeneralVertexSetKeyedBuilder<VID_T, LabelT, grape::EmptyType> {
 public:
  using res_t = GeneralVertexSet<VID_T, LabelT, grape::EmptyType>;
  using ele_tuple_t = typename res_t::ele_tuple_t;
  using data_tuple_t = typename res_t::data_tuple_t;
  using index_ele_tuple_t = typename res_t::index_ele_tuple_t;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;
  static constexpr bool is_collection_builder = false;
  static constexpr bool is_general_vertex_set_builder = true;

  GeneralVertexSetKeyedBuilder(const res_t& from)
      : GeneralVertexSetKeyedBuilder(from.Size(), from.GetLabels()) {}

  GeneralVertexSetKeyedBuilder(size_t size, const std::vector<LabelT>& labels)
      : labels_(labels) {
    vec_.reserve(size);
    bitsets_.resize(labels.size());
    VLOG(10) << "Create general vertex builder : " << size
             << ", labels: " << labels.size();
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].init(size);
    }
  }

  int32_t insert(const index_ele_tuple_t& tuple, const data_tuple_t& data) {
    CHECK(std::get<1>(tuple) < bitsets_.size());
    auto global_id = GlobalId(std::get<1>(tuple), std::get<2>(tuple));
    if (IsNull(global_id)) {
      return -1;
    }
    if (global_id_map_.find(global_id) != global_id_map_.end()) {
      return global_id_map_[global_id];
    } else {
      global_id_map_[global_id] = ind_;
      vec_.emplace_back(std::get<2>(tuple));
      if (vec_.size() - 1 > bitsets_[std::get<1>(tuple)].cardinality()) {
        VLOG(10) << "vec size: " << vec_.size() << ", bitset size: "
                 << bitsets_[std::get<1>(tuple)].cardinality();
        for (size_t i = 0; i < bitsets_.size(); ++i) {
          bitsets_[i].resize(2 * bitsets_[i].cardinality());
        }
      }
      bitsets_[std::get<1>(tuple)].set_bit(vec_.size() - 1);
      return ind_++;
    }
  }

  res_t Build() {
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      LOG(INFO) << "Shrink bitset: " << i
                << ", from size: " << bitsets_[i].cardinality()
                << " to size: " << vec_.size();
      bitsets_[i].resize(vec_.size());
    }
    return res_t(std::move(vec_), std::move(labels_), std::move(bitsets_));
  }

  size_t Size() const { return vec_.size(); }

 private:
  size_t ind_;
  std::vector<VID_T> vec_;
  std::vector<LabelT> labels_;
  std::vector<grape::Bitset> bitsets_;
  std::unordered_map<GlobalId, size_t> global_id_map_;
};

template <typename VID_T, typename LabelT, typename... T>
class GeneralVertexSetIter {
 public:
  using lid_t = VID_T;
  using self_type_t = GeneralVertexSetIter<VID_T, LabelT, T...>;
  using index_ele_tuple_t = std::tuple<size_t, size_t, VID_T, std::tuple<T...>>;
  using ele_tuple_t =
      typename GeneralVertexSet<VID_T, LabelT, T...>::ele_tuple_t;
  using data_tuple_t = std::tuple<VID_T, std::tuple<T...>>;

  GeneralVertexSetIter(const std::vector<VID_T>& vec,
                       const std::vector<std::tuple<T...>>& data_vec,
                       const std::vector<std::string>& prop_names,
                       const std::vector<grape::Bitset>& bitsets,
                       const std::vector<LabelT>& labels, size_t ind)
      : vec_(vec),
        data_vec_(data_vec),
        prop_names_(prop_names),
        bitsets_(bitsets),
        labels_(labels),
        ind_(ind) {}

  ele_tuple_t GetElement() const {
    size_t label_ind = 0;
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      if (bitsets_[i].get_bit(ind_)) {
        label_ind = i;
        break;
      }
    }
    return std::make_tuple(GlobalId(labels_[label_ind], vec_[ind_]),
                           data_vec_[ind_]);
  }

  data_tuple_t GetData() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    size_t label_ind = 0;
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      if (bitsets_[i].get_bit(ind_)) {
        label_ind = i;
        break;
      }
    }
    return std::make_tuple(ind_, label_ind, vec_[ind_], data_vec_[ind_]);
  }

  lid_t GetVertex() const { return vec_[ind_]; }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
  }

  inline self_type_t operator++(int) {
    self_type_t ret(*this);
    ++ind_;
    return ret;
  }

  // We may never compare to other kind of iterators
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

 private:
  const std::vector<VID_T>& vec_;
  const std::vector<std::tuple<T...>>& data_vec_;
  const std::vector<std::string>& prop_names_;
  const std::vector<grape::Bitset>& bitsets_;
  const std::vector<LabelT>& labels_;
  size_t ind_;
};

template <typename VID_T, typename LabelT>
class GeneralVertexSetIter<VID_T, LabelT, grape::EmptyType> {
 public:
  using lid_t = VID_T;
  using self_type_t = GeneralVertexSetIter<VID_T, LabelT, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, size_t, VID_T>;
  using ele_tuple_t =
      typename GeneralVertexSet<VID_T, LabelT, grape::EmptyType>::ele_tuple_t;
  using data_tuple_t = std::tuple<VID_T>;

  GeneralVertexSetIter(const std::vector<VID_T>& vec,
                       const std::vector<grape::Bitset>& bitsets,
                       const std::vector<LabelT>& labels, size_t ind)
      : vec_(vec), bitsets_(bitsets), labels_(labels), ind_(ind) {}

  ele_tuple_t GetElement() const {
    size_t label_ind = 0;
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      if (bitsets_[i].get_bit(ind_)) {
        label_ind = i;
        break;
      }
    }
    return GlobalId(labels_[label_ind], vec_[ind_]);
  }

  data_tuple_t GetData() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    size_t label_ind = 0;
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      if (bitsets_[i].get_bit(ind_)) {
        label_ind = i;
        break;
      }
    }
    return std::make_tuple(ind_, label_ind, vec_[ind_]);
  }

  lid_t GetVertex() const { return vec_[ind_]; }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
  }

  inline self_type_t operator++(int) {
    self_type_t ret(*this);
    ++ind_;
    return ret;
  }

  // We may never compare to other kind of iterators
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

 private:
  const std::vector<VID_T>& vec_;
  const std::vector<grape::Bitset>& bitsets_;
  const std::vector<LabelT>& labels_;
  size_t ind_;
};

template <typename VID_T, typename LabelT, typename... T>
class GeneralVertexSet {
 public:
  using lid_t = VID_T;
  using self_type_t = GeneralVertexSet<VID_T, LabelT, T...>;
  using iterator = GeneralVertexSetIter<VID_T, LabelT, T...>;
  using index_ele_tuple_t = std::tuple<size_t, size_t, VID_T, std::tuple<T...>>;
  using ele_tuple_t = std::tuple<GlobalId, std::tuple<T...>>;
  using data_tuple_t = std::tuple<VID_T, std::tuple<T...>>;
  using flat_t = self_type_t;
  using EntityValueType = VID_T;
  using builder_t = GeneralVertexSetBuilder<VID_T, LabelT, T...>;

  static constexpr bool is_vertex_set = true;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_general_set = true;
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_row_vertex_set = false;
  GeneralVertexSet(std::vector<VID_T>&& vec,
                   std::vector<std::tuple<T...>>&& data_vec,
                   std::vector<std::string>&& prop_names,
                   std::vector<LabelT>&& label_names,
                   std::vector<grape::Bitset>&& bitsets)
      : vec_(std::move(vec)),
        data_vec_(std::move(data_vec)),
        label_names_(std::move(label_names)),
        prop_names_(std::move(prop_names)) {
    CHECK(label_names_.size() == bitsets.size());
    CHECK(vec_.size() == data_vec_.size());
    bitsets_.resize(bitsets.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets_[i].swap(bitsets[i]);
    }
    if (bitsets_.size() > 0) {
      VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
               << ", bitset size: " << bitsets_[0].cardinality()
               << ", ind_ele_t: " << demangle(index_ele_tuple_t());
    }
  }

  GeneralVertexSet(GeneralVertexSet&& other)
      : vec_(std::move(other.vec_)),
        data_vec_(std::move(other.data_vec_)),
        label_names_(std::move(other.label_names_)),
        prop_names_(std::move(other.prop_names_)) {
    bitsets_.resize(other.bitsets_.size());
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].swap(other.bitsets_[i]);
    }
    if (bitsets_.size() > 0) {
      VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
               << ", bitset size: " << bitsets_[0].cardinality();
    }
  }

  GeneralVertexSet(const GeneralVertexSet& other)
      : vec_(other.vec_),
        data_vec_(other.data_vec_),
        label_names_(other.label_names_),
        prop_names_(other.prop_names_) {
    bitsets_.resize(other.bitsets_.size());
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].copy(other.bitsets_[i]);
    }
    if (bitsets_.size() > 0) {
      VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
               << ", bitset size: " << bitsets_[0].cardinality();
    }
  }

  iterator begin() const {
    return iterator(vec_, data_vec_, prop_names_, bitsets_, label_names_, 0);
  }

  iterator end() const {
    return iterator(vec_, data_vec_, prop_names_, bitsets_, label_names_,
                    vec_.size());
  }

  builder_t CreateBuilder() const {
    return builder_t(vec_, data_vec_, prop_names_, label_names_, bitsets_);
  }

  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels,
                         EXPRESSION& exprs,
                         std::vector<PROP_GETTER>& prop_getter) const {
    // TODO: vector-based cols should be able to be selected with
    // certain rows.

    auto tuple =
        general_project_vertices_impl(vec_, data_vec_, bitsets_, label_names_,
                                      filter_labels, exprs, prop_getter);
    auto copied_label_names(label_names_);
    auto copied_prop_names(prop_names_);
    auto set = self_type_t(
        std::move(std::get<0>(tuple)), std::move(std::get<1>(tuple)),
        std::move(copied_prop_names), std::move(copied_label_names),
        std::move(std::get<2>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<3>(tuple)));
  }

  // project without expression.
  template <size_t num_labels, typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels) const {
    // TODO: vector-based cols should be able to be selected with
    // certain rows.

    auto tuple = general_project_vertices_no_expr_impl(
        vec_, data_vec_, bitsets_, label_names_, filter_labels);
    auto copied_label_names(label_names_);
    auto copied_prop_names(prop_names_);
    auto set = self_type_t(
        std::move(std::get<0>(tuple)), std::move(std::get<1>(tuple)),
        std::move(copied_prop_names), std::move(copied_label_names),
        std::move(std::get<2>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<3>(tuple)));
  }

  const std::vector<LabelT>& GetLabels() const { return label_names_; }

  LabelT GetLabel(size_t i) const { return label_names_[i]; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill res with vertex labels
    for (size_t i = 0; i < vec_.size(); ++i) {
      for (size_t j = 0; j < bitsets_.size(); ++j) {
        if (bitsets_[j].get_bit(i)) {
          res.emplace_back(label_names_[j]);
          break;
        }
      }
    }
    return res;
  }

  // generate label indices.
  std::vector<uint8_t> GenerateLabelIndices() const {
    std::vector<uint8_t> label_indices;
    label_indices.resize(vec_.size(), 255);
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      for (size_t j = 0; j < bitsets_[i].cardinality(); ++j) {
        if (bitsets_[i].get_bit(j)) {
          CHECK(label_indices[j] == 255);
          label_indices[j] = i;
        }
      }
    }
    return label_indices;
  }

  const std::vector<grape::Bitset>& GetBitsets() const { return bitsets_; }

  const std::vector<VID_T>& GetVertices() const { return vec_; }

  const std::vector<std::tuple<T...>>& GetDataVec() const { return data_vec_; }

  const auto& GetPropNames() const { return prop_names_; }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVerticesWithLabel(
      label_t label_id) const {
    // find label_id in label_names_
    auto it = std::find(label_names_.begin(), label_names_.end(), label_id);
    if (it == label_names_.end()) {
      return std::make_pair(std::vector<VID_T>(), std::vector<int32_t>());
    } else {
      auto ind = std::distance(label_names_.begin(), it);
      return GetVertices(ind);
    }
  }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVertices(
      size_t ind) const {
    CHECK(ind < bitsets_.size());
    std::vector<VID_T> res;
    std::vector<int32_t> active_ind;
    size_t cnt = bitsets_[ind].count();
    res.reserve(cnt);
    active_ind.reserve(cnt);
    for (size_t i = 0; i < bitsets_[ind].cardinality(); ++i) {
      if (bitsets_[ind].get_bit(i)) {
        res.push_back(vec_[i]);
        active_ind.push_back(i);
      }
    }
    VLOG(10) << "Got vertices of tag: " << ind
             << ", res vertices: " << res.size()
             << ", active_ind: " << active_ind.size();
    return std::make_pair(std::move(res), std::move(active_ind));
  }

  // subset with indices.
  void SubSetWithIndices(std::vector<size_t>& indices) {
    std::vector<VID_T> res_vec;
    std::vector<std::tuple<T...>> res_data_vec;
    std::vector<grape::Bitset> res_bitsets(bitsets_.size());
    for (auto& i : res_bitsets) {
      i.init(indices.size());
    }
    res_vec.reserve(indices.size());
    res_data_vec.reserve(indices.size());
    for (auto i : indices) {
      size_t label_ind = 0;
      for (; label_ind < bitsets_.size(); ++label_ind) {
        if (bitsets_[label_ind].get_bit(i)) {
          break;
        }
      }
      CHECK(label_ind < bitsets_.size());
      res_bitsets[label_ind].set_bit(res_vec.size());
      res_vec.emplace_back(vec_[i]);
      res_data_vec.emplace_back(data_vec_[i]);
    }
    vec_.swap(res_vec);
    data_vec_.swap(res_data_vec);
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].swap(res_bitsets[i]);
    }
  }

  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<lid_t> next_vids;
    std::vector<std::tuple<T...>> next_data_vec;
    size_t next_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[GeneralVertexSet] size: " << Size()
             << " Project self, next size: " << next_size;

    next_vids.reserve(next_size);
    next_data_vec.reserve(next_size);
    std::vector<grape::Bitset> next_sets(bitsets_.size());
    for (auto& i : next_sets) {
      i.init(next_size);
    }
    VLOG(10) << "after init";
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      size_t ind = 0;
      while (ind < bitsets_.size()) {
        if (bitsets_[ind].get_bit(i)) {
          break;
        }
        ind += 1;
      }
      CHECK(ind < bitsets_.size());
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        // VLOG(10) << "Project: " << vids_[i];
        next_sets[ind].set_bit(next_vids.size());
        next_vids.push_back(vec_[i]);
        next_data_vec.push_back(data_vec_[i]);
      }
    }

    auto copied_label_names(label_names_);
    auto copied_prop_names(prop_names_);
    return self_type_t(std::move(next_vids), std::move(next_data_vec),
                       std::move(copied_prop_names),
                       std::move(copied_label_names), std::move(next_sets));
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    CHECK(cur_offset.back() == vec_.back())
        << "neq : " << cur_offset.back() << ", " << vec_.back();
    std::vector<lid_t> res_vec;
    std::vector<std::tuple<T...>> res_data_vec;
    std::vector<grape::Bitset> res_bitsets(bitsets_.size());
    size_t total_cnt = repeat_vec.back();
    VLOG(10) << "Repeat current vertices num: " << vec_.size() << ", to "
             << total_cnt;
    for (size_t i = 0; i < res_bitsets.size(); ++i) {
      res_bitsets[i].init(total_cnt);
    }
    {
      auto label_indices = GenerateLabelIndices();
      size_t cur_ind = 0;
      res_vec.reserve(repeat_vec.back());
      res_data_vec.reserve(repeat_vec.back());
      for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
        auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
        for (size_t j = 0; j < times_to_repeat; ++j) {
          for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
            res_vec.emplace_back(vec_[k]);
            res_data_vec.emplace_back(data_vec_[k]);
            CHECK(label_indices[k] < res_bitsets.size());
            res_bitsets[label_indices[k]].set_bit(cur_ind++);
          }
        }
      }
      CHECK(cur_ind == repeat_vec.back());
    }
    vec_.swap(res_vec);
    data_vec_.swap(res_data_vec);
    bitsets_.swap(res_bitsets);
    VLOG(10) << "Finish Repeat general vertex";
  }

  // Usually after sort.
  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t_...>>);
    auto res_vids_and_data_tuples =
        generalSetFlatImpl<col_ind>(index_ele_tuple, vec_, data_vec_, bitsets_);
    auto labels_copied(label_names_);
    auto prop_names_copied(prop_names_);
    return self_type_t(std::move(std::get<0>(res_vids_and_data_tuples)),
                       std::move(std::get<1>(res_vids_and_data_tuples)),
                       std::move(prop_names_copied), std::move(labels_copied),
                       std::move(std::get<2>(res_vids_and_data_tuples)));
  }

  template <size_t Is, size_t MyIs, typename... PropT>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            const std::string& prop_name,
                            std::vector<offset_t>& repeat_array) const {
    using cur_prop = std::tuple_element_t<Is, std::tuple<PropT...>>;
    using my_prop = std::tuple_element_t<MyIs, std::tuple<T...>>;
    if constexpr (std::is_same_v<cur_prop, my_prop>) {
      CHECK(MyIs < prop_names_.size());
      if (prop_name == prop_names_[MyIs]) {
        VLOG(10) << "Found builtin property " << prop_name;
        CHECK(repeat_array.size() == data_vec_.size());
        size_t ind = 0;
        for (size_t i = 0; i < repeat_array.size(); ++i) {
          for (size_t j = 0; j < repeat_array[i]; ++j) {
            std::get<Is>(tuples[ind]) = std::get<0>(data_vec_[i]);
            ind += 1;
          }
        }
      }
    }
  }

  template <size_t Is, typename... PropT, size_t... MyIs>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            std::string& prop_name,
                            std::vector<offset_t>& repeat_array,
                            std::index_sequence<MyIs...>) const {
    (fillBuiltinPropsImpl<Is, MyIs>(tuples, prop_name, repeat_array), ...);
  }

  template <typename... PropT, size_t... Is>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            PropNameArray<PropT...>& prop_names,
                            std::vector<offset_t>& repeat_array,
                            std::index_sequence<Is...>) const {
    (fillBuiltinPropsImpl<Is, PropT...>(tuples, std::get<Is>(prop_names),
                                        repeat_array,
                                        std::index_sequence<sizeof...(T)>()),
     ...);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) const {
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  // No repeat array is not provided
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    // TO be implemented.
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::vector<std::tuple<T...>> data_vec_;
  std::vector<LabelT> label_names_;
  std::vector<std::string> prop_names_;
  std::vector<grape::Bitset> bitsets_;
};

/// @brief GeneralVertexSet are designed for the case we need to store multiple
/// label vertex in a mixed manner
/// @tparam VID_T
/// @tparam LabelT
template <typename VID_T, typename LabelT>
class GeneralVertexSet<VID_T, LabelT, grape::EmptyType> {
 public:
  using lid_t = VID_T;
  using self_type_t = GeneralVertexSet<VID_T, LabelT, grape::EmptyType>;
  using iterator = GeneralVertexSetIter<VID_T, LabelT, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, size_t, VID_T>;
  using ele_tuple_t = GlobalId;
  using data_tuple_t = std::tuple<VID_T>;
  using flat_t = self_type_t;
  using EntityValueType = VID_T;
  using builder_t = GeneralVertexSetBuilder<VID_T, LabelT, grape::EmptyType>;

  static constexpr bool is_vertex_set = true;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_general_set = true;
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_row_vertex_set = false;
  GeneralVertexSet(std::vector<VID_T>&& vec, std::vector<LabelT>&& label_names,
                   std::vector<grape::Bitset>&& bitsets)
      : vec_(std::move(vec)), label_names_(std::move(label_names)) {
    CHECK(label_names_.size() == bitsets.size());
    bitsets_.resize(bitsets.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets_[i].swap(bitsets[i]);
    }
    if (bitsets_.size() > 0) {
      VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
               << ", bitset size: " << bitsets_[0].cardinality()
               << ", ind_ele_t: " << demangle(index_ele_tuple_t())
               << ", label names: " << gs::to_string(label_names_);
    }
  }

  GeneralVertexSet(GeneralVertexSet&& other)
      : vec_(std::move(other.vec_)),
        label_names_(std::move(other.label_names_)) {
    bitsets_.resize(other.bitsets_.size());
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].swap(other.bitsets_[i]);
    }
    if (bitsets_.size() > 0) {
      VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
               << ", bitset size: " << bitsets_[0].cardinality()
               << ", labels: " << gs::to_string(label_names_);
    }
  }

  GeneralVertexSet(const GeneralVertexSet& other)
      : vec_(other.vec_), label_names_(other.label_names_) {
    bitsets_.resize(other.bitsets_.size());
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].copy(other.bitsets_[i]);
    }
    if (bitsets_.size() > 0) {
      VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
               << ", bitset size: " << bitsets_[0].cardinality();
    }
  }

  iterator begin() const { return iterator(vec_, bitsets_, label_names_, 0); }

  iterator end() const {
    return iterator(vec_, bitsets_, label_names_, vec_.size());
  }

  builder_t CreateBuilder() const {
    return builder_t(vec_, label_names_, bitsets_);
  }

  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels,
                         EXPRESSION& exprs,
                         std::vector<PROP_GETTER>& prop_getter) const {
    // TODO: vector-based cols should be able to be selected with
    // certain rows.

    auto tuple = general_project_vertices_impl(
        vec_, bitsets_, label_names_, filter_labels, exprs, prop_getter);
    auto copied_label_names(label_names_);
    auto set = self_type_t(std::move(std::get<0>(tuple)),
                           std::move(copied_label_names),
                           std::move(std::get<1>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<2>(tuple)));
  }

  // project without expression.
  template <size_t num_labels, typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels) const {
    // TODO: vector-based cols should be able to be selected with
    // certain rows.

    auto tuple = general_project_vertices_no_expr_impl(
        vec_, bitsets_, label_names_, filter_labels);
    auto copied_label_names(label_names_);
    auto set = self_type_t(std::move(std::get<0>(tuple)),
                           std::move(copied_label_names),
                           std::move(std::get<1>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<2>(tuple)));
  }

  const std::vector<LabelT>& GetLabels() const { return label_names_; }

  LabelT GetLabel(size_t i) const { return label_names_[i]; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill res with vertex labels
    for (size_t i = 0; i < vec_.size(); ++i) {
      for (size_t j = 0; j < bitsets_.size(); ++j) {
        if (bitsets_[j].get_bit(i)) {
          res.emplace_back(label_names_[j]);
          break;
        }
      }
    }
    return res;
  }

  // generate label indices.
  std::vector<uint8_t> GenerateLabelIndices() const {
    std::vector<uint8_t> label_indices;
    label_indices.resize(vec_.size(), 255);
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      for (size_t j = 0; j < bitsets_[i].cardinality(); ++j) {
        if (bitsets_[i].get_bit(j)) {
          CHECK(label_indices[j] == 255);
          label_indices[j] = i;
        }
      }
    }
    return label_indices;
  }

  const std::vector<grape::Bitset>& GetBitsets() const { return bitsets_; }

  const std::vector<VID_T>& GetVertices() const { return vec_; }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVerticesWithLabel(
      label_t label_id) const {
    // find label_id in label_names_
    auto it = std::find(label_names_.begin(), label_names_.end(), label_id);
    if (it == label_names_.end()) {
      return std::make_pair(std::vector<VID_T>(), std::vector<int32_t>());
    } else {
      auto ind = std::distance(label_names_.begin(), it);
      return GetVertices(ind);
    }
  }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVertices(
      size_t ind) const {
    CHECK(ind < bitsets_.size());
    std::vector<VID_T> res;
    std::vector<int32_t> active_ind;
    size_t cnt = bitsets_[ind].count();
    res.reserve(cnt);
    active_ind.reserve(cnt);
    for (size_t i = 0; i < bitsets_[ind].cardinality(); ++i) {
      if (bitsets_[ind].get_bit(i)) {
        res.push_back(vec_[i]);
        active_ind.push_back(i);
      }
    }
    VLOG(10) << "Got vertices of tag: " << ind
             << ", res vertices: " << res.size()
             << ", active_ind: " << active_ind.size();
    return std::make_pair(std::move(res), std::move(active_ind));
  }

  // subset with indices.
  void SubSetWithIndices(std::vector<size_t>& indices) {
    std::vector<VID_T> res_vec;
    std::vector<grape::Bitset> res_bitsets(bitsets_.size());
    for (auto& i : res_bitsets) {
      i.init(indices.size());
    }
    res_vec.reserve(indices.size());
    for (auto i : indices) {
      size_t label_ind = 0;
      for (; label_ind < bitsets_.size(); ++label_ind) {
        if (bitsets_[label_ind].get_bit(i)) {
          break;
        }
      }
      CHECK(label_ind < bitsets_.size());
      res_bitsets[label_ind].set_bit(res_vec.size());
      res_vec.emplace_back(vec_[i]);
    }
    vec_.swap(res_vec);
    for (size_t i = 0; i < bitsets_.size(); ++i) {
      bitsets_[i].swap(res_bitsets[i]);
    }
  }

  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<lid_t> next_vids;
    size_t next_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[GeneralVertexSet] size: " << Size()
             << " Project self, next size: " << next_size
             << "repeat array size: " << repeat_array.size();

    next_vids.reserve(next_size);
    std::vector<grape::Bitset> next_sets(bitsets_.size());
    for (auto& i : next_sets) {
      i.init(next_size);
    }
    VLOG(10) << "after init";
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      size_t label_ind = 0;
      while (label_ind < bitsets_.size()) {
        if (bitsets_[label_ind].get_bit(i)) {
          break;
        }
        label_ind += 1;
      }
      CHECK(label_ind < bitsets_.size());
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        next_sets[label_ind].set_bit(next_vids.size());
        next_vids.push_back(vec_[i]);
      }
    }

    auto copied_label_names(label_names_);
    return self_type_t(std::move(next_vids), std::move(copied_label_names),
                       std::move(next_sets));
  }

  // std::vector<offset_t> dedup_on_internal_id() {
  //   std::unordered_set<uint64_t> appeared_vids;
  //   std::vector<offset_t> offsets;
  //   std::vector<VID_T> res_vec;
  //   std::vector<grape::Bitset> res_bitsets(bitsets_.size());
  //   for (auto& i : res_bitsets) {
  //     i.init(vec_.size());
  //   }
  //   offsets.push_back(0);
  //   auto label_indices = GenerateLabelIndices();
  //   CHECK(label_indices.size() == vec_.size());
  //   for (size_t i = 0; i < vec_.size(); ++i) {
  //     uint64_t vid = vec_[i];
  //     vid = vid | (static_cast<uint64_t>(label_indices[i]) << 56);
  //     if (appeared_vids.find(vid) == appeared_vids.end()) {
  //       appeared_vids.insert(vid);
  //       res_vec.push_back(vec_[i]);
  //       CHECK(label_indices[i] < res_bitsets.size());
  //       res_bitsets[label_indices[i]].set_bit(res_vec.size() - 1);
  //     }
  //     offsets.push_back(res_vec.size());
  //   }
  //   vec_.swap(res_vec);
  //   bitsets_.swap(res_bitsets);
  //   for (auto i = 0; i < bitsets_.size(); ++i) {
  //     bitsets_[i].resize(res_vec.size());
  //   }
  //   return offsets;
  // }

  // template <typename PropT>
  // std::vector<offset_t> Dedup(const PropertySelector<PropT>& prop_selector) {
  //   if constexpr (std::is_same_v<PropT, grape::EmptyType>) {
  //     return dedup_on_internal_id();
  //   } else if constexpr (std::is_same_v<PropT, LabelKey>) {
  //     return dedup_on_label();
  //   } else {
  //     LOG(FATAL) << "Not supported: " << demangle(typeid(PropT).name());
  //   }
  // }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    CHECK(cur_offset.back() == vec_.back())
        << "neq : " << cur_offset.back() << ", " << vec_.back();
    std::vector<lid_t> res_vec;
    std::vector<grape::Bitset> res_bitsets(bitsets_.size());
    size_t total_cnt = repeat_vec.back();
    VLOG(10) << "Repeat current vertices num: " << vec_.size() << ", to "
             << total_cnt;
    for (size_t i = 0; i < res_bitsets.size(); ++i) {
      res_bitsets[i].init(total_cnt);
    }
    {
      auto label_indices = GenerateLabelIndices();
      size_t cur_ind = 0;
      res_vec.reserve(repeat_vec.back());
      for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
        auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
        for (size_t j = 0; j < times_to_repeat; ++j) {
          for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
            res_vec.emplace_back(vec_[k]);
            CHECK(label_indices[k] < res_bitsets.size());
            res_bitsets[label_indices[k]].set_bit(cur_ind++);
          }
        }
      }
      CHECK(cur_ind == repeat_vec.back());
    }
    vec_.swap(res_vec);
    bitsets_.swap(res_bitsets);
    VLOG(10) << "Finish Repeat general vertex";
  }

  // Usually after sort.
  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t_...>>);
    auto res_vids_and_data_tuples =
        generalSetFlatImpl<col_ind>(index_ele_tuple, vec_, bitsets_);
    auto labels_copied(label_names_);
    return self_type_t(std::move(res_vids_and_data_tuples.first),
                       std::move(labels_copied),
                       std::move(res_vids_and_data_tuples.second));
  }

  template <size_t Is, typename... PropT>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            std::string& prop_name,
                            std::vector<offset_t>& repeat_array) const {
    if constexpr (std::is_same_v<std::tuple_element_t<Is, std::tuple<PropT...>>,
                                 Dist>) {
      if (prop_name == "dist") {
        LOG(FATAL) << "Not supported";
      }
    }
  }

  template <typename... PropT, size_t... Is>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            PropNameArray<PropT...>& prop_names,
                            std::vector<offset_t>& repeat_array,
                            std::index_sequence<Is...>) const {
    (fillBuiltinPropsImpl<Is, PropT...>(tuples, std::get<Is>(prop_names),
                                        repeat_array),
     ...);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) const {
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  // No repeat array is not provided
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    LOG(WARNING) << "not supported";
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::vector<LabelT> label_names_;
  std::vector<grape::Bitset> bitsets_;
};

/// keyed general vertex set

template <typename VID_T, typename LabelT>
auto make_general_set(std::vector<VID_T>&& vec,
                      std::vector<LabelT>&& label_names,
                      std::vector<grape::Bitset>&& bitsets) {
  return GeneralVertexSet<VID_T, LabelT, grape::EmptyType>(
      std::move(vec), std::move(label_names), std::move(bitsets));
}

template <typename VID_T, typename LabelT>
auto make_general_set(std::vector<VID_T>&& vec,
                      const std::vector<LabelT>& label_names,
                      std::vector<grape::Bitset>&& bitsets) {
  auto copied_label_names(label_names);
  return GeneralVertexSet<VID_T, LabelT, grape::EmptyType>(
      std::move(vec), std::move(copied_label_names), std::move(bitsets));
}

template <typename VID_T, typename LabelT, typename... T>
auto make_general_set(std::vector<VID_T>&& vec,
                      std::vector<std::tuple<T...>>&& data_vec,
                      std::vector<std::string>&& prop_names,
                      std::vector<LabelT>&& label_names,
                      std::vector<grape::Bitset>&& bitsets) {
  return GeneralVertexSet<VID_T, LabelT, T...>(
      std::move(vec), std::move(data_vec), std::move(prop_names),
      std::move(label_names), std::move(bitsets));
}

std::vector<std::vector<int32_t>> bitsets_to_vids_inds(
    const std::vector<grape::Bitset>& bitset) {
  auto num_labels = bitset.size();
  std::vector<std::vector<int32_t>> res(num_labels);
  if (num_labels == 0) {
    return res;
  }
  auto limit_size = bitset[0].cardinality();
  VLOG(10) << "old bitset limit size: " << limit_size;
  for (size_t i = 0; i < num_labels; ++i) {
    auto count = bitset[i].count();
    res[i].reserve(count);
    for (size_t j = 0; j < limit_size; ++j) {
      if (bitset[i].get_bit(j)) {
        res[i].emplace_back(j);
      }
    }
  }
  {
    size_t cnt = 0;
    for (auto& a : res) {
      cnt += a.size();
    }
    CHECK(cnt == limit_size) << " check failed: " << cnt << ", " << limit_size;
  }
  return res;
}

template <typename... T, typename GRAPH_INTERFACE, typename LabelT,
          typename... SET_T, size_t... Is>
static auto get_property_tuple_general_impl(
    const GRAPH_INTERFACE& graph,
    const GeneralVertexSet<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                           SET_T...>& general_set,
    const std::tuple<PropertySelector<T>...>& selectors,
    std::index_sequence<Is...>) {
  auto label_vec = general_set.GetLabels();
  const auto& vids = general_set.GetVertices();
  const auto& bitset = general_set.GetBitsets();
  using prop_getter_tuple =
      std::tuple<typename GRAPH_INTERFACE::prop_getter_t<T>...>;
  std::vector<prop_getter_tuple> prop_getters;
  for (size_t i = 0; i < label_vec.size(); ++i) {
    prop_getters.emplace_back(get_prop_getters_from_selectors_single_label(
        graph, label_vec[i], selectors));
  }
  std::vector<std::tuple<T...>> data_tuples;
  data_tuples.reserve(vids.size());
  for (size_t i = 0; i < vids.size(); ++i) {
    for (size_t j = 0; j < label_vec.size(); ++j) {
      if (bitset[j].get_bit(i)) {
        data_tuples.emplace_back(
            get_view_from_prop_getters(prop_getters[j], vids[i]));
        break;
      }
    }
  }
  return data_tuples;
}

template <typename... T, typename GRAPH_INTERFACE, typename LabelT,
          typename... SET_T>
static auto get_property_tuple_general(
    const GRAPH_INTERFACE& graph,
    const GeneralVertexSet<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                           SET_T...>& general_set,
    const std::tuple<PropertySelector<T>...>& selectors) {
  return get_property_tuple_general_impl(
      graph, general_set, selectors, std::make_index_sequence<sizeof...(T)>());
}

}  // namespace gs

#endif  // ENGINES_HQPS_DS_MULTI_VERTEX_SET_GENERAL_VERTEX_SET_H_
