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
#ifndef ENGINES_HQPS_DS_MULTI_VERTEX_SET_TWO_LABEL_VERTEX_SET_H_
#define ENGINES_HQPS_DS_MULTI_VERTEX_SET_TWO_LABEL_VERTEX_SET_H_

#include <array>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "grape/util.h"
#include "grape/utils/bitset.h"

namespace gs {

template <typename VID_T, typename LabelT, typename... T>
class TwoLabelVertexSetImpl;

template <typename VID_T, typename LabelT, typename... T>
class TwoLabelVertexSetImplBuilder {
 public:
  using res_t = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;
  using ele_tuple_t = std::tuple<int32_t, VID_T>;
  using data_tuple_t = std::tuple<T...>;
  using index_ele_tuple_t = std::tuple<size_t, int32_t, VID_T>;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = true;
  static constexpr bool is_collection_builder = false;

  TwoLabelVertexSetImplBuilder(
      size_t size, const std::array<LabelT, 2>& labels,
      const std::array<std::string, sizeof...(T)>& props)
      : labels_(labels), props_(props) {
    vec_.reserve(size);
    data_.reserve(size);
    bitset_.init(size);
  }

  TwoLabelVertexSetImplBuilder(
      const TwoLabelVertexSetImplBuilder<VID_T, LabelT, T...>& other) {
    vec_.reserve(other.vec_.capacity());
    data_.reserve(other.data_.capacity());
    bitset_.copy(other.bitset_);
    labels_ = other.labels_;
  }

  void Insert(const index_ele_tuple_t& tuple, const data_tuple_t& data) {
    vec_.emplace_back(std::get<2>(tuple));
    data_.emplace_back(data);
    if (std::get<1>(tuple) == 0) {
      auto new_size = vec_.size();
      while (new_size > bitset_.cardinality()) {
        bitset_.resize(bitset_.cardinality() * 2);
      }
      bitset_.set_bit(vec_.size() - 1);
    }
  }

  res_t Build() {
    VLOG(10) << "Try to resize from " << bitset_.cardinality() << ", to "
             << vec_.size();
    bitset_.resize(vec_.size());
    return res_t(std::move(vec_), std::move(data_), std::move(labels_),
                 std::move(props_), std::move(bitset_));
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::vector<data_tuple_t> data_;
  std::array<LabelT, 2> labels_;
  std::array<std::string, sizeof...(T)> props_;
  grape::Bitset bitset_;
};

template <typename VID_T, typename LabelT>
class TwoLabelVertexSetImplBuilder<VID_T, LabelT, grape::EmptyType> {
 public:
  using res_t = TwoLabelVertexSetImpl<VID_T, LabelT, grape::EmptyType>;
  using ele_tuple_t = std::tuple<int32_t, VID_T>;
  using index_ele_tuple_t = std::tuple<size_t, int32_t, VID_T>;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = true;
  static constexpr bool is_collection_builder = false;

  TwoLabelVertexSetImplBuilder(size_t size, const std::array<LabelT, 2>& labels)
      : labels_(labels) {
    VLOG(10) << "two label set:" << std::to_string(labels[0]) << " "
             << std::to_string(labels[1]);
    vec_.reserve(size);
    bitset_.init(size);
  }

  TwoLabelVertexSetImplBuilder(
      const TwoLabelVertexSetImplBuilder<VID_T, LabelT, grape::EmptyType>&
          other) {
    vec_ = other.vec_;
    bitset_.copy(other.bitset_);
    labels_ = other.labels_;
  }

  void Insert(const index_ele_tuple_t& tuple) {
    //(ind, label_ind, vid)
    vec_.emplace_back(std::get<2>(tuple));
    if (std::get<1>(tuple) == 0) {
      auto new_size = vec_.size();
      while (new_size > bitset_.cardinality()) {
        bitset_.resize(bitset_.cardinality() * 2);
      }
      bitset_.set_bit(vec_.size() - 1);
    }
  }

  res_t Build() {
    VLOG(10) << "Try to resize from " << bitset_.cardinality() << ", to "
             << vec_.size();
    bitset_.resize(vec_.size());
    return res_t(std::move(vec_), std::move(labels_), std::move(bitset_));
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::array<LabelT, 2> labels_;
  grape::Bitset bitset_;
};
template <typename LabelT, size_t filter_label_num>
std::vector<bool> filter_labels(
    const std::array<LabelT, filter_label_num>& filter_labels,
    const std::array<LabelT, 2>& old_labels) {
  std::vector<bool> label_flag(2, false);
  if constexpr (filter_label_num == 0) {
    // set label_flat to all true
    label_flag[0] = true;
    label_flag[1] = true;
  } else {
    std::unordered_set<LabelT> set;
    for (auto l : filter_labels) {
      set.insert(l);
    }
    for (size_t i = 0; i < old_labels.size(); ++i) {
      if (set.find(old_labels[i]) != set.end()) {
        label_flag[i] = true;
      }
    }
  }
  return label_flag;
}

// return the old labels, that are active in filter.
template <typename VID_T, typename LabelT, typename EXPR, typename PROP_GETTER,
          size_t filter_num_labels>
auto two_label_project_vertices_impl(
    const std::vector<VID_T>& old_vec, const grape::Bitset& old_bit_set,
    const std::array<LabelT, 2>& old_labels,
    const std::array<LabelT, filter_num_labels>& filtering_labels,
    const EXPR& expr, const std::array<PROP_GETTER, 2>& prop_getters) {
  std::vector<VID_T> res_vec;
  grape::Bitset res_bitset;
  // reserve enough size for bitset.
  res_bitset.init(old_vec.size());

  std::vector<bool> label_flag = filter_labels(filtering_labels, old_labels);
  std::vector<offset_t> offset;
  res_vec.reserve(old_vec.size());
  offset.reserve(old_vec.size() + 1);

  offset.emplace_back(0);
  double t0 = -grape::GetCurrentTime();
  for (size_t i = 0; i < old_vec.size(); ++i) {
    if (old_bit_set.get_bit(i) && label_flag[0]) {
      auto vid = old_vec[i];
      if (std::apply(expr, prop_getters[0].get_view(vid))) {
        res_bitset.set_bit(res_vec.size());
        res_vec.emplace_back(old_vec[i]);
      }
    } else if (label_flag[1] &&
               std::apply(expr, prop_getters[1].get_view(old_vec[i]))) {
      res_vec.emplace_back(old_vec[i]);
    }

    offset.emplace_back(res_vec.size());
  }
  t0 += grape::GetCurrentTime();
  VLOG(10) << "expr + copy cost: " << t0;

  res_bitset.resize(res_vec.size());

  return std::make_tuple(std::move(res_vec), std::move(res_bitset),
                         std::move(offset));
}

// filter with labels.
template <typename VID_T, typename LabelT, size_t filter_num_labels>
auto two_label_project_vertices_impl(
    const std::vector<VID_T>& old_vec, const grape::Bitset& old_bit_set,
    const std::array<LabelT, 2>& old_labels,
    const std::array<LabelT, filter_num_labels>& filtering_labels) {
  std::vector<VID_T> res_vec;
  grape::Bitset res_bitset;
  // reserve enough size for bitset.
  res_bitset.init(old_vec.size());

  auto label_flag = filter_labels(filtering_labels, old_labels);

  std::vector<offset_t> offset;
  res_vec.reserve(old_vec.size());
  offset.reserve(old_vec.size() + 1);

  offset.emplace_back(0);
  double t0 = -grape::GetCurrentTime();
  for (size_t i = 0; i < old_vec.size(); ++i) {
    if (old_bit_set.get_bit(i) && label_flag[0]) {
      res_bitset.set_bit(res_vec.size());
      res_vec.emplace_back(old_vec[i]);
    } else if (label_flag[1]) {
      res_vec.emplace_back(old_vec[i]);
    }
    offset.emplace_back(res_vec.size());
  }
  t0 += grape::GetCurrentTime();
  VLOG(10) << "expr + copy cost: " << t0;

  res_bitset.resize(res_vec.size());

  return std::make_tuple(std::move(res_vec), std::move(res_bitset),
                         std::move(offset));
}

template <typename VID_T, typename data_tuple_t, typename LabelT, typename EXPR,
          typename PRO_GETTER_T, size_t filter_num_labels>
auto two_label_project_vertices_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<data_tuple_t>& old_data, const grape::Bitset& old_bit_set,
    const std::array<LabelT, 2>& old_labels,
    const std::array<LabelT, filter_num_labels>& filtering_labels,
    const EXPR& expr, const std::array<PRO_GETTER_T, 2>& prop_getters) {
  std::vector<VID_T> res_vec;
  std::vector<data_tuple_t> res_data;
  grape::Bitset res_bitset;
  // reserve enough size for bitset.
  res_bitset.init(old_vec.size());

  std::vector<bool> label_flag = filter_labels(filtering_labels, old_labels);
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (size_t i = 0; i < old_vec.size(); ++i) {
    if (old_bit_set.get_bit(i) && label_flag[0]) {
      auto vid = old_vec[i];
      if (std::apply(expr, prop_getters[0](vid))) {
        res_bitset.set_bit(res_vec.size());
        res_vec.emplace_back(old_vec[i]);
        res_data.emplace_back(old_data[i]);
      }
    } else if (label_flag[1] && std::apply(expr, prop_getters[1](old_vec[i]))) {
      res_vec.emplace_back(old_vec[i]);
      res_data.emplace_back(old_data[i]);
    }

    offset.emplace_back(res_vec.size());
  }

  res_bitset.resize(res_vec.size());

  return std::make_tuple(std::move(res_vec), std::move(res_data),
                         std::move(res_bitset), std::move(offset));
}

// filter with labels.
template <typename VID_T, typename data_tuple_t, typename LabelT,
          size_t filter_num_labels>
auto two_label_project_vertices_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<data_tuple_t>& old_data, const grape::Bitset& old_bit_set,
    const std::array<LabelT, 2>& old_labels,
    const std::array<LabelT, filter_num_labels>& filtering_labels) {
  std::vector<VID_T> res_vec;
  std::vector<data_tuple_t> res_data;
  grape::Bitset res_bitset;
  // reserve enough size for bitset.
  res_bitset.init(old_vec.size());

  std::vector<bool> label_flag = filter_labels(filtering_labels, old_labels);
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (size_t i = 0; i < old_vec.size(); ++i) {
    if (old_bit_set.get_bit(i) && label_flag[0]) {
      auto vid = old_vec[i];
      res_bitset.set_bit(res_vec.size());
      res_vec.emplace_back(old_vec[i]);
      res_data.emplace_back(old_data[i]);
    } else if (label_flag[1]) {
      res_vec.emplace_back(old_vec[i]);
      res_data.emplace_back(old_data[i]);
    }

    offset.emplace_back(res_vec.size());
  }

  res_bitset.resize(res_vec.size());

  return std::make_tuple(std::move(res_vec), std::move(res_data),
                         std::move(res_bitset), std::move(offset));
}

template <size_t Is, typename VID_T, typename QueryT, typename... T,
          typename EXPR,
          typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
void filter_with_select_prop(
    const std::vector<bool>& label_flag, const std::vector<VID_T>& old_vec,
    const std::vector<std::tuple<T...>>& old_data,
    const grape::Bitset& old_bitset, std::vector<VID_T>& res_vec,
    std::vector<std::tuple<T...>>& res_data, grape::Bitset& res_bitset,
    std::vector<size_t>& offset, const NamedProperty<QueryT>& query_prop,
    const std::array<std::string, sizeof...(T)>& my_prop, EXPR& expr) {
  using indexed_prop_t = std::tuple_element_t<Is, std::tuple<T...>>;
  if constexpr (std::is_same_v<indexed_prop_t, QueryT>) {
    if (query_prop.names[0] == my_prop.names[Is]) {
      VLOG(10) << "Found satisfied prop: " << query_prop.names[0]
               << " at index: " << Is;
      offset.emplace_back(0);
      res_vec.reserve(old_vec.size() / 2);
      res_data.reserve(old_vec.size() / 2);

      for (size_t i = 0; i < old_vec.size(); ++i) {
        auto& data = old_data[i];
        if (expr(std::get<Is>(data))) {
          if (old_bitset.get_bit(i) && label_flag[0]) {
            res_bitset.set_bit(res_vec.size());
            res_vec.emplace_back(old_vec[i]);
            res_data.emplace_back(old_data[i]);
          } else if (label_flag[1]) {
            res_vec.emplace_back(old_vec[i]);
            res_data.emplace_back(old_data[i]);
          }
        }
        offset.emplace_back(res_vec.size());
      }
      return;
    }
  } else {
    filter_with_select_prop<Is + 1>(label_flag, old_vec, old_data, old_bitset,
                                    res_vec, res_data, res_bitset, offset,
                                    query_prop, my_prop, expr);
  }
}

template <size_t Is, typename VID_T, typename QueryT, typename... T,
          typename EXPR,
          typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
void filter_with_select_prop(
    const std::vector<bool>& label_flag, const std::vector<VID_T>& old_vec,
    const std::vector<std::tuple<T...>>& old_data,
    const grape::Bitset& old_bitset, std::vector<VID_T>& res_vec,
    std::vector<std::tuple<T...>>& res_data, grape::Bitset& res_bitset,
    std::vector<size_t>& offset, const NamedProperty<QueryT>& query_prop,
    const std::array<std::string, sizeof...(T)>& my_prop, EXPR& expr) {
  LOG(FATAL) << "Query property: " << gs::to_string(query_prop.names)
             << "not found in :" << gs::to_string(my_prop.names);
}

// Implementation for the required data is already in data vector.
template <typename VID_T, typename... T, typename LabelT, typename EXPR,
          size_t filter_num_labels>
auto two_label_project_vertices_internal_impl(
    const std::vector<VID_T>& old_vec,
    const std::vector<std::tuple<T...>>& old_data,
    const grape::Bitset& old_bitset, const std::array<LabelT, 2>& old_labels,
    const std::array<LabelT, filter_num_labels>& filter_labels,
    const std::array<std::string, sizeof...(T)>& old_prop_names,
    const EXPR& expr) {
  std::vector<VID_T> res_vec;
  std::vector<std::tuple<T...>> res_data;
  grape::Bitset res_bitset;
  // reserve enough size for bitset.
  res_bitset.init(old_vec.size());

  std::vector<bool> label_flag(2, false);
  if constexpr (filter_num_labels == 0) {
    // set label_flat to all true
    label_flag[0] = true;
    label_flag[1] = true;
  } else {
    std::unordered_set<LabelT> set;
    for (auto l : filter_labels) {
      set.insert(l);
    }
    for (size_t i = 0; i < old_labels.size(); ++i) {
      if (set.find(old_labels[i]) != set.end()) {
        label_flag[i] = true;
      }
    }
  }
  VLOG(10) << "selected label ids: "
           << ", out of size: " << old_labels.size();

  // check the required property.
  std::vector<offset_t> offset;
  auto expr_prop = expr.Properties();
  filter_with_select_prop<0>(label_flag, old_vec, old_data, old_bitset, res_vec,
                             res_data, res_bitset, offset, expr_prop,
                             old_prop_names, expr);

  res_bitset.resize(res_vec.size());
  VLOG(10) << "filter " << res_vec.size()
           << " from old set of size: " << old_vec.size()
           << ", label0 cnt: " << res_bitset.count() << "/"
           << res_bitset.cardinality();

  return std::make_tuple(std::move(res_vec), std::move(res_data),
                         std::move(res_bitset), std::move(offset));
}

template <size_t col_ind, typename... index_ele_tuple_t, typename lid_t>
auto twoLabelSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    const std::vector<lid_t>& origin_vids, const grape::Bitset& origin_bitset) {
  size_t dst_size = index_ele_tuples.size();
  std::vector<lid_t> res_vids;
  grape::Bitset res_bitset;
  res_vids.reserve(dst_size);
  res_bitset.init(dst_size);

  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());

    if (origin_bitset.get_bit(ind)) {
      res_bitset.set_bit(res_vids.size());
    }

    res_vids.emplace_back(origin_vids[ind]);
  }
  return std::make_pair(std::move(res_vids), std::move(res_bitset));
}

template <size_t col_ind, typename data_tuple_t, typename... index_ele_tuple_t,
          typename lid_t>
auto twoLabelSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    const std::vector<lid_t>& origin_vids,
    const std::vector<data_tuple_t>& origin_data,
    const grape::Bitset& origin_bitset) {
  size_t dst_size = index_ele_tuples.size();
  std::vector<lid_t> res_vids;
  std::vector<data_tuple_t> res_data;
  grape::Bitset res_bitset;
  res_vids.reserve(dst_size);
  res_data.reserve(dst_size);
  res_bitset.init(dst_size);

  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());

    if (origin_bitset.get_bit(ind)) {
      res_bitset.set_bit(res_vids.size());
    }

    res_vids.emplace_back(origin_vids[ind]);
    res_data.emplace_back(origin_data[ind]);
  }
  return std::make_tuple(std::move(res_vids), std::move(res_data),
                         std::move(res_bitset));
}

template <typename VID_T, typename... T>
class TwoLabelVertexSetIter {
 public:
  using lid_t = VID_T;
  using self_type_t = TwoLabelVertexSetIter<VID_T, T...>;
  using ele_tuple_t = std::pair<int32_t, VID_T>;
  using index_ele_tuple_t = std::tuple<size_t, int32_t, VID_T>;

  using data_tuple_t = std::tuple<T...>;

  TwoLabelVertexSetIter(const std::vector<VID_T>& vec,
                        const std::vector<data_tuple_t>& data,
                        const grape::Bitset& bitset, size_t ind)
      : vec_(vec), data_(data), bitset_(bitset), ind_(ind) {}

  ele_tuple_t GetElement() const {
    if (bitset_.get_bit(ind_)) {
      return std::make_pair(0, vec_[ind_]);
    } else {
      return std::make_pair(1, vec_[ind_]);
    }
  }

  data_tuple_t GetData() const { return data_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    if (bitset_.get_bit(ind_)) {
      return std::make_tuple(ind_, 0, vec_[ind_]);
    } else {
      return std::make_tuple(ind_, 1, vec_[ind_]);
    }
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
  const std::vector<data_tuple_t>& data_;
  const grape::Bitset& bitset_;
  size_t ind_;
};

template <typename VID_T>
class TwoLabelVertexSetIter<VID_T, grape::EmptyType> {
 public:
  using lid_t = VID_T;
  using self_type_t = TwoLabelVertexSetIter<VID_T, grape::EmptyType>;
  using ele_tuple_t = std::pair<int32_t, lid_t>;
  using index_ele_tuple_t = std::tuple<size_t, int32_t, VID_T>;

  using data_tuple_t = std::tuple<grape::EmptyType>;

  TwoLabelVertexSetIter(const std::vector<VID_T>& vec,
                        const grape::Bitset& bitset, size_t ind)
      : vec_(vec), bitset_(bitset), ind_(ind) {}

  ele_tuple_t GetElement() const {
    if (bitset_.get_bit(ind_)) {
      return std::make_pair(0, vec_[ind_]);
    } else {
      return std::make_pair(1, vec_[ind_]);
    }
  }

  data_tuple_t GetData() const { return std::make_tuple(grape::EmptyType()); }

  index_ele_tuple_t GetIndexElement() const {
    if (bitset_.get_bit(ind_)) {
      return std::make_tuple(ind_, 0, vec_[ind_]);
    } else {
      return std::make_tuple(ind_, 1, vec_[ind_]);
    }
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
  const grape::Bitset& bitset_;
  size_t ind_;
};

/* General vertex set, can contain multiple label vertices.
 */
template <typename VID_T, typename LabelT, typename... T>
class TwoLabelVertexSetImpl {
 public:
  using lid_t = VID_T;
  using self_type_t = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;
  using iterator = TwoLabelVertexSetIter<VID_T, T...>;
  using index_ele_tuple_t = std::tuple<size_t, int32_t, VID_T>;
  using data_tuple_t = std::tuple<T...>;
  using flat_t = self_type_t;
  using EntityValueType = VID_T;
  using builder_t = TwoLabelVertexSetImplBuilder<VID_T, LabelT, T...>;

  static constexpr bool is_vertex_set = true;
  static constexpr bool is_general_set = false;
  static constexpr bool is_row_vertex_set = false;
  static constexpr bool is_two_label_set = true;
  static constexpr size_t num_labels = 2;
  static constexpr size_t num_props = sizeof...(T);
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_label = false;
  TwoLabelVertexSetImpl(std::vector<VID_T>&& vec,
                        std::vector<std::tuple<T...>>&& data_tuple,
                        std::array<LabelT, 2>&& label_names,
                        std::array<std::string, num_props>&& named_property,
                        grape::Bitset&& bitset)
      : vec_(std::move(vec)),
        data_tuple_(std::move(data_tuple)),
        label_names_(std::move(label_names)),
        named_property_(std::move(named_property)) {
    CHECK(vec.size() == data_tuple.size());
    bitset_.swap(bitset);
  }

  TwoLabelVertexSetImpl(TwoLabelVertexSetImpl&& other)
      : vec_(std::move(other.vec_)),
        data_tuple_(std::move(other.data_tuple_)),
        label_names_(std::move(other.label_names_)),
        named_property_(std::move(other.named_property_)) {
    bitset_.swap(other.bitset_);
  }

  TwoLabelVertexSetImpl(const TwoLabelVertexSetImpl& other)
      : vec_(other.vec_),
        data_tuple_(other.data_tuple_),
        label_names_(other.label_names_),
        named_property_(other.named_property_) {
    bitset_.copy(other.bitset_);
  }

  builder_t CreateBuilder() const {
    return builder_t(vec_.size(), label_names_, named_property_);
  }

  iterator begin() const { return iterator(vec_, data_tuple_, bitset_, 0); }

  iterator end() const {
    return iterator(vec_, data_tuple_, bitset_, vec_.size());
  }

  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels,
                         EXPRESSION& exprs,
                         std::array<PROP_GETTER, 2>& prop_getters) const {
    // TODO: vector-based cols should be able to be selected with
    // certain rows.

    auto tuple = two_label_project_vertices_impl(vec_, data_tuple_, bitset_,
                                                 label_names_, filter_labels,
                                                 exprs, prop_getters);
    auto copied_label_names(label_names_);
    auto copied_prop_names(named_property_);
    auto set = self_type_t(
        std::move(std::get<0>(tuple)), std::move(std::get<1>(tuple)),
        std::move(copied_label_names), std::move(copied_prop_names),
        std::move(std::get<2>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<3>(tuple)));
  }

  // project vertices with only labels filtering
  template <size_t num_labels, typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels) const {
    auto tuple = two_label_project_vertices_impl(vec_, data_tuple_, bitset_,
                                                 label_names_, filter_labels);
    auto copied_label_names(label_names_);
    auto copied_prop_names(named_property_);
    auto set = self_type_t(
        std::move(std::get<0>(tuple)), std::move(std::get<1>(tuple)),
        std::move(copied_label_names), std::move(copied_prop_names),
        std::move(std::get<2>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<3>(tuple)));
  }

  template <typename EXPRESSION, size_t num_labels,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices_internal(std::array<LabelT, num_labels>& filter_labels,
                                  EXPRESSION& exprs) const {
    // We assume expr only applies on one column
    auto tuple = two_label_project_vertices_internal_impl(
        vec_, data_tuple_, bitset_, label_names_, filter_labels,
        named_property_, exprs);
    auto copied_label_names(label_names_);
    auto copied_prop_names(named_property_);
    auto set = self_type_t(
        std::move(std::get<0>(tuple)), std::move(std::get<1>(tuple)),
        std::move(copied_label_names), std::move(copied_prop_names),
        std::move(std::get<2>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<3>(tuple)));
  }

  const std::array<LabelT, 2>& GetLabels() const { return label_names_; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill with each vertex's label
    for (size_t i = 0; i < vec_.size(); ++i) {
      if (bitset_.get_bit(i)) {
        res.emplace_back(label_names_[0]);
      } else {
        res.emplace_back(label_names_[1]);
      }
    }
    return res;
  }

  LabelT GetLabel(size_t i) const { return label_names_[i]; }

  const grape::Bitset& GetBitset() const { return bitset_; }

  grape::Bitset& GetMutableBitset() { return bitset_; }

  const std::vector<VID_T>& GetVertices() const { return vec_; }

  const std::vector<data_tuple_t>& GetDataVec() const { return data_tuple_; }

  const std::array<std::string, sizeof...(T)>& GetPropNames() const {
    return named_property_;
  }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVertices(
      size_t ind) const {
    CHECK(ind < 2);
    CHECK(bitset_.cardinality() == vec_.size());
    std::vector<VID_T> res;
    std::vector<int32_t> active_ind;
    // 0 denotes label0, 1 denotes label1.
    size_t cnt;
    if (ind == 0) {
      cnt = bitset_.count();
    } else {
      cnt = bitset_.cardinality() - bitset_.count();
    }
    res.reserve(cnt);
    active_ind.reserve(cnt);
    if (ind == 0) {
      for (size_t i = 0; i < bitset_.cardinality(); ++i) {
        if (bitset_.get_bit(i)) {
          res.emplace_back(vec_[i]);
          active_ind.emplace_back(i);
        }
      }
    } else {
      for (size_t i = 0; i < bitset_.cardinality(); ++i) {
        if (!bitset_.get_bit(i)) {
          res.emplace_back(vec_[i]);
          active_ind.emplace_back(i);
        }
      }
    }

    VLOG(10) << "Got vertices of tag: " << ind
             << ", res vertices: " << res.size()
             << ", active_ind size:  " << active_ind.size();
    return std::make_pair(std::move(res), std::move(active_ind));
  }

  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<lid_t> next_vids;
    std::vector<std::tuple<T...>> next_datas;
    size_t next_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[TwoLabelVertexSetImpl] size: " << Size()
             << " Project self, next size: " << next_size;

    next_vids.reserve(next_size);
    next_datas.reserve(next_size);
    grape::Bitset next_set;
    next_set.init(next_size);
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      if (bitset_.get_bit(i)) {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          // VLOG(10) << "Project: " << vids_[i];
          next_set.set_bit(next_vids.size());
          next_vids.push_back(vec_[i]);
          next_datas.push_back(data_tuple_[i]);
        }
      } else {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          // VLOG(10) << "Project: " << vids_[i];
          //   next_set.set_bit(next_vids.size());
          next_vids.push_back(vec_[i]);
          next_datas.push_back(data_tuple_[i]);
        }
      }
    }

    auto copied_label_names(label_names_);
    auto copied_named_prop(named_property_);
    return self_type_t(std::move(next_vids), std::move(next_datas),
                       std::move(copied_label_names),
                       std::move(copied_named_prop), std::move(next_set));
  }

  // Usually after sort.
  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t_...>>);
    auto res_vids_and_data_tuples = twoLabelSetFlatImpl<col_ind>(
        index_ele_tuple, vec_, data_tuple_, bitset_);
    auto labels_copied(label_names_);
    auto copied_named_prop(named_property_);
    return self_type_t(std::move(std::get<0>(res_vids_and_data_tuples)),
                       std::move(std::get<1>(res_vids_and_data_tuples)),
                       std::move(labels_copied), std::move(copied_named_prop),
                       std::move(std::get<2>(res_vids_and_data_tuples)));
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
                        PropNameArray<PropT...>& prop_names) const {
    LOG(WARNING) << "not supported";
  }

  template <typename PropT>
  void fillBuiltinProps(std::vector<PropT>& tuples,
                        PropNameArray<PropT>& prop_names) const {
    LOG(WARNING) << "not supported";
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "Not implemented";
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::vector<std::tuple<T...>> data_tuple_;
  std::array<LabelT, 2> label_names_;
  std::array<std::string, sizeof...(T)> named_property_;
  grape::Bitset bitset_;
};

/// @brief //////////////////////Specialization for empty data type.
/// @tparam VID_T
/// @tparam LabelT
/// @tparam ...T
template <typename VID_T, typename LabelT>
class TwoLabelVertexSetImpl<VID_T, LabelT, grape::EmptyType> {
 public:
  using lid_t = VID_T;
  using self_type_t = TwoLabelVertexSetImpl<VID_T, LabelT, grape::EmptyType>;
  using iterator = TwoLabelVertexSetIter<VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, int32_t, VID_T>;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  using flat_t = self_type_t;
  using EntityValueType = VID_T;
  using builder_t =
      TwoLabelVertexSetImplBuilder<VID_T, LabelT, grape::EmptyType>;

  static constexpr bool is_vertex_set = true;
  static constexpr bool is_general_set = false;
  static constexpr bool is_two_label_set = true;
  static constexpr bool is_row_vertex_set = false;
  static constexpr size_t num_labels = 2;
  static constexpr size_t num_props = 0;
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_label = false;
  TwoLabelVertexSetImpl(std::vector<VID_T>&& vec,
                        std::array<LabelT, 2>&& label_names,
                        grape::Bitset&& bitset)
      : vec_(std::move(vec)), label_names_(std::move(label_names)) {
    bitset_.swap(bitset);
  }

  TwoLabelVertexSetImpl(TwoLabelVertexSetImpl&& other)
      : vec_(std::move(other.vec_)),
        label_names_(std::move(other.label_names_)) {
    bitset_.swap(other.bitset_);
  }

  TwoLabelVertexSetImpl(const TwoLabelVertexSetImpl& other)
      : vec_(other.vec_), label_names_(other.label_names_) {
    bitset_.copy(other.bitset_);
  }

  builder_t CreateBuilder() const {
    return builder_t(bitset_.cardinality(), label_names_);
  }

  template <typename... T>
  TwoLabelVertexSetImpl<VID_T, LabelT, T...> WithData(
      std::vector<std::tuple<T...>>&& data,
      std::array<std::string, sizeof...(T)>&& named_prop) const {
    auto copied_vec(vec_);
    grape::Bitset copied_bitset;
    copied_bitset.copy(bitset_);
    auto copied_label_names(label_names_);
    return TwoLabelVertexSetImpl<VID_T, LabelT, T...>(
        std::move(copied_vec), std::move(data), std::move(copied_label_names),
        std::move(named_prop), std::move(copied_bitset));
  }

  iterator begin() const { return iterator(vec_, bitset_, 0); }

  iterator end() const { return iterator(vec_, bitset_, vec_.size()); }

  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER_T,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels,
                         EXPRESSION& exprs,
                         std::array<PROP_GETTER_T, 2>& prop_getters) const {
    // TODO: vector-based cols should be able to be selected with
    // certain rows.

    auto tuple = two_label_project_vertices_impl(
        vec_, bitset_, label_names_, filter_labels, exprs, prop_getters);
    auto copied_label_names(label_names_);
    auto set = self_type_t(std::move(std::get<0>(tuple)),
                           std::move(copied_label_names),
                           std::move(std::get<1>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<2>(tuple)));
  }

  // project vertices with only filter labels
  template <size_t num_labels, typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels) const {
    auto tuple = two_label_project_vertices_impl(vec_, bitset_, label_names_,
                                                 filter_labels);
    auto copied_label_names(label_names_);
    auto set = self_type_t(std::move(std::get<0>(tuple)),
                           std::move(copied_label_names),
                           std::move(std::get<1>(tuple)));
    return std::make_pair(std::move(set), std::move(std::get<2>(tuple)));
  }

  const std::array<LabelT, 2>& GetLabels() const { return label_names_; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill with each vertex's label
    for (size_t i = 0; i < vec_.size(); ++i) {
      if (bitset_.get_bit(i)) {
        res.emplace_back(label_names_[0]);
      } else {
        res.emplace_back(label_names_[1]);
      }
    }
    return res;
  }

  LabelT GetLabel(size_t i) const { return label_names_[i]; }

  const grape::Bitset& GetBitset() const { return bitset_; }

  grape::Bitset& GetMutableBitset() { return bitset_; }

  const std::vector<VID_T>& GetVertices() const { return vec_; }

  std::vector<VID_T>& GetMutableVertices() { return vec_; }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVertices(
      size_t ind) const {
    CHECK(ind < 2);
    CHECK(bitset_.cardinality() == vec_.size());
    std::vector<VID_T> res;
    std::vector<int32_t> active_ind;
    // 0 denotes label0, 1 denotes label1.
    size_t cnt;
    if (ind == 0) {
      cnt = bitset_.count();
    } else {
      cnt = bitset_.cardinality() - bitset_.count();
    }
    res.reserve(cnt);
    active_ind.reserve(cnt);
    if (ind == 0) {
      for (size_t i = 0; i < bitset_.cardinality(); ++i) {
        if (bitset_.get_bit(i)) {
          res.push_back(vec_[i]);
          active_ind.push_back(i);
        }
      }
    } else {
      for (size_t i = 0; i < bitset_.cardinality(); ++i) {
        if (!bitset_.get_bit(i)) {
          res.push_back(vec_[i]);
          active_ind.push_back(i);
        }
      }
    }

    VLOG(10) << "Got vertices of tag: " << ind
             << ", res vertices: " << res.size()
             << ", active_ind size:  " << active_ind.size();
    return std::make_pair(std::move(res), std::move(active_ind));
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
    VLOG(10) << "[TwoLabelVertexSetImpl] size: " << Size()
             << " Project self, next size: " << next_size;

    next_vids.reserve(next_size);
    grape::Bitset next_set;
    next_set.init(next_size);
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      if (bitset_.get_bit(i)) {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          next_set.set_bit(next_vids.size());
          next_vids.push_back(vec_[i]);
        }
      } else {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          next_vids.push_back(vec_[i]);
        }
      }
    }

    auto copied_label_names(label_names_);
    return self_type_t(std::move(next_vids), std::move(copied_label_names),
                       std::move(next_set));
  }

  // Usually after sort.
  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t_...>>);
    auto res_vids_and_data_tuples =
        twoLabelSetFlatImpl<col_ind>(index_ele_tuple, vec_, bitset_);
    auto labels_copied(label_names_);
    return self_type_t(std::move(res_vids_and_data_tuples.first),
                       std::move(labels_copied),
                       std::move(res_vids_and_data_tuples.second));
  }

  template <size_t Is, typename... PropT>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            std::string& prop_name,
                            std::vector<offset_t>& repeat_array) {
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
                            std::index_sequence<Is...>) {
    (fillBuiltinPropsImpl<Is, PropT...>(tuples, std::get<Is>(prop_names),
                                        repeat_array),
     ...);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) {
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  // No repeat array is not provided
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    LOG(WARNING) << "not supported";
  }

  void SubSetWithIndices(std::vector<size_t>& indices) {
    std::vector<VID_T> new_vec;
    grape::Bitset new_bitset;
    new_vec.reserve(indices.size());
    new_bitset.init(indices.size());
    {
      size_t i = 0;
      for (auto& index : indices) {
        new_vec.emplace_back(vec_[index]);
        if (bitset_.get_bit(index)) {
          new_bitset.set_bit(i);
        }
        ++i;
      }
    }
    vec_.swap(new_vec);
    // safe?
    bitset_.swap(new_bitset);
    VLOG(10) << "after subset: " << vec_.size()
             << ",count: " << bitset_.count();
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    std::vector<VID_T> new_vec;
    grape::Bitset new_bitset;
    new_vec.reserve(repeat_vec.back());
    // estimate size
    {
      size_t tmp_size = 0;
      for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
        auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
        tmp_size += (cur_offset[i + 1] - cur_offset[i]) * times_to_repeat;
      }
      new_bitset.init(tmp_size);
    }

    for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (size_t j = 0; j < times_to_repeat; ++j) {
        for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          new_vec.emplace_back(vec_[k]);
          if (bitset_.get_bit(k)) {
            new_bitset.set_bit(new_vec.size() - 1);
          }
        }
      }
    }
    vec_.swap(new_vec);
    bitset_.swap(new_bitset);
    VLOG(10) << "Finish repeat on two label set";
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<VID_T> vec_;
  std::array<LabelT, 2> label_names_;
  grape::Bitset bitset_;
};

template <typename VID_T, typename LabelT, typename... T>
using TwoLabelVertexSet = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;

template <typename VID_T, typename LabelT>
auto make_two_label_set(std::vector<VID_T>&& vec,
                        std::array<LabelT, 2>&& label_names,
                        grape::Bitset&& bitset) {
  return TwoLabelVertexSet<VID_T, LabelT, grape::EmptyType>(
      std::move(vec), std::move(label_names), std::move(bitset));
}

template <typename VID_T, typename LabelT>
auto make_two_label_set(std::vector<VID_T>&& vec,
                        const std::array<LabelT, 2>& label_names,
                        grape::Bitset&& bitset) {
  auto copied(label_names);
  return TwoLabelVertexSet<VID_T, LabelT, grape::EmptyType>(
      std::move(vec), std::move(copied), std::move(bitset));
}

template <typename VID_T, typename... T, typename LabelT>
auto make_two_label_set(std::vector<VID_T>&& vec,
                        std::vector<std::tuple<T...>>&& data,
                        std::array<LabelT, 2>&& label_names,
                        std::array<std::string, sizeof...(T)>&& prop_names,
                        grape::Bitset&& bitset) {
  return TwoLabelVertexSet<VID_T, LabelT, T...>(
      std::move(vec), std::move(data), std::move(label_names),
      std::move(prop_names), std::move(bitset));
}

template <typename vertex_id_t>
static std::pair<std::array<std::vector<vertex_id_t>, 2>,
                 std::array<std::vector<int32_t>, 2>>
two_label_bitset_to_vids_indsV2(const grape::Bitset& bitset,
                                const std::vector<vertex_id_t>& old_vids) {
  std::array<std::vector<int32_t>, 2> res;
  std::array<std::vector<vertex_id_t>, 2> res_vids;
  auto limit_size = bitset.cardinality();
  VLOG(10) << "old bitset limit size: " << limit_size;
  auto label0_cnt = bitset.count();
  res[0].reserve(label0_cnt);
  res_vids[0].reserve(label0_cnt);
  res[1].reserve(limit_size - label0_cnt);
  res_vids[1].reserve(limit_size - label0_cnt);
  for (size_t i = 0; i < limit_size; ++i) {
    if (bitset.get_bit(i)) {
      res[0].emplace_back(i);
      res_vids[0].emplace_back(old_vids[i]);
    } else {
      res[1].emplace_back(i);
      res_vids[1].emplace_back(old_vids[i]);
    }
  }
  return std::make_pair(std::move(res_vids), std::move(res));
}

template <typename... T, typename GRAPH_INTERFACE, typename LabelT,
          typename... VERTEX_SET_PROP_T>
static auto get_property_tuple_two_label(
    const GRAPH_INTERFACE& graph,
    const TwoLabelVertexSet<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                            VERTEX_SET_PROP_T...>& general_set,
    const std::array<std::string, sizeof...(T)>& prop_names) {
  auto& label_array = general_set.GetLabels();

  // Get data for multilabel vertices, mixed
  // double t1 = -grape::GetCurrentTime();
  auto data_tuples = graph.template GetVertexPropsFromVidV2<T...>(
      general_set.GetVertices(), label_array, general_set.GetBitset(),
      prop_names);

  return data_tuples;
}

template <typename GRAPH_INTERFACE, typename LabelT,
          typename... VERTEX_SET_PROP_T, typename... NamedProp>
static auto get_property_tuple_two_label(
    const GRAPH_INTERFACE& graph,
    const TwoLabelVertexSetImpl<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                                VERTEX_SET_PROP_T...>& general_set,
    const std::tuple<NamedProp...>& named_prop) {
  std::array<std::string, sizeof...(NamedProp)> prop_names;
  size_t ind = 0;
  std::apply([&prop_names,
              &ind](auto&... args) { ((prop_names[ind++] = args.name), ...); },
             named_prop);
  return get_property_tuple_two_label<typename NamedProp::prop_t...>(
      graph, general_set, prop_names);
}

}  // namespace gs

#endif  // ENGINES_HQPS_DS_MULTI_VERTEX_SET_TWO_LABEL_VERTEX_SET_H_
