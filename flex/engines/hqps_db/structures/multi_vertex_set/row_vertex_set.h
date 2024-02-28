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
#ifndef ENGINES_HQPS_DS_MULTI_VERTEX_SET_ROW_VERTEX_SET_H_
#define ENGINES_HQPS_DS_MULTI_VERTEX_SET_ROW_VERTEX_SET_H_

#include <string>
#include <tuple>
#include <vector>

#include "grape/util.h"

// Vertex set in with data in rows.
namespace gs {

template <typename T>
class Collection;

namespace internal {

template <size_t Proj_Is, size_t My_Is, typename... DATA_T, typename... PropT>
void fillBuiltinPropsImpl(
    const std::vector<std::tuple<DATA_T...>>& datas,
    const std::array<std::string, sizeof...(DATA_T)>& set_prop_names,
    std::vector<std::tuple<PropT...>>& tuples, const std::string& prop_name,
    const std::vector<offset_t>& repeat_array) {
  using cur_prop = std::tuple_element_t<Proj_Is, std::tuple<PropT...>>;
  using my_prop = std::tuple_element_t<My_Is, std::tuple<DATA_T...>>;
  VLOG(10) << "ProjId: " << Proj_Is << ", MyId: " << My_Is << ", "
           << " input prop_name: " << prop_name << ", "
           << typeid(cur_prop).name() << ", " << typeid(my_prop).name()
           << ",prop_name " << set_prop_names[My_Is]
           << ", eq: " << gs::to_string(std::is_same_v<cur_prop, my_prop>);
  if constexpr (std::is_same_v<cur_prop, my_prop>) {
    if (prop_name == set_prop_names[My_Is]) {
      VLOG(10) << "Found built-in property " << prop_name;
      CHECK(repeat_array.size() == datas.size());
      size_t ind = 0;
      for (size_t i = 0; i < repeat_array.size(); ++i) {
        for (size_t j = 0; j < repeat_array[i]; ++j) {
          std::get<Proj_Is>(tuples[ind]) = std::get<0>(datas[i]);
          ind += 1;
        }
      }
    }
  }
}

template <size_t Is, typename... DATA_T, typename... PropT, size_t... MyIs>
void fillBuiltinPropsImpl(
    const std::vector<std::tuple<DATA_T...>>& datas,
    const std::array<std::string, sizeof...(DATA_T)>& set_prop_names,
    std::vector<std::tuple<PropT...>>& tuples, const std::string& prop_name,
    const std::vector<offset_t>& repeat_array, std::index_sequence<MyIs...>) {
  (fillBuiltinPropsImpl<Is, MyIs>(datas, set_prop_names, tuples, prop_name,
                                  repeat_array),
   ...);
}

template <typename... DATA_T, typename... PropT, size_t... Is>
void fillBuiltinPropsImpl(
    const std::vector<std::tuple<DATA_T...>>& datas,
    const std::array<std::string, sizeof...(DATA_T)>& set_prop_names,
    std::vector<std::tuple<PropT...>>& tuples,
    const PropNameArray<PropT...>& prop_names,
    const std::vector<offset_t>& repeat_array, std::index_sequence<Is...>) {
  (fillBuiltinPropsImpl<Is>(datas, set_prop_names, tuples,
                            std::get<Is>(prop_names), repeat_array,
                            std::make_index_sequence<sizeof...(DATA_T)>()),
   ...);
}
template <typename... DATA_T, typename... PropT>
void fillBuiltinPropsImpl(
    const std::vector<std::tuple<DATA_T...>>& datas,
    const std::array<std::string, sizeof...(DATA_T)>& set_prop_names,
    std::vector<std::tuple<PropT...>>& tuples,
    const PropNameArray<PropT...>& prop_names,
    const std::vector<offset_t>& repeat_array) {
  return gs::internal::fillBuiltinPropsImpl(
      datas, set_prop_names, tuples, prop_names, repeat_array,
      std::make_index_sequence<sizeof...(PropT)>());
}
}  // namespace internal

template <typename... DATA_T, typename... PropT>
void fillBuiltinPropsImpl(
    const std::vector<std::tuple<DATA_T...>>& datas,
    const std::array<std::string, sizeof...(DATA_T)>& set_prop_names,
    std::vector<std::tuple<PropT...>>& tuples,
    const PropNameArray<PropT...>& prop_names,
    const std::vector<offset_t>& repeat_array) {
  return gs::internal::fillBuiltinPropsImpl(datas, set_prop_names, tuples,
                                            prop_names, repeat_array);
}

template <typename LabelT, typename VID_T, typename... T>
class RowVertexSetImpl;

// RowSetBuilder
template <typename LabelT, typename VID_T, typename... T>
class RowVertexSetImplBuilder {
 public:
  using result_t = RowVertexSetImpl<LabelT, VID_T, T...>;
  using data_tuple_t = std::tuple<T...>;
  static constexpr bool is_row_vertex_set_builder = true;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;

  RowVertexSetImplBuilder(LabelT v_label,
                          std::array<std::string, sizeof...(T)> prop_names)
      : v_label_(v_label), prop_names_(prop_names) {}

  RowVertexSetImplBuilder(
      const RowVertexSetImplBuilder<LabelT, VID_T, T...>& other) {
    v_label_ = other.v_label_;
    prop_names_ = other.prop_names_;
  }

  void Insert(VID_T&& vid, data_tuple_t&& data) {
    vids_.emplace_back(std::move(vid));
    datas_.emplace_back(std::move(data));
  }

  void Insert(const VID_T& vid, const data_tuple_t& data) {
    vids_.push_back(vid);
    datas_.push_back(data);
  }

  void Insert(const std::tuple<size_t, VID_T>& ind_ele,
              const data_tuple_t& data) {
    vids_.push_back(std::get<1>(ind_ele));
    datas_.push_back(data);
  }

  void Insert(const std::tuple<size_t, VID_T, std::tuple<T...>>& flat_eles) {
    vids_.push_back(std::get<1>(flat_eles));
    datas_.push_back(std::get<2>(flat_eles));
  }

  result_t Build() {
    return result_t(std::move(vids_), v_label_, std::move(datas_),
                    std::move(prop_names_));
  }

  size_t Size() const { return vids_.size(); }

 private:
  std::vector<VID_T> vids_;
  std::vector<data_tuple_t> datas_;
  LabelT v_label_;
  std::array<std::string, sizeof...(T)> prop_names_;
};

// RowSetBuilder
template <typename LabelT, typename VID_T>
class RowVertexSetImplBuilder<LabelT, VID_T, grape::EmptyType> {
 public:
  using result_t = RowVertexSetImpl<LabelT, VID_T, grape::EmptyType>;

  static constexpr bool is_row_vertex_set_builder = true;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;

  RowVertexSetImplBuilder(LabelT v_label) : v_label_(v_label) {}

  RowVertexSetImplBuilder(const RowVertexSetImplBuilder& rhs)
      : vids_(rhs.vids_), v_label_(rhs.v_label_) {}

  void Insert(VID_T&& vid) { vids_.emplace_back(vid); }

  void Insert(const VID_T& vid) { vids_.push_back(vid); }

  void Insert(const std::tuple<size_t, VID_T>& flat_eles) {
    vids_.push_back(std::get<1>(flat_eles));
  }

  result_t Build() { return result_t(std::move(vids_), v_label_); }

  size_t Size() const { return vids_.size(); }

 private:
  std::vector<VID_T> vids_;
  LabelT v_label_;
};

template <typename LabelT, typename VID_T, typename... T>
using RowVertexSetBuilder = RowVertexSetImplBuilder<LabelT, VID_T, T...>;

template <typename VID_T, typename... T>
class RowVertexSetIter {
 public:
  using lid_t = VID_T;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;

  using data_tuple_t = std::tuple<T...>;
  using self_type_t = RowVertexSetIter<VID_T, T...>;

  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T, std::tuple<T...>>;
  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  RowVertexSetIter(const std::vector<lid_t>& vids,
                   const std::vector<data_tuple_t>& datas, size_t ind)
      : vids_(vids), datas_(datas), cur_ind_(ind) {}

  lid_t GetElement() const { return vids_[cur_ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(cur_ind_, vids_[cur_ind_]);
  }

  flat_ele_tuple_t GetFlatElement() const {
    return std::make_tuple(cur_ind_, vids_[cur_ind_], GetData());
  }

  lid_t GetVertex() const { return vids_[cur_ind_]; }

  data_tuple_t GetData() const { return datas_[cur_ind_]; }

  template <size_t I>
  auto GetCol() const {
    // TODO: fixme
    return std::get<I>(datas_[cur_ind_]);
  }

  inline const self_type_t& operator++() {
    ++cur_ind_;
    return *this;
  }

  inline self_type_t operator++(int) {
    self_type_t ret(*this);
    ++cur_ind_;
    return ret;
  }

  // We may never compare to other kind of iterators
  inline bool operator==(const self_type_t& rhs) const {
    return cur_ind_ == rhs.cur_ind_;
  }

  inline bool operator!=(const self_type_t& rhs) const {
    return cur_ind_ != rhs.cur_ind_;
  }

  inline bool operator<(const self_type_t& rhs) const {
    return cur_ind_ < rhs.cur_ind_;
  }

  inline const self_type_t& operator*() const { return *this; }

  inline const self_type_t* operator->() const { return this; }

 private:
  const std::vector<lid_t>& vids_;
  const std::vector<data_tuple_t>& datas_;
  size_t cur_ind_;
};

template <typename VID_T>
class RowVertexSetIter<VID_T, grape::EmptyType> {
 public:
  using lid_t = VID_T;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  using self_type_t = RowVertexSetIter<VID_T, grape::EmptyType>;
  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T>;
  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  RowVertexSetIter(const std::vector<lid_t>& vids, size_t ind)
      : vids_(vids), cur_ind_(ind) {}

  lid_t GetElement() const { return vids_[cur_ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(cur_ind_, vids_[cur_ind_]);
  }

  flat_ele_tuple_t GetFlatElement() const {
    return std::make_tuple(cur_ind_, vids_[cur_ind_]);
  }

  lid_t GetVertex() const { return vids_[cur_ind_]; }

  data_tuple_t GetData() const { return grape::EmptyType(); }

  inline const self_type_t& operator++() {
    ++cur_ind_;
    return *this;
  }

  inline self_type_t operator++(int) {
    self_type_t ret(*this);
    ++cur_ind_;
    return ret;
  }

  // We may never compare to other kind of iterators
  inline bool operator==(const self_type_t& rhs) const {
    return cur_ind_ == rhs.cur_ind_;
  }

  inline bool operator!=(const self_type_t& rhs) const {
    return cur_ind_ != rhs.cur_ind_;
  }

  inline bool operator<(const self_type_t& rhs) const {
    return cur_ind_ < rhs.cur_ind_;
  }

  inline const self_type_t& operator*() const { return *this; }

  inline const self_type_t* operator->() const { return this; }

 private:
  const std::vector<lid_t>& vids_;
  size_t cur_ind_;
};

template <typename lid_t, typename data_tuple_t>
static std::pair<std::vector<lid_t>, std::vector<data_tuple_t>>
RowSetSubSetImpl(const std::vector<lid_t>& old_vids,
                 const std::vector<data_tuple_t>& old_datas,
                 std::vector<offset_t>& indices) {
  VLOG(10) << "RowSetSubSetImple";
  std::vector<lid_t> new_vids(indices.size(), 0);
  std::vector<data_tuple_t> new_datas;
  new_datas.reserve(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    new_vids[i] = old_vids[indices[i]];
    new_datas.emplace_back(old_datas[indices[i]]);
  }
  return std::make_pair(std::move(new_vids), std::move(new_datas));
}

template <typename lid_t>
static std::vector<lid_t> RowSetSubSetImpl(const std::vector<lid_t>& old_vids,
                                           std::vector<offset_t>& indices) {
  VLOG(10) << "RowSetSubSetImple";
  std::vector<lid_t> new_vids(indices.size(), 0);
  for (size_t i = 0; i < indices.size(); ++i) {
    new_vids[i] = old_vids[indices[i]];
  }
  return new_vids;
}

template <typename lid_t, typename data_tuple_t>
std::vector<offset_t> RowSetDedupImpl(
    const std::vector<lid_t>& ori_lids,
    const std::vector<data_tuple_t>& ori_datas, std::vector<lid_t>& res_lids,
    std::vector<data_tuple_t>& res_datas) {
  std::vector<offset_t> offsets;
  VLOG(10) << "lid size" << ori_lids.size();
  offsets.reserve(ori_lids.size());

  // TODO: replace with bitset.
  std::unordered_map<lid_t, size_t> v2lid;
  size_t cnt = 0;
  for (size_t i = 0; i < ori_lids.size(); ++i) {
    offsets.emplace_back(cnt);
    auto ret = v2lid.insert({ori_lids[i], i});
    if (ret.second == true) {
      cnt += 1;
      res_lids.emplace_back(ori_lids[i]);
      res_datas.emplace_back(ori_datas[i]);
    }
  }
  offsets.emplace_back(cnt);

  // VLOG(10) << "in dedup: offsets: " << gs::to_string(offsets);
  // VLOG(10) << "in dedup: vids : " << gs::to_string(res_lids);
  return offsets;
}

template <typename lid_t>
std::vector<offset_t> RowSetDedupImpl(const std::vector<lid_t>& ori_lids,
                                      std::vector<lid_t>& res_lids) {
  std::vector<offset_t> offsets;
  VLOG(10) << "lid size" << ori_lids.size();
  offsets.reserve(ori_lids.size());

  // TODO: replace with bitset.
  std::unordered_map<lid_t, size_t> v2lid;
  size_t cnt = 0;
  for (size_t i = 0; i < ori_lids.size(); ++i) {
    offsets.emplace_back(cnt);
    auto ret = v2lid.insert({ori_lids[i], i});
    if (ret.second == true) {
      cnt += 1;
      res_lids.emplace_back(ori_lids[i]);
    }
  }
  offsets.emplace_back(cnt);

  return offsets;
}

template <size_t col_ind, typename... index_ele_tuple_t, typename lid_t,
          typename data_tuple_t>
auto rowSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    std::vector<lid_t>& origin_vids, std::vector<data_tuple_t>& origin_datas) {
  std::vector<lid_t> res_vids;
  std::vector<data_tuple_t> res_data_tuple;
  res_vids.reserve(index_ele_tuples.size());
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto& ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());
    res_vids.emplace_back(origin_vids[ind]);
    res_data_tuple.emplace_back(origin_datas[ind]);
  }
  return std::make_pair(std::move(res_vids), std::move(res_data_tuple));
}

// if num_labels == 0, we deem it as take all labels.
template <typename lid_t, typename EXPRESSION, size_t num_labels,
          typename LabelT, typename PROP_GETTER,
          typename RES_T = std::pair<std::vector<lid_t>, std::vector<offset_t>>>
RES_T row_project_vertices_impl(const std::vector<lid_t>& lids,
                                LabelT cur_label,
                                std::array<LabelT, num_labels>& labels,
                                EXPRESSION& expr,
                                std::array<PROP_GETTER, 1>& prop_getters) {
  std::vector<offset_t> offsets;
  std::vector<lid_t> new_lids;
  size_t cnt = 0;
  offsets.reserve(lids.size() + 1);
  int label_ind = -1;

  // FIXME: no repeated labels.
  if constexpr (num_labels == 0) {
    VLOG(10) << "take all labels";
    label_ind = 0;  // whatever greater than -1.
  } else {
    for (size_t i = 0; i < num_labels; ++i) {
      if (cur_label == labels[i])
        label_ind = i;
    }
  }
  if (label_ind == -1) {
    VLOG(10) << "No label found in query params";
    // for current set, we don't need.
    auto size = lids.size();
    for (size_t i = 0; i < size; ++i) {
      offsets.emplace_back(cnt);
    }
  } else {
    // VLOG(10) << "Found label in query params";
    auto& cur_prop_getter = prop_getters[0];
    for (size_t i = 0; i < lids.size(); ++i) {
      offsets.emplace_back(cnt);
      auto cur_lid = lids[i];
      auto prop = cur_prop_getter.get_view(cur_lid);
      if (std::apply(expr, prop)) {
        // if (expr(eles[i])) {
        new_lids.emplace_back(cur_lid);
        cnt += 1;
      }
    }
    offsets.emplace_back(cnt);
  }
  VLOG(10) << "Project vertices, new lids" << new_lids.size()
           << ", offset size: " << offsets.size();
  return std::make_pair(std::move(new_lids), std::move(offsets));
}
template <
    typename lid_t, typename EXPRESSION, size_t num_labels, typename LabelT,
    typename data_tuple_t, typename PROP_GETTER,
    typename RES_T = std::tuple<std::vector<lid_t>, std::vector<data_tuple_t>,
                                std::vector<offset_t>>>
RES_T row_project_vertices_impl(
    const std::vector<lid_t>& lids, const std::vector<data_tuple_t>& datas,
    LabelT cur_label, std::array<LabelT, num_labels>& labels, EXPRESSION& expr,
    std::array<PROP_GETTER, 1>& prop_getters) {  // temporary property
  std::vector<offset_t> offsets;
  std::vector<lid_t> new_lids;
  std::vector<data_tuple_t> new_datas;
  size_t cnt = 0;
  offsets.reserve(lids.size() + 1);
  int label_ind = -1;
  if constexpr (num_labels == 0) {
    VLOG(10) << "num_labels == 0";
    label_ind = 0;  // whatever greater than -1.
  } else {
    // FIXME: no repeated labels.
    for (size_t i = 0; i < num_labels; ++i) {
      if (cur_label == labels[i])
        label_ind = i;
    }
  }
  if (label_ind == -1) {
    VLOG(10) << "No label found in query params";
    // for current set, we don't need.
    auto size = lids.size();
    for (size_t i = 0; i < size; ++i) {
      offsets.emplace_back(cnt);
    }
  } else {
    VLOG(10) << "Found label in query params";
    auto& cur_prop_getter = prop_getters[0];
    for (size_t i = 0; i < lids.size(); ++i) {
      offsets.emplace_back(cnt);
      auto cur_lid = lids[i];
      auto prop = cur_prop_getter.get_view(cur_lid);
      if (std::apply(expr, prop)) {
        new_lids.emplace_back(lids[i]);
        new_datas.emplace_back(datas[i]);
        cnt += 1;
      }
    }
    offsets.emplace_back(cnt);
  }
  return std::make_tuple(std::move(new_lids), std::move(new_datas),
                         std::move(offsets));
}

// select certain labels from set
template <typename lid_t, size_t num_labels, typename LabelT,
          typename RES_T = std::pair<std::vector<lid_t>, std::vector<offset_t>>>
RES_T select_labels(const std::vector<lid_t>& lids, LabelT cur_label,
                    std::array<LabelT, num_labels>& labels) {
  std::vector<offset_t> offsets;
  size_t cnt = 0;
  offsets.reserve(lids.size() + 1);
  int label_ind = -1;
  std::vector<lid_t> new_lids;
  if constexpr (num_labels == 0) {
    label_ind = 0;
  } else {
    // FIXME: no repeated labels.
    for (size_t i = 0; i < num_labels; ++i) {
      if (cur_label == labels[i])
        label_ind = i;
    }
  }
  if (label_ind == -1) {
    VLOG(10) << "No label found in query params";
    // for current set, we don't need.
    auto size = lids.size();
    for (size_t i = 0; i < size; ++i) {
      offsets.emplace_back(cnt);
    }
  } else {
    for (size_t i = 0; i < lids.size(); ++i) {
      offsets.emplace_back(i);
    }
    offsets.emplace_back(lids.size());
    new_lids = lids;  // copy the vids.
  }
  return std::make_pair(std::move(new_lids), std::move(offsets));
}

template <
    typename lid_t, typename data_tuple_t, size_t num_labels, typename LabelT,
    typename RES_T = std::tuple<std::vector<lid_t>, std::vector<data_tuple_t>,
                                std::vector<offset_t>>>
RES_T select_labels(const std::vector<lid_t>& lids,
                    const std::vector<data_tuple_t>& data_tuples,
                    LabelT cur_label, std::array<LabelT, num_labels>& labels) {
  std::vector<offset_t> offsets;
  size_t cnt = 0;
  offsets.reserve(lids.size() + 1);
  int label_ind = -1;
  std::vector<lid_t> new_lids;
  std::vector<data_tuple_t> new_data_tuples;
  // FIXME: no repeated labels.
  if constexpr (num_labels == 0) {
    label_ind = 0;
  } else {
    for (size_t i = 0; i < num_labels; ++i) {
      if (cur_label == labels[i])
        label_ind = i;
    }
  }
  if (label_ind == -1) {
    VLOG(10) << "No label found in query params";
    // for current set, we don't need.
    auto size = lids.size();
    for (size_t i = 0; i < size; ++i) {
      offsets.emplace_back(cnt);
    }
  } else {
    for (size_t i = 0; i < lids.size(); ++i) {
      offsets.emplace_back(i);
    }
    offsets.emplace_back(lids.size());
    new_lids = lids;  // copy the vids.
    new_data_tuples = data_tuples;
  }
  return std::make_tuple(std::move(new_lids), std::move(new_data_tuples),
                         std::move(offsets));
}

template <size_t col_ind, typename... index_ele_tuple_t, typename lid_t>
auto rowSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    std::vector<lid_t>& origin_vids) {
  std::vector<lid_t> res_vids;
  res_vids.reserve(index_ele_tuples.size());
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto& ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());
    res_vids.emplace_back(origin_vids[ind]);
  }
  return res_vids;
}

template <int tag_id, int res_id, int... Is, typename lid_t>
auto row_project_with_repeat_array_impl(
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

template <int tag_id, int res_id, int... Is, typename lid_t, typename... T>
auto row_project_with_repeat_array_impl(
    const KeyAlias<tag_id, res_id, Is...>& key_alias,
    const std::vector<size_t>& repeat_array, const std::vector<lid_t>& old_lids,
    const std::vector<std::tuple<T...>>& old_datas) {
  using res_t = std::vector<std::tuple<
      typename gs::tuple_element<Is, std::tuple<T..., lid_t>>::type...>>;

  res_t res_vec;
  for (size_t i = 0; i < repeat_array.size(); ++i) {
    for (size_t j = 0; j < repeat_array[i]; ++j) {
      auto tuple = std::tuple_cat(old_datas[i], std::make_tuple(old_lids[i]));
      res_vec.emplace_back(std::make_tuple(gs::get_from_tuple<Is>(tuple)...));
    }
  }
  return res_vec;
}

// We assume the indices are in order, no dup
template <typename lid_t, typename data_tuple_t>
std::tuple<std::vector<lid_t>, std::vector<data_tuple_t>, std::vector<offset_t>>
row_filter_with_indices_impl(std::vector<size_t>& indices,
                             std::vector<lid_t>& vids,
                             std::vector<data_tuple_t>& datas,
                             JoinKind join_kind) {
  std::vector<offset_t> res_offset;
  std::vector<lid_t> res_vids;
  std::vector<data_tuple_t> res_datas;

  res_offset.reserve(vids.size() + 1);

  size_t indices_ind = 0;
  if (join_kind == JoinKind::InnerJoin) {
    res_vids.reserve(indices.size());
    res_datas.reserve(indices.size());
    size_t vid_ind = 0;
    res_offset.emplace_back(0);
    for (; vid_ind < vids.size(); ++vid_ind) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind++;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] == vid_ind) {
          res_vids.push_back(vids[vid_ind]);
          res_datas.push_back(datas[vid_ind]);
        }
      }
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == indices.size());
    CHECK(res_datas.size() == indices.size());
    CHECK(res_offset.size() == vids.size() + 1);
  } else {
    res_vids.reserve(vids.size() - indices.size());
    res_datas.reserve(vids.size() - indices.size());
    res_offset.emplace_back(res_vids.size());
    size_t vid_ind = 0;
    while (vid_ind < vids.size()) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind += 1;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] != vid_ind) {
          res_vids.emplace_back(vids[vid_ind]);
          res_datas.push_back(datas[vid_ind]);
        }
      } else {
        res_vids.emplace_back(vids[vid_ind]);
        res_datas.push_back(datas[vid_ind]);
      }
      vid_ind += 1;
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == vids.size() - indices.size());
    CHECK(res_datas.size() == vids.size() - indices.size());
    CHECK(res_offset.size() == vids.size() + 1);
  }
  // VLOG(10) << "res offset: " << gs::to_string(res_offset);
  // VLOG(10) << "res vids: " << gs::to_string(res_vids);
  return std::make_tuple(std::move(res_vids), std::move(res_datas),
                         std::move(res_offset));
}

// We assume the indices are in order, no dup
template <typename lid_t>
std::pair<std::vector<lid_t>, std::vector<offset_t>>
row_filter_with_indices_impl(std::vector<size_t>& indices,
                             std::vector<lid_t>& vids, JoinKind join_kind) {
  std::vector<offset_t> res_offset;
  std::vector<lid_t> res_vids;

  res_offset.reserve(vids.size() + 1);

  size_t indices_ind = 0;
  if (join_kind == JoinKind::InnerJoin) {
    res_vids.reserve(indices.size());
    size_t vid_ind = 0;
    res_offset.emplace_back(0);
    for (; vid_ind < vids.size(); ++vid_ind) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind++;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] == vid_ind) {
          res_vids.push_back(vids[vid_ind]);
        }
      }
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == indices.size());
    CHECK(res_offset.size() == vids.size() + 1);
  } else {
    res_vids.reserve(vids.size() - indices.size());
    res_offset.emplace_back(res_vids.size());
    size_t vid_ind = 0;
    while (vid_ind < vids.size()) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind += 1;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] != vid_ind) {
          res_vids.emplace_back(vids[vid_ind]);
        }
      } else {
        res_vids.emplace_back(vids[vid_ind]);
      }
      vid_ind += 1;
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == vids.size() - indices.size());
    CHECK(res_offset.size() == vids.size() + 1);
  }
  // VLOG(10) << "res offset: " << gs::to_string(res_offset);
  // VLOG(10) << "res vids: " << gs::to_string(res_vids);
  return std::make_pair(std::move(res_vids), std::move(res_offset));
}

template <typename lid_t>
std::pair<std::vector<lid_t>, std::vector<offset_t>>
subSetWithRemovedIndicesImpl(std::vector<offset_t>& removed_indices,
                             std::vector<offset_t>& indices_range,
                             std::vector<lid_t>& old_vids) {
  CHECK(old_vids.size() == indices_range.back());
  std::vector<lid_t> res_vids;
  std::vector<offset_t> res_indices_range;
  size_t res_ind_left = 0;
  // TODO: we can know the size exactly.
  res_vids.reserve(old_vids.size());
  size_t removed_ind = 0;
  for (size_t ind = 0; ind < indices_range.size() - 1; ++ind) {
    if (removed_ind >= removed_indices.size() ||
        ind < removed_indices[removed_ind]) {
      res_indices_range.emplace_back(res_ind_left);
      int left = indices_range[ind];
      int right = indices_range[ind + 1];
      res_ind_left += (right - left);
      for (int j = left; j < right; ++j) {
        res_vids.emplace_back(old_vids[j]);
      }
    } else if (ind == removed_indices[removed_ind]) {
      removed_ind += 1;
    } else {
      LOG(FATAL) << "not possible" << ind << ", " << removed_ind
                 << ", :" << gs::to_string(removed_indices) << ", "
                 << gs::to_string(indices_range);
    }
  }
  res_indices_range.emplace_back(res_ind_left);
  return std::make_pair(std::move(res_vids), std::move(res_indices_range));
}

template <typename lid_t, typename data_tuple_t>
std::tuple<std::vector<lid_t>, std::vector<data_tuple_t>, std::vector<offset_t>>
subSetWithRemovedIndicesImpl(std::vector<offset_t>& removed_indices,
                             std::vector<offset_t>& indices_range,
                             std::vector<lid_t>& old_vids,
                             std::vector<data_tuple_t>& old_data) {
  CHECK(old_vids.size() == indices_range.back());
  std::vector<lid_t> res_vids;
  std::vector<data_tuple_t> res_datas;
  std::vector<offset_t> res_indices_range;
  size_t res_ind_left = 0;
  // TODO: how can we know the size exactly.
  // res_vids.reserve(old_vids.size());
  // old_data.reserve()
  size_t removed_ind = 0;
  for (size_t ind = 0; ind < indices_range.size() - 1; ++ind) {
    if (removed_ind >= removed_indices.size() ||
        ind < removed_indices[removed_ind]) {
      res_indices_range.emplace_back(res_ind_left);
      int left = indices_range[ind];
      int right = indices_range[ind + 1];
      res_ind_left += (right - left);
      for (size_t j = left; j < right; ++j) {
        res_vids.emplace_back(old_vids[j]);
        res_datas.emplace_back(old_data[j]);
      }
    } else if (ind == removed_indices[removed_ind]) {
      removed_ind += 1;
    } else {
      LOG(FATAL) << "not possible" << ind << ", " << removed_ind
                 << ", :" << gs::to_string(removed_indices) << ", "
                 << gs::to_string(indices_range);
    }
  }
  res_indices_range.emplace_back(res_ind_left);
  return std::make_tuple(std::move(res_vids), std::move(res_datas),
                         std::move(res_indices_range));
}

template <typename LabelT, typename VID_T, typename... T>
class RowVertexSetImpl {
 public:
  using element_type = VID_T;
  using element_t = VID_T;
  using lid_t = VID_T;
  using data_tuple_t = std::tuple<T...>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T, std::tuple<T...>>;
  using flat_t = RowVertexSetImpl<LabelT, VID_T, T...>;
  using iterator = RowVertexSetIter<VID_T, T...>;
  using self_type_t = RowVertexSetImpl<LabelT, VID_T, T...>;
  using EntityValueType = VID_T;
  using builder_t = RowVertexSetImplBuilder<LabelT, VID_T, T...>;

  template <typename... Ts>
  using with_data_t = RowVertexSetImpl<LabelT, VID_T, T..., Ts...>;
  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  static constexpr bool is_keyed = false;
  static constexpr bool is_vertex_set = true;
  static constexpr bool is_row_vertex_set = true;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_edge_set = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_general_set = false;
  static constexpr bool is_collection = false;

  static constexpr size_t num_props = sizeof...(T);

  explicit RowVertexSetImpl(std::vector<lid_t>&& vids, LabelT v_label,
                            std::vector<data_tuple_t>&& data_tuples,
                            std::array<std::string, sizeof...(T)>&& prop_names)
      : vids_(std::move(vids)),
        v_label_(v_label),
        data_tuples_(std::move(data_tuples)),
        prop_names_(std::move(prop_names)) {
    CHECK(vids.size() == data_tuples.size());
  }

  explicit RowVertexSetImpl(
      std::vector<lid_t>&& vids, LabelT v_label,
      std::vector<data_tuple_t>&& data_tuples,
      const std::array<std::string, sizeof...(T)>& prop_names)
      : vids_(std::move(vids)),
        v_label_(v_label),
        data_tuples_(std::move(data_tuples)),
        prop_names_(prop_names) {
    CHECK(vids.size() == data_tuples.size());
  }

  RowVertexSetImpl(self_type_t&& other) noexcept
      : vids_(std::move(other.vids_)),
        v_label_(other.v_label_),
        data_tuples_(std::move(other.data_tuples_)),
        prop_names_(std::move(other.prop_names_)) {}

  RowVertexSetImpl(const self_type_t& other) noexcept
      : vids_(other.vids_),
        v_label_(other.v_label_),
        data_tuples_(other.data_tuples_),
        prop_names_(other.prop_names_) {}

  iterator begin() const { return iterator(vids_, data_tuples_, 0); }

  iterator end() const { return iterator(vids_, data_tuples_, vids_.size()); }

  size_t Size() const { return vids_.size(); }

  builder_t CreateBuilder() const { return builder_t(v_label_, prop_names_); }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    std::vector<lid_t> res_vec;
    std::vector<data_tuple_t> res_datas;
    res_vec.reserve(repeat_vec.back());
    for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (size_t j = 0; j < times_to_repeat; ++j) {
        for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          res_vec.emplace_back(vids_[k]);
          res_datas.emplace_back(data_tuples_[k]);
        }
      }
    }
    vids_.swap(res_vec);
    data_tuples_.swap(res_datas);
  }

  // create a copy
  self_type_t CreateCopy() const {
    std::vector<lid_t> copied_lids = vids_;
    std::vector<data_tuple_t> copied_data = data_tuples_;
    return self_type_t(std::move(copied_lids), v_label_,
                       std::move(copied_data));
  }

  const LabelT& GetLabel() const { return v_label_; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill res with v_label_
    res.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      res.emplace_back(v_label_);
    }
    return res;
  }

  const std::vector<lid_t>& GetVertices() const { return vids_; }

  std::vector<lid_t>& GetMutableVertices() { return vids_; }

  const auto& GetDataVec() const { return data_tuples_; }

  const std::array<std::string, sizeof...(T)>& GetPropNames() const {
    return prop_names_;
  }
  std::vector<lid_t>&& MoveVertices() { return std::move(vids_); }

  std::vector<size_t> GenerateKeys() const {
    std::vector<size_t> res;
    res.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      res.emplace_back(i);
    }
    return res;
  }

  // According to the given indices, filter in place.
  void SubSetWithIndices(std::vector<size_t>& indices) {
    // VLOG(10) << "subset with " << gs::to_string(indices);
    auto vids_and_tuples = RowSetSubSetImpl(vids_, data_tuples_, indices);
    vids_.swap(vids_and_tuples.first);
    data_tuples_.swap(vids_and_tuples.second);
    // VLOG(10) << "after subset: " << vids_.size();
  }

  auto WithIndices(std::vector<size_t>& indices) {
    auto vids_and_tuples = RowSetSubSetImpl(vids_, data_tuples_, indices);
    return self_type_t(std::move(vids_and_tuples.first), v_label_,
                       std::move(vids_and_tuples.second), prop_names_);
  }

  // all dedup are done inplace
  std::vector<offset_t> Dedup() {
    std::vector<lid_t> vids;
    std::vector<data_tuple_t> data_tuples;
    auto offset = RowSetDedupImpl(vids_, data_tuples_, vids, data_tuples);
    vids_.swap(vids);
    data_tuples_.swap(data_tuples);
    return offset;
  }

  // Filter current vertices with expression.
  template <typename EXPR>
  std::pair<self_type_t, std::vector<offset_t>> Filter(EXPR&& expr) {
    // Expression contains the property name, we extract vertex store here.
    static constexpr size_t num_args = EXPR::num_args;
    static_assert(num_args == sizeof...(T));

    size_t cur = 0;
    std::vector<offset_t> offset;
    std::vector<lid_t> res_lids;
    std::vector<data_tuple_t> res_data_tuples;
    offset.reserve(Size() + 1);
    for (auto iter : *this) {
      offset.emplace_back(cur);
      if (expr(iter.GetVertex())) {
        res_lids.emplace_back(iter.GetVertex());
        res_data_tuples.emplace_back(iter.GetData());
        cur += 1;
      }
    }
    offset.emplace_back(cur);
    auto new_set = self_type_t(std::move(res_lids), v_label_,
                               std::move(res_data_tuples), prop_names_);
    return std::make_pair(std::move(new_set), std::move(offset));
  }

  // Usually after
  template <size_t col_ind, typename... index_ele_tuple_t>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t...>>);
    auto res_vids_and_data_tuples =
        rowSetFlatImpl<col_ind>(index_ele_tuple, vids_, data_tuples_);
    return self_type_t(std::move(res_vids_and_data_tuples.first), v_label_,
                       std::move(res_vids_and_data_tuples.second), prop_names_);
  }

  // size_t... Is denotes the ind of data array need to project.
  //-1 denote it self.
  template <
      int tag_id, int Fs, int... Is,
      typename std::enable_if<(sizeof...(Is) > 0)>::type* = nullptr,
      typename res_t = Collection<std::tuple<
          typename gs::tuple_element<Fs, std::tuple<T..., lid_t>>::type,
          typename gs::tuple_element<Is, std::tuple<T..., lid_t>>::type...>>>
  res_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                               KeyAlias<tag_id, Fs, Is...>& key_alias) const {
    auto res_vec = row_project_with_repeat_array_impl(key_alias, repeat_array,
                                                      vids_, data_tuples_);
    return res_t(std::move(res_vec));
  }

  template <int tag_id, int Fs,
            typename std::enable_if<Fs != -1>::type* = nullptr,
            typename res_t = Collection<std::tuple<
                typename gs::tuple_element<Fs, std::tuple<T..., lid_t>>::type>>>
  res_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                               KeyAlias<tag_id, Fs>& key_alias) const {
    auto res_vec = row_project_with_repeat_array_impl(key_alias, repeat_array,
                                                      vids_, data_tuples_);
    return res_t(std::move(res_vec));
  }

  // project my self.
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<lid_t> vids;
    std::vector<data_tuple_t> data_tuples;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        // VLOG(10) << "Project: " << vids_[i];
        vids.push_back(vids_[i]);
        data_tuples.push_back(data_tuples_[i]);
      }
    }

    return self_type_t(std::move(vids), v_label_, std::move(data_tuples),
                       std::move(prop_names_));
  }

  // project vertices when expression udf
  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER,
            typename std::enable_if<
                (!std::is_same_v<EXPRESSION, TruePredicate>)>::type* = nullptr,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr,
                         std::array<PROP_GETTER, 1>& prop_getters) const {
    auto new_lids_datas_and_offset = row_project_vertices_impl(
        vids_, data_tuples_, v_label_, labels, expr, prop_getters);
    self_type_t res_set(
        std::move(std::get<0>(new_lids_datas_and_offset)), v_label_,
        std::move(std::get<1>(new_lids_datas_and_offset)), prop_names_);

    return std::make_pair(std::move(res_set),
                          std::move(std::get<2>(new_lids_datas_and_offset)));
  }

  // only project certain labels, without any expression.
  template <size_t num_labels, typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels) const {
    auto new_lids_datas_and_offset =
        select_labels(vids_, data_tuples_, v_label_, labels);
    self_type_t res_set(
        std::move(std::get<0>(new_lids_datas_and_offset)), v_label_,
        std::move(std::get<1>(new_lids_datas_and_offset)), prop_names_);

    return std::make_pair(std::move(res_set),
                          std::move(std::get<2>(new_lids_datas_and_offset)));
  }

  std::vector<offset_t> FilterWithIndices(std::vector<size_t>& offset,
                                          JoinKind join_kind) {
    auto tuple =
        row_filter_with_indices_impl(offset, vids_, data_tuples_, join_kind);
    vids_.swap(std::get<0>(tuple));
    data_tuples_.swap(std::get<1>(tuple));
    return std::get<2>(tuple);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names,
                        const std::vector<offset_t>& repeat_array) {
    fillBuiltinPropsImpl(data_tuples_, prop_names_, tuples, prop_names,
                         repeat_array);
  }

  // fill builtin props without repeat array.
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names) {
    std::vector<offset_t> repeat_array(vids_.size(), 1);
    fillBuiltinPropsImpl(data_tuples_, prop_names_, tuples, prop_names,
                         repeat_array);
  }

  // In places
  template <typename... Ts>
  RowVertexSetImpl<LabelT, VID_T, T..., Ts...> WithNewData(
      std::vector<std::tuple<Ts...>>&& new_datas) {
    CHECK(new_datas.size() == new_datas.size());
    std::vector<std::tuple<T..., Ts...>> new_data_tuples;
    new_data_tuples.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      new_data_tuples.emplace_back(
          std::tuple_cat(std::move(data_tuples_[i]), std::move(new_datas[i])));
    }
    return RowVertexSetImpl<LabelT, VID_T, T..., Ts...>(
        std::move(vids_), v_label_, std::move(new_data_tuples),
        std::move(prop_names_));
  }

  std::vector<offset_t> SubSetWithRemovedIndices(
      std::vector<size_t>& removed_indices,
      std::vector<size_t>& indices_range) {
    auto vids_and_new_offset_range = subSetWithRemovedIndicesImpl(
        removed_indices, indices_range, vids_, data_tuples_);
    vids_.swap(std::get<0>(vids_and_new_offset_range));
    data_tuples_.swap(std::get<1>(vids_and_new_offset_range));
    return std::get<2>(vids_and_new_offset_range);
  }

 private:
  std::vector<lid_t> vids_;
  LabelT v_label_;
  std::vector<data_tuple_t> data_tuples_;
  std::array<std::string, sizeof...(T)> prop_names_;
};

template <typename LabelT, typename VID_T>
class RowVertexSetImpl<LabelT, VID_T, grape::EmptyType> {
 public:
  using element_t = VID_T;
  using element_type = VID_T;
  using lid_t = VID_T;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T>;

  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  using flat_t = RowVertexSetImpl<LabelT, VID_T, grape::EmptyType>;
  using iterator = RowVertexSetIter<VID_T, grape::EmptyType>;
  using self_type_t = RowVertexSetImpl<LabelT, VID_T, grape::EmptyType>;
  using EntityValueType = VID_T;
  using builder_t = RowVertexSetImplBuilder<LabelT, VID_T, grape::EmptyType>;

  template <typename... T>
  using with_data_t = RowVertexSetImpl<LabelT, VID_T, T...>;

  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  static constexpr bool is_keyed = false;
  static constexpr bool is_vertex_set = true;
  static constexpr bool is_row_vertex_set = true;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_edge_set = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_general_set = false;
  static constexpr bool is_collection = false;

  static constexpr size_t num_props = 0;

  explicit RowVertexSetImpl(std::vector<lid_t>&& vids, LabelT v_label)
      : vids_(std::move(vids)), v_label_(v_label) {}

  RowVertexSetImpl(self_type_t&& other) noexcept
      : vids_(std::move(other.vids_)), v_label_(other.v_label_) {}

  RowVertexSetImpl(const self_type_t& other) noexcept
      : vids_(other.vids_), v_label_(other.v_label_) {}

  iterator begin() const { return iterator(vids_, 0); }

  iterator end() const { return iterator(vids_, vids_.size()); }

  size_t Size() const { return vids_.size(); }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    std::vector<lid_t> res_vec;
    res_vec.reserve(repeat_vec.back());
    for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (size_t j = 0; j < times_to_repeat; ++j) {
        for (size_t k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          res_vec.emplace_back(vids_[k]);
          // VLOG(10) << "j: "<<j<<":k"<<k <<",val:"<< vids_[k];
        }
      }
    }
    vids_.swap(res_vec);
  }

  builder_t CreateBuilder() const { return builder_t(v_label_); }

  // create a copy
  self_type_t CreateCopy() const {
    std::vector<lid_t> copied_lids = vids_;
    return self_type_t(std::move(copied_lids), v_label_);
  }

  const LabelT& GetLabel() const { return v_label_; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill res with v_label_
    res.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      res.emplace_back(v_label_);
    }
    return res;
  }

  const std::vector<lid_t>& GetVertices() const { return vids_; }
  std::vector<lid_t>& GetMutableVertices() { return vids_; }
  std::vector<lid_t>&& MoveVertices() { return std::move(vids_); }

  std::vector<size_t> GenerateKeys() const {
    std::vector<size_t> res;
    res.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      res.emplace_back(i);
    }
    return res;
  }

  // According to the given indices, filter in place.
  void SubSetWithIndices(std::vector<size_t>& indices) {
    auto vids = RowSetSubSetImpl(vids_, indices);
    vids_.swap(vids);
    VLOG(10) << "after subset: " << vids_.size();
  }

  auto WithIndices(std::vector<size_t>& indices) const {
    auto vids = RowSetSubSetImpl(vids_, indices);
    return self_type_t(std::move(vids), v_label_);
  }

  // all dedup are done inplace
  std::vector<offset_t> Dedup() {
    std::vector<lid_t> vids;
    auto offset = RowSetDedupImpl(vids_, vids);
    vids_.swap(vids);
    return offset;
  }

  // Filter current vertices with expression.
  template <typename EXPR>
  std::pair<self_type_t, std::vector<offset_t>> Filter(EXPR&& expr) {
    size_t cur = 0;
    std::vector<offset_t> offset;
    std::vector<lid_t> res_lids;
    offset.reserve(Size() + 1);
    for (auto iter : *this) {
      offset.emplace_back(cur);
      if (expr(iter.GetVertex())) {
        res_lids.emplace_back(iter.GetVertex());
        cur += 1;
      }
    }
    offset.emplace_back(cur);
    auto new_set = self_type_t(std::move(res_lids), v_label_);
    return std::make_pair(std::move(new_set), std::move(offset));
  }

  // Usually after
  template <size_t col_ind, typename... index_ele_tuple_t>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t...>>);
    auto res_vids = rowSetFlatImpl<col_ind>(index_ele_tuple, vids_);
    return self_type_t(std::move(res_vids), v_label_);
  }

  // size_t... Is denotes the ind of data array need to project.
  //-1 denote it self.
  template <int tag_id, int Fs, int... Is,
            typename std::enable_if<(sizeof...(Is) > 0)>::type* = nullptr,
            typename res_t = Collection<std::tuple<
                typename gs::tuple_element<Fs, std::tuple<lid_t>>::type,
                typename gs::tuple_element<Is, std::tuple<lid_t>>::type...>>>
  res_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                               KeyAlias<tag_id, Fs, Is...>& key_alias) const {
    auto res_vec =
        row_project_with_repeat_array_impl(key_alias, repeat_array, vids_);
    return res_t(std::move(res_vec));
  }

  template <
      int tag_id, int Fs, typename std::enable_if<Fs != -1>::type* = nullptr,
      typename res_t = Collection<
          std::tuple<typename gs::tuple_element<Fs, std::tuple<lid_t>>::type>>>
  res_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                               KeyAlias<tag_id, Fs>& key_alias) const {
    auto res_vec =
        row_project_with_repeat_array_impl(key_alias, repeat_array, vids_);
    return res_t(std::move(res_vec));
  }

  // project my self.
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<lid_t> vids;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        // VLOG(10) << "Project: " << vids_[i];
        vids.push_back(vids_[i]);
      }
    }
    return self_type_t(std::move(vids), v_label_);
  }

  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER,
            typename std::enable_if<
                (!std::is_same_v<EXPRESSION, TruePredicate>)>::type* = nullptr,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels,
                         EXPRESSION& exprs,
                         std::array<PROP_GETTER, 1>& prop_getter) const {
    // TODO: vector-based cols should be able to be selected with certain rows.

    auto new_lids_and_offsets =
        row_project_vertices_impl(vids_, v_label_, labels, exprs, prop_getter);
    self_type_t res_set(std::move(new_lids_and_offsets.first), v_label_);

    return std::make_pair(std::move(res_set),
                          std::move(new_lids_and_offsets.second));
  }

  // only project certain labels, without any expression.
  template <size_t num_labels, typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels) const {
    // TODO: vector-based cols should be able to be selected with certain rows.

    auto new_lids_datas_and_offset = select_labels(vids_, v_label_, labels);
    self_type_t res_set(std::move(new_lids_datas_and_offset.first), v_label_);

    return std::make_pair(std::move(res_set),
                          std::move(new_lids_datas_and_offset.second));
  }

  std::vector<offset_t> FilterWithIndices(std::vector<size_t>& offset,
                                          JoinKind join_kind) {
    auto pair = row_filter_with_indices_impl(offset, vids_, join_kind);
    vids_.swap(pair.first);
    return pair.second;
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names,
                        const std::vector<offset_t>& repeat_array) const {
    VLOG(10) << "Skip filling built-in props for empty prop row vertex set";
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names) const {
    VLOG(10) << "Skip filling built-in props for empty prop row vertex set";
  }

  // In places
  template <typename... Ts>
  RowVertexSetImpl<LabelT, VID_T, Ts...> WithNewData(
      std::vector<std::tuple<Ts...>>&& new_datas) {
    CHECK(vids_.size() == new_datas.size());
    return RowVertexSetImpl<LabelT, VID_T, Ts...>(std::move(vids_), v_label_,
                                                  std::move(new_datas));
  }

  // Removed_indices is not with respect to current set's indices.
  // It refer to the indices_range's index.
  // removed = [1]
  // indices_range = [0, 3, 5, 8]
  // Then we should remove eles in [3,5)
  // indices became
  // [0, 3, 6],
  // num _elements 8 -> 6
  // return the new offset range
  std::vector<offset_t> SubSetWithRemovedIndices(
      std::vector<size_t>& removed_indices,
      std::vector<size_t>& indices_range) {
    auto vids_and_new_offset_range =
        subSetWithRemovedIndicesImpl(removed_indices, indices_range, vids_);
    vids_.swap(vids_and_new_offset_range.first);
    return vids_and_new_offset_range.second;
  }

 private:
  std::vector<lid_t> vids_;
  LabelT v_label_;
};

// VID_T can be std::optional<VID_T> or VID_T directly.
template <typename LabelT, typename VID_T, typename... T>
using RowVertexSet = RowVertexSetImpl<LabelT, VID_T, T...>;

template <typename LabelT, typename VID_T>
using DefaultRowVertexSet = RowVertexSet<LabelT, VID_T, grape::EmptyType>;

template <typename LabelT, typename VID_T, typename... T>
auto make_row_vertex_set(std::vector<VID_T>&& lids, LabelT label,
                         std::vector<std::tuple<T...>>&& data_tuples,
                         std::array<std::string, sizeof...(T)>&& prop_strs) {
  return RowVertexSet<LabelT, VID_T, T...>(
      std::move(lids), label, std::move(data_tuples), std::move(prop_strs));
}

template <typename VID_T, typename LabelT>
auto make_default_row_vertex_set(std::vector<VID_T>&& lids, LabelT label) {
  return DefaultRowVertexSet<LabelT, VID_T>(std::move(lids), label);
}

}  // namespace gs

#endif  // ENGINES_HQPS_DS_MULTI_VERTEX_SET_ROW_VERTEX_SET_H_
