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
#ifndef ENGINES_HQPS_DS_MULTI_VERTEX_SET_KEYED_ROW_VERTEX_SET_H_
#define ENGINES_HQPS_DS_MULTI_VERTEX_SET_KEYED_ROW_VERTEX_SET_H_

#include "glog/logging.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "grape/util.h"

namespace gs {

//////////////////////////VertexIter///////////////////////

template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
class KeyedRowVertexSetBuilderImpl;

// 0. Keyed Vector-base vertex set iterator.
template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
class KeyedRowVertexSetIter {
 public:
  using key_t = KEY_T;
  using lid_t = VID_T;
  using data_tuple_t = typename std::tuple<T...>;
  using self_type_t = KeyedRowVertexSetIter<LabelT, KEY_T, VID_T, T...>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T, std::tuple<T...>>;

  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  KeyedRowVertexSetIter(const std::vector<KEY_T>& keys,
                        const std::vector<lid_t>& vids,
                        const std::vector<data_tuple_t>& datas, LabelT v_label,
                        size_t ind)
      : keys_(keys), vids_(vids), datas_(datas), v_label_(v_label), ind_(ind) {}

  KeyedRowVertexSetIter(const self_type_t& other)
      : keys_(other.keys_),
        vids_(other.vids_),
        datas_(other.datas_),
        v_label_(other.v_label_),
        ind_(other.ind_) {}
  ~KeyedRowVertexSetIter() {}

  lid_t GetElement() const { return vids_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, vids_[ind_]);
  }

  flat_ele_tuple_t GetFlatElement() const {
    return std::make_tuple(ind_, vids_[ind_], GetData());
  }

  key_t GetKey() const { return keys_[ind_]; }

  lid_t GetVertex() const { return vids_[ind_]; }

  data_tuple_t GetData() const { return datas_[ind_]; }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
  }

  inline self_type_t operator++(int) {
    self_type_t ret(*this);
    ++ind_;
    return ret;
  }

  inline self_type_t& operator=(const self_type_t& rhs) {
    if (*this == rhs)
      return *this;
    ind_ == rhs.ind_;
    vids_ = rhs.vids_;
    keys_ = rhs.keys_;
    datas_ = rhs.datas_;
    return *this;
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

 protected:
  const std::vector<key_t>& keys_;
  const std::vector<lid_t>& vids_;
  const std::vector<data_tuple_t>& datas_;
  LabelT v_label_;
  size_t ind_;  // index for keys_.
};

//////////////////////////VertexIter///////////////////////

// 0. Keyed Vector-base vertex set iterator.
template <typename LabelT, typename KEY_T, typename VID_T>
class KeyedRowVertexSetIter<LabelT, KEY_T, VID_T, grape::EmptyType> {
 public:
  using key_t = KEY_T;
  using lid_t = VID_T;
  using data_tuple_t = typename std::tuple<grape::EmptyType>;
  using self_type_t =
      KeyedRowVertexSetIter<LabelT, KEY_T, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;

  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T>;

  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  KeyedRowVertexSetIter(const std::vector<KEY_T>& keys,
                        const std::vector<lid_t>& vids, LabelT v_label,
                        size_t ind)
      : keys_(keys), vids_(vids), v_label_(v_label), ind_(ind) {}

  KeyedRowVertexSetIter(const self_type_t& other)
      : keys_(other.keys_),
        vids_(other.vids_),
        v_label_(other.v_label_),
        ind_(other.ind_) {}
  ~KeyedRowVertexSetIter() {}

  lid_t GetElement() const { return vids_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, vids_[ind_]);
  }

  flat_ele_tuple_t GetFlatElement() const {
    return std::make_tuple(ind_, vids_[ind_]);
  }

  key_t GetKey() const { return keys_[ind_]; }

  lid_t GetVertex() const { return vids_[ind_]; }

  data_tuple_t GetData() const { return std::make_tuple(grape::EmptyType()); }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
  }

  inline self_type_t operator++(int) {
    self_type_t ret(*this);
    ++ind_;
    return ret;
  }

  inline self_type_t& operator=(const self_type_t& rhs) {
    if (*this == rhs)
      return *this;
    ind_ == rhs.ind_;
    vids_ = rhs.vids_;
    keys_ = rhs.keys_;
    return *this;
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

 protected:
  const std::vector<key_t>& keys_;
  const std::vector<lid_t>& vids_;
  LabelT v_label_;
  size_t ind_;  // index for keys_.
};

template <size_t col_ind, typename... index_ele_tuple_t, typename key_t,
          typename lid_t, typename data_tuple_t>
std::tuple<std::vector<key_t>, std::vector<lid_t>, std::vector<data_tuple_t>>
keyedRowFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    std::vector<key_t>& origin_keys, std::vector<lid_t>& origin_vids,
    std::vector<data_tuple_t>& origin_datas) {
  std::vector<lid_t> res_vids;
  std::vector<key_t> res_keys;
  std::vector<data_tuple_t> res_datas;
  res_vids.reserve(index_ele_tuples.size());
  res_keys.reserve(index_ele_tuples.size());
  res_datas.reserve(index_ele_tuples.size());
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto& ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());
    res_vids.emplace_back(origin_vids[ind]);
    res_keys.emplace_back(origin_keys[ind]);
    res_datas.emplace_back(origin_datas[ind]);
  }
  return std::make_tuple(res_keys, res_vids, res_datas);
}

template <size_t col_ind, typename... index_ele_tuple_t, typename key_t,
          typename lid_t>
std::tuple<std::vector<key_t>, std::vector<lid_t>> keyedRowFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    std::vector<key_t>& origin_keys, std::vector<lid_t>& origin_vids) {
  std::vector<lid_t> res_vids;
  std::vector<key_t> res_keys;
  res_vids.reserve(index_ele_tuples.size());
  res_keys.reserve(index_ele_tuples.size());
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto& ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());
    res_vids.emplace_back(origin_vids[ind]);
    res_keys.emplace_back(origin_keys[ind]);
  }
  return std::make_tuple(res_keys, res_vids);
}

template <typename lid_t, typename key_t, typename data_tuple_t>
std::tuple<std::vector<lid_t>, std::vector<key_t>, std::vector<data_tuple_t>,
           std::vector<offset_t>>
keyed_row_filter_with_indices_impl(std::vector<size_t>& indices,
                                   std::vector<lid_t>& vids,
                                   std::vector<key_t>& keys,
                                   std::vector<data_tuple_t>& datas,
                                   JoinKind join_kind) {
  std::vector<offset_t> res_offset;
  std::vector<lid_t> res_vids;
  std::vector<key_t> res_keys;
  std::vector<data_tuple_t> res_datas;

  res_offset.reserve(vids.size() + 1);

  size_t indices_ind = 0;
  if (join_kind == JoinKind::InnerJoin) {
    res_vids.reserve(indices.size());
    res_keys.reserve(indices.size());
    res_datas.reserve(indices.size());
    size_t vid_ind = 0;
    res_offset.emplace_back(res_vids.size());
    for (; vid_ind < vids.size(); ++vid_ind) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind++;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] == vid_ind) {
          res_vids.emplace_back(vids[vid_ind]);
          res_keys.emplace_back(keys[vid_ind]);
          res_datas.emplace_back(datas[vid_ind]);
        }
      }
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == indices.size());
  } else {
    res_vids.reserve(vids.size() - indices.size());
    res_keys.reserve(vids.size() - indices.size());
    res_datas.reserve(vids.size() - indices.size());
    size_t vid_ind = 0;
    res_offset.emplace_back(res_vids.size());
    while (vid_ind < vids.size()) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind += 1;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] != vid_ind) {
          res_vids.emplace_back(vids[vid_ind]);
          res_keys.emplace_back(keys[vid_ind]);
          res_datas.emplace_back(datas[vid_ind]);
        }
      } else {
        res_vids.emplace_back(vids[vid_ind]);
        res_keys.emplace_back(keys[vid_ind]);
        res_datas.emplace_back(datas[vid_ind]);
      }
      vid_ind += 1;
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == vids.size() - indices.size());
  }
  CHECK(res_offset.size() == vids.size() + 1);
  VLOG(10) << "res offset: " << gs::to_string(res_offset);
  VLOG(10) << "res vids: " << gs::to_string(res_vids);
  VLOG(10) << "res keys: " << gs::to_string(res_keys);
  return std::make_tuple(std::move(res_vids), std::move(res_keys),
                         std::move(res_datas), std::move(res_offset));
}
template <typename lid_t, typename key_t>
std::tuple<std::vector<lid_t>, std::vector<key_t>, std::vector<offset_t>>
keyed_row_filter_with_indices_impl(std::vector<size_t>& indices,
                                   std::vector<lid_t>& vids,
                                   std::vector<key_t>& keys,
                                   JoinKind join_kind) {
  std::vector<offset_t> res_offset;
  std::vector<lid_t> res_vids;
  std::vector<key_t> res_keys;

  res_offset.reserve(vids.size() + 1);

  size_t indices_ind = 0;
  if (join_kind == JoinKind::InnerJoin) {
    res_vids.reserve(indices.size());
    res_keys.reserve(indices.size());
    size_t vid_ind = 0;
    res_offset.emplace_back(res_vids.size());
    for (; vid_ind < vids.size(); ++vid_ind) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind++;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] == vid_ind) {
          res_vids.emplace_back(vids[vid_ind]);
          res_keys.emplace_back(keys[vid_ind]);
        }
      }
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == indices.size());
  } else {
    res_vids.reserve(vids.size() - indices.size());
    res_keys.reserve(vids.size() - indices.size());

    size_t vid_ind = 0;
    res_offset.emplace_back(res_vids.size());
    while (vid_ind < vids.size()) {
      while (indices_ind < indices.size() && indices[indices_ind] < vid_ind) {
        indices_ind += 1;
      }
      if (indices_ind < indices.size()) {
        if (indices[indices_ind] != vid_ind) {
          res_vids.emplace_back(vids[vid_ind]);
          res_keys.emplace_back(keys[vid_ind]);
        }
      } else {
        res_vids.emplace_back(vids[vid_ind]);
        res_keys.emplace_back(keys[vid_ind]);
      }
      vid_ind += 1;
      res_offset.emplace_back(res_vids.size());
    }
    CHECK(res_vids.size() == vids.size() - indices.size());
  }
  CHECK(res_offset.size() == vids.size() + 1);
  VLOG(10) << "res offset: " << gs::to_string(res_offset);
  VLOG(10) << "res vids: " << gs::to_string(res_vids);
  VLOG(10) << "res keys: " << gs::to_string(res_keys);
  return std::make_tuple(std::move(res_vids), std::move(res_keys),
                         std::move(res_offset));
}

template <typename key_t, typename lid_t, typename EXPRESSION,
          size_t num_labels, typename LabelT, typename data_tuple_t,
          typename ELE_TUPLE,
          typename RES_T =
              std::tuple<std::vector<key_t>, std::vector<lid_t>,
                         std::vector<data_tuple_t>, std::vector<offset_t>>>
RES_T keyed_row_project_vertices_impl(
    const std::vector<key_t>& keys, const std::vector<lid_t>& lids,
    const std::vector<data_tuple_t>& datas, LabelT cur_label,
    std::array<LabelT, num_labels>& labels, EXPRESSION& expr,
    std::vector<ELE_TUPLE>& eles) {  // temporary property
  // TODO: vector-based cols should be able to be selected with certain rows.
  std::vector<offset_t> offsets;
  std::vector<key_t> new_keys;
  std::vector<lid_t> new_lids;
  std::vector<data_tuple_t> new_datas;
  size_t cnt = 0;
  offsets.reserve(lids.size() + 1);
  int label_ind = -1;
  if constexpr (num_labels == 0) {
    label_ind = 0;  // neq -1
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
    for (size_t i = 0; i < lids.size(); ++i) {
      offsets.emplace_back(cnt);
      if (expr(eles[i])) {
        new_keys.emplace_back(keys[i]);
        new_lids.emplace_back(lids[i]);
        new_datas.emplace_back(datas[i]);
        cnt += 1;
      }
    }
    offsets.emplace_back(cnt);
  }
  return std::make_tuple(std::move(new_keys), std::move(new_lids),
                         std::move(new_datas), std::move(offsets));
}

template <typename key_t, typename lid_t, typename EXPRESSION,
          size_t num_labels, typename LabelT, typename ELE_TUPLE,
          typename RES_T = std::tuple<std::vector<key_t>, std::vector<lid_t>,
                                      std::vector<offset_t>>>
RES_T keyed_row_project_vertices_impl(
    const std::vector<key_t>& keys, const std::vector<lid_t>& lids,
    LabelT cur_label, std::array<LabelT, num_labels>& labels, EXPRESSION& expr,
    std::vector<ELE_TUPLE>& eles) {  // temporary property
  // TODO: vector-based cols should be able to be selected with certain rows.
  std::vector<offset_t> offsets;
  std::vector<key_t> new_keys;
  std::vector<lid_t> new_lids;
  size_t cnt = 0;
  offsets.reserve(lids.size() + 1);
  int label_ind = -1;
  if constexpr (num_labels == 0) {
    label_ind = 0;  // neq -1
  } else {
    // FIXME: no repeated labels.
    for (size_t i = 0; i < num_labels; ++i) {
      if (cur_label == labels[i])
        label_ind = i;
    }
  }
  // FIXME: no repeated labels.
  for (size_t i = 0; i < num_labels; ++i) {
    if (cur_label == labels[i])
      label_ind = i;
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
    for (size_t i = 0; i < lids.size(); ++i) {
      offsets.emplace_back(cnt);
      if (expr(eles[i])) {
        new_keys.emplace_back(keys[i]);
        new_lids.emplace_back(lids[i]);
        cnt += 1;
      }
    }
    offsets.emplace_back(cnt);
  }
  return std::make_tuple(std::move(new_keys), std::move(new_lids),
                         std::move(offsets));
}

/////////////////Keyed vertex set impl////////////////////
template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
class KeyedRowVertexSetImpl {
 public:
  using key_t = KEY_T;
  using self_type_t = KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>;
  using lid_t = VID_T;
  using data_tuple_t = std::tuple<T...>;
  using flat_t = self_type_t;

  using iterator = KeyedRowVertexSetIter<LabelT, KEY_T, VID_T, T...>;
  using filtered_vertex_set = self_type_t;
  using ground_vertex_set_t = RowVertexSet<LabelT, VID_T, T...>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T, std::tuple<T...>>;
  using EntityValueType = VID_T;

  template <typename... Ts>
  using with_data_t = KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T..., Ts...>;

  using builder_t = RowVertexSetBuilder<LabelT, VID_T, T...>;

  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  static constexpr bool is_keyed = true;
  static constexpr bool is_vertex_set = true;
  static constexpr bool is_edge_set = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_general_set = false;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_row_vertex_set = true;

  explicit KeyedRowVertexSetImpl(std::vector<key_t>&& keys,
                                 std::vector<lid_t>&& vids,
                                 std::vector<data_tuple_t>&& datas,
                                 LabelT v_label,
                                 std::array<std::string, sizeof...(T)> names)
      : keys_(std::move(keys)),
        vids_(std::move(vids)),
        datas_(std::move(datas)),
        v_label_(v_label),
        prop_names_(names) {
    // check_col_len();
  }

  KeyedRowVertexSetImpl(self_type_t&& other) noexcept
      : keys_(std::move(other.keys_)),
        vids_(std::move(other.vids_)),
        datas_(std::move(other.datas_)),
        v_label_(other.v_label_),
        prop_names_(other.prop_names_) {}

  KeyedRowVertexSetImpl(const self_type_t& other) noexcept
      : keys_(other.keys_),
        vids_(other.vids_),
        datas_(other.datas_),
        v_label_(other.v_label_),
        prop_names_(other.prop_names_) {}

  builder_t CreateBuilder() const { return builder_t(v_label_, prop_names_); }

  iterator begin() const { return iterator(keys_, vids_, datas_, v_label_, 0); }

  iterator end() const {
    return iterator(keys_, vids_, datas_, v_label_, keys_.size());
  }

  const std::vector<data_tuple_t>& GetDataVec() const { return datas_; }

  size_t Size() const { return keys_.size(); }

  LabelT GetLabel() const { return v_label_; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill res with vertex labels
    res.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      res.emplace_back(v_label_);
    }
    return res;
  }

  const std::array<std::string, sizeof...(T)>& GetPropNames() const {
    return prop_names_;
  }

  const std::vector<lid_t>& GetVertices() const { return vids_; }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

  // Unwrap keyed vertex set to unkeyed vertex set.
  ground_vertex_set_t ToGround(JoinKind&& join_kind) {
    std::vector<lid_t> vec;
    std::vector<data_tuple_t> datas;
    if (join_kind == JoinKind::InnerJoin) {
      vec.reserve(keys_.size());
      datas.reserve(keys_.size());
      for (auto ind : keys_) {
        vec.push_back(vids_[ind]);
        datas.push_back(datas_[ind]);
        // For cols check if it need to apply keys.
      }
    } else {  // anti join.
      vec.reserve(vids_.size() - keys_.size());
      datas.reserve(vids_.size() - keys_.size());
      size_t key_ind = 0;
      size_t vid_ind = 0;
      while (vid_ind < vids_.size() && key_ind < keys_.size()) {
        if (vid_ind < keys_[key_ind]) {
          vec.push_back(vids_[vid_ind]);
          datas.push_back(datas_[vid_ind]);
        } else {
          while (key_ind < keys_.size() && vid_ind >= keys_[key_ind]) {
            key_ind += 1;
          }
        }
        vid_ind += 1;
      }
      while (vid_ind < vids_.size()) {
        vec.push_back(vids_[vid_ind]);
        datas.push_back(datas_[vid_ind]);
        vid_ind += 1;
      }
    }
    return ground_vertex_set_t(std::move(vec), v_label_, std::move(datas),
                               std::move(prop_names_));
  }

  template <size_t col_ind, typename... index_ele_tuple_t>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t...>>);
    auto res_keys_vids =
        keyedRowFlatImpl<col_ind>(index_ele_tuple, keys_, vids_, datas_);
    return self_type_t(std::move(std::get<0>(res_keys_vids)),
                       std::move(std::get<1>(res_keys_vids)),
                       std::move(std::get<2>(res_keys_vids)), v_label_,
                       std::move(prop_names_));
  }

  std::vector<offset_t> FilterWithIndices(std::vector<size_t>& indices,
                                          JoinKind join_kind) {
    auto tuple = keyed_row_filter_with_indices_impl(indices, vids_, keys_,
                                                    datas_, join_kind);
    vids_.swap(std::get<0>(tuple));
    keys_.swap(std::get<1>(tuple));
    datas_.swap(std::get<2>(tuple));
    return std::get<3>(tuple);
  }

  template <typename EXPRESSION, size_t num_labels, typename ELE_TUPLE,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, std::vector<ELE_TUPLE>& eles) const {
    // TODO: vector-based cols should be able to be selected with certain rows.

    auto new_lids_datas_and_offset = keyed_row_project_vertices_impl(
        keys_, vids_, datas_, v_label_, labels, expr, eles);
    self_type_t res_set(std::move(std::get<0>(new_lids_datas_and_offset)),
                        std::move(std::get<1>(new_lids_datas_and_offset)),
                        std::move(std::get<2>(new_lids_datas_and_offset)),
                        v_label_, std::move(prop_names_));

    return std::make_pair(std::move(res_set),
                          std::move(std::get<3>(new_lids_datas_and_offset)));
  }

  // projectwithRepeatArray, projecting myself
  template <int tag_id, int Fs, typename std::enable_if_t<Fs == -1>* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<offset_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) {
    std::vector<key_t> new_keys;
    std::vector<lid_t> new_vids;
    std::vector<data_tuple_t> new_datas;

    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        new_keys.push_back(keys_[i]);
        new_vids.push_back(vids_[i]);
        new_datas.push_back(datas_[i]);
      }
    }
    return self_type_t(std::move(new_keys), std::move(new_vids),
                       std::move(new_datas), v_label_, std::move(prop_names_));
  }

  template <typename... Ts>
  KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, Ts...> WithNewData(
      std::vector<std::tuple<Ts...>>&& new_datas) {
    CHECK(vids_.size() == new_datas.size());
    return KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, Ts...>(
        std::move(keys_), std::move(vids_), std::move(new_datas), v_label_,
        std::move(prop_names_));
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names) {
    std::vector<offset_t> repeat_array(vids_.size(), 1);
    fillBuiltinPropsImpl(datas_, prop_names_, tuples, prop_names, repeat_array);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names,
                        const std::vector<offset_t>& repeat_array) {
    fillBuiltinPropsImpl(datas_, prop_names_, tuples, prop_names, repeat_array);
  }

 protected:
  std::vector<key_t> keys_;
  std::vector<lid_t> vids_;
  std::vector<data_tuple_t> datas_;
  LabelT v_label_;
  std::array<std::string, sizeof...(T)> prop_names_;
};

////////////////////////////////////

template <typename LabelT, typename KEY_T, typename VID_T>
class KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, grape::EmptyType> {
 public:
  using key_t = KEY_T;
  using self_type_t =
      KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, grape::EmptyType>;
  using lid_t = VID_T;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  using flat_t = self_type_t;

  using iterator =
      KeyedRowVertexSetIter<LabelT, KEY_T, VID_T, grape::EmptyType>;
  using filtered_vertex_set = self_type_t;
  using ground_vertex_set_t = RowVertexSet<LabelT, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  // from this tuple, we can reconstruct the partial set.
  using flat_ele_tuple_t = std::tuple<size_t, VID_T>;
  using EntityValueType = VID_T;

  template <typename... Ts>
  using with_data_t = KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, Ts...>;

  using builder_t = RowVertexSetBuilder<LabelT, VID_T, grape::EmptyType>;

  static constexpr VID_T NULL_VID = std::numeric_limits<VID_T>::max();

  static constexpr bool is_keyed = true;
  static constexpr bool is_vertex_set = true;
  static constexpr bool is_edge_set = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_general_set = false;
  static constexpr bool is_two_label_set = false;

  explicit KeyedRowVertexSetImpl(std::vector<key_t>&& keys,
                                 std::vector<lid_t>&& vids, LabelT v_label)
      : keys_(std::move(keys)), vids_(std::move(vids)), v_label_(v_label) {
    // check_col_len();
  }

  KeyedRowVertexSetImpl(self_type_t&& other) noexcept
      : keys_(std::move(other.keys_)),
        vids_(std::move(other.vids_)),
        v_label_(other.v_label_) {}

  KeyedRowVertexSetImpl(const self_type_t& other) noexcept
      : keys_(other.keys_), vids_(other.vids_), v_label_(other.v_label_) {}

  iterator begin() const { return iterator(keys_, vids_, v_label_, 0); }

  iterator end() const {
    return iterator(keys_, vids_, v_label_, keys_.size());
  }

  size_t Size() const { return keys_.size(); }

  LabelT GetLabel() const { return v_label_; }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    // fill res with vertex labels
    res.reserve(vids_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      res.emplace_back(v_label_);
    }
    return res;
  }

  const std::vector<lid_t>& GetVertices() const { return vids_; }

  builder_t CreateBuilder() const { return builder_t(v_label_); }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

  // Unwrap keyed vertex set to unkeyed vertex set.
  ground_vertex_set_t ToGround(JoinKind&& join_kind) {
    std::vector<lid_t> vec;
    if (join_kind == JoinKind::InnerJoin) {
      vec.reserve(keys_.size());
      for (auto ind : keys_) {
        vec.push_back(vids_[ind]);
        // For cols check if it need to apply keys.
      }
    } else {  // anti join.
      vec.reserve(vids_.size() - keys_.size());
      size_t key_ind = 0;
      size_t vid_ind = 0;
      while (vid_ind < vids_.size() && key_ind < keys_.size()) {
        if (vid_ind < keys_[key_ind]) {
          vec.push_back(vids_[vid_ind]);
        } else {
          while (key_ind < keys_.size() && vid_ind >= keys_[key_ind]) {
            key_ind += 1;
          }
        }
        vid_ind += 1;
      }
      while (vid_ind < vids_.size()) {
        vec.push_back(vids_[vid_ind]);
        vid_ind += 1;
      }
    }
    return ground_vertex_set_t(std::move(vec), v_label_);
  }

  template <size_t col_ind, typename... index_ele_tuple_t>
  flat_t Flat(std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuple) {
    static_assert(col_ind <
                  std::tuple_size_v<std::tuple<index_ele_tuple_t...>>);
    auto res_keys_vids =
        keyedRowFlatImpl<col_ind>(index_ele_tuple, keys_, vids_);
    return self_type_t(std::move(std::get<0>(res_keys_vids)),
                       std::move(std::get<1>(res_keys_vids)), v_label_);
  }

  std::vector<offset_t> FilterWithIndices(std::vector<size_t>& indices,
                                          JoinKind join_kind) {
    auto tuple =
        keyed_row_filter_with_indices_impl(indices, vids_, keys_, join_kind);
    vids_.swap(std::get<0>(tuple));
    keys_.swap(std::get<1>(tuple));
    return std::get<2>(tuple);
  }

  template <typename EXPRESSION, size_t num_labels, typename ELE_TUPLE,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& labels,
                         EXPRESSION& expr, std::vector<ELE_TUPLE>& eles) const {
    // TODO: vector-based cols should be able to be selected with certain rows.

    auto new_lids_datas_and_offset = keyed_row_project_vertices_impl(
        keys_, vids_, v_label_, labels, expr, eles);
    self_type_t res_set(std::move(std::get<0>(new_lids_datas_and_offset)),
                        std::move(std::get<1>(new_lids_datas_and_offset)),
                        v_label_);

    return std::make_pair(std::move(res_set),
                          std::move(std::get<2>(new_lids_datas_and_offset)));
  }

  // projectwithRepeatArray, projecting myself
  template <int tag_id, int Fs, typename std::enable_if_t<Fs == -1>* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<offset_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) {
    std::vector<key_t> new_keys;
    std::vector<lid_t> new_vids;

    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        new_keys.push_back(keys_[i]);
        new_vids.push_back(vids_[i]);
      }
    }
    return self_type_t(std::move(new_keys), std::move(new_vids), v_label_);
  }

  template <typename... Ts>
  KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, Ts...> WithNewData(
      std::vector<std::tuple<Ts...>>&& new_datas) {
    CHECK(vids_.size() == new_datas.size());
    return KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, Ts...>(
        std::move(keys_), std::move(vids_), std::move(new_datas), v_label_);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names,
                        const std::vector<offset_t>& repeat_array) const {
    LOG(WARNING) << "not implemented";
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names) const {
    LOG(WARNING) << "not implemented";
  }

 protected:
  std::vector<key_t> keys_;
  std::vector<lid_t> vids_;
  LabelT v_label_;
};

template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
using KeyedRowVertexSet = KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>;

template <typename LabelT, typename VID_T>
using DefaultKeyedRowVertexSet =
    KeyedRowVertexSetImpl<LabelT, VID_T, VID_T, EmptyCol>;

template <typename LabelT, typename key_t, typename VID_T, typename... T>
auto MakeKeyedRowVertexSet(std::vector<key_t>&& keys, std::vector<VID_T>&& vec,
                           std::vector<std::tuple<T...>>&& datas,
                           LabelT label) {
  return KeyedRowVertexSet<LabelT, key_t, VID_T, T...>(
      std::move(keys), std::move(vec), std::move(datas), label);
}

//////////////////////////////KeyedRowVertexSetBuilder///////////////////////

template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
class KeyedRowVertexSetBuilderImpl {
 public:
  using key_t = KEY_T;
  using lid_t = VID_T;
  using data_tuple_t = std::tuple<T...>;
  using build_res_t = KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>;

  KeyedRowVertexSetBuilderImpl(LabelT label,
                               std::array<std::string, sizeof...(T)> prop_names)
      : label_(label), prop_names_(prop_names), ind_(0) {}

  KeyedRowVertexSetBuilderImpl(const RowVertexSet<LabelT, VID_T, T...>& old_set)
      : label_(old_set.GetLabel()),
        prop_names_(old_set.GetPropNames()),
        ind_(0) {}

  KeyedRowVertexSetBuilderImpl(
      const KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>& old_set)
      : label_(old_set.GetLabel()),
        prop_names_(old_set.GetPropNames()),
        ind_(0) {}

  size_t insert(std::tuple<size_t, VID_T> ele_tuple, data_tuple_t data_tuple) {
    auto key = std::get<1>(ele_tuple);
    if (prop2ind_.find(key) != prop2ind_.end()) {
      return prop2ind_[key];
    } else {
      prop2ind_[key] = ind_;
      keys_.emplace_back(key);
      vids_.emplace_back(key);
      datas_.emplace_back(data_tuple);
      return ind_++;
    }
  }

  size_t insert(const VID_T& key, data_tuple_t data_tuple) {
    if (prop2ind_.find(key) != prop2ind_.end()) {
      return prop2ind_[key];
    } else {
      prop2ind_[key] = ind_;
      keys_.emplace_back(key);
      vids_.emplace_back(key);
      datas_.emplace_back(data_tuple);
      return ind_++;
    }
  }

  size_t insert(const std::tuple<VID_T, data_tuple_t>& ele_tuple) {
    return insert(std::get<0>(ele_tuple), std::get<1>(ele_tuple));
  }

  build_res_t Build() {
    return build_res_t(std::move(keys_), std::move(vids_), std::move(datas_),
                       label_, std::move(prop_names_));
  }

 private:
  LabelT label_;
  // Keep the mapping from lid to ind. So we can directly make the lids
  // array when building.
  std::unordered_map<key_t, size_t> prop2ind_;
  std::vector<key_t> keys_;
  std::vector<lid_t> vids_;
  std::vector<data_tuple_t> datas_;
  size_t ind_;
  std::array<std::string, sizeof...(T)> prop_names_;
};

template <typename LabelT, typename KEY_T, typename VID_T>
class KeyedRowVertexSetBuilderImpl<LabelT, KEY_T, VID_T, grape::EmptyType> {
 public:
  using key_t = KEY_T;
  using lid_t = VID_T;
  using build_res_t =
      KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, grape::EmptyType>;

  KeyedRowVertexSetBuilderImpl(LabelT label) : label_(label), ind_(0) {}

  KeyedRowVertexSetBuilderImpl(
      const RowVertexSet<LabelT, VID_T, grape::EmptyType>& old_set)
      : label_(old_set.GetLabel()), ind_(0) {}

  size_t insert(std::tuple<size_t, VID_T> ele_tuple) {
    auto key = std::get<1>(ele_tuple);
    if (prop2ind_.find(key) != prop2ind_.end()) {
      return prop2ind_[key];
    } else {
      prop2ind_[key] = ind_;
      keys_.emplace_back(key);
      vids_.emplace_back(key);
      return ind_++;
    }
  }

  size_t insert(const VID_T& key) {
    if (prop2ind_.find(key) != prop2ind_.end()) {
      return prop2ind_[key];
    } else {
      prop2ind_[key] = ind_;
      keys_.emplace_back(key);
      vids_.emplace_back(key);
      return ind_++;
    }
  }

  build_res_t Build() {
    return build_res_t(std::move(keys_), std::move(vids_), label_);
  }

 private:
  LabelT label_;
  // Keep the mapping from lid to ind. So we can directly make the lids
  // array when building.
  std::unordered_map<key_t, size_t> prop2ind_;
  std::vector<key_t> keys_;
  std::vector<lid_t> vids_;
  size_t ind_;
};

template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
using KeyedRowVertexSetBuilder =
    KeyedRowVertexSetBuilderImpl<LabelT, KEY_T, VID_T, T...>;

}  // namespace gs

#endif  // ENGINES_HQPS_DS_MULTI_VERTEX_SET_KEYED_ROW_VERTEX_SET_H_
