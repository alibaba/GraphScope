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

#include "flex/engines/hqps_db/core/utils/hqps_type.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

namespace gs {

// return the old labels, that are active in filter.
template <typename VID_T, typename LabelT, typename EXPR, typename PROP_GETTER,
          size_t old_num_labels, size_t filter_num_labels>
auto general_project_vertices_impl(
    const std::vector<VID_T>& old_vec,
    const std::array<grape::Bitset, old_num_labels>& old_bit_sets,
    const std::array<LabelT, old_num_labels>& old_labels,
    const std::array<LabelT, filter_num_labels>& filter_labels,
    const EXPR& expr,
    const std::array<PROP_GETTER, old_num_labels>& prop_getters) {
  std::vector<VID_T> res_vec;
  std::array<grape::Bitset, old_num_labels> res_bitsets;
  // reserve enough size for bitset.
  for (auto i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].init(old_vec.size());
  }
  std::vector<size_t> select_label_id;
  if constexpr (filter_num_labels == 0) {
    for (auto i = 0; i < old_labels.size(); ++i) {
      select_label_id.emplace_back(i);
    }
  } else {
    std::unordered_set<LabelT> set;
    for (auto l : filter_labels) {
      set.insert(l);
    }
    for (auto i = 0; i < old_labels.size(); ++i) {
      if (set.find(old_labels[i]) != set.end()) {
        select_label_id.emplace_back(i);
      }
    }
  }
  VLOG(10) << "selected label ids: " << gs::to_string(select_label_id)
           << ", out of size: " << old_labels.size();
  std::vector<offset_t> offset;

  offset.emplace_back(0);
  for (auto i = 0; i < old_vec.size(); ++i) {
    for (auto label_id : select_label_id) {
      if (old_bit_sets[label_id].get_bit(i)) {
        auto eles = prop_getters[label_id].get_view(old_vec[i]);
        if (expr(eles)) {
          res_bitsets[label_id].set_bit(res_vec.size());
          res_vec.push_back(old_vec[i]);
          break;
        }
      }
    }
    offset.emplace_back(res_vec.size());
  }
  for (auto i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (auto j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  // resize bitset.
  for (auto i = 0; i < old_num_labels; ++i) {
    res_bitsets[i].resize(res_vec.size());
  }
  for (auto i = 0; i < res_vec.size(); ++i) {
    bool flag = false;
    for (auto j = 0; j < old_num_labels; ++j) {
      flag |= res_bitsets[j].get_bit(i);
    }
    CHECK(flag) << "check fail at ind: " << i;
  }
  return std::make_tuple(std::move(res_vec), std::move(res_bitsets),
                         std::move(offset));
}

template <int tag_id, int res_id, int... Is, typename lid_t>
auto general_project_with_repeat_array_impl(
    const KeyAlias<tag_id, res_id, Is...>& key_alias,
    const std::vector<size_t>& repeat_array,
    const std::vector<lid_t>& old_lids) {
  using res_t = std::vector<
      std::tuple<typename gs::tuple_element<Is, std::tuple<lid_t>>::type...>>;

  res_t res_vec;
  for (auto i = 0; i < repeat_array.size(); ++i) {
    for (auto j = 0; j < repeat_array[i]; ++j) {
      auto tuple = std::make_tuple(old_lids[i]);
      res_vec.emplace_back(std::make_tuple(gs::get_from_tuple<Is>(tuple)...));
    }
  }
  return res_vec;
}

template <size_t col_ind, typename... index_ele_tuple_t, typename lid_t,
          size_t N>
auto generalSetFlatImpl(
    std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples,
    const std::vector<lid_t>& origin_vids,
    const std::array<grape::Bitset, N>& origin_bitsets) {
  size_t dst_size = index_ele_tuples.size();
  std::vector<lid_t> res_vids;
  std::array<grape::Bitset, N> res_bitsets;
  res_vids.reserve(dst_size);
  for (auto i = 0; i < N; ++i) {
    res_bitsets[i].init(dst_size);
  }
  for (auto ele : index_ele_tuples) {
    auto& cur = std::get<col_ind>(ele);
    //(ind, vid)
    auto ind = std::get<0>(cur);
    CHECK(ind < origin_vids.size());

    for (auto i = 0; i < N; ++i) {
      if (origin_bitsets[i].get_bit(ind)) {
        res_bitsets[i].set_bit(res_vids.size());
        break;
      }
    }
    res_vids.emplace_back(origin_vids[ind]);
  }
  return std::make_pair(std::move(res_vids), std::move(res_bitsets));
}

template <typename VID_T, size_t N>
class GeneralVertexSetIter {
 public:
  using lid_t = VID_T;
  using self_type_t = GeneralVertexSetIter<VID_T, N>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  using data_tuple_t = std::tuple<VID_T>;

  GeneralVertexSetIter(const std::vector<VID_T>& vec,
                       const std::array<grape::Bitset, N>& bitsets, size_t ind)
      : vec_(vec), bitsets_(bitsets), ind_(ind) {}

  lid_t GetElement() const { return vec_[ind_]; }

  data_tuple_t GetData() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, vec_[ind_]);
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
  const std::array<grape::Bitset, N>& bitsets_;
  size_t ind_;
};

/// @brief GeneralVertexSet are designed for the case we need to store multiple
/// label vertex in a mixed manner
/// @tparam VID_T
/// @tparam LabelT
/// @tparam N
template <typename VID_T, typename LabelT, size_t N>
class GeneralVertexSet {
 public:
  using lid_t = VID_T;
  using self_type_t = GeneralVertexSet<VID_T, LabelT, N>;
  using iterator = GeneralVertexSetIter<VID_T, N>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T>;
  using data_tuple_t = std::tuple<VID_T>;
  using flat_t = self_type_t;
  using EntityValueType = VID_T;

  static constexpr bool is_vertex_set = true;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_general_set = true;
  static constexpr size_t num_labels = N;
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_label = false;
  GeneralVertexSet(std::vector<VID_T>&& vec,
                   std::array<LabelT, N>&& label_names,
                   std::array<grape::Bitset, N>&& bitsets)
      : vec_(std::move(vec)), label_names_(std::move(label_names)) {
    for (auto i = 0; i < N; ++i) {
      bitsets_[i].swap(bitsets[i]);
    }
    VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
             << ", bitset size: " << bitsets_[0].cardinality();
  }

  GeneralVertexSet(GeneralVertexSet&& other)
      : vec_(std::move(other.vec_)),
        label_names_(std::move(other.label_names_)) {
    for (auto i = 0; i < N; ++i) {
      bitsets_[i].swap(other.bitsets_[i]);
    }
    VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
             << ", bitset size: " << bitsets_[0].cardinality();
  }

  GeneralVertexSet(const GeneralVertexSet& other)
      : vec_(other.vec_), label_names_(other.label_names_) {
    for (auto i = 0; i < N; ++i) {
      bitsets_[i].copy(other.bitsets_[i]);
    }
    VLOG(10) << "[GeneralVertexSet], size: " << vec_.size()
             << ", bitset size: " << bitsets_[0].cardinality();
  }

  iterator begin() const { return iterator(vec_, bitsets_, 0); }

  iterator end() const { return iterator(vec_, bitsets_, vec_.size()); }

  template <typename EXPRESSION, size_t num_labels, typename PROP_GETTER,
            typename RES_SET_T = self_type_t,
            typename RES_T = std::pair<RES_SET_T, std::vector<offset_t>>>
  RES_T project_vertices(std::array<LabelT, num_labels>& filter_labels,
                         EXPRESSION& exprs,
                         std::array<PROP_GETTER, N>& prop_getter) const {
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

  const std::array<LabelT, N>& GetLabels() const { return label_names_; }

  LabelT GetLabel(size_t i) const { return label_names_[i]; }

  const std::array<grape::Bitset, N>& GetBitsets() const { return bitsets_; }

  const std::vector<VID_T>& GetVertices() const { return vec_; }

  std::pair<std::vector<VID_T>, std::vector<int32_t>> GetVertices(
      size_t ind) const {
    CHECK(ind < N);
    std::vector<VID_T> res;
    std::vector<int32_t> active_ind;
    size_t cnt = bitsets_[ind].count();
    res.reserve(cnt);
    active_ind.reserve(cnt);
    for (auto i = 0; i < bitsets_[ind].cardinality(); ++i) {
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

  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<lid_t> next_vids;
    size_t next_size = 0;
    for (auto i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[GeneralVertexSet] size: " << Size()
             << " Project self, next size: " << next_size;

    next_vids.reserve(next_size);
    std::array<grape::Bitset, N> next_sets;
    for (auto& i : next_sets) {
      i.init(next_size);
    }
    VLOG(10) << "after init";
    for (auto i = 0; i < repeat_array.size(); ++i) {
      size_t ind = 0;
      while (ind < N) {
        if (bitsets_[ind].get_bit(i)) {
          break;
        }
        ind += 1;
      }
      CHECK(ind < N);
      for (auto j = 0; j < repeat_array[i]; ++j) {
        // VLOG(10) << "Project: " << vids_[i];
        next_sets[ind].set_bit(next_vids.size());
        next_vids.push_back(vec_[i]);
      }
    }

    auto copied_label_names(label_names_);
    return self_type_t(std::move(next_vids), std::move(copied_label_names),
                       std::move(next_sets));
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
  std::array<LabelT, N> label_names_;
  std::array<grape::Bitset, N> bitsets_;
};

template <typename VID_T, typename LabelT, size_t N>
auto make_general_set(std::vector<VID_T>&& vec,
                      std::array<LabelT, N>&& label_names,
                      std::array<grape::Bitset, N>&& bitsets) {
  return GeneralVertexSet<VID_T, LabelT, N>(
      std::move(vec), std::move(label_names), std::move(bitsets));
}

template <size_t num_labels>
static std::array<std::vector<int32_t>, num_labels> bitsets_to_vids_inds(
    const std::array<grape::Bitset, num_labels>& bitset) {
  std::array<std::vector<int32_t>, num_labels> res;
  auto limit_size = bitset[0].size();
  VLOG(10) << "old bitset limit size: " << limit_size;
  for (auto i = 0; i < num_labels; ++i) {
    auto count = bitset[i].count();
    res[i].reserve(count);
    for (auto j = 0; j < limit_size; ++j) {
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
          size_t num_old_labels>
static auto get_property_tuple_general(
    const GRAPH_INTERFACE& graph,
    const GeneralVertexSet<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                           num_old_labels>& general_set,
    const std::array<std::string, sizeof...(T)>& prop_names) {
  auto label_array = general_set.GetLabels();
  auto vids_inds = bitsets_to_vids_inds(general_set.GetBitsets());

  auto data_tuples = graph.template GetVertexPropsFromVid<T...>(
      general_set.GetVertices(), label_array, vids_inds, prop_names);

  return data_tuples;
}

template <typename... T, typename GRAPH_INTERFACE, typename LabelT,
          size_t num_old_labels>
static auto get_property_tuple_general(
    const GRAPH_INTERFACE& graph,
    const GeneralVertexSet<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                           num_old_labels>& general_set,
    const std::tuple<NamedProperty<T>...>& named_prop) {
  std::array<std::string, sizeof...(T)> prop_names;
  int ind = 0;
  std::apply([&prop_names,
              &ind](auto&&... args) { ((prop_names[ind++] = args.name), ...); },
             named_prop);
  return get_property_tuple_general<T...>(graph, general_set, prop_names);
}

}  // namespace gs

#endif  // ENGINES_HQPS_DS_MULTI_VERTEX_SET_GENERAL_VERTEX_SET_H_
