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
#ifndef ENGINES_HQPS_DS_COLLECTION_H_
#define ENGINES_HQPS_DS_COLLECTION_H_

#include <tuple>
#include <unordered_set>
#include <vector>

#include "flex/engines/hqps_db/core/null_record.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/core/utils/props.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "grape/utils/bitset.h"

#include <boost/functional/hash.hpp>
#include "grape/util.h"

namespace gs {

class EmptyCol {
 public:
  using value_type = grape::EmptyType;
};

// After operator like group, we need to extract the property or the count to
// separate column.
// We use collection to implement this abstraction.
// Currently we may not use it like vertex_set/edge_set, i.e., no dedup, no
// flat, not subset on collection.

template <typename LabelT, typename VID_T, typename... T>
class RowVertexSetImpl;

template <typename LabelT, typename VID_T, typename... T>
class RowVertexSetImplBuilder;

template <typename VID_T, typename LabelT, typename... T>
class TwoLabelVertexSetImpl;

template <typename VID_T, typename LabelT, typename... T>
class TwoLabelVertexSetImplBuilder;

// untypedEdgeSet
template <typename VID_T, typename LabelT, typename SUB_GRAPH_T>
class UnTypedEdgeSet;

template <typename T>
class Collection;

template <typename T>
class CollectionBuilder {
 public:
  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;
  static constexpr bool is_collection_builder = true;
  using result_t = Collection<T>;

  CollectionBuilder() {}

  // insert tuple at index ind.
  void Insert(T&& t) { vec_.emplace_back(std::move(t)); }

  void Insert(const T& t) { vec_.push_back(t); }

  // insert index ele tuple
  void Insert(const std::tuple<size_t, T>& t) {
    vec_.emplace_back(std::get<1>(t));
  }

  Collection<T> Build() {
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return Collection<T>(std::move(vec_));
  }

 private:
  std::vector<T> vec_;
};

// Building for collection which appears as the key in group by
template <typename T>
class KeyedCollectionBuilder {
 public:
  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;
  static constexpr bool is_collection_builder = true;
  using result_t = Collection<T>;
  KeyedCollectionBuilder() {}

  KeyedCollectionBuilder(const Collection<T>& old) { vec_.reserve(old.Size()); }

  template <typename LabelT, typename VID_T, typename... TS>
  KeyedCollectionBuilder(
      const RowVertexSetImpl<LabelT, VID_T, TS...>& row_vertex_set) {
    vec_.reserve(row_vertex_set.Size());
  }

  // insert returning a unique index for the inserted element
  size_t insert(const T& t) {
    if (map_.find(t) == map_.end()) {
      map_[t] = vec_.size();
      vec_.push_back(t);
      return vec_.size() - 1;
    } else {
      return map_[t];
    }
  }

  size_t insert(T&& t) {
    if (map_.find(t) == map_.end()) {
      map_[t] = vec_.size();
      vec_.emplace_back(std::move(t));
      return vec_.size() - 1;
    } else {
      return map_[t];
    }
  }

  size_t Insert(const std::tuple<size_t, T>& t) {
    return insert(std::get<1>(t));
  }

  Collection<T> Build() {
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return Collection<T>(std::move(vec_));
  }

 private:
  std::unordered_map<T, size_t, boost::hash<T>> map_;
  std::vector<T> vec_;
};

template <typename T>
class CollectionIter {
 public:
  using data_tuple_t = std::tuple<T>;
  using inner_iter_t = typename std::vector<T>::const_iterator;
  using self_type_t = CollectionIter<T>;
  using index_ele_tuple_t = std::tuple<size_t, T>;
  CollectionIter(const std::vector<T>& vec, size_t ind)
      : vec_(vec), ind_(ind) {}

  T GetElement() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, vec_[ind_]);
  }

  T GetData() const { return vec_[ind_]; }

  inline CollectionIter<T>& operator++() {
    ++ind_;
    return *this;
  };
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
  const std::vector<T>& vec_;
  size_t ind_;
};

// specialization for T is tuple, and only contains one element.
template <typename T>
class CollectionIter<std::tuple<T>> {
 public:
  using element_type = std::tuple<T>;
  using data_tuple_t = std::tuple<element_type>;
  using inner_iter_t = typename std::vector<element_type>::const_iterator;
  using self_type_t = CollectionIter<element_type>;
  using index_ele_tuple_t = std::tuple<size_t, T>;
  CollectionIter(const std::vector<element_type>& vec, size_t ind)
      : vec_(vec), ind_(ind) {}

  T GetElement() const { return std::get<0>(vec_[ind_]); }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, std::get<0>(vec_[ind_]));
  }

  T GetData() const { return std::get<0>(vec_[ind_]); }

  inline CollectionIter<std::tuple<T>>& operator++() {
    ++ind_;
    return *this;
  };
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
  size_t ind_;
  const std::vector<element_type>& vec_;
};

template <typename T>
class Collection {
 public:
  using element_type = T;
  using value_type = T;
  using iterator = CollectionIter<T>;
  using data_tuple_t = typename iterator::data_tuple_t;
  using index_ele_tuple_t = typename iterator::index_ele_tuple_t;
  using flat_t = Collection<T>;
  using self_type_t = Collection<T>;
  using EntityValueType = T;

  using builder_t = CollectionBuilder<T>;

  static constexpr bool is_collection = true;
  static constexpr bool is_keyed = false;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_vertex_set = false;
  static constexpr bool is_edge_set = false;
  static constexpr bool is_two_label_set = false;
  static constexpr bool is_general_set = false;
  static constexpr bool is_row_vertex_set = false;

  Collection() {}
  Collection(size_t cap) { vec_.reserve(cap); }
  Collection(std::vector<T>&& vec) : vec_(std::move(vec)) {}
  Collection(Collection<T>&& other) : vec_(std::move(other.vec_)) {}
  Collection(const Collection<T>& other) : vec_(other.vec_) {}

  ~Collection() {}
  size_t Size() const { return vec_.size(); }

  builder_t CreateBuilder() const { return builder_t(); }

  // Append empty entries to make length == the give args
  void MakeUpTo(size_t dstLen) {
    if (dstLen <= vec_.size()) {
      return;
    }
    vec_.resize(dstLen);
  }

  // For the input offset array, add default value for null entry.
  std::pair<Collection<T>, std::vector<offset_t>> apply(
      const std::vector<offset_t>& offset) {
    size_t new_size = offset.size() - 1;

    VLOG(10) << "Extend " << vec_.size() << " to size: " << new_size;

    std::vector<T> new_vec;
    std::vector<offset_t> new_offset;
    new_offset.reserve(new_size + 1);
    new_vec.reserve(new_size);
    new_offset.emplace_back(0);
    for (size_t i = 0; i < new_size; ++i) {
      if (offset[i] >= offset[i + 1]) {
        new_vec.emplace_back(T());
      } else {
        for (auto j = offset[i]; j < offset[i + 1]; ++j) {
          new_vec.push_back(vec_[j]);
        }
      }
      new_offset.emplace_back(new_vec.size());
    }
    Collection<T> new_set(std::move(new_vec));
    return std::make_pair(std::move(new_set), std::move(new_offset));
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    std::vector<T> res;
    res.reserve(repeat_vec.back());
    CHECK(repeat_vec.size() == cur_offset.size())
        << "repeat vec:" << gs::to_string(repeat_vec)
        << ", cur offset: " << gs::to_string(cur_offset);
    for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (size_t j = 0; j < times_to_repeat; ++j) {
        for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          res.push_back(vec_[k]);
        }
      }
    }
    VLOG(10) << "new vids: " << gs::to_string(res);
    vec_.swap(res);
  }

  template <size_t col_ind, typename... index_ele_tuple_t>
  auto Flat(std::vector<std::tuple<index_ele_tuple_t...>>& index_ele_tuples) {
    std::vector<T> res_vids;
    res_vids.reserve(index_ele_tuples.size());
    for (auto ele : index_ele_tuples) {
      auto& cur = std::get<col_ind>(ele);
      //(ind, vid)
      auto& ind = std::get<0>(cur);
      CHECK(ind < vec_.size());
      res_vids.emplace_back(vec_[ind]);
    }
    return self_type_t(std::move(res_vids));
  }

  // project my self.
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<T> res;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        // VLOG(10) << "Project: " << vids_[i];
        res.push_back(vec_[i]);
      }
    }
    return self_type_t(std::move(res));
  }

  void SubSetWithIndices(std::vector<size_t>& indices) {
    std::vector<T> res;
    res.reserve(indices.size());
    for (auto ind : indices) {
      res.emplace_back(vec_[ind]);
    }
    vec_.swap(res);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    LOG(WARNING) << " Not implemented";
  }

  std::vector<offset_t> Dedup() {
    std::vector<offset_t> offsets;
    std::vector<T> new_vec;
    std::unordered_map<T, size_t, boost::hash<T>> map;
    for (size_t ind = 0; ind < vec_.size(); ++ind) {
      offsets.emplace_back(new_vec.size());
      auto ret = map.insert({vec_[ind], ind});
      if (ret.second == true) {
        new_vec.emplace_back(vec_[ind]);
      }
    }
    offsets.emplace_back(new_vec.size());
    vec_.swap(new_vec);
    return offsets;
  }

  iterator begin() const { return iterator(vec_, 0); }

  iterator end() const { return iterator(vec_, vec_.size()); }

  T Get(size_t ind) const { return vec_[ind]; }

  const std::vector<T> GetVector() const { return vec_; }

 private:
  std::vector<T> vec_;
};

template <typename T>
using CollectionOfVec = Collection<std::vector<T>>;

// the tag is used when receiving tuple, and apply aggregate function on tuple.
template <int tag>
class CountBuilder {
 public:
  CountBuilder() {}

  // insert tuple at index ind.
  // if the ele_value equal to invalid_value, then do not insert.
  template <typename ELE_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE& tuple, const DATA_TUPLE& data) {
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back(0);
    }
    using cur_ele_tuple = typename gs::tuple_element<tag, ELE_TUPLE>::type;
    auto& cur_ele = gs::get_from_tuple<tag>(tuple);
    // currently we support vertex ele tuple and edge tuple.
    if constexpr (std::tuple_size<cur_ele_tuple>::value == 2) {
      auto& ele = std::get<1>(cur_ele);
      using vid_t = typename std::tuple_element<1, cur_ele_tuple>::type;
      if (ele != NullRecordCreator<vid_t>::GetNull()) {
        ++vec_[ind];
      } else {
        VLOG(10) << "ele is null";
      }
    } else {
      VLOG(10) << "inc:" << ind << ", " << gs::to_string(tuple);
      ++vec_[ind];
    }
  }

  Collection<size_t> Build() {
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return Collection<size_t>(std::move(vec_));
  }

 private:
  std::vector<size_t> vec_;
};

// Prop Count Builder.
template <int tag, typename PropGetterT>
class PropCountBuilder {
 public:
  PropCountBuilder(PropGetterT&& prop_getter)
      : prop_getter_(std::move(prop_getter)) {}

  // insert tuple at index ind.
  // if the ele_value equal to invalid_value, then do not insert.
  template <typename ELE_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE& tuple, const DATA_TUPLE& data) {
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back(0);
    }

    auto& cur_ele = gs::get_from_tuple<tag>(tuple);
    // get prop from prop getter
    auto props = prop_getter_.get_view(cur_ele);
    // get the type of props
    using props_t = decltype(props);

    if (props != NullRecordCreator<props_t>::GetNull()) {
      ++vec_[ind];
    } else {
      VLOG(10) << "ele is null, ind: " << ind
               << "ele:" << gs::to_string(cur_ele);
    }
  }

  Collection<size_t> Build() { return Collection<size_t>(std::move(vec_)); }

 private:
  std::vector<size_t> vec_;
  PropGetterT prop_getter_;
};

template <int... tag>
class MultiColCountBuilder {
 public:
  MultiColCountBuilder() {}

  // insert tuple at index ind.
  // if the ele_value equal to invalid_value, then do not insert.
  template <typename ELE_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE& tuple, const DATA_TUPLE& data) {
    auto cur_ele_tuple =
        std::tuple_cat(tuple_slice<1>(gs::get_from_tuple<tag>(tuple))...);
    while (vec_.size() <= ind) {
      vec_.emplace_back(0);
    }
    using cur_ele_tuple_t =
        std::remove_const_t<std::remove_reference_t<decltype(cur_ele_tuple)>>;
    // remove the const and reference for each type in cur_ele_tuple_t
    using cur_ele_tuple_rm_const_ref_t =
        typename ConstRefRemoveHelper<cur_ele_tuple_t>::type;
    if (cur_ele_tuple !=
        NullRecordCreator<cur_ele_tuple_rm_const_ref_t>::GetNull()) {
      ++vec_[ind];
    } else {
      VLOG(10) << "ele is null";
    }
  }

  Collection<size_t> Build() { return Collection<size_t>(std::move(vec_)); }

 private:
  std::vector<size_t> vec_;
};

template <int tag_id, typename T, typename Enable = void>
class DistinctCountBuilder;

// Count the distinct edges of UntypedEdgeSet.
// We assume each edge  in UnTypeEdgeSet  is unique, so just count the index of
// index_ele_tuple_t.
template <int tag_id, typename EDGE_SET_T>
class DistinctCountBuilder<
    tag_id, EDGE_SET_T,
    typename std::enable_if<(EDGE_SET_T::is_edge_set)>::type> {
 public:
  using edge_set_t = EDGE_SET_T;
  using index_ele_t = typename edge_set_t::index_ele_tuple_t;
  DistinctCountBuilder(const edge_set_t& edge_set) {
    edges_num_ = edge_set.Size();
  }

  template <typename ELE_TUPLE_T, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE_T& tuple, const DATA_TUPLE& data) {
    auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    while (vec_.size() <= ind) {
      vec_.emplace_back(grape::Bitset(edges_num_));
    }
    auto& cur_bitset = vec_[ind];
    auto cur_ind = std::get<0>(cur_ind_ele);
    if (cur_ind < edges_num_) {
      cur_bitset.set_bit(cur_ind);
    } else {
      LOG(FATAL) << "Invalid edge index: " << cur_ind
                 << ", edges num: " << edges_num_;
    }
  }

  Collection<size_t> Build() {
    std::vector<size_t> res;
    res.reserve(vec_.size());
    for (auto& bitset : vec_) {
      res.emplace_back(bitset.count());
    }
    return Collection<size_t>(std::move(res));
  }

 private:
  std::vector<grape::Bitset> vec_;
  size_t edges_num_;
};

// count the distinct number of received elements.
template <int tag_id, typename LabelT, typename VID_T, typename... T>
class DistinctCountBuilder<tag_id, RowVertexSetImpl<LabelT, VID_T, T...>> {
 public:
  DistinctCountBuilder(const std::vector<VID_T>& vertices) {
    // find out the range of vertices inside vector, and use a bitset to count
    for (auto v : vertices) {
      min_v = std::min(min_v, v);
      max_v = std::max(max_v, v);
    }
    range_size = max_v - min_v + 1;
  }

  template <typename ELE_TUPLE_T, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE_T& tuple, const DATA_TUPLE& data) {
    auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    static_assert(
        std::is_same_v<
            std::tuple_element_t<1, std::remove_const_t<std::remove_reference_t<
                                        decltype(cur_ind_ele)>>>,
            VID_T>,
        "Type not match");
    while (vec_.size() <= ind) {
      vec_.emplace_back(grape::Bitset(range_size));
    }
    auto& cur_bitset = vec_[ind];
    auto cur_v = std::get<1>(cur_ind_ele);
    cur_bitset.set_bit(cur_v - min_v);
    // VLOG(10) << "tag id: " << tag_id << "insert at ind: " << ind
    //          << ",value : " << cur_v << ", res: " << cur_bitset.count();
  }

  Collection<size_t> Build() {
    std::vector<size_t> res;
    res.reserve(vec_.size());
    for (auto& bitset : vec_) {
      res.emplace_back(bitset.count());
    }
    return Collection<size_t>(std::move(res));
  }

 private:
  std::vector<grape::Bitset> vec_;
  VID_T min_v, max_v, range_size;
};

// specialization for DistinctCountBuilder for num_labels=2
template <int tag_id, typename VID_T, typename LabelT, typename... T>
class DistinctCountBuilder<tag_id, TwoLabelVertexSetImpl<VID_T, LabelT, T...>> {
 public:
  DistinctCountBuilder(const grape::Bitset& bitset,
                       const std::vector<VID_T>& vids) {
    // find out the range of vertices inside vector, and use a bitset to count
    for (size_t i = 0; i < vids.size(); ++i) {
      auto v = vids[i];
      if (bitset.get_bit(i)) {
        min_v[0] = std::min(min_v[0], v);
        max_v[0] = std::max(max_v[0], v);
      } else {
        min_v[1] = std::min(min_v[1], v);
        max_v[1] = std::max(max_v[1], v);
      }
    }
    range_size[0] = max_v[0] - min_v[0] + 1;
    range_size[1] = max_v[1] - min_v[0] + 1;
    VLOG(10) << "Min: " << min_v[0] << ", range size: " << range_size[0];
    VLOG(10) << "Min: " << min_v[1] << ", range size: " << range_size[1];
  }

  template <typename ELE_TUPLE_T, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE_T& tuple, const DATA_TUPLE& data) {
    auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    static_assert(
        std::is_same_v<
            std::tuple_element_t<2, std::remove_const_t<std::remove_reference_t<
                                        decltype(cur_ind_ele)>>>,
            VID_T>,
        "Type not match");
    auto label_ind = std::get<1>(cur_ind_ele);
    while (vec_[label_ind].size() <= ind) {
      vec_[label_ind].emplace_back(grape::Bitset(range_size[label_ind]));
    }

    auto& cur_bitset = vec_[label_ind][ind];
    auto cur_v = std::get<2>(cur_ind_ele);
    cur_bitset.set_bit(cur_v - min_v[label_ind]);
    VLOG(10) << "tag id: " << tag_id << "insert at ind: " << ind
             << ",value : " << cur_v << ", res: " << cur_bitset.count();
  }

  Collection<size_t> Build() {
    std::vector<size_t> res;
    auto max_ind = std::max(vec_[0].size(), vec_[1].size());
    res.resize(max_ind, 0);
    for (auto label_ind = 0; label_ind < 2; ++label_ind) {
      for (size_t i = 0; i < vec_[label_ind].size(); ++i) {
        res[i] += vec_[label_ind][i].count();
      }
    }
    return Collection<size_t>(std::move(res));
  }

 private:
  std::array<std::vector<grape::Bitset>, 2> vec_;
  std::array<VID_T, 2> min_v, max_v, range_size;
};

// DistinctCountBuilder for PathSet
template <int tag_id, typename PATH_SET_T>
class DistinctCountBuilder<
    tag_id, PATH_SET_T,
    typename std::enable_if<PATH_SET_T::is_path_set>::type> {
 public:
  using path_set_t = PATH_SET_T;
  using index_ele_t = typename path_set_t::index_ele_tuple_t;
  DistinctCountBuilder(const path_set_t& path_set) {
    paths_num_ = path_set.Size();
  }

  template <typename ELE_TUPLE_T, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE_T& tuple, const DATA_TUPLE& data) {
    auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    while (vec_.size() <= ind) {
      vec_.emplace_back(grape::Bitset(paths_num_));
    }
    auto& cur_bitset = vec_[ind];
    auto cur_ind = std::get<0>(cur_ind_ele);
    if (cur_ind < paths_num_) {
      cur_bitset.set_bit(cur_ind);
    } else {
      LOG(FATAL) << "Invalid path set index: " << cur_ind
                 << ", path set num: " << paths_num_;
    }
  }

  Collection<size_t> Build() {
    std::vector<size_t> res;
    res.reserve(vec_.size());
    for (auto& bitset : vec_) {
      res.emplace_back(bitset.count());
    }
    return Collection<size_t>(std::move(res));
  }

 private:
  std::vector<grape::Bitset> vec_;
  size_t paths_num_;
};

// DistinctCountBuilder for multiple sets together
template <typename SET_TUPLE_T, int... TAG_IDs>
class MultiColDistinctCountBuilder;

template <typename... SET_Ts, int... TAG_IDs>
class MultiColDistinctCountBuilder<std::tuple<SET_Ts...>, TAG_IDs...> {
 public:
  using set_ele_t = std::tuple<typename SET_Ts::element_t...>;
  MultiColDistinctCountBuilder() {}

  template <typename ELE_TUPLE_T, typename DATA_TUPLE>
  void insert(size_t ind, const ELE_TUPLE_T& tuple, const DATA_TUPLE& data) {
    // construct the ref tuple from tuple,with TAG_IDS,
    // get element tuple from index_ele_tuple_t
    auto cur_ele_tuple =
        std::tuple_cat(tuple_slice<1>(gs::get_from_tuple<TAG_IDs>(tuple))...);

    while (vec_of_set_.size() <= ind) {
      vec_of_set_.emplace_back(
          std::unordered_set<set_ele_t, boost::hash<set_ele_t>>());
    }
    auto& cur_set = vec_of_set_[ind];
    cur_set.insert(cur_ele_tuple);
    VLOG(10) << "tuple: " << gs::to_string(cur_ele_tuple)
             << ",all ele: " << gs::to_string(tuple) << "insert at ind: " << ind
             << ", res: " << cur_set.size();
  }

  Collection<size_t> Build() {
    std::vector<size_t> res;
    res.reserve(vec_of_set_.size());
    for (auto& set : vec_of_set_) {
      res.emplace_back(set.size());
    }
    return Collection<size_t>(std::move(res));
  }

 private:
  std::vector<std::unordered_set<set_ele_t, boost::hash<set_ele_t>>>
      vec_of_set_;
};

template <typename T, int tag_id>
class SumBuilder {
 public:
  SumBuilder() {}
  SumBuilder(size_t cap) { vec_.resize(cap, (T) 0); }

  // insert tuple at index ind.
  template <typename IND_ELE_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const IND_ELE_TUPLE& tuple, const DATA_TUPLE& data) {
    const auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back((T) 0);
    }
    vec_[ind] += std::get<1>(cur_ind_ele);
  }

  Collection<T> Build() {
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return Collection<T>(std::move(vec_));
  }

 private:
  std::vector<T> vec_;
};

template <typename GI, typename T, int tag_id>
class MinBuilder {
 public:
  MinBuilder(const Collection<T>& set, const GI& graph,
             PropNameArray<T> prop_names) {
    // vec_.resize(set.Size(), std::numeric_limits<T>::max());
  }
  MinBuilder() {}
  MinBuilder(size_t cap) { vec_.resize(cap, (T) 0); }

  // insert tuple at index ind.
  template <typename IND_ELE_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const IND_ELE_TUPLE& tuple, const DATA_TUPLE& data) {
    const auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    VLOG(10) << "Insert min with ind: " << ind
             << ", value: " << std::get<1>(cur_ind_ele)
             << ", vec size: " << vec_.size();
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back(std::numeric_limits<T>::max());
    }
    vec_[ind] = std::min(vec_[ind], std::get<1>(cur_ind_ele));
  }

  Collection<T> Build() {
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return Collection<T>(std::move(vec_));
  }

 private:
  std::vector<T> vec_;
};

template <typename GI, typename T, int tag_id>
class MaxBuilder {
 public:
  MaxBuilder(const Collection<T>& set, const GI& graph,
             PropNameArray<T> prop_names) {
    vec_.resize(set.Size(), std::numeric_limits<T>::min());
  }
  MaxBuilder() {}
  MaxBuilder(size_t cap) { vec_.resize(cap, (T) 0); }

  // insert tuple at index ind.
  template <typename IND_ELE_TUPLE, typename DATA_TUPLE_T>
  void insert(size_t ind, const IND_ELE_TUPLE& tuple,
              const DATA_TUPLE_T& data) {
    const auto& cur_ind_ele = gs::get_from_tuple<tag_id>(tuple);
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back(std::numeric_limits<T>::max());
    }
    vec_[ind] = std::max(vec_[ind], std::get<1>(cur_ind_ele));
  }

  Collection<T> Build() {
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return Collection<T>(std::move(vec_));
  }

 private:
  std::vector<T> vec_;
};

template <typename GI, typename SET_T, typename T, int tag_id>
class FirstBuilder;

// FirstBuilder
template <typename GI, typename C_T, int tag_id>
class FirstBuilder<GI, Collection<C_T>, grape::EmptyType, tag_id> {
 public:
  FirstBuilder(const Collection<C_T>& set, const GI& graph,
               PropNameArray<grape::EmptyType> prop_names) {
    CHECK(prop_names.size() == 1);
    CHECK(prop_names[0] == "none" || prop_names[0] == "None" ||
          prop_names[0] == "");
  }

  template <typename IND_ELE_T, typename DATA_ELE_T>
  void insert(size_t ind, const IND_ELE_T& tuple,
              const DATA_ELE_T& data_tuple) {
    if (ind < vec_.size()) {
      return;
    } else if (ind == vec_.size()) {
      vec_.emplace_back(std::get<1>(gs::get_from_tuple<tag_id>(tuple)));
    } else {
      LOG(FATAL) << "Can not insert with ind: " << ind
                 << ", which cur size is : " << vec_.size();
    }
  }

  Collection<C_T> Build() { return Collection<C_T>(std::move(vec_)); }

 private:
  std::vector<C_T> vec_;
};

// firstBuilder for vertex set, with data tuple
template <typename GI, typename LabelT, typename VID_T, typename... OLD_T,
          int tag_id>
class FirstBuilder<GI, RowVertexSetImpl<LabelT, VID_T, OLD_T...>,
                   grape::EmptyType, tag_id> {
 public:
  using set_t = RowVertexSetImpl<LabelT, VID_T, OLD_T...>;
  using builder_t = RowVertexSetImplBuilder<LabelT, VID_T, OLD_T...>;
  FirstBuilder(const set_t& set, const GI& graph,
               PropNameArray<grape::EmptyType> prop_names)
      : builder_(set.GetLabel(), set.GetPropNames()) {}

  template <typename IND_ELE_T, typename DATA_TUPLE_T>
  void insert(size_t ind, const IND_ELE_T& tuple,
              const DATA_TUPLE_T& data_tuple) {
    if (ind < builder_.Size()) {
      return;
    } else if (ind == builder_.Size()) {
      builder_.Insert(tuple, data_tuple);
    } else {
      LOG(FATAL) << "Can not insert with ind: " << ind
                 << ", which cur size is : " << builder_.size();
    }
  }

  set_t Build() { return builder_.Build(); }

 private:
  builder_t builder_;
};

template <typename GI, typename LabelT, typename VID_T, int tag_id>
class FirstBuilder<GI, RowVertexSetImpl<LabelT, VID_T, grape::EmptyType>,
                   grape::EmptyType, tag_id> {
 public:
  using set_t = RowVertexSetImpl<LabelT, VID_T, grape::EmptyType>;
  using builder_t = RowVertexSetImplBuilder<LabelT, VID_T, grape::EmptyType>;
  FirstBuilder(const set_t& set, const GI& graph,
               PropNameArray<grape::EmptyType> prop_names)
      : builder_(set.GetLabel(), set.GetPropNames()) {}

  template <typename IND_ELE_T, typename DATA_TUPLE_T>
  void insert(size_t ind, const IND_ELE_T& tuple,
              const DATA_TUPLE_T& data_tuple) {
    if (ind < builder_.Size()) {
      return;
    } else if (ind == builder_.Size()) {
      builder_.Insert(tuple);
    } else {
      LOG(FATAL) << "Can not insert with ind: " << ind
                 << ", which cur size is : " << builder_.Size();
    }
  }

  set_t Build() { return builder_.Build(); }

 private:
  builder_t builder_;
};

// first builder for two label set
template <typename GI, typename VID_T, typename LabelT, int tag_id>
class FirstBuilder<GI, TwoLabelVertexSetImpl<VID_T, LabelT, grape::EmptyType>,
                   grape::EmptyType, tag_id> {
 public:
  using set_t = TwoLabelVertexSetImpl<VID_T, LabelT, grape::EmptyType>;
  using builder_t =
      TwoLabelVertexSetImplBuilder<VID_T, LabelT, grape::EmptyType>;
  FirstBuilder(const set_t& set, const GI& graph,
               PropNameArray<grape::EmptyType> prop_names)
      : builder_(set.Size(), set.GetLabels()) {}

  template <typename IND_ELE_T, typename DATA_TUPLE_T>
  void insert(size_t ind, const IND_ELE_T& tuple,
              const DATA_TUPLE_T& data_tuple) {
    if (ind < builder_.Size()) {
      return;
    } else if (ind == builder_.Size()) {
      builder_.Insert(gs::get_from_tuple<tag_id>(tuple));
    } else {
      LOG(FATAL) << "Can not insert with ind: " << ind
                 << ", which cur size is : " << builder_.Size();
    }
  }

  set_t Build() { return builder_.Build(); }

 private:
  builder_t builder_;
};

template <typename GI, typename VID_T, typename LabelT, typename... T,
          int tag_id>
class FirstBuilder<GI, TwoLabelVertexSetImpl<VID_T, LabelT, T...>,
                   grape::EmptyType, tag_id> {
 public:
  using set_t = TwoLabelVertexSetImpl<VID_T, LabelT, T...>;
  using builder_t = TwoLabelVertexSetImplBuilder<VID_T, LabelT, T...>;
  FirstBuilder(const set_t& set, const GI& graph,
               PropNameArray<grape::EmptyType> prop_names)
      // we should use a size which indicate the context size
      : builder_(set.Size(), set.GetLabels()) {}

  template <typename IND_ELE_T, typename DATA_TUPLE_T>
  void insert(size_t ind, const IND_ELE_T& tuple,
              const DATA_TUPLE_T& data_tuple) {
    if (ind < builder_.Size()) {
      return;
    } else if (ind == builder_.Size()) {
      builder_.Insert(gs::get_from_tuple<tag_id>(tuple),
                      gs::get_from_tuple<tag_id>(data_tuple));
    } else {
      LOG(FATAL) << "Can not insert with ind: " << ind
                 << ", which cur size is : " << builder_.Size();
    }
  }

  set_t Build() { return builder_.Build(); }

 private:
  builder_t builder_;
};

template <typename T, typename GRAPH_INTERFACE, typename SET_T, int tag_id>
class CollectionOfSetBuilder;

template <typename T, typename GRAPH_INTERFACE, typename LabelT, typename VID_T,
          typename... OLD_T, int tag_id>
class CollectionOfSetBuilder<
    T, GRAPH_INTERFACE, RowVertexSetImpl<LabelT, VID_T, OLD_T...>, tag_id> {
 public:
  using set_t = RowVertexSetImpl<LabelT, VID_T, OLD_T...>;
  using index_ele_t = typename set_t::index_ele_tuple_t;
  using set_ele_t = typename set_t::element_t;
  using graph_prop_getter_t =
      typename GRAPH_INTERFACE::template single_prop_getter_t<T>;
  using PROP_GETTER_T =
      RowVertexSetPropGetter<tag_id, graph_prop_getter_t,
                             typename set_t::index_ele_tuple_t>;
  CollectionOfSetBuilder(const RowVertexSetImpl<LabelT, VID_T, OLD_T...>& set,
                         const GRAPH_INTERFACE& graph,
                         PropNameArray<T> prop_names)
      : prop_getter_(
            create_prop_getter_impl<tag_id, T>(set, graph, prop_names[0])) {}

  // insert tuple at index ind.
  template <typename IND_TUPLE>
  void insert(size_t ind, IND_TUPLE& tuple) {
    while (vec_.size() <= ind) {
      vec_.emplace_back(std::vector<T>());
    }
    auto cur = gs::get_from_tuple<tag_id>(tuple);
    using ele_t = typename set_t::index_ele_tuple_t;
    if (NullRecordCreator<ele_t>::GetNull() == cur) {
      return;
    }

    vec_[ind].emplace_back(prop_getter_.get_view(cur));
  }

  template <typename IND_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const IND_TUPLE& tuple, const DATA_TUPLE& data) {
    while (vec_.size() <= ind) {
      vec_.emplace_back(std::vector<T>());
    }
    auto cur = gs::get_from_tuple<tag_id>(tuple);
    using ele_t = typename set_t::index_ele_tuple_t;
    if (NullRecordCreator<ele_t>::GetNull() == cur) {
      return;
    }

    vec_[ind].emplace_back(prop_getter_.get_view(cur));
  }

  CollectionOfVec<T> Build() {
    // Make it unique.
    for (size_t i = 0; i < vec_.size(); ++i) {
      sort(vec_[i].begin(), vec_[i].end());
      vec_[i].erase(unique(vec_[i].begin(), vec_[i].end()), vec_[i].end());
    }
    // VLOG(10) << "Finish building counter" << gs::to_string(vec_);
    return CollectionOfVec<T>(std::move(vec_));
  }

 private:
  std::vector<std::vector<T>> vec_;
  PROP_GETTER_T prop_getter_;
};

// To vector
template <typename T, typename GI, typename SET_T, int tag_id>
class CollectionOfVecBuilder;

template <typename T, typename GI, int tag_id>
class CollectionOfVecBuilder<T, GI, Collection<T>, tag_id> {
 public:
  CollectionOfVecBuilder(const GI& graph, const Collection<T>& set,
                         PropNameArray<T> prop_names) {}

  // insert tuple at index ind.
  template <typename IND_TUPLE>
  void insert(size_t ind, IND_TUPLE& tuple) {
    auto cur = std::get<1>(gs::get_from_tuple<tag_id>(tuple));
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back(std::vector<T>());
    }
    using input_ele_t = typename std::remove_reference<decltype(cur)>::type;
    if (NullRecordCreator<input_ele_t>::GetNull() == cur) {
      return;
    }
    // emplace the element to vector
    vec_[ind].emplace_back(cur);
  }

  template <typename IND_TUPLE, typename DATA_TUPLE>
  void insert(size_t ind, const IND_TUPLE& tuple, const DATA_TUPLE& data) {
    return insert(ind, tuple);
  }

  CollectionOfVec<T> Build() { return CollectionOfVec<T>(std::move(vec_)); }

 private:
  std::vector<std::vector<T>> vec_;
};

// organizing one property of vertex set to vector.
template <typename PropT, typename GRAPH_INTERFACE, typename LabelT,
          typename VID_T, typename... VERTEX_SET_TT, int tag_id>
class CollectionOfVecBuilder<PropT, GRAPH_INTERFACE,
                             RowVertexSetImpl<LabelT, VID_T, VERTEX_SET_TT...>,
                             tag_id> {
 public:
  using set_t = RowVertexSetImpl<LabelT, VID_T, VERTEX_SET_TT...>;
  using graph_prop_getter_t =
      typename GRAPH_INTERFACE::template single_prop_getter_t<PropT>;
  using PROP_GETTER_T =
      RowVertexSetPropGetter<tag_id, graph_prop_getter_t,
                             typename set_t::index_ele_tuple_t>;
  CollectionOfVecBuilder(const set_t& set, const GRAPH_INTERFACE& graph,
                         PropNameArray<PropT>& prop_names)
      : prop_getter_(create_prop_getter_impl<tag_id, PropT>(set, graph,
                                                            prop_names[0])) {}

  // insert tuple at index ind.
  template <typename IND_TUPLE>
  void insert(size_t ind, IND_TUPLE& tuple) {
    auto cur = gs::get_from_tuple<tag_id>(tuple);
    // just count times.
    while (vec_.size() <= ind) {
      vec_.emplace_back(std::vector<PropT>());
    }
    using input_ele_t = typename std::remove_reference<decltype(cur)>::type;
    if (NullRecordCreator<input_ele_t>::GetNull() == cur) {
      return;
    }
    // emplace the element to vector
    vec_[ind].emplace_back(prop_getter_.get_view(cur));
  }

  CollectionOfVec<PropT> Build() {
    return CollectionOfVec<PropT>(std::move(vec_));
  }

 private:
  std::vector<std::vector<PropT>> vec_;
  PROP_GETTER_T prop_getter_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_DS_COLLECTION_H_
