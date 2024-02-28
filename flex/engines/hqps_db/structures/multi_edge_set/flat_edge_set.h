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
#ifndef ENGINES_HQPS_ENGINE_DS_MULTI_EDGE_SET_FLAT_EDGE_SET_H_
#define ENGINES_HQPS_ENGINE_DS_MULTI_EDGE_SET_FLAT_EDGE_SET_H_

#include <string>
#include <vector>

#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/general_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"

#include "glog/logging.h"

namespace gs {

template <typename VID_T, typename LabelT, typename EDATA_T>
class FlatEdgeSet;

template <typename VID_T, typename LabelT, typename EDATA_T>
class SingleLabelEdgeSet;

template <typename VID_T, typename LabelT, typename EDATA_T>
class FlatEdgeSetBuilder {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, EDATA_T>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using result_t = FlatEdgeSet<VID_T, LabelT, EDATA_T>;

  static constexpr bool is_flat_edge_set_builder = true;
  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;

  FlatEdgeSetBuilder(
      std::vector<std::array<LabelT, 3>>&& label_triplet,
      std::vector<std::vector<std::string>> prop_names,
      const std::vector<uint8_t>&
          label_triplet_ind,  // label_triplet_ind is needed to create
                              // new label_triplet_ind with index_ele
      Direction direction)
      : label_triplet_(std::move(label_triplet)),
        prop_names_(prop_names),
        label_triplet_ind_(label_triplet_ind),
        direction_(direction) {}

  // There could be null record.
  void Insert(const index_ele_tuple_t& tuple) {
    vec_.push_back(std::get<1>(tuple));
    if (!IsNull(std::get<1>(tuple))) {
      label_triplet_ind_new_.push_back(label_triplet_ind_[std::get<0>(tuple)]);
    } else {
      label_triplet_ind_new_.push_back(NullRecordCreator<LabelT>::GetNull());
    }
  }

  result_t Build() {
    return result_t(std::move(vec_), label_triplet_, prop_names_,
                    std::move(label_triplet_ind_new_), direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::vector<std::array<LabelT, 3>> label_triplet_;
  std::vector<std::vector<std::string>> prop_names_;
  std::vector<uint8_t> label_triplet_ind_;
  std::vector<uint8_t> label_triplet_ind_new_;
  Direction direction_;
};

template <typename VID_T, typename LabelT, typename EDATA_T>
class FlatEdgeSetIter {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, EDATA_T>;
  using self_type_t = FlatEdgeSetIter<VID_T, LabelT, EDATA_T>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using data_tuple_t = ele_tuple_t;
  FlatEdgeSetIter(const std::vector<ele_tuple_t>& vec, size_t ind,
                  const std::vector<uint8_t>& label_triplet_ind,
                  const std::vector<std::array<LabelT, 3>>& label_triplet,
                  const std::vector<std::vector<std::string>>& prop_names)
      : vec_(vec),
        ind_(ind),
        label_triplet_(label_triplet),
        label_triplet_ind_(label_triplet_ind),
        prop_names_(prop_names) {}

  ele_tuple_t GetElement() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetElement());
  }
  LabelT GetEdgeLabel() const {
    return label_triplet_[label_triplet_ind_[ind_]][2];
  }

  VID_T GetSrc() const { return std::get<0>(vec_[ind_]); }

  LabelT GetSrcLabel() const {
    return label_triplet_[label_triplet_ind_[ind_]][0];
  }

  VID_T GetDst() const { return std::get<1>(vec_[ind_]); }

  LabelT GetDstLabel() const {
    return label_triplet_[label_triplet_ind_[ind_]][1];
  }

  const EDATA_T& GetData() const { return std::get<2>(vec_[ind_]); }

  const std::vector<std::string>& GetPropNames() const {
    return prop_names_[label_triplet_ind_[ind_]];
  }

  size_t GetIndex() const { return ind_; }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
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

 private:
  const std::vector<ele_tuple_t>& vec_;
  const std::vector<uint8_t>& label_triplet_ind_;
  const std::vector<std::array<LabelT, 3>>& label_triplet_;
  const std::vector<std::vector<std::string>>& prop_names_;
  size_t ind_;
};

// for FlatEdgeSet with only one label triplet, we should use SingleLabelEdgeSet
template <typename VID_T, typename LabelT, typename EDATA_T>
class FlatEdgeSet {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, EDATA_T>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using iterator = FlatEdgeSetIter<VID_T, LabelT, EDATA_T>;
  using self_type_t = FlatEdgeSet<VID_T, LabelT, EDATA_T>;
  using flat_t = self_type_t;
  using data_tuple_t = ele_tuple_t;
  using builder_t = FlatEdgeSetBuilder<VID_T, LabelT, EDATA_T>;

  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_src = false;
  static constexpr bool is_multi_dst_label = false;

  FlatEdgeSet(std::vector<ele_tuple_t>&& vec,
              std::vector<std::array<LabelT, 3>>&& label_triplet,
              std::vector<std::vector<std::string>> prop_names,
              std::vector<uint8_t>&& label_triplet_ind, Direction direction)
      : vec_(std::move(vec)),
        label_triplet_(std::move(label_triplet)),
        prop_names_(prop_names),
        label_triplet_ind_(std::move(label_triplet_ind)),
        direction_(direction) {
    CHECK(label_triplet_ind_.size() == vec_.size());
    CHECK(prop_names_.size() == label_triplet_.size());
    uint8_t max_ind = 0;
    for (size_t i = 0; i < label_triplet_ind_.size(); ++i) {
      max_ind = std::max(max_ind, label_triplet_ind_[i]);
    }
    CHECK(max_ind < label_triplet_.size())
        << "max_ind: " << max_ind
        << ", label_triplet_.size(): " << label_triplet_.size();
  }

  iterator begin() const {
    return iterator(vec_, 0, label_triplet_ind_, label_triplet_, prop_names_);
  }

  iterator end() const {
    return iterator(vec_, vec_.size(), label_triplet_ind_, label_triplet_,
                    prop_names_);
  }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    res.reserve(vec_.size());
    for (size_t i = 0; i < vec_.size(); ++i) {
      auto ind = label_triplet_ind_[i];
      res.emplace_back(label_triplet_[ind][2]);
    }
    return res;
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<ele_tuple_t> res;
    std::vector<uint8_t> label_triplet_ind;
    res.reserve(index_ele_tuple.size());
    label_triplet_ind.reserve(index_ele_tuple.size());
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      res.emplace_back(std::get<1>(cur_ind_ele));
      label_triplet_ind.emplace_back(
          label_triplet_ind_[std::get<0>(cur_ind_ele)]);
    }
    auto copied_label_triplet = label_triplet_;
    return FlatEdgeSet(std::move(res), std::move(copied_label_triplet),
                       prop_names_, std::move(label_triplet_ind), direction_);
  }

  void fillBuiltinPropsImpl(std::vector<EDATA_T>& tuples,
                            const std::vector<std::string>& prop_names,
                            const std::vector<size_t>& repeat_array) {
    // Make sure this is correct.
    std::vector<bool> is_built_in(prop_names_.size(), false);
    for (size_t i = 0; i < prop_names_.size(); ++i) {
      if (prop_names_[i].size() == 1 && prop_names_[i][0] == prop_names[0]) {
        is_built_in[i] = true;
      }
    }

    VLOG(10) << "Found built-in property" << prop_names[0];
    CHECK(repeat_array.size() == Size());
    size_t cur_ind = 0;
    for (size_t i = 0; i < vec_.size(); ++i) {
      auto cur_label_ind = label_triplet_ind_[i];

      auto repeat_times = repeat_array[i];
      if (!is_built_in[cur_label_ind]) {
        for (size_t j = 0; j < repeat_times; ++j) {
          cur_ind += 1;
        }
      } else {
        for (size_t j = 0; j < repeat_times; ++j) {
          CHECK(cur_ind < tuples.size());
          std::get<0>(tuples[cur_ind]) = std::get<0>(std::get<2>(vec_[i]));
          cur_ind += 1;
        }
      }
    }
  }

  void fillBuiltinProps(std::vector<EDATA_T>& tuples,
                        const PropNameArray<EDATA_T>& prop_names,
                        const std::vector<size_t>& repeat_array) {
    auto vec = array_to_vec(prop_names);
    fillBuiltinPropsImpl(tuples, vec, repeat_array);
  }

  // fill builtin props without repeat array.
  void fillBuiltinProps(std::vector<EDATA_T>& tuples,
                        const PropNameArray<EDATA_T>& prop_names) {
    std::vector<size_t> repeat_array(vec_.size(), 1);
    auto vec = array_to_vec(prop_names);
    fillBuiltinPropsImpl(tuples, vec, repeat_array);
  }

  size_t Size() const { return vec_.size(); }

  template <size_t num_labels, typename FILTER_T>
  std::pair<GeneralVertexSet<VID_T, LabelT, grape::EmptyType>,
            std::vector<size_t>>
  GetVertices(VOpt v_opt, const std::array<LabelT, num_labels>& labels,
              FILTER_T& expr) const {
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    std::vector<label_t> tmp_labels = array_to_vec(labels);
    std::vector<label_t> req_labels;

    label_t max_label = 0;
    {
      std::unordered_set<LabelT> valid_label_set;
      for (size_t i = 0; i < label_triplet_.size(); ++i) {
        if (v_opt == VOpt::Start) {
          valid_label_set.emplace(label_triplet_[i][0]);
        } else if (v_opt == VOpt::End || v_opt == VOpt::Other) {
          valid_label_set.emplace(label_triplet_[i][1]);
        } else {
          LOG(FATAL) << "Unknown v_opt: " << v_opt;
        }
      }
      // if request labels are empty, we get all labels
      if (tmp_labels.size() == 0) {
        for (auto iter : valid_label_set) {
          tmp_labels.emplace_back(iter);
        }
      }
      // get labels both in valid_label_set and labels

      for (auto iter : tmp_labels) {
        if (valid_label_set.find(iter) != valid_label_set.end()) {
          req_labels.push_back(iter);
          max_label = std::max(max_label, iter);
        }
      }
      VLOG(10) << "req_labels size: " << req_labels.size()
               << ", query label size: " << tmp_labels.size();
    }

    std::vector<grape::Bitset> res_bitset(req_labels.size());
    for (size_t i = 0; i < res_bitset.size(); ++i) {
      res_bitset[i].init(Size());
    }
    std::vector<int32_t> label_to_ind(max_label + 1, -1);
    {
      for (size_t i = 0; i < req_labels.size(); ++i) {
        label_to_ind[i] = i;
      }
    }
    size_t cur_ind = 0;
    for (auto iter : *this) {
      label_t cur_label;
      VID_T cur_vid;
      if (v_opt == VOpt::Start) {
        cur_label = label_triplet_[iter.GetIndex()][0];
        cur_vid = iter.GetSrc();
      } else if (v_opt == VOpt::End || v_opt == VOpt::Other) {
        cur_label = label_triplet_[iter.GetIndex()][1];
        cur_vid = iter.GetDst();
      } else {
        LOG(FATAL) << "Unknown v_opt: " << v_opt;
      }
      auto ind = label_to_ind[cur_label];
      if (ind != -1) {
        res_bitset[ind].set_bit(cur_ind);
        vids.emplace_back(cur_vid);
      }
      offsets.emplace_back(vids.size());
    }
    // resize bitset
    for (size_t i = 0; i < res_bitset.size(); ++i) {
      res_bitset[i].resize(vids.size());
    }

    auto set =
        make_general_set(std::move(vids), req_labels, std::move(res_bitset));
    return std::make_pair(std::move(set), std::move(offsets));
  }

  // implement ProjectWithRepeatArray
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<ele_tuple_t> new_vec;
    std::vector<uint8_t> new_label_triplet_ind;
    size_t next_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[FlatEdgeSet] size: " << Size()
             << " Project self, next size: " << next_size;

    new_vec.reserve(next_size);
    new_label_triplet_ind.reserve(next_size);

    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        new_vec.emplace_back(vec_[i]);
        new_label_triplet_ind.emplace_back(label_triplet_ind_[i]);
      }
    }
    auto copied_label_triplet = label_triplet_;

    return self_type_t(std::move(new_vec), std::move(copied_label_triplet),
                       prop_names_, std::move(new_label_triplet_ind),
                       direction_);
  }

  void SubSetWithIndices(std::vector<size_t>& indices) {
    std::vector<ele_tuple_t> res_vec;
    std::vector<uint8_t> res_label_triplet_ind;
    res_vec.reserve(indices.size());
    res_label_triplet_ind.reserve(indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      res_vec.emplace_back(vec_[indices[i]]);
      res_label_triplet_ind.emplace_back(label_triplet_ind_[indices[i]]);
    }
    vec_.swap(res_vec);
    label_triplet_ind_.swap(res_label_triplet_ind);
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    std::vector<ele_tuple_t> res_vec;
    std::vector<uint8_t> res_label_triplet_ind;
    res_vec.reserve(repeat_vec.back());
    res_label_triplet_ind.reserve(repeat_vec.back());
    for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (size_t j = 0; j < times_to_repeat; ++j) {
        for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          res_vec.emplace_back(vec_[k]);
          res_label_triplet_ind.emplace_back(label_triplet_ind_[k]);
        }
      }
    }
    vec_.swap(res_vec);
    label_triplet_ind_.swap(res_label_triplet_ind);
  }

  builder_t CreateBuilder() const {
    return builder_t(label_triplet_, prop_names_, label_triplet_ind_,
                     direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::vector<std::array<label_t, 3>> label_triplet_;
  std::vector<std::vector<std::string>> prop_names_;
  std::vector<uint8_t> label_triplet_ind_;
  Direction direction_;
};

template <typename VID_T, typename LabelT, typename EDATA_T>
class SingleLabelEdgeSetBuilder {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, EDATA_T>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using result_t = SingleLabelEdgeSet<VID_T, LabelT, EDATA_T>;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;

  SingleLabelEdgeSetBuilder(const std::array<LabelT, 3>& label_triplet,
                            std::vector<std::string> prop_names,
                            Direction direction)
      : label_triplet_(label_triplet),
        prop_names_(prop_names),
        direction_(direction) {}

  // There could be null record.
  void Insert(const index_ele_tuple_t& tuple) {
    vec_.push_back(std::get<1>(tuple));
  }

  void Insert(const ele_tuple_t& tuple) { vec_.push_back(tuple); }

  result_t Build() {
    return result_t(std::move(vec_), std::move(label_triplet_), prop_names_,
                    direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::array<LabelT, 3> label_triplet_;
  std::vector<std::string> prop_names_;
  Direction direction_;
};

template <typename VID_T, typename LabelT, typename EDATA_T>
class SingleLabelEdgeSetIter {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, EDATA_T>;
  using self_type_t = SingleLabelEdgeSetIter<VID_T, LabelT, EDATA_T>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using data_tuple_t = ele_tuple_t;
  SingleLabelEdgeSetIter(const std::vector<ele_tuple_t>& vec, size_t ind,
                         const std::array<LabelT, 3>& label_triplet,
                         const std::vector<std::string>& prop_names)
      : vec_(vec),
        ind_(ind),
        label_triplet_(label_triplet),
        prop_names_(prop_names) {}

  ele_tuple_t GetElement() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetElement());
  }
  LabelT GetEdgeLabel() const { return label_triplet_[2]; }

  VID_T GetSrc() const { return std::get<0>(vec_[ind_]); }

  LabelT GetSrcLabel() const { return label_triplet_[0]; }

  VID_T GetDst() const { return std::get<1>(vec_[ind_]); }

  LabelT GetDstLabel() const { return label_triplet_[1]; }

  const EDATA_T& GetData() const { return std::get<2>(vec_[ind_]); }

  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

  size_t GetIndex() const { return ind_; }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
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

 private:
  const std::vector<ele_tuple_t>& vec_;
  size_t ind_;
  const std::array<LabelT, 3>& label_triplet_;
  const std::vector<std::string>& prop_names_;
};

// Define a single label triplet edge set.
template <typename VID_T, typename LabelT, typename EDATA_T>
class SingleLabelEdgeSet {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, EDATA_T>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using iterator = SingleLabelEdgeSetIter<VID_T, LabelT, EDATA_T>;
  using self_type_t = SingleLabelEdgeSet<VID_T, LabelT, EDATA_T>;
  using flat_t = self_type_t;
  using data_tuple_t = ele_tuple_t;
  using builder_t = SingleLabelEdgeSetBuilder<VID_T, LabelT, EDATA_T>;

  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_src = false;
  static constexpr bool is_multi_dst_label = false;

  SingleLabelEdgeSet(std::vector<ele_tuple_t>&& vec,
                     std::array<LabelT, 3>&& label_triplet,
                     std::vector<std::string> prop_names, Direction direction)
      : vec_(std::move(vec)),
        label_triplet_(std::move(label_triplet)),
        prop_names_(prop_names),
        direction_(direction) {}

  iterator begin() const {
    return iterator(vec_, 0, label_triplet_, prop_names_);
  }

  iterator end() const {
    return iterator(vec_, vec_.size(), label_triplet_, prop_names_);
  }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    res.reserve(vec_.size());
    for (size_t i = 0; i < vec_.size(); ++i) {
      res.emplace_back(label_triplet_[2]);
    }
    return res;
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<EDATA_T>>> res;
    res.reserve(index_ele_tuple.size());
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      res.emplace_back(std::get<1>(cur_ind_ele));
    }
    return SingleLabelEdgeSet(std::move(res), label_triplet_, prop_names_,
                              direction_);
  }

  // we assume edata_t is tuple
  void fillBuiltinPropsImpl(std::vector<EDATA_T>& tuples,
                            const std::vector<std::string>& prop_names,
                            const std::vector<size_t>& repeat_array) {
    CHECK(prop_names.size() == 1);
    CHECK(prop_names_.size() == 1);
    // Make sure this is correct.
    if (prop_names[0] == prop_names_[0]) {
      VLOG(10) << "Found built-in property" << prop_names[0];
      CHECK(repeat_array.size() == Size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < vec_.size(); ++i) {
        auto repeat_times = repeat_array[i];
        for (size_t j = 0; j < repeat_times; ++j) {
          CHECK(cur_ind < tuples.size());
          std::get<0>(tuples[cur_ind]) = std::get<0>(std::get<2>(vec_[i]));
          cur_ind += 1;
        }
      }
    }
  }

  void fillBuiltinProps(std::vector<EDATA_T>& tuples,
                        const PropNameArray<EDATA_T>& prop_names,
                        const std::vector<size_t>& repeat_array) {
    auto vec = array_to_vec(prop_names);
    fillBuiltinPropsImpl(tuples, vec, repeat_array);
  }

  // fill builtin props without repeat array.
  void fillBuiltinProps(std::vector<EDATA_T>& tuples,
                        const PropNameArray<EDATA_T>& prop_names) {
    std::vector<size_t> repeat_array(vec_.size(), 1);
    auto vec = array_to_vec(prop_names);
    fillBuiltinPropsImpl(tuples, vec, repeat_array);
  }

  size_t Size() const { return vec_.size(); }

  template <size_t num_labels, typename FILTER_T>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, const std::array<LabelT, num_labels>& labels,
              FILTER_T& expr) const {
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    std::vector<label_t> tmp_labels = array_to_vec(labels);
    std::vector<label_t> req_labels;

    label_t cur_label = 0;
    {
      if (v_opt == VOpt::Start) {
        cur_label = label_triplet_[0];
      } else if (v_opt == VOpt::End || v_opt == VOpt::Other) {
        cur_label = label_triplet_[1];
      } else {
        LOG(FATAL) << "Unknown v_opt: " << v_opt;
      }
    }
    if (tmp_labels.size() > 0 && std::find(tmp_labels.begin(), tmp_labels.end(),
                                           cur_label) == tmp_labels.end()) {
      for (size_t i = 0; i < vec_.size(); ++i) {
        offsets.emplace_back(0);
      }
      return std::make_pair(RowVertexSet<LabelT, VID_T, grape::EmptyType>(
                                std::move(vids), cur_label),
                            std::move(offsets));
    } else {
      for (auto iter : *this) {
        VID_T cur_vid;
        if (v_opt == VOpt::Start) {
          cur_vid = iter.GetSrc();
        } else if (v_opt == VOpt::End || v_opt == VOpt::Other) {
          cur_vid = iter.GetDst();
        } else {
          LOG(FATAL) << "Unknown v_opt: " << v_opt;
        }
        vids.emplace_back(cur_vid);
        offsets.emplace_back(vids.size());
      }

      auto set = make_default_row_vertex_set(std::move(vids), cur_label);
      return std::make_pair(std::move(set), std::move(offsets));
    }
  }

  // implement ProjectWithRepeatArray
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<ele_tuple_t> new_vec;
    size_t next_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[FlatEdgeSet] size: " << Size()
             << " Project self, next size: " << next_size;

    new_vec.reserve(next_size);

    for (size_t i = 0; i < repeat_array.size(); ++i) {
      for (size_t j = 0; j < repeat_array[i]; ++j) {
        new_vec.emplace_back(vec_[i]);
      }
    }

    auto copy_label_triplet = label_triplet_;
    return self_type_t(std::move(new_vec), std::move(copy_label_triplet),
                       prop_names_, direction_);
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    std::vector<ele_tuple_t> res_vec;
    res_vec.reserve(repeat_vec.back());
    for (size_t i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (size_t j = 0; j < times_to_repeat; ++j) {
        for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          res_vec.emplace_back(vec_[k]);
        }
      }
    }
    vec_.swap(res_vec);
  }

  void SubSetWithIndices(std::vector<size_t>& indices) {
    std::vector<ele_tuple_t> res_vec;
    res_vec.reserve(indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      res_vec.emplace_back(vec_[indices[i]]);
    }
    vec_.swap(res_vec);
  }

  builder_t CreateBuilder() const {
    return builder_t(label_triplet_, prop_names_, direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::array<label_t, 3> label_triplet_;
  std::vector<std::string> prop_names_;
  Direction direction_;
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_DS_MULTI_EDGE_SET_FLAT_EDGE_SET_H_