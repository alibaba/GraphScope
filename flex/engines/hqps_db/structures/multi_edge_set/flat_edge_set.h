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
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"

#include "glog/logging.h"

namespace gs {

template <typename VID_T, typename LabelT, size_t N, typename... EDATA_T>
class FlatEdgeSet;

template <typename VID_T, typename LabelT, size_t N, typename... EDATA_T>
class FlatEdgeSetBuilder {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using result_t = FlatEdgeSet<VID_T, LabelT, N, EDATA_T...>;

  static constexpr bool is_flat_edge_set_builder = true;
  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_general_edge_set_builder = false;
  static constexpr bool is_two_label_set_builder = false;

  FlatEdgeSetBuilder(
      std::array<LabelT, N> src_labels, LabelT dst_label, LabelT edge_label,
      std::array<std::string, sizeof...(EDATA_T)> prop_names,
      const std::vector<LabelT>& label_vec,  // label_vec is needed to create
                                             // new label_vec with index_ele
      Direction direction)
      : src_labels_(src_labels),
        dst_label_(dst_label),
        edge_label_(edge_label),
        prop_names_(prop_names),
        label_vec_(label_vec),
        direction_(direction) {}

  // There could be null record.
  void Insert(const index_ele_tuple_t& tuple) {
    vec_.push_back(std::get<1>(tuple));
    if (!IsNull(std::get<1>(tuple))) {
      label_vec_new_.push_back(label_vec_[std::get<0>(tuple)]);
    } else {
      label_vec_new_.push_back(NullRecordCreator<LabelT>::GetNull());
    }
  }

  result_t Build() {
    return result_t(std::move(vec_), edge_label_, src_labels_, dst_label_,
                    prop_names_, std::move(label_vec_new_), direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::array<LabelT, N> src_labels_;
  LabelT dst_label_;
  LabelT edge_label_;
  std::array<std::string, sizeof...(EDATA_T)> prop_names_;
  std::vector<LabelT> label_vec_;
  std::vector<LabelT> label_vec_new_;
  Direction direction_;
};

template <typename VID_T, typename... EDATA_T>
class FlatEdgeSetIter {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>;
  using self_type_t = FlatEdgeSetIter<VID_T, EDATA_T...>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using data_tuple_t = ele_tuple_t;
  FlatEdgeSetIter(const std::vector<ele_tuple_t>& vec, size_t ind)
      : vec_(vec), ind_(ind) {}

  ele_tuple_t GetElement() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetElement());
  }

  VID_T GetSrc() const { return std::get<0>(vec_[ind_]); }

  VID_T GetDst() const { return std::get<1>(vec_[ind_]); }

  const std::tuple<EDATA_T...>& GetData() const {
    return std::get<2>(vec_[ind_]);
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
  size_t ind_;
};
template <typename VID_T>
class FlatEdgeSetIter<VID_T, grape::EmptyType> {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using self_type_t = FlatEdgeSetIter<VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using data_tuple_t = ele_tuple_t;
  FlatEdgeSetIter(const std::vector<ele_tuple_t>& vec, size_t ind)
      : vec_(vec), ind_(ind) {}

  ele_tuple_t GetElement() const { return vec_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetElement());
  }

  VID_T GetSrc() const { return std::get<0>(vec_[ind_]); }

  VID_T GetDst() const { return std::get<1>(vec_[ind_]); }

  const std::tuple<grape::EmptyType>& GetData() const {
    return std::get<2>(vec_[ind_]);
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
  size_t ind_;
};

template <typename VID_T, typename LabelT, size_t N, typename... EDATA_T>
class FlatEdgeSet {
 public:
  using ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using iterator = FlatEdgeSetIter<VID_T, EDATA_T...>;
  using self_type_t = FlatEdgeSet<VID_T, LabelT, N, EDATA_T...>;
  using flat_t = self_type_t;
  using data_tuple_t = ele_tuple_t;
  using builder_t = FlatEdgeSetBuilder<VID_T, LabelT, N, EDATA_T...>;

  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_src = false;
  static constexpr bool is_multi_dst_label = false;

  FlatEdgeSet(std::vector<ele_tuple_t>&& vec, LabelT edge_label,
              std::array<LabelT, N> src_labels, LabelT dst_label,
              std::array<std::string, sizeof...(EDATA_T)> prop_names,
              std::vector<LabelT>&& label_vec, Direction direction)
      : vec_(std::move(vec)),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        prop_names_(prop_names),
        label_vec_(std::move(label_vec)),
        direction_(direction) {
    CHECK(label_vec_.size() == vec_.size());
  }

  iterator begin() const { return iterator(vec_, 0); }

  iterator end() const { return iterator(vec_, vec_.size()); }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>> res;
    std::vector<LabelT> label_vec;
    res.reserve(index_ele_tuple.size());
    label_vec.reserve(index_ele_tuple.size());
    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      res.emplace_back(std::get<1>(cur_ind_ele));
      label_vec.emplace_back(label_vec_[std::get<0>(cur_ind_ele)]);
    }
    return FlatEdgeSet(std::move(res), edge_label_, src_labels_, dst_label_,
                       prop_names_, std::move(label_vec), direction_);
  }

  template <
      size_t InnerIs, size_t Is, typename... PropT,
      typename std::enable_if<(InnerIs == sizeof...(EDATA_T))>::type* = nullptr>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            const std::string& prop_name,
                            const std::vector<size_t>& repeat_array) {}

  template <
      size_t InnerIs, size_t Is, typename... PropT,
      typename std::enable_if<(InnerIs < sizeof...(EDATA_T))>::type* = nullptr>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            const std::string& prop_name,
                            const std::vector<size_t>& repeat_array) {
    using inner_col_type =
        typename std::tuple_element_t<InnerIs, std::tuple<EDATA_T...>>;
    if constexpr (std::is_same_v<std::tuple_element_t<Is, std::tuple<PropT...>>,
                                 inner_col_type>) {
      if (prop_name == prop_names_[InnerIs]) {
        VLOG(10) << "Found builin property" << prop_names_[InnerIs];
        CHECK(repeat_array.size() == Size());
        size_t prop_ind = 0;
        for (auto i = 0; i < vec_.size(); ++i) {
          auto repeat_times = repeat_array[i];
          for (auto j = 0; j < repeat_times; ++j) {
            CHECK(prop_ind < tuples.size());
            std::get<Is>(tuples[prop_ind]) =
                std::get<InnerIs>(std::get<2>(vec_[i]));
            prop_ind += 1;
          }
        }
      }
    } else {
      fillBuiltinPropsImpl<InnerIs + 1, Is, PropT...>(tuples, prop_name,
                                                      repeat_array);
    }
  }

  template <size_t Is, typename... PropT>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            const std::string& prop_name,
                            const std::vector<size_t>& repeat_array) {
    fillBuiltinPropsImpl<0, Is, PropT...>(tuples, prop_name, repeat_array);
  }

  template <typename... PropT, size_t... Is>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            const PropNameArray<PropT...>& prop_names,
                            const std::vector<size_t>& repeat_array,
                            std::index_sequence<Is...>) {
    (fillBuiltinPropsImpl<Is, PropT...>(tuples, std::get<Is>(prop_names),
                                        repeat_array),
     ...);
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names,
                        const std::vector<size_t>& repeat_array) {
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  // fill builtin props without repeat array.
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        const PropNameArray<PropT...>& prop_names) {
    std::vector<size_t> repeat_array(vec_.size(), 1);
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  size_t Size() const { return vec_.size(); }

  template <typename EXPR, size_t num_labels>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<LabelT, num_labels>& labels,
              EXPR& expr) const {
    CHECK(check_edge_dir_consist_vopt(direction_, v_opt));
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    // TODO: check labels.
    bool flag = false;
    for (auto l : labels) {
      if (l == dst_label_) {
        flag = true;
      }
    }
    if (flag) {
      for (auto iter : *this) {
        vids.emplace_back(iter.GetDst());
        offsets.emplace_back(vids.size());
      }
    } else {
      size_t size = Size();
      for (auto i = 0; i < size; ++i) {
        offsets.emplace_back(0);
      }
    }
    auto set = MakeDefaultRowVertexSet(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  // implement ProjectWithRepeatArray
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  self_type_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                     KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<ele_tuple_t> new_vec;
    std::vector<LabelT> new_label_vec;
    size_t next_size = 0;
    for (auto i = 0; i < repeat_array.size(); ++i) {
      next_size += repeat_array[i];
    }
    VLOG(10) << "[FlatEdgeSet] size: " << Size()
             << " Project self, next size: " << next_size;

    new_vec.reserve(next_size);
    new_label_vec.reserve(next_size);

    for (auto i = 0; i < repeat_array.size(); ++i) {
      for (auto j = 0; j < repeat_array[i]; ++j) {
        new_vec.emplace_back(vec_[i]);
        new_label_vec.emplace_back(label_vec_[i]);
      }
    }

    return self_type_t(std::move(new_vec), edge_label_, src_labels_, dst_label_,
                       prop_names_, std::move(new_label_vec), direction_);
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    CHECK(cur_offset.size() == repeat_vec.size());
    std::vector<ele_tuple_t> res_vec;
    std::vector<LabelT> res_label_vec;
    res_vec.reserve(repeat_vec.back());
    res_label_vec.reserve(repeat_vec.back());
    for (auto i = 0; i + 1 < cur_offset.size(); ++i) {
      auto times_to_repeat = repeat_vec[i + 1] - repeat_vec[i];
      for (auto j = 0; j < times_to_repeat; ++j) {
        for (auto k = cur_offset[i]; k < cur_offset[i + 1]; ++k) {
          res_vec.emplace_back(vec_[k]);
          res_label_vec.emplace_back(label_vec_[k]);
        }
      }
    }
    vec_.swap(res_vec);
    label_vec_.swap(res_label_vec);
  }

  builder_t CreateBuilder() const {
    return builder_t(src_labels_, dst_label_, edge_label_, prop_names_,
                     label_vec_, direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::array<LabelT, N> src_labels_;
  LabelT dst_label_, edge_label_;
  std::array<std::string, sizeof...(EDATA_T)> prop_names_;
  std::vector<LabelT> label_vec_;
  Direction direction_;
};

template <typename VID_T, size_t N, typename LabelT>
class FlatEdgeSet<VID_T, LabelT, N, grape::EmptyType> {
 public:
  // TODO: use std::tuple<VID_T,VID_T> is enough
  using ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, ele_tuple_t>;
  using iterator = FlatEdgeSetIter<VID_T, grape::EmptyType>;
  using self_type_t = FlatEdgeSet<VID_T, LabelT, N, grape::EmptyType>;
  using flat_t = self_type_t;
  using data_tuple_t = ele_tuple_t;

  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_src = false;
  static constexpr bool is_multi_dst_label = false;

  FlatEdgeSet(std::vector<ele_tuple_t>&& vec, LabelT edge_label,
              std::array<LabelT, N> src_labels, LabelT dst_label,
              std::vector<LabelT>&& label_vec, Direction& dire)
      : vec_(std::move(vec)),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        label_vec_(std::move(label_vec)),
        direction_(dire) {
    CHECK(label_vec_.size() == vec_.size());
  }

  iterator begin() const { return iterator(vec_, 0); }

  iterator end() const { return iterator(vec_, vec_.size()); }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, grape::EmptyType>> res;
    std::vector<int32_t> label_vec;
    res.reserve(index_ele_tuple.size());
    label_vec.reserve(index_ele_tuple.size());
    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      res.push_back(std::get<1>(cur_ind_ele));
      label_vec.push_back(label_vec_[std::get<0>(cur_ind_ele)]);
    }
    return FlatEdgeSet(std::move(res), edge_label_, src_labels_, dst_label_,
                       std::move(label_vec));
  }
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<size_t>& repeat_array) {
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  // fill builtin props withour repeat array.
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    std::vector<size_t> repeat_array(vec_.size(), 1);
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

  template <typename EXPR, size_t num_labels>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<LabelT, num_labels>& labels,
              EXPR& expr) const {
    // We only contains one label for dst vertices.
    CHECK(v_opt == VOpt::End);
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    // TODO: check labels.
    bool flag = false;
    for (auto l : labels) {
      if (l == dst_label_) {
        flag = true;
      }
    }
    if (flag) {
      for (auto iter : *this) {
        vids.emplace_back(iter.GetDst());
        offsets.emplace_back(vids.size());
      }
    } else {
      size_t size = Size();
      for (auto i = 0; i < size; ++i) {
        offsets.emplace_back(0);
      }
    }
    auto set = MakeDefaultRowVertexSet(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  size_t Size() const { return vec_.size(); }

 private:
  std::vector<ele_tuple_t> vec_;
  std::array<LabelT, N> src_labels_;
  LabelT dst_label_, edge_label_;
  std::vector<LabelT> label_vec_;
  Direction direction_;
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_DS_MULTI_EDGE_SET_FLAT_EDGE_SET_H_