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
#ifndef ENGINES_HQPS_ENGINE_DS_EDGE_MULTI_EDGE_SET_ADJ_EDGE_SET_H_
#define ENGINES_HQPS_ENGINE_DS_EDGE_MULTI_EDGE_SET_ADJ_EDGE_SET_H_

#include <array>
#include <string>
#include <tuple>
#include <vector>

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "grape/types.h"

namespace gs {

/**
 * @brief AdjEdgeSetBuilder Works for AdjEdgeSet,
 *
 * @tparam VID_T
 * @tparam LabelT
 * @tparam EDATA_T
 */
template <typename GI, typename LabelT, typename VID_T, typename... EDATA_T>
class AdjEdgeSetBuilder {
  using index_ele_tuple_t =
      std::tuple<size_t, VID_T, VID_T, std::tuple<EDATA_T...>>;
  using res_ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>;
  using res_t = FlatEdgeSet<VID_T, LabelT, std::tuple<EDATA_T...>>;

 public:
  static constexpr bool is_adj_edge_set_builder = true;
  AdjEdgeSetBuilder(LabelT src_label, LabelT dst_label, LabelT edge_label,
                    std::array<std::string, sizeof...(EDATA_T)> prop_names,
                    Direction direc)
      : src_label_(src_label),
        dst_label_(dst_label),
        edge_label_(edge_label),
        prop_names_(prop_names),
        direction_(direc) {}

  void Insert(const index_ele_tuple_t& tuple) {
    vec_.emplace_back(gs::remove_nth_element<0>(tuple));
  }

  res_t Build() {
    std::vector<LabelT> label_vec(vec_.size());
    std::fill(label_vec.begin(), label_vec.end(), 0);
    std::vector<std::array<label_t, 3>> triplets;
    triplets.emplace_back(src_label_, dst_label_, edge_label_);
    auto prop_names_vec = array_to_vec(prop_names_);
    return res_t(std::move(vec_), std::move(triplets), prop_names_vec,
                 std::move(label_vec), direction_);
  }

 private:
  std::vector<res_ele_tuple_t> vec_;
  LabelT src_label_, dst_label_, edge_label_;
  std::array<std::string, sizeof...(EDATA_T)> prop_names_;
  Direction direction_;
};

template <typename GI, typename VID_T, typename... EDATA_T>
class AdjEdgeSetIter {
 public:
  using self_type_t = AdjEdgeSetIter<GI, VID_T, EDATA_T...>;
  using adj_list_t = typename GI::template adj_list_t<EDATA_T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using adj_list_array_t = typename GI::template adj_list_array_t<EDATA_T...>;
  using ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>;
  using index_ele_tuple_t =
      std::tuple<size_t, VID_T, VID_T, std::tuple<EDATA_T...>>;
  AdjEdgeSetIter(const std::vector<VID_T>& vids,
                 const adj_list_array_t& adj_lists, size_t ind,
                 const std::vector<std::string>& prop_names)
      : vids_(vids), adj_lists_(adj_lists), ind_(ind) {
    if (ind_ == vids_.size()) {
      begin_ = adj_list_iter_t();
      end_ = adj_list_iter_t();
    } else {
      while (ind_ < vids_.size()) {
        auto cur_adj_list = adj_lists_.get(ind_);
        begin_ = cur_adj_list.begin();
        end_ = cur_adj_list.end();
        if (begin_ != end_) {
          break;
        }
        ind_ += 1;
      }
      if (ind_ < vids_.size()) {
        VLOG(10) << "Found first valid edge at: " << ind_;
      } else {
        begin_ = adj_list_iter_t();
        end_ = adj_list_iter_t();
      }
    }
  }

  inline VID_T GetSrc() const { return vids_[ind_]; }
  inline VID_T GetDst() const { return begin_.neighbor(); }
  inline const std::tuple<EDATA_T...> GetData() const {
    return begin_.properties();
  }

  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

  inline ele_tuple_t GetElement() const {
    return std::tuple{GetSrc(), GetDst(), GetData()};
  }

  inline index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetSrc(), GetDst(), GetData());
  }

  inline const self_type_t& operator++() {
    ++begin_;
    if (begin_ == end_) {
      ind_ += 1;
      while (ind_ < vids_.size()) {
        auto cur_adj_list = adj_lists_.get(ind_);
        begin_ = cur_adj_list.begin();
        end_ = cur_adj_list.end();
        if (begin_ != end_) {
          break;
        }
        ind_ += 1;
      }
      if (ind_ >= vids_.size()) {
        begin_ = adj_list_iter_t();
      }
    }
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
  const std::vector<VID_T>& vids_;
  const adj_list_array_t& adj_lists_;
  size_t ind_;
  adj_list_iter_t begin_, end_;
  const std::vector<std::string>& prop_names_;
};

template <typename GI, typename VID_T>
class AdjEdgeSetIter<GI, VID_T, grape::EmptyType> {
 public:
  using self_type_t = AdjEdgeSetIter<GI, VID_T, grape::EmptyType>;
  using adj_t = typename GI::template adj_t<>;
  using adj_list_array_t =
      typename GI::template adj_list_array_t<grape::EmptyType>;
  using adj_list_t = typename GI::template adj_list_t<grape::EmptyType>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, VID_T, grape::EmptyType>;
  AdjEdgeSetIter(const std::vector<VID_T>& vids,
                 const adj_list_array_t& adj_lists, size_t ind,
                 const std::vector<std::string>& prop_names)
      : vids_(vids), adj_lists_(adj_lists), ind_(ind), prop_names_(prop_names) {
    CHECK(prop_names_.size() == 0);
    if (ind_ == vids_.size()) {
      // begin_ = end_ = nullptr;
      begin_ = adj_list_iter_t();
      end_ = adj_list_iter_t();
    } else {
      while (ind_ < vids_.size()) {
        auto cur_adj_list = adj_lists_.get(ind_);
        begin_ = cur_adj_list.begin();
        end_ = cur_adj_list.end();
        if (begin_ != end_) {
          break;
        }
        ind_ += 1;
      }
      if (ind_ < vids_.size()) {
        VLOG(10) << "Found first valid edge at: " << ind_;
      } else {
        begin_ = adj_list_iter_t();
        end_ = adj_list_iter_t();
      }
    }
  }

  inline const self_type_t& operator++() {
    ++begin_;
    if (begin_ == end_) {
      ind_ += 1;
      while (ind_ < vids_.size()) {
        auto cur_adj_list = adj_lists_.get(ind_);
        begin_ = cur_adj_list.begin();
        end_ = cur_adj_list.end();
        if (begin_ != end_) {
          break;
        }
        ind_ += 1;
      }
      if (ind_ >= vids_.size()) {
        begin_ = adj_list_iter_t();
      }
    }
    return *this;
  }

  inline VID_T GetSrc() const { return vids_[ind_]; }
  inline VID_T GetDst() const { return begin_.neighbor(); }
  inline grape::EmptyType GetData() const { return grape::EmptyType(); }
  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

  inline ele_tuple_t GetElement() const {
    return std::tuple{GetSrc(), GetDst(), GetData()};
  }

  inline index_ele_tuple_t GetIndexElement() const {
    // TODO: consider direction.
    return std::make_tuple(ind_, GetSrc(), GetDst(), GetData());
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
  const std::vector<VID_T>& vids_;
  const adj_list_array_t& adj_lists_;
  size_t ind_;
  adj_list_iter_t begin_, end_;
  const std::vector<std::string>& prop_names_;
};

template <typename GI, typename VID_T, typename LabelT, typename... EDATA_T>
class AdjEdgeSet {
 public:
  using iterator = AdjEdgeSetIter<GI, VID_T, EDATA_T...>;
  using self_type_t = AdjEdgeSet<GI, VID_T, LabelT, EDATA_T...>;
  using flat_t = FlatEdgeSet<VID_T, LabelT, std::tuple<EDATA_T...>>;
  using data_tuple_t = std::tuple<EDATA_T...>;
  using adj_t = typename GI::template adj_t<EDATA_T...>;
  using adj_list_array_t = typename GI::template adj_list_array_t<EDATA_T...>;
  using adj_list_t = typename GI::template adj_list_t<EDATA_T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iter_t>;
  using flat_ele_t = index_ele_tuple_t;
  // a builder which can receive AdjEdgeSet's elements and build a flatEdgeSet.
  using builder_t = AdjEdgeSetBuilder<GI, VID_T, LabelT, EDATA_T...>;

  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;

  AdjEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
             LabelT edge_label, LabelT src_label, LabelT dst_label,
             std::vector<std::string> prop_names, Direction dir)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_label_(src_label),
        dst_label_(dst_label),
        prop_names_(prop_names),
        dir_(dir) {
    size_ = 0;
    for (size_t i = 0; i < adj_lists_.size(); ++i) {
      size_ += adj_lists_.get(i).size();
    }
  }

  builder_t CreateBuilder() const {
    return builder_t(src_label_, dst_label_, edge_label_, prop_names_, dir_);
  }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> label_vec(Size());
    std::fill(label_vec.begin(), label_vec.end(), {edge_label_});
    return label_vec;
  }

  iterator begin() const { return iterator(vids_, adj_lists_, 0, prop_names_); }

  iterator end() const {
    return iterator(vids_, adj_lists_, vids_.size(), prop_names_);
  }

  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

  template <typename EXPR, size_t num_labels>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<LabelT, num_labels>& labels,
              EXPR& expr) const {
    if (dir_ == Direction::In) {
      CHECK(v_opt == VOpt::Start || v_opt == VOpt::Other);
    } else if (dir_ == Direction::Out) {
      CHECK(v_opt == VOpt::End || v_opt == VOpt::Other);
    }
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    vids.reserve(Size());
    offsets.emplace_back(0);
    for (auto iter : *this) {
      vids.emplace_back(iter.GetDst());
      offsets.emplace_back(vids.size());
    }
    auto set = make_default_row_vertex_set(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>> res;
    res.reserve(index_ele_tuple.size());
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      auto nbr = std::get<2>(cur_ind_ele);
      // auto ele = std::get<2>(cur_ind_ele);
      // auto nbr = std::get<1>(ele);
      res.emplace_back(std::make_tuple(std::get<1>(cur_ind_ele),
                                       nbr->neighbor(), nbr->properties()));
    }
    // TODO :better label vec
    std::vector<int32_t> label_vec(res.size(), 0);
    return flat_t(std::move(res), edge_label_, {src_label_}, dst_label_,
                  {prop_names_}, std::move(label_vec));
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) {
    LOG(WARNING) << "No implemented";
  }

  // fill builtin props without repeat array.
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    LOG(WARNING) << "No implemented";
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

  size_t Size() const { return size_; }

 private:
  size_t size_;
  std::vector<VID_T> vids_;
  LabelT edge_label_, src_label_, dst_label_;
  adj_list_array_t adj_lists_;
  std::vector<std::string> prop_names_;
  Direction dir_;
};

template <typename GI, typename VID_T, typename LabelT>
class AdjEdgeSet<GI, VID_T, LabelT, grape::EmptyType> {
 public:
  using iterator = AdjEdgeSetIter<GI, VID_T, grape::EmptyType>;
  using self_type_t = AdjEdgeSet<GI, VID_T, LabelT, grape::EmptyType>;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  using flat_t = FlatEdgeSet<VID_T, LabelT, grape::EmptyType>;
  using adj_list_array_t =
      typename GI::template adj_list_array_t<grape::EmptyType>;
  using adj_list_t = typename GI::template adj_list_t<grape::EmptyType>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iter_t>;
  using flat_ele_t = index_ele_tuple_t;
  using builder_t = AdjEdgeSetBuilder<GI, VID_T, LabelT, grape::EmptyType>;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = false;
  AdjEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
             LabelT edge_label, LabelT src_label, LabelT dst_label,
             Direction dir)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_label_(src_label),
        dst_label_(dst_label),
        dir_(dir) {
    size_ = 0;
    for (size_t i = 0; i < adj_lists_.size(); ++i) {
      size_ += adj_lists_.get(i).size();
    }
  }

  builder_t CreateBuilder() const {
    return builder_t(src_label_, dst_label_, edge_label_, dir_);
  }

  iterator begin() const { return iterator(vids_, adj_lists_, 0, prop_names_); }

  iterator end() const {
    return iterator(vids_, adj_lists_, vids_.size(), prop_names_);
  }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> label_vec(Size());
    std::fill(label_vec.begin(), label_vec.end(), {edge_label_});
    return label_vec;
  }

  size_t Size() const { return size_; }

  template <typename EXPR, size_t num_labels>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<LabelT, num_labels>& labels,
              EXPR& expr) const {
    if (dir_ == Direction::In) {
      CHECK(v_opt == VOpt::Start || v_opt == VOpt::Other);
    } else if (dir_ == Direction::Out) {
      CHECK(v_opt == VOpt::End || v_opt == VOpt::Other);
    }
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    for (auto iter : *this) {
      vids.emplace_back(iter.GetDst());
      offsets.emplace_back(vids.size());
    }
    auto set = make_default_row_vertex_set(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, grape::EmptyType>> res;
    res.reserve(index_ele_tuple.size());
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      auto iter = std::get<2>(cur_ind_ele);
      auto src = std::get<1>(cur_ind_ele);
      res.emplace_back(
          std::make_tuple(src, iter->neighbor(), grape::EmptyType()));
    }
    return flat_t(std::move(res));
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) {
    LOG(WARNING) << "No implemented";
  }

  // fill builtin props without repeat array.
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    LOG(WARNING) << "No implemented";
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

 private:
  size_t size_;
  std::vector<VID_T> vids_;
  LabelT edge_label_, src_label_, dst_label_;
  adj_list_array_t adj_lists_;
  Direction dir_;
  std::vector<std::string> prop_names_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_DS_EDGE_MULTI_EDGE_SET_ADJ_EDGE_SET_H_
