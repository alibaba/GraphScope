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
  using res_t = FlatEdgeSet<VID_T, LabelT, 1, EDATA_T...>;

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
    std::fill(label_vec.begin(), label_vec.end(), src_label_);
    return res_t(std::move(vec_), edge_label_, {src_label_}, dst_label_,
                 prop_names_, std::move(label_vec), direction_);
  }

 private:
  std::vector<res_ele_tuple_t> vec_;
  LabelT src_label_, dst_label_, edge_label_;
  std::array<std::string, sizeof...(EDATA_T)> prop_names_;
  Direction direction_;
};

template <typename GI, typename LabelT, typename VID_T>
class AdjEdgeSetBuilder<GI, LabelT, VID_T, grape::EmptyType> {
  using index_ele_tuple_t = std::tuple<size_t, VID_T, VID_T, grape::EmptyType>;
  using res_ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using res_t = FlatEdgeSet<VID_T, LabelT, 1, grape::EmptyType>;

 public:
  static constexpr bool is_adj_edge_set_builder = true;
  AdjEdgeSetBuilder(LabelT src_label, LabelT dst_label, LabelT edge_label,
                    Direction direc)
      : src_label_(src_label),
        dst_label_(dst_label),
        edge_label_(edge_label),
        direction_(direc) {}

  void Insert(const index_ele_tuple_t& tuple) {
    vec_.emplace_back(gs::remove_nth_element<0>(tuple));
  }

  res_t Build() {
    std::vector<LabelT> label_vec(vec_.size());
    std::fill(label_vec.begin(), label_vec.end(), src_label_);
    return res_t(std::move(vec_), edge_label_, {src_label_}, dst_label_,
                 std::move(label_vec), direction_);
  }

 private:
  std::vector<res_ele_tuple_t> vec_;
  LabelT src_label_, dst_label_, edge_label_;
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
                 const adj_list_array_t& adj_lists, size_t ind)
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
};

template <typename GI, typename VID_T>
class AdjEdgeSetIter<GI, VID_T, grape::EmptyType> {
 public:
  using self_type_t = AdjEdgeSetIter<GI, VID_T, grape::EmptyType>;
  using adj_t = typename GI::template adj_t<>;
  using adj_list_array_t = typename GI::template adj_list_array_t<>;
  using adj_list_t = typename GI::template adj_list_t<>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, VID_T, grape::EmptyType>;
  AdjEdgeSetIter(const std::vector<VID_T>& vids,
                 const adj_list_array_t& adj_lists, size_t ind)
      : vids_(vids), adj_lists_(adj_lists), ind_(ind) {
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
};

/// @brief Multi label edge set iter.
/// @tparam VID_T
/// @tparam ...T
template <size_t N, typename GI, typename VID_T, typename... T>
class MulLabelSrcGrootEdgeSetIter {
 public:
  using self_type_t = MulLabelSrcGrootEdgeSetIter<N, GI, VID_T, T...>;
  // using adj_t = typename GI::template adj_t<T...>;
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  // set_ind, inner_ind,
  using index_ele_tuple_t = std::tuple<size_t, size_t, ele_tuple_t>;
  MulLabelSrcGrootEdgeSetIter(const std::array<std::vector<VID_T>, N>& vids,
                              const std::array<std::vector<size_t>, N>& offsets,
                              const std::array<adj_list_array_t, N>& adj_lists,
                              size_t ind)
      : vids_(vids),
        offsets_(offsets),
        adj_lists_(adj_lists),
        cur_ind_(ind),
        set_ind_(0) {
    if (cur_ind_ == offsets[0].size() - 1) {
      VLOG(10) << "end iter";
      // begin_  =end_ = nullptr;
      // while(adj_lists_[cur_ind_].get)
    } else {
      for (auto i = 0; i < N; ++i) {
        local_ind_[i] = 0;
      }
      // begin_  =end_ = nullptr;
      VLOG(10) << "begin iter";
      probe_for_next();
    }
  }

  inline const self_type_t& operator++() {
    ++begin_;
    // ++local_ind_[set_ind_];
    probe_for_next();
    return *this;
  }

  void probe_for_next() {
    bool flag = false;
    if (begin_.valid() && end_.valid()) {
      if (begin_ != end_) {
        return;
      } else if (begin_ == end_) {
        ++local_ind_[set_ind_];
      }
    }

    while (cur_ind_ < offsets_[0].size() - 1) {
      while (set_ind_ < N) {
        // VLOG(10) << "probe for next: " << cur_ind_ << ", set_ind : " <<
        // set_ind_
        //          << "local ind: " << local_ind_[set_ind_]
        //          << ", range: " << offsets_[set_ind_][cur_ind_] << ","
        //          << offsets_[set_ind_][cur_ind_ + 1];
        if (local_ind_[set_ind_] >= offsets_[set_ind_][cur_ind_] &&
            local_ind_[set_ind_] < offsets_[set_ind_][cur_ind_ + 1]) {
          begin_ = adj_lists_[set_ind_].get(local_ind_[set_ind_]).begin();
          end_ = adj_lists_[set_ind_].get(local_ind_[set_ind_]).end();
          if (begin_ != end_) {
            flag = true;
            break;
          } else {
            ++local_ind_[set_ind_];
          }
          // set_ind_ += 1;
        } else {
          set_ind_ += 1;
        }
      }
      if (flag) {
        break;
      }
      set_ind_ = 0;
      cur_ind_ += 1;
    }
    if (cur_ind_ < offsets_[0].size() - 1) {
      // VLOG(10) << "found next: " << cur_ind_ << ", set_ind : " << set_ind_
      //          << "local ind: " << local_ind_[set_ind_] << ", "
      //          << begin_->neighbor() << ", "
      //          << vids_[set_ind_][local_ind_[set_ind_]];
    } else {
      VLOG(10) << "reach end" << cur_ind_;
      // begin_ = end_ = nullptr;
    }
    // int t;
    // std::cin >> t;
  }

  inline VID_T GetSrc() const { return vids_[set_ind_][local_ind_[set_ind_]]; }
  inline VID_T GetDst() const { return begin_.neighbor(); }
  inline const std::tuple<T...>& GetData() const { return begin_.properties(); }

  inline Edge<VID_T, T...> GetElement() const {
    return Edge<VID_T, T...>(GetSrc(), GetDst(), GetData());
  }

  inline bool operator==(const self_type_t& rhs) const {
    return cur_ind_ == rhs.cur_ind_;
  }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(set_ind_, local_ind_[set_ind_],
                           std::make_tuple(GetSrc(), begin_));
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
  const std::array<std::vector<VID_T>, N>& vids_;
  const std::array<std::vector<size_t>, N>& offsets_;
  const std::array<adj_list_array_t, N>& adj_lists_;
  size_t set_ind_;
  size_t cur_ind_;
  std::array<size_t, N> local_ind_;
  adj_list_iter_t begin_, end_;
};

template <size_t N, typename GI, typename VID_T>
class MulLabelSrcGrootEdgeSetIter<N, GI, VID_T, grape::EmptyType> {
 public:
  using self_type_t =
      MulLabelSrcGrootEdgeSetIter<N, GI, VID_T, grape::EmptyType>;
  // using adj_t = typename GI::template adj_t<>;
  using adj_list_array_t = typename GI::template adj_list_array_t<>;
  using adj_list_t = typename GI::template adj_list_t<>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  // set_ind, inner_ind,
  using index_ele_tuple_t = std::tuple<size_t, size_t, ele_tuple_t>;
  MulLabelSrcGrootEdgeSetIter(const std::array<std::vector<VID_T>, N>& vids,
                              const std::array<std::vector<size_t>, N>& offsets,
                              const std::array<adj_list_array_t, N>& adj_lists,
                              size_t ind)
      : vids_(vids),
        offsets_(offsets),
        adj_lists_(adj_lists),
        cur_ind_(ind),
        set_ind_(0),
        data_(std::make_tuple(grape::EmptyType())) {
    if (cur_ind_ == offsets[0].size() - 1) {
      VLOG(10) << "end iter";
      // while(adj_lists_[cur_ind_].get)
    } else {
      for (auto i = 0; i < N; ++i) {
        local_ind_[i] = 0;
      }
      VLOG(10) << "begin iter";
      probe_for_next();
    }
  }

  inline const self_type_t& operator++() {
    CHECK(begin_);
    ++begin_;
    // ++local_ind_[set_ind_];
    probe_for_next();
    return *this;
  }

  void probe_for_next() {
    bool flag = false;
    if (begin_.valid() && end_.valid()) {
      if (begin_ != end_) {
        return;
      } else if (begin_ == end_) {
        ++local_ind_[set_ind_];
      }
    }

    while (cur_ind_ < offsets_[0].size() - 1) {
      while (set_ind_ < N) {
        if (local_ind_[set_ind_] >= offsets_[set_ind_][cur_ind_] &&
            local_ind_[set_ind_] < offsets_[set_ind_][cur_ind_ + 1]) {
          begin_ = adj_lists_[set_ind_].get(local_ind_[set_ind_]).begin();
          end_ = adj_lists_[set_ind_].get(local_ind_[set_ind_]).end();
          if (begin_ != end_) {
            flag = true;
            break;
          } else {
            ++local_ind_[set_ind_];
          }
        } else {
          set_ind_ += 1;
        }
      }
      if (flag) {
        break;
      }
      set_ind_ = 0;
      cur_ind_ += 1;
    }
  }

  inline VID_T GetSrc() const { return vids_[set_ind_][local_ind_[set_ind_]]; }
  inline VID_T GetDst() const { return begin_.neighbor(); }
  inline const std::tuple<grape::EmptyType>& GetData() const { return data_; }

  inline Edge<VID_T, grape::EmptyType> GetElement() const {
    return Edge<VID_T, grape::EmptyType>(GetSrc(), GetDst(), GetData());
  }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(set_ind_, local_ind_[set_ind_],
                           std::make_tuple(GetSrc(), begin_));
  }

  inline bool operator==(const self_type_t& rhs) const {
    return cur_ind_ == rhs.cur_ind_;
  }

  inline bool operator!=(const self_type_t& rhs) const {
    // VLOG(10) << "judge: " << cur_ind_ << " vs: " << rhs.cur_ind_;
    return cur_ind_ != rhs.cur_ind_;
  }

  inline bool operator<(const self_type_t& rhs) const {
    return cur_ind_ < rhs.cur_ind_;
  }

  inline const self_type_t& operator*() const { return *this; }

  inline const self_type_t* operator->() const { return this; }

 private:
  const std::array<std::vector<VID_T>, N>& vids_;
  const std::array<std::vector<size_t>, N>& offsets_;
  const std::array<adj_list_array_t, N>& adj_lists_;
  size_t set_ind_;
  size_t cur_ind_;
  std::array<size_t, N> local_ind_;
  adj_list_iter_t begin_, end_;
  std::tuple<grape::EmptyType> data_;
};

template <typename GI, typename VID_T, typename LabelT, typename... EDATA_T>
class AdjEdgeSet {
 public:
  using iterator = AdjEdgeSetIter<GI, VID_T, EDATA_T...>;
  using self_type_t = AdjEdgeSet<GI, VID_T, LabelT, EDATA_T...>;
  using flat_t = FlatEdgeSet<VID_T, LabelT, 1, EDATA_T...>;
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
  static constexpr bool is_multi_dst_label = false;

  AdjEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
             LabelT edge_label, LabelT src_label, LabelT dst_label,
             std::array<LabelT, sizeof...(EDATA_T)> prop_names, Direction dir)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_label_(src_label),
        dst_label_(dst_label),
        prop_names_(prop_names),
        dir_(dir) {
    size_ = 0;
    for (auto i = 0; i < adj_lists_.size(); ++i) {
      size_ += adj_lists_.get(i).size();
    }
  }

  builder_t CreateBuilder() const {
    return builder_t(src_label_, dst_label_, edge_label_, prop_names_, dir_);
  }

  iterator begin() const { return iterator(vids_, adj_lists_, 0); }

  iterator end() const { return iterator(vids_, adj_lists_, vids_.size()); }

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
    auto set = MakeDefaultRowVertexSet(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>> res;
    res.reserve(index_ele_tuple.size());
    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      auto nbr = std::get<2>(cur_ind_ele);
      // auto ele = std::get<2>(cur_ind_ele);
      // auto nbr = std::get<1>(ele);
      res.emplace_back(std::make_tuple(std::get<1>(cur_ind_ele),
                                       nbr->neighbor(), nbr->properties()));
    }
    // TODO :better label vec
    std::vector<int32_t> label_vec(res.size(), src_label_);
    return flat_t(std::move(res), edge_label_, {src_label_}, dst_label_,
                  prop_names_, std::move(label_vec));
  }

  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names,
                        std::vector<offset_t>& repeat_array) {
    LOG(WARNING) << "No implemented";
  }

  // fill builtin props withour repeat array.
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
  std::array<LabelT, sizeof...(EDATA_T)> prop_names_;
  Direction dir_;
};

template <typename GI, typename VID_T, typename LabelT>
class AdjEdgeSet<GI, VID_T, LabelT, grape::EmptyType> {
 public:
  using iterator = AdjEdgeSetIter<GI, VID_T, grape::EmptyType>;
  using self_type_t = AdjEdgeSet<GI, VID_T, LabelT, grape::EmptyType>;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  using flat_t = FlatEdgeSet<VID_T, LabelT, 1, grape::EmptyType>;
  using adj_list_array_t = typename GI::template adj_list_array_t<>;
  using adj_list_t = typename GI::template adj_list_t<>;
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
    for (auto i = 0; i < adj_lists_.size(); ++i) {
      size_ += adj_lists_.get(i).size();
    }
  }

  builder_t CreateBuilder() const {
    return builder_t(src_label_, dst_label_, edge_label_, dir_);
  }

  iterator begin() const { return iterator(vids_, adj_lists_, 0); }

  iterator end() const { return iterator(vids_, adj_lists_, vids_.size()); }

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
    auto set = MakeDefaultRowVertexSet(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, grape::EmptyType>> res;
    res.reserve(index_ele_tuple.size());
    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
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

  // fill builtin props withour repeat array.
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
};

template <size_t N, typename GI, typename VID_T, typename LabelT,
          typename... EDATA_T>
class MulLabelSrcGrootEdgeSet {
 public:
  static constexpr size_t num_src_labels = N;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_src = true;
  using iterator = MulLabelSrcGrootEdgeSetIter<N, GI, VID_T, EDATA_T...>;
  using self_type_t = MulLabelSrcGrootEdgeSet<N, GI, VID_T, LabelT, EDATA_T...>;
  using data_tuple_t = std::tuple<EDATA_T...>;
  using adj_t = typename GI::template adj_t<EDATA_T...>;
  using adj_list_t = typename GI::template adj_list_t<EDATA_T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using adj_list_array_t = typename GI::template adj_list_array_t<EDATA_T...>;
  // src, nbr.
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  // set_ind, inner_ind,
  using index_ele_tuple_t = std::tuple<size_t, size_t, ele_tuple_t>;
  using flat_t = FlatEdgeSet<VID_T, LabelT, num_src_labels, EDATA_T...>;
  MulLabelSrcGrootEdgeSet(
      std::array<std::vector<VID_T>, N>&& vids,
      std::array<std::vector<size_t>, N>&& offsets,
      std::array<adj_list_array_t, N>&& adj_lists,
      std::array<std::string, sizeof...(EDATA_T)> prop_names, LabelT edge_label,
      std::array<LabelT, N> src_labels, LabelT dst_label)
      : vids_(std::move(vids)),
        offsets_(std::move(offsets)),
        adj_lists_(std::move(adj_lists)),
        prop_names_(prop_names),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label) {
    VLOG(10) << "Finish construction";
  }

  iterator begin() const {
    VLOG(10) << "create begin iter: 0 :  " << offsets_[0].size() - 1;
    return iterator(vids_, offsets_, adj_lists_, 0);
  }

  iterator end() const {
    // VLOG(10) << gs::to_string(offsets_[0]);
    VLOG(10) << "create end iter: 0 :  " << offsets_[0].size() - 1;
    return iterator(vids_, offsets_, adj_lists_, offsets_[0].size() - 1);
  }

  size_t Size() const {
    size_t size = 0;
    for (auto i = 0; i < N; ++i) {
      for (auto j = 0; j < adj_lists_[i].size(); ++j) {
        size += adj_lists_[i].get(j).size();
      }
    }
    return size;
  }

  size_t NumEdgesFromSrc(size_t i) const {
    CHECK(i < num_src_labels);
    size_t size = 0;
    for (auto j = 0; j < adj_lists_[i].size(); ++j) {
      size += adj_lists_[i].get(j).size();
    }
    return size;
  }

  template <typename EXPR, size_t num_labels>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<LabelT, num_labels>& labels,
              EXPR& expr) const {
    CHECK(v_opt == VOpt::End);
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    vids.reserve(Size());
    offsets.reserve(Size());
    offsets.emplace_back(0);
    for (auto iter : *this) {
      vids.emplace_back(iter.GetDst());
      offsets.emplace_back(vids.size());
    }
    auto set = MakeDefaultRowVertexSet(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  template <
      size_t InnerIs, size_t Is, typename... PropT,
      typename std::enable_if<(InnerIs == sizeof...(EDATA_T))>::type* = nullptr>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            std::string& prop_name,
                            std::vector<offset_t>& repeat_array) {}

  // TODO: make use of repeat array.
  template <
      size_t InnerIs, size_t Is, typename... PropT,
      typename std::enable_if<(InnerIs < sizeof...(EDATA_T))>::type* = nullptr>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            std::string& prop_name,
                            std::vector<offset_t>& repeat_array) {
    using inner_col_type =
        typename std::tuple_element_t<InnerIs, std::tuple<EDATA_T...>>;
    if constexpr (std::is_same_v<std::tuple_element_t<Is, std::tuple<PropT...>>,
                                 inner_col_type>) {
      if (prop_name == prop_names_[InnerIs]) {
        VLOG(10) << "Found builin property" << prop_names_[InnerIs];
        CHECK(repeat_array.size() == Size());
        size_t ind = 0;
        size_t prop_ind = 0;
        for (auto iter : *this) {
          auto repeat_times = repeat_array[ind];
          for (auto j = 0; j < repeat_times; ++j) {
            CHECK(prop_ind < tuples.size());
            std::get<Is>(tuples[prop_ind]) = std::get<InnerIs>(iter.GetData());
            prop_ind += 1;
          }
          ind += 1;
        }
      }
    } else {
      fillBuiltinPropsImpl<InnerIs + 1, Is, PropT...>(tuples, prop_name,
                                                      repeat_array);
    }
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<EDATA_T...>>> res;
    std::vector<int32_t> label_vec;
    res.reserve(index_ele_tuple.size());
    label_vec.reserve(index_ele_tuple.size());

    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      auto ele = std::get<2>(cur_ind_ele);
      auto nbr = std::get<1>(ele);
      res.emplace_back(std::make_tuple(std::get<0>(ele), nbr->neighbor(),
                                       nbr->properties()));
      label_vec.emplace_back(src_labels_[std::get<0>(cur_ind_ele)]);
    }
    return FlatEdgeSet(std::move(res), edge_label_, src_labels_, dst_label_,
                       prop_names_, std::move(label_vec));
  }

  template <size_t Is, typename... PropT>
  void fillBuiltinPropsImpl(std::vector<std::tuple<PropT...>>& tuples,
                            std::string& prop_name,
                            std::vector<offset_t>& repeat_array) {
    fillBuiltinPropsImpl<0, Is, PropT...>(tuples, prop_name, repeat_array);
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

  // fill builtin props withour repeat array.
  template <typename... PropT>
  void fillBuiltinProps(std::vector<std::tuple<PropT...>>& tuples,
                        PropNameArray<PropT...>& prop_names) {
    std::vector<offset_t> repeat_array(vids_.size(), 1);
    fillBuiltinPropsImpl(tuples, prop_names, repeat_array,
                         std::make_index_sequence<sizeof...(PropT)>());
  }

 private:
  std::array<std::vector<VID_T>, N> vids_;
  std::array<std::vector<size_t>, N> offsets_;
  LabelT edge_label_, dst_label_;
  std::array<LabelT, N> src_labels_;
  std::array<adj_list_array_t, N> adj_lists_;
  std::array<std::string, sizeof...(EDATA_T)> prop_names_;
};

template <size_t N, typename GI, typename VID_T, typename LabelT>
class MulLabelSrcGrootEdgeSet<N, GI, VID_T, LabelT, grape::EmptyType> {
 public:
  static constexpr size_t num_src_labels = N;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_multi_dst_label = false;
  using iterator = MulLabelSrcGrootEdgeSetIter<N, GI, VID_T, grape::EmptyType>;
  using self_type_t =
      MulLabelSrcGrootEdgeSet<N, GI, VID_T, LabelT, grape::EmptyType>;
  using flat_t = self_type_t;
  using data_tuple_t = std::tuple<grape::EmptyType>;
  using adj_t = typename GI::template adj_t<>;
  using adj_list_array_t = typename GI::template adj_list_array_t<>;

  MulLabelSrcGrootEdgeSet(std::array<std::vector<VID_T>, N>&& vids,
                          std::array<std::vector<size_t>, N>&& offsets,
                          adj_list_array_t&& adj_lists, LabelT edge_label,
                          std::array<LabelT, N> src_labels, LabelT dst_label)
      : vids_(std::move(vids)),
        offsets_(std::move(offsets)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label) {}

  iterator begin() const { return iterator(vids_, offsets_, adj_lists_, 0); }

  iterator end() const {
    return iterator(vids_, offsets_, adj_lists_, offsets_[0].size() - 1);
  }

  size_t Size() const {
    size_t size = 0;
    for (auto i = 0; i < N; ++i) {
      for (auto j = 0; j < adj_lists_[i].size(); ++j) {
        size += adj_lists_[i].get(j).size();
      }
    }
    return size;
  }

  template <typename EXPR, size_t num_labels>
  std::pair<RowVertexSet<LabelT, VID_T, grape::EmptyType>, std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<LabelT, num_labels>& labels,
              EXPR& expr) const {
    CHECK(v_opt == VOpt::End);
    std::vector<offset_t> offsets;
    std::vector<VID_T> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    for (auto iter : *this) {
      vids.emplace_back(iter.GetDst());
      offsets.emplace_back(vids.size());
    }
    auto set = MakeDefaultRowVertexSet(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

 private:
  std::array<std::vector<VID_T>, N> vids_;
  std::array<std::vector<size_t>, N> offsets_;
  LabelT edge_label_, dst_label_;
  std::array<LabelT, N> src_labels_;
  std::array<adj_list_array_t, N> adj_lists_;
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_DS_EDGE_MULTI_EDGE_SET_ADJ_EDGE_SET_H_
