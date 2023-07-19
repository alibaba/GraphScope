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
#ifndef ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_GENERAL_EDGE_SET_H_
#define ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_GENERAL_EDGE_SET_H_

#include <array>
#include <vector>
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
namespace gs {

template <size_t N, typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSet;

template <size_t N, typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSetBuilder {};

template <typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, T...> {
 public:
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<T...>>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using res_t = FlatEdgeSet<VID_T, LabelT, 2, T...>;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = true;
  static constexpr bool is_two_label_set_builder = false;

  static constexpr size_t num_props = sizeof...(T);
  GeneralEdgeSetBuilder(size_t edge_size, const grape::Bitset& bitset,
                        std::array<std::string, num_props> prop_names,
                        LabelT edge_label, std::array<LabelT, 2> src_labels,
                        LabelT dst_label, Direction dir)
      : bitset_(bitset),
        prop_names_(prop_names),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        direction_(dir) {
    vec_.reserve(edge_size);
  }

  void Insert(const index_ele_tuple_t& tuple) {
    // TODO: support inserting null record.
    auto ind = std::get<0>(tuple);
    auto src = std::get<1>(tuple);
    auto adj_iter = std::get<2>(tuple);
    auto dst = adj_iter.neighbor();
    auto props = adj_iter.properties();
    vec_.emplace_back(src, dst, props);
    if (bitset_.get_bit(ind)) {
      label_vec_.emplace_back(src_labels_[0]);
    } else {
      label_vec_.emplace_back(src_labels_[1]);
    }
  }

  res_t Build() {
    return res_t(std::move(vec_), edge_label_, src_labels_, dst_label_,
                 prop_names_, std::move(label_vec_), direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::vector<LabelT> label_vec_;
  std::array<std::string, num_props> prop_names_;
  LabelT edge_label_;
  std::array<LabelT, 2> src_labels_;
  LabelT dst_label_;
  const grape::Bitset& bitset_;
  Direction direction_;
};

template <typename GI, typename VID_T, typename LabelT>
class GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, grape::EmptyType> {
 public:
  using adj_list_array_t = typename GI::template adj_list_array_t<>;
  using adj_list_t = typename GI::template adj_list_t<>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using res_t = FlatEdgeSet<VID_T, LabelT, 2, grape::EmptyType>;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = true;
  static constexpr bool is_two_label_set_builder = false;

  GeneralEdgeSetBuilder(size_t edge_size, const grape::Bitset& bitset,
                        LabelT edge_label, std::array<LabelT, 2> src_labels,
                        LabelT dst_label, Direction dir)
      : bitset_(bitset),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        direction_(dir) {
    vec_.reserve(edge_size);
  }

  void Insert(const index_ele_tuple_t& tuple) {
    // TODO: support inserting null record.
    auto ind = std::get<0>(tuple);
    auto src = std::get<1>(tuple);
    auto adj_iter = std::get<2>(tuple);
    auto dst = adj_iter.neighbor();
    auto props = adj_iter.properties();
    vec_.emplace_back(src, dst, props);
    if (bitset_.get_bit(ind)) {
      label_vec_.emplace_back(src_labels_[0]);
    } else {
      label_vec_.emplace_back(src_labels_[1]);
    }
  }

  res_t Build() {
    return res_t(std::move(vec_), edge_label_, src_labels_, dst_label_,
                 std::move(label_vec_), direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::vector<LabelT> label_vec_;
  LabelT edge_label_;
  std::array<LabelT, 2> src_labels_;
  LabelT dst_label_;
  const grape::Bitset& bitset_;
  Direction direction_;
};
template <typename GI, typename VID_T, typename... T>
class GeneralEdgeSetIter {
 public:
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iterator>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using self_type_t = GeneralEdgeSetIter<GI, VID_T, T...>;

  GeneralEdgeSetIter(const std::vector<VID_T>& vids,
                     const adj_list_array_t& adj_lists, size_t ind)
      : vids_(vids), adj_lists_(adj_lists), ind_(ind) {
    if (ind_ == 0) {
      probe_next_valid_adj();
    }
  }

  // copy constructor
  GeneralEdgeSetIter(const self_type_t& other)
      : vids_(other.vids_),
        adj_lists_(other.adj_lists_),
        ind_(other.ind_),
        cur_adj_list_(other.cur_adj_list_),
        begin_(other.begin_),
        end_(other.end_) {}

  inline VID_T GetSrc() const { return vids_[ind_]; }
  inline VID_T GetDst() const { return begin_.neighbor(); }
  inline const std::tuple<T...>& GetData() const { return begin_.properties(); }

  ele_tuple_t GetElement() const { return ele_tuple_t(GetSrc(), begin_); }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetSrc(), begin_);
  }

  VID_T GetVertex() const { return vids_[ind_]; }

  inline const self_type_t& operator++() {
    if (ind_ < vids_.size()) {
      ++begin_;
      if (begin_ == end_) {
        ++ind_;
        probe_next_valid_adj();
      }
    }
    return *this;
  }

  void probe_next_valid_adj() {
    while (ind_ < vids_.size()) {
      cur_adj_list_ = adj_lists_.get(ind_);
      begin_ = cur_adj_list_.begin();
      end_ = cur_adj_list_.end();
      if (begin_ != end_) {
        break;
      }
      ind_ += 1;
    }
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
  const std::vector<VID_T>& vids_;
  const adj_list_array_t& adj_lists_;
  adj_list_t cur_adj_list_;
  adj_list_iterator begin_, end_;
  size_t ind_;
};

template <typename GI, typename VID_T>
class GeneralEdgeSetIter<GI, VID_T, grape::EmptyType> {
 public:
  using adj_list_array_t = typename GI::template adj_list_array_t<>;
  using adj_list_t = typename GI::template adj_list_t<>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iterator>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using self_type_t = GeneralEdgeSetIter<GI, VID_T, grape::EmptyType>;

  GeneralEdgeSetIter(const std::vector<VID_T>& vids,
                     const adj_list_array_t& adj_lists, size_t ind)
      : vids_(vids), adj_lists_(adj_lists), ind_(ind) {
    if (ind_ == 0) {
      probe_next_valid_adj();
    }
  }

  // copy constructor
  GeneralEdgeSetIter(const self_type_t& other)
      : vids_(other.vids_),
        adj_lists_(other.adj_lists_),
        ind_(other.ind_),
        cur_adj_list_(other.cur_adj_list_),
        begin_(other.begin_),
        end_(other.end_) {}

  inline VID_T GetSrc() const { return vids_[ind_]; }
  inline VID_T GetDst() const { return begin_.neighbor(); }
  inline const std::tuple<grape::EmptyType> GetData() const {
    return std::make_tuple(grape::EmptyType());
  }

  ele_tuple_t GetElement() const { return ele_tuple_t(GetSrc(), begin_); }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetSrc(), begin_);
  }

  VID_T GetVertex() const { return vids_[ind_]; }

  inline const self_type_t& operator++() {
    if (ind_ < vids_.size()) {
      ++begin_;
      if (begin_ == end_) {
        ++ind_;
        probe_next_valid_adj();
      }
    }
    return *this;
  }

  void probe_next_valid_adj() {
    while (ind_ < vids_.size()) {
      cur_adj_list_ = adj_lists_.get(ind_);
      begin_ = cur_adj_list_.begin();
      end_ = cur_adj_list_.end();
      if (begin_ != end_) {
        break;
      }
      ind_ += 1;
    }
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
  const std::vector<VID_T>& vids_;
  const adj_list_array_t& adj_lists_;
  adj_list_t cur_adj_list_;
  adj_list_iterator begin_, end_;
  size_t ind_;
};

template <size_t N, typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSet {
 public:
  static constexpr size_t num_src_labels = N;
  static constexpr size_t num_props = sizeof...(T);
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = false;
  using lid_t = VID_T;

  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;

  using iterator = GeneralEdgeSetIter<GI, VID_T, T...>;
  GeneralEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
                 std::array<grape::Bitset, N>&& bitsets,
                 const std::array<std::string, num_props>& prop_names,
                 LabelT edge_label,
                 std::array<LabelT, num_src_labels>&& src_labels,
                 LabelT dst_label)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        prop_names_(prop_names),
        edge_label_(edge_label),
        src_labels_(std::move(src_labels)),
        dst_label_(dst_label) {
    bitsets_.swap(bitsets);
  }

  GeneralEdgeSet(GeneralEdgeSet<N, GI, VID_T, LabelT, T...>&& other)
      : vids_(std::move(other.vids_)),
        adj_lists_(std::move(other.adj_lists_)),
        bitsets_(std::move(other.bitsets_)),
        prop_names_(other.prop_names_),
        edge_label_(other.edge_label_),
        src_labels_(std::move(other.src_labels_)),
        dst_label_(other.dst_label_) {}

  iterator begin() const { return iterator(vids_, adj_lists_, 0); }

  iterator end() const { return iterator(vids_, adj_lists_, vids_.size()); }

 private:
  LabelT edge_label_, dst_label_;
  std::array<LabelT, N> src_labels_;

  std::array<std::string, num_props> prop_names_;

  std::vector<VID_T> vids_;
  adj_list_array_t adj_lists_;
  std::array<grape::Bitset, N> bitsets_;
};

// general edge set stores multi src labels but only one dst label
// Which stores the nbr ptrs rather than edge.
template <typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSet<2, GI, VID_T, LabelT, T...> {
 public:
  static constexpr size_t num_src_labels = 2;
  static constexpr size_t num_props = sizeof...(T);
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = false;
  using lid_t = VID_T;
  using flat_t = FlatEdgeSet<VID_T, LabelT, 2, T...>;

  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iter_t>;

  using iterator = GeneralEdgeSetIter<GI, VID_T, T...>;
  using builder_t = GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, T...>;
  GeneralEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
                 grape::Bitset&& bitsets,
                 const std::array<std::string, num_props>& prop_names,
                 LabelT edge_label, std::array<LabelT, 2> src_labels,
                 LabelT dst_label, Direction dir)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        prop_names_(prop_names),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        size_(0),
        dir_(dir) {
    bitsets_.swap(bitsets);
  }

  GeneralEdgeSet(GeneralEdgeSet<2, GI, VID_T, LabelT, T...>&& other)
      : vids_(std::move(other.vids_)),
        adj_lists_(std::move(other.adj_lists_)),
        prop_names_(other.prop_names_),
        edge_label_(other.edge_label_),
        src_labels_(std::move(other.src_labels_)),
        dst_label_(other.dst_label_),
        size_(0),
        dir_(other.dir_) {
    bitsets_.swap(other.bitsets_);
  }

  iterator begin() const { return iterator(vids_, adj_lists_, 0); }

  iterator end() const { return iterator(vids_, adj_lists_, vids_.size()); }

  size_t Size() const {
    if (size_ == 0) {
      for (auto i = 0; i < adj_lists_.size(); ++i) {
        auto adj = adj_lists_.get(i);
        size_ += adj.size();
      }
    }
    return size_;
  }

  builder_t CreateBuilder() const {
    return builder_t(Size(), bitsets_, prop_names_, edge_label_, src_labels_,
                     dst_label_, dir_);
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<T...>>> res;
    res.reserve(index_ele_tuple.size());
    std::vector<LabelT> label_vec(index_ele_tuple.size(), (LabelT) 0);
    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      auto ind = std::get<0>(cur_ind_ele);
      auto nbr = std::get<2>(cur_ind_ele);
      res.emplace_back(std::make_tuple(std::get<1>(cur_ind_ele),
                                       nbr->neighbor(), nbr->properties()));
      if (!bitsets_.get_bit(ind)) {
        // label_vec[i] = 1;
        label_vec[i] = src_labels_[1];
      } else {
        label_vec[i] = src_labels_[0];
      }
    }
    return flat_t(std::move(res), edge_label_, src_labels_, dst_label_,
                  prop_names_, std::move(label_vec), dir_);
  }

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

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

 private:
  mutable size_t size_;
  LabelT edge_label_, dst_label_;
  std::array<LabelT, 2> src_labels_;

  std::array<std::string, num_props> prop_names_;

  std::vector<VID_T> vids_;
  adj_list_array_t adj_lists_;
  grape::Bitset bitsets_;  // bitset of src vertices.
  Direction dir_;
};

// general edge set stores multi src labels but only one dst label
template <typename GI, typename VID_T, typename LabelT>
class GeneralEdgeSet<2, GI, VID_T, LabelT, grape::EmptyType> {
 public:
  static constexpr size_t num_src_labels = 2;
  static constexpr size_t num_props = 0;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = false;
  using lid_t = VID_T;
  using flat_t = FlatEdgeSet<VID_T, LabelT, 2, grape::EmptyType>;

  using adj_list_t = typename GI::template adj_list_t<>;
  using adj_list_array_t = typename GI::template adj_list_array_t<>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iter_t>;

  using iterator = GeneralEdgeSetIter<GI, VID_T, grape::EmptyType>;
  using builder_t =
      GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, grape::EmptyType>;
  GeneralEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
                 grape::Bitset&& bitsets, LabelT edge_label,
                 std::array<LabelT, 2> src_labels, LabelT dst_label,
                 Direction dir)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        size_(0),
        dir_(dir) {
    bitsets_.swap(bitsets);
  }

  GeneralEdgeSet(GeneralEdgeSet<2, GI, VID_T, LabelT, grape::EmptyType>&& other)
      : vids_(std::move(other.vids_)),
        adj_lists_(std::move(other.adj_lists_)),
        edge_label_(other.edge_label_),
        src_labels_(std::move(other.src_labels_)),
        dst_label_(other.dst_label_),
        size_(0),
        dir_(other.dir_) {
    bitsets_.swap(other.bitsets_);
  }

  iterator begin() const { return iterator(vids_, adj_lists_, 0); }

  iterator end() const { return iterator(vids_, adj_lists_, vids_.size()); }

  size_t Size() const {
    if (size_ == 0) {
      for (auto i = 0; i < adj_lists_.size(); ++i) {
        auto adj = adj_lists_.get(i);
        size_ += adj.size();
      }
    }
    return size_;
  }

  builder_t CreateBuilder() const {
    return builder_t(Size(), bitsets_, edge_label_, src_labels_, dst_label_,
                     dir_);
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<VID_T, VID_T, grape::EmptyType>> res;
    res.reserve(index_ele_tuple.size());
    std::vector<LabelT> label_vec(index_ele_tuple.size(), (LabelT) 0);
    for (auto i = 0; i < index_ele_tuple.size(); ++i) {
      auto cur_ind_ele = std::get<col_ind>(index_ele_tuple[i]);
      auto ind = std::get<0>(cur_ind_ele);
      auto nbr = std::get<2>(cur_ind_ele);
      res.emplace_back(std::make_tuple(std::get<1>(cur_ind_ele),
                                       nbr->neighbor(), grape::EmptyType()));
      if (!bitsets_.get_bit(ind)) {
        // label_vec[i] = 1;
        label_vec[i] = src_labels_[1];
      } else {
        label_vec[i] = src_labels_[0];
      }
    }
    // TODO :better label vec
    // std::vector<int32_t> label_vec(res.size(), 0);
    // for (auto i = 0; i < bitsets_.size(); ++i) {
    //   if (!bitsets_.get_bit(i)) {
    //     label_vec[i] = 1;
    //   }
    // }
    return flat_t(std::move(res), edge_label_, src_labels_, dst_label_,
                  std::move(label_vec));
  }

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

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

 private:
  mutable size_t size_;
  LabelT edge_label_, dst_label_;
  std::array<LabelT, 2> src_labels_;

  std::vector<VID_T> vids_;
  adj_list_array_t adj_lists_;
  grape::Bitset bitsets_;  // bitset of src vertices.
  Direction dir_;
};
}  // namespace gs
#endif  // ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_GENERAL_EDGE_SET_H_