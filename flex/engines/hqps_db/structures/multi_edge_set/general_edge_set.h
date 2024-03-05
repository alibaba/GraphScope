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

template <size_t edge_label_num, typename GI, typename VID_T, typename LabelT,
          typename... EDGE_PROP_TUPLE>
class GeneralEdgeSet;

template <size_t edge_label_num, typename GI, typename VID_T, typename LabelT,
          typename... EDGE_PROP_TUPLE>
class GeneralEdgeSetBuilder {};

// specialization for 2 labels, with column types the same
template <typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, std::tuple<T...>,
                            std::tuple<T...>> {
 public:
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, VID_T, std::tuple<T...>>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using res_t = FlatEdgeSet<VID_T, LabelT, std::tuple<T...>>;

  static constexpr bool is_row_vertex_set_builder = false;
  static constexpr bool is_flat_edge_set_builder = false;
  static constexpr bool is_general_edge_set_builder = true;
  static constexpr bool is_two_label_set_builder = false;

  static constexpr size_t num_props = sizeof...(T);
  GeneralEdgeSetBuilder(size_t edge_size, const grape::Bitset& bitset,
                        std::vector<std::string> prop_names,
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
      label_vec_.emplace_back(0);
    } else {
      label_vec_.emplace_back(1);
    }
  }

  res_t Build() {
    std::vector<std::array<LabelT, 3>> label_triplets;
    label_triplets.emplace_back(
        std::array<LabelT, 3>{src_labels_[0], dst_label_, dst_label_});
    label_triplets.emplace_back(
        std::array<LabelT, 3>{src_labels_[1], dst_label_, dst_label_});
    std::vector<std::vector<std::string>> prop_names;
    prop_names.emplace_back(prop_names_);
    prop_names.emplace_back(prop_names_);
    return res_t(std::move(vec_), std::move(label_triplets), prop_names,
                 std::move(label_vec_), direction_);
  }

 private:
  std::vector<ele_tuple_t> vec_;
  std::vector<LabelT> label_vec_;
  std::vector<std::string> prop_names_;
  LabelT edge_label_;
  std::array<LabelT, 2> src_labels_;
  LabelT dst_label_;
  const grape::Bitset& bitset_;
  Direction direction_;
};

template <typename GI, typename VID_T, typename LabelT>
class GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, std::tuple<grape::EmptyType>,
                            std::tuple<grape::EmptyType>> {
 public:
  using adj_list_array_t =
      typename GI::template adj_list_array_t<grape::EmptyType>;
  using adj_list_t = typename GI::template adj_list_t<grape::EmptyType>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, VID_T, grape::EmptyType>;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using res_t = FlatEdgeSet<VID_T, LabelT, std::tuple<grape::EmptyType>>;

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
template <typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSetIter {
 public:
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iterator>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using self_type_t = GeneralEdgeSetIter<GI, VID_T, LabelT, T...>;

  GeneralEdgeSetIter(const std::vector<VID_T>& vids,
                     const adj_list_array_t& adj_lists,
                     const grape::Bitset& bitsets,
                     const std::array<LabelT, 2>& src_labels, LabelT dst_label,
                     LabelT edge_label, size_t ind)
      : vids_(vids),
        adj_lists_(adj_lists),
        bitsets_(bitsets),
        src_labels_(src_labels),
        dst_label_(dst_label),
        edge_label_(edge_label),
        ind_(ind) {
    if (ind_ == 0) {
      probe_next_valid_adj();
    }
  }

  // copy constructor
  GeneralEdgeSetIter(const self_type_t& other)
      : vids_(other.vids_),
        adj_lists_(other.adj_lists_),
        bitsets_(other.bitsets_),
        src_labels_(other.src_labels_),
        dst_label_(other.dst_label_),
        edge_label_(other.edge_label_),
        ind_(other.ind_),
        cur_adj_list_(other.cur_adj_list_),
        begin_(other.begin_),
        end_(other.end_) {}

  inline LabelT GetEdgeLabel() const { return edge_label_; }

  inline VID_T GetSrc() const { return vids_[ind_]; }

  inline LabelT GetSrcLabel() const {
    if (bitsets_.get_bit(ind_)) {
      return src_labels_[0];
    } else {
      return src_labels_[1];
    }
  }

  inline VID_T GetDst() const { return begin_.neighbor(); }

  inline LabelT GetDstLabel() const { return dst_label_; }

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
  const grape::Bitset& bitsets_;
  adj_list_iterator begin_, end_;
  const std::array<LabelT, 2>& src_labels_;
  LabelT dst_label_;
  LabelT edge_label_;
  size_t ind_;
};

template <typename GI, typename VID_T, typename LabelT>
class GeneralEdgeSetIter<GI, VID_T, LabelT, grape::EmptyType> {
 public:
  using adj_list_array_t =
      typename GI::template adj_list_array_t<grape::EmptyType>;
  using adj_list_t = typename GI::template adj_list_t<grape::EmptyType>;
  using adj_list_iterator = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iterator>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iterator>;
  using self_type_t = GeneralEdgeSetIter<GI, VID_T, LabelT, grape::EmptyType>;

  GeneralEdgeSetIter(const std::vector<VID_T>& vids,
                     const adj_list_array_t& adj_lists,
                     const grape::Bitset& bitsets,
                     const std::array<LabelT, 2>& src_labels, LabelT dst_label,
                     LabelT edge_label, size_t ind)
      : vids_(vids),
        adj_lists_(adj_lists),
        bitsets_(bitsets),
        src_labels_(src_labels),
        dst_label_(dst_label),
        edge_label_(edge_label),
        ind_(ind) {
    if (ind_ == 0) {
      probe_next_valid_adj();
    }
  }

  // copy constructor
  GeneralEdgeSetIter(const self_type_t& other)
      : vids_(other.vids_),
        adj_lists_(other.adj_lists_),
        bitsets_(other.bitsets_),
        src_labels_(other.src_labels_),
        dst_label_(other.dst_label_),
        edge_label_(other.edge_label_),
        ind_(other.ind_),
        cur_adj_list_(other.cur_adj_list_),
        begin_(other.begin_),
        end_(other.end_) {}

  inline LabelT GetEdgeLabel() const { return edge_label_; }

  inline VID_T GetSrc() const { return vids_[ind_]; }

  inline LabelT GetSrcLabel() const {
    if (bitsets_.get_bit(ind_)) {
      return src_labels_[0];
    } else {
      return src_labels_[1];
    }
  }

  inline VID_T GetDst() const { return begin_.neighbor(); }

  inline LabelT GetDstLabel() const { return dst_label_; }

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
  const grape::Bitset& bitsets_;
  const std::array<LabelT, 2>& src_labels_;
  LabelT dst_label_;
  LabelT edge_label_;
  adj_list_t cur_adj_list_;
  adj_list_iterator begin_, end_;
  size_t ind_;
};

template <size_t edge_label_num, typename GI, typename VID_T, typename LabelT,
          typename... PROP_TUPLE>
class GeneralEdgeSet {};
//  public:
//   static constexpr size_t num_src_labels = edge_label_num;
//   static constexpr bool is_edge_set = true;
//   static constexpr bool is_multi_dst_label = false;
//   using lid_t = VID_T;
//   static_assert(edge_label_num == sizeof...(PROP_TUPLE),
//                 "The number of edge labels should be equal to the number of "
//                 "property tuples.");

//   using adj_list_array_tuple_t =
//       std::tuple<typename GetAdjListArrayT<GI, PROP_TUPLE>::type...>;

//   GeneralEdgeSet(
//       std::array<std::vector<VID_T>, edge_label_num>&& vids,
//       adj_list_array_tuple_t&& adj_lists,
//       std::tuple<PropNameArray<PROP_TUPLE>...>& prop_names,
//       std::array<std::array<LabelT, 3>, edge_label_num> edge_label_triplet)
//       : vids_(std::move(vids)),
//         adj_lists_(std::move(adj_lists)),
//         prop_names_(prop_names),
//         edge_label_triplet_(edge_label_triplet) {}

//   GeneralEdgeSet(
//       GeneralEdgeSet<edge_label_num, GI, VID_T, LabelT, PROP_TUPLE...>&&
//       other) : vids_(std::move(other.vids_)),
//         adj_lists_(std::move(other.adj_lists_)),
//         prop_names_(other.prop_names_),
//         edge_label_triplet_(std::move(other.edge_label_triplet_)) {}

//  private:
//   std::array<std::vector<VID_T>, edge_label_num> vids_;
//   std::tuple<PropNameArray<PROP_TUPLE>...> prop_names_;
//   std::array<std::array<LabelT, 3>, edge_label_num> edge_label_triplet_;
//   adj_list_array_tuple_t adj_lists_;
// };

// general edge set stores multi src labels but only one dst label
// Which stores the nbr ptrs rather than edge.
template <typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSet<2, GI, VID_T, LabelT, std::tuple<T...>, std::tuple<T...>> {
 public:
  static constexpr size_t num_src_labels = 2;
  static constexpr size_t num_props = sizeof...(T);
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = false;
  using lid_t = VID_T;
  using flat_t = FlatEdgeSet<VID_T, LabelT, std::tuple<T...>>;

  using adj_list_t = typename GI::template adj_list_t<T...>;
  using adj_list_array_t = typename GI::template adj_list_array_t<T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iter_t>;
  using self_type_t =
      GeneralEdgeSet<2, GI, VID_T, LabelT, std::tuple<T...>, std::tuple<T...>>;

  using iterator = GeneralEdgeSetIter<GI, VID_T, LabelT, T...>;
  using builder_t = GeneralEdgeSetBuilder<2, GI, VID_T, LabelT,
                                          std::tuple<T...>, std::tuple<T...>>;
  GeneralEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
                 grape::Bitset&& bitsets, std::vector<std::string>& prop_names,
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

  GeneralEdgeSet(self_type_t&& other)
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

  iterator begin() const {
    return iterator(vids_, adj_lists_, bitsets_, src_labels_, dst_label_,
                    edge_label_, 0);
  }

  iterator end() const {
    return iterator(vids_, adj_lists_, bitsets_, src_labels_, dst_label_,
                    edge_label_, vids_.size());
  }

  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

  std::vector<LabelKey> GetLabelVec() const {
    VLOG(1) << "GetLabelVec for general edge set.";
    std::vector<LabelKey> res;
    res.reserve(Size());
    for (size_t i = 0; i < Size(); ++i) {
      res.emplace_back(edge_label_);
    }
    return res;
  }

  size_t Size() const {
    if (size_ == 0) {
      for (size_t i = 0; i < adj_lists_.size(); ++i) {
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
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
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
    auto set = make_default_row_vertex_set(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented";
  }

  // todo support for multi properties.
  template <typename EDATA_T>
  void fillBuiltinPropsImpl(std::vector<std::tuple<EDATA_T>>& tuples,
                            const std::vector<std::string>& prop_names,
                            const std::vector<size_t>& repeat_array) {
    // Make sure this is correct.
    std::vector<bool> is_built_in(prop_names_.size(), false);
    for (size_t i = 0; i < prop_names_.size(); ++i) {
      if (prop_names_[i].size() == 1 && prop_names_[0] == prop_names[0]) {
        is_built_in[i] = true;
      }
    }
  }

  template <size_t My_Is, typename EDATA_T>
  void fillBuiltinPropsImpl(std::vector<std::tuple<EDATA_T>>& tuples,
                            const std::vector<std::string>& prop_names,
                            const std::vector<size_t>& repeat_array) {
    using cur_prop = std::tuple_element_t<My_Is, std::tuple<T...>>;
    if constexpr (std::is_same_v<cur_prop, EDATA_T>) {
      if (prop_names.size() == 1 && prop_names[0] == prop_names_[My_Is]) {
        VLOG(10) << "Found built-in property" << prop_names[0];
        CHECK(repeat_array.size() == Size());
        size_t cur_ind = 0;
        size_t iter_ind = 0;
        for (auto iter : *this) {
          auto edata = iter.GetData();
          auto repeat_times = repeat_array[iter_ind];
          for (size_t j = 0; j < repeat_times; ++j) {
            CHECK(cur_ind < tuples.size());
            std::get<0>(tuples[cur_ind]) = std::get<My_Is>(edata);
            cur_ind += 1;
          }
          iter_ind += 1;
        }
      }
    } else {
      fillBuiltinPropsImpl<My_Is + 1>(tuples, prop_names, repeat_array);
    }
  }

  template <typename EDATA_T>
  void fillBuiltinProps(std::vector<std::tuple<EDATA_T>>& tuples,
                        const PropNameArray<EDATA_T>& prop_names,
                        const std::vector<size_t>& repeat_array) {
    auto vec = array_to_vec(prop_names);
    fillBuiltinPropsImpl<0, EDATA_T>(tuples, vec, repeat_array);
  }

  // fill builtin props without repeat array.
  template <typename EDATA_T>
  void fillBuiltinProps(std::vector<std::tuple<EDATA_T>>& tuples,
                        const PropNameArray<EDATA_T>& prop_names) {
    std::vector<size_t> repeat_array(Size(), 1);
    auto vec = array_to_vec(prop_names);
    fillBuiltinPropsImpl<0, EDATA_T>(tuples, vec, repeat_array);
  }

  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  flat_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<T...>>> res;
    size_t total_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      total_size += repeat_array[i];
    }
    res.reserve(total_size);
    std::vector<uint8_t> triplet_ind;
    triplet_ind.reserve(total_size);
    std::vector<std::array<LabelT, 3>> label_triplets;
    label_triplets.emplace_back(
        std::array<LabelT, 3>{src_labels_[0], dst_label_, edge_label_});
    label_triplets.emplace_back(
        std::array<LabelT, 3>{src_labels_[1], dst_label_, edge_label_});
    size_t cur_ind = 0;
    for (auto iter : *this) {
      auto repeat_times = repeat_array[cur_ind];
      for (size_t j = 0; j < repeat_times; ++j) {
        res.emplace_back(
            std::make_tuple(iter.GetSrc(), iter.GetDst(), iter.GetData()));
        auto src_label = iter.GetSrcLabel();
        if (src_label == src_labels_[0]) {
          triplet_ind.emplace_back(0);
        } else {
          triplet_ind.emplace_back(1);
        }
        cur_ind += 1;
      }
    }
    std::vector<std::vector<std::string>> prop_names;
    prop_names.emplace_back(prop_names_);
    prop_names.emplace_back(prop_names_);
    return flat_t(std::move(res), std::move(label_triplets), prop_names,
                  std::move(triplet_ind), dir_);
  }

 private:
  mutable size_t size_;
  LabelT edge_label_, dst_label_;
  std::array<LabelT, 2> src_labels_;

  std::vector<std::string> prop_names_;

  std::vector<VID_T> vids_;
  adj_list_array_t adj_lists_;
  grape::Bitset bitsets_;  // bitset of src vertices.
  Direction dir_;
};

// general edge set stores multi src labels but only one dst label
template <typename GI, typename VID_T, typename LabelT>
class GeneralEdgeSet<2, GI, VID_T, LabelT, std::tuple<grape::EmptyType>,
                     std::tuple<grape::EmptyType>> {
 public:
  static constexpr size_t num_src_labels = 2;
  static constexpr size_t num_props = 0;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = false;
  using lid_t = VID_T;
  using flat_t = FlatEdgeSet<VID_T, LabelT, std::tuple<grape::EmptyType>>;

  using adj_list_t = typename GI::template adj_list_t<grape::EmptyType>;
  using adj_list_array_t =
      typename GI::template adj_list_array_t<grape::EmptyType>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using ele_tuple_t = std::tuple<VID_T, adj_list_iter_t>;
  using data_tuple_t = ele_tuple_t;
  using index_ele_tuple_t = std::tuple<size_t, VID_T, adj_list_iter_t>;

  using iterator = GeneralEdgeSetIter<GI, VID_T, LabelT, grape::EmptyType>;
  using builder_t =
      GeneralEdgeSetBuilder<2, GI, VID_T, LabelT, std::tuple<grape::EmptyType>,
                            std::tuple<grape::EmptyType>>;
  GeneralEdgeSet(std::vector<VID_T>&& vids, adj_list_array_t&& adj_lists,
                 grape::Bitset&& bitsets, std::vector<std::string>& prop_names,
                 LabelT edge_label, std::array<LabelT, 2> src_labels,
                 LabelT dst_label, Direction dir)
      : vids_(std::move(vids)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_labels_(src_labels),
        dst_label_(dst_label),
        size_(0),
        dir_(dir),
        prop_names_(prop_names) {
    bitsets_.swap(bitsets);
  }

  GeneralEdgeSet(GeneralEdgeSet<2, GI, VID_T, LabelT, grape::EmptyType>&& other)
      : vids_(std::move(other.vids_)),
        adj_lists_(std::move(other.adj_lists_)),
        edge_label_(other.edge_label_),
        src_labels_(std::move(other.src_labels_)),
        dst_label_(other.dst_label_),
        size_(0),
        dir_(other.dir_),
        prop_names_(other.prop_names_) {
    bitsets_.swap(other.bitsets_);
  }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    VLOG(1) << "GetLabelVec for general edge set.";
    res.reserve(Size());
    for (size_t i = 0; i < Size(); ++i) {
      res.emplace_back(edge_label_);
    }
    return res;
  }

  iterator begin() const {
    return iterator(vids_, adj_lists_, bitsets_, src_labels_, dst_label_,
                    edge_label_, 0);
  }

  iterator end() const {
    return iterator(vids_, adj_lists_, bitsets_, src_labels_, dst_label_,
                    edge_label_, vids_.size());
  }

  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

  size_t Size() const {
    if (size_ == 0) {
      for (size_t i = 0; i < adj_lists_.size(); ++i) {
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
    for (size_t i = 0; i < index_ele_tuple.size(); ++i) {
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
    auto set = make_default_row_vertex_set(std::move(vids), dst_label_);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  // implement ProjectWithRepeatArray
  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  flat_t ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                                KeyAlias<tag_id, Fs>& key_alias) const {
    std::vector<std::tuple<VID_T, VID_T, std::tuple<grape::EmptyType>>> res;
    size_t total_size = 0;
    for (size_t i = 0; i < repeat_array.size(); ++i) {
      total_size += repeat_array[i];
    }
    res.reserve(total_size);
    std::vector<uint8_t> triplet_ind;
    triplet_ind.reserve(total_size);
    std::vector<std::array<LabelT, 3>> label_triplets;
    label_triplets.emplace_back(
        std::array<LabelT, 3>{src_labels_[0], dst_label_, edge_label_});
    label_triplets.emplace_back(
        std::array<LabelT, 3>{src_labels_[1], dst_label_, edge_label_});
    size_t cur_ind = 0;
    for (auto iter : *this) {
      auto repeat_times = repeat_array[cur_ind];
      for (size_t j = 0; j < repeat_times; ++j) {
        res.emplace_back(std::make_tuple(iter.GetSrc(), iter.GetDst(),
                                         std::make_tuple(grape::EmptyType())));
        auto src_label = iter.GetSrcLabel();
        if (src_label == src_labels_[0]) {
          triplet_ind.emplace_back(0);
        } else {
          triplet_ind.emplace_back(1);
        }
        cur_ind += 1;
      }
    }
    std::vector<std::vector<std::string>> prop_names;
    prop_names.emplace_back(prop_names_);
    prop_names.emplace_back(prop_names_);
    return flat_t(std::move(res), std::move(label_triplets), prop_names,
                  std::move(triplet_ind), dir_);
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
  std::vector<std::string> prop_names_;
};  // namespace gs
}  // namespace gs
#endif  // ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_GENERAL_EDGE_SET_H_