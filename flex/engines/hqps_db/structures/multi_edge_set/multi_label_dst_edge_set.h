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
#ifndef ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_LABEL_DST_EDGE_SET_H_
#define ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_LABEL_DST_EDGE_SET_H_

#include <string>
#include <unordered_set>
#include <vector>

#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/two_label_vertex_set.h"

#include "grape/types.h"

namespace gs {

template <size_t num_labels, typename GRAPH_T, typename... T>
class MultiLabelDstEdgeSetIter {
 public:
  using label_id_t = typename GRAPH_T::label_id_t;
  using vertex_id_t = typename GRAPH_T::vertex_id_t;
  using adj_list_t = typename GRAPH_T::template adj_list_t<T...>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using adj_list_array_t = typename GRAPH_T::template adj_list_array_t<T...>;
  using index_ele_tuple_t = std::tuple<size_t, vertex_id_t, adj_list_iter_t>;
  using self_type_t = MultiLabelDstEdgeSetIter<num_labels, GRAPH_T, T...>;

  MultiLabelDstEdgeSetIter(
      const std::vector<vertex_id_t>& src_vertices,
      const std::array<adj_list_array_t, num_labels>& adj_lists, size_t ind)
      : src_vertices_(src_vertices),
        adj_lists_(adj_lists),
        ind_(ind),
        cur_label_ind_(0) {
    if (ind_ == src_vertices_.size()) {
      begin_ = adj_list_iter_t();
      end_ = adj_list_iter_t();
    } else {
      probe_for_next();
    }
    // init valid_lables_ with all true;
    for (size_t i = 0; i < num_labels; i++) {
      valid_labels_[i] = true;
    }
  }

  MultiLabelDstEdgeSetIter(
      const std::vector<vertex_id_t>& src_vertices,
      const std::array<adj_list_array_t, num_labels>& adj_lists, size_t ind,
      std::array<label_id_t, num_labels>& labels)
      : src_vertices_(src_vertices),
        adj_lists_(adj_lists),
        ind_(ind),
        cur_label_ind_(0),
        valid_labels_(labels) {
    if (ind_ == src_vertices_.size()) {
      begin_ = adj_list_iter_t();
      end_ = adj_list_iter_t();
    } else {
      probe_for_next();
    }
  }

  void probe_for_next() {
    while (ind_ < src_vertices_.size()) {
      while (cur_label_ind_ < num_labels) {
        if (valid_labels_[cur_label_ind_]) {
          auto cur_adj_list = adj_lists_[cur_label_ind_].get(ind_);
          begin_ = cur_adj_list.begin();
          end_ = cur_adj_list.end();
          if (begin_ != end_) {
            break;
          }
        }
        cur_label_ind_++;
      }
      if (cur_label_ind_ < num_labels) {
        break;
      } else {
        ind_++;
        // may be optimized
        cur_label_ind_ = 0;
      }
    }
  }

  inline vertex_id_t GetSrc() const { return src_vertices_[ind_]; }
  inline vertex_id_t GetDst() const { return begin_.neighbor(); }
  inline size_t GetLabelInd() const { return cur_label_ind_; }
  inline const std::tuple<T...> GetData() const { return begin_.properties(); }

  inline Edge<vertex_id_t, T...> GetElement() const {
    return Edge<vertex_id_t, T...>(GetSrc(), GetDst(), GetData());
  }

  inline index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetSrc(), begin_);
  }

  inline const self_type_t& operator++() {
    ++begin_;
    if (begin_ == end_) {
      ++cur_label_ind_;

      if (cur_label_ind_ >= num_labels) {
        ++ind_;
        cur_label_ind_ = 0;
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
  const std::vector<vertex_id_t>& src_vertices_;
  const std::array<adj_list_array_t, num_labels>& adj_lists_;
  size_t ind_;
  size_t cur_label_ind_;
  adj_list_iter_t begin_, end_;
  std::array<label_id_t, num_labels> valid_labels_;
};

// specialization for MultiLabelDstEdgeSetIter with grape::EmptyType
template <size_t num_labels, typename GRAPH_T>
class MultiLabelDstEdgeSetIter<num_labels, GRAPH_T, grape::EmptyType> {
 public:
  using label_id_t = typename GRAPH_T::label_id_t;
  using vertex_id_t = typename GRAPH_T::vertex_id_t;
  using adj_list_t = typename GRAPH_T::template adj_list_t<>;
  using adj_list_iter_t = typename adj_list_t::iterator;
  using adj_list_array_t = typename GRAPH_T::template adj_list_array_t<>;
  using index_ele_tuple_t = std::tuple<size_t, vertex_id_t, adj_list_iter_t>;
  using self_type_t =
      MultiLabelDstEdgeSetIter<num_labels, GRAPH_T, grape::EmptyType>;

  MultiLabelDstEdgeSetIter(
      const std::vector<vertex_id_t>& src_vertices,
      const std::array<adj_list_array_t, num_labels>& adj_lists, size_t ind)
      : src_vertices_(src_vertices),
        adj_lists_(adj_lists),
        ind_(ind),
        cur_label_ind_(0) {
    if (ind_ == src_vertices_.size()) {
      begin_ = adj_list_iter_t();
      end_ = adj_list_iter_t();
    } else {
      probe_for_next();
    }
    // init valid_lables_ with all true;
    for (size_t i = 0; i < num_labels; i++) {
      valid_labels_[i] = true;
    }
  }
  MultiLabelDstEdgeSetIter(
      const std::vector<vertex_id_t>& src_vertices,
      const std::array<adj_list_array_t, num_labels>& adj_lists, size_t ind,
      std::array<bool, num_labels> valid_labels)
      : src_vertices_(src_vertices),
        adj_lists_(adj_lists),
        ind_(ind),
        cur_label_ind_(0),
        valid_labels_(valid_labels) {
    if (ind_ == src_vertices_.size()) {
      begin_ = adj_list_iter_t();
      end_ = adj_list_iter_t();
    } else {
      probe_for_next();
    }
  }

  void probe_for_next() {
    while (ind_ < src_vertices_.size()) {
      while (cur_label_ind_ < num_labels) {
        if (valid_labels_[cur_label_ind_]) {
          auto cur_adj_list = adj_lists_[cur_label_ind_].get(ind_);
          begin_ = cur_adj_list.begin();
          end_ = cur_adj_list.end();

          if (begin_ != end_) {
            break;
          }
        }
        cur_label_ind_++;
      }
      if (cur_label_ind_ < num_labels) {
        break;
      } else {
        ind_++;
        cur_label_ind_ = 0;
      }
    }
    VLOG(10) << "after probe for next: " << ind_ << " " << cur_label_ind_;
  }

  inline vertex_id_t GetSrc() const { return src_vertices_[ind_]; }
  inline vertex_id_t GetDst() const { return begin_.neighbor(); }
  inline size_t GetLabelInd() const { return cur_label_ind_; }
  inline const std::tuple<grape::EmptyType> GetData() const {
    return std::make_tuple(grape::EmptyType());
  }

  inline Edge<vertex_id_t, grape::EmptyType> GetElement() const {
    return Edge<vertex_id_t, grape::EmptyType>(GetSrc(), GetDst(), GetData());
  }

  inline index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, GetSrc(), begin_);
  }

  inline const self_type_t& operator++() {
    ++begin_;
    if (begin_ == end_) {
      ++cur_label_ind_;

      if (cur_label_ind_ >= num_labels) {
        ++ind_;
        cur_label_ind_ = 0;
      }
      probe_for_next();
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
  const std::vector<vertex_id_t>& src_vertices_;
  const std::array<adj_list_array_t, num_labels>& adj_lists_;
  size_t ind_;
  size_t cur_label_ind_;
  adj_list_iter_t begin_, end_;
  std::array<bool, num_labels> valid_labels_;  // label_inds
};

// Multiple label dst edge set
template <size_t num_labels, typename GRAPH_T, typename... T>
class MultiLabelDstEdgeSet {
 public:
  using label_id_t = typename GRAPH_T::label_id_t;
  using vertex_id_t = typename GRAPH_T::vertex_id_t;
  using adj_list_array_t = typename GRAPH_T::template adj_list_array_t<T...>;
  using iterator = MultiLabelDstEdgeSetIter<num_labels, GRAPH_T, T...>;
  using index_ele_tuple_t = typename iterator::index_ele_tuple_t;
  using self_type_t = MultiLabelDstEdgeSet<num_labels, GRAPH_T, T...>;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = true;

  MultiLabelDstEdgeSet(std::vector<vertex_id_t>&& src_vertices,
                       std::array<adj_list_array_t, num_labels>&& adj_lists,
                       label_id_t edge_label, label_id_t src_label,
                       std::array<label_id_t, num_labels> other_label,
                       Direction dir)
      : src_vertices_(std::move(src_vertices)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_label_(src_label),
        other_label_(other_label),
        dir_(dir) {
    size_ = 0;
    for (auto i = 0; i < num_labels; ++i) {
      for (auto j = 0; j < src_vertices_.size(); ++j) {
        size_ += adj_lists_[i].get(j).size();
      }
    }
  }

  iterator begin() const { return iterator(src_vertices_, adj_lists_, 0); }
  iterator end() const {
    return iterator(src_vertices_, adj_lists_, src_vertices_.size());
  }

  size_t size() const { return size_; }

 private:
  std::vector<vertex_id_t> src_vertices_;
  std::array<adj_list_array_t, num_labels> adj_lists_;
  label_id_t edge_label_, src_label_;
  std::array<label_id_t, num_labels> other_label_;
  size_t size_;
  Direction dir_;
};

template <size_t num_labels, typename GRAPH_T>
class MultiLabelDstEdgeSet<num_labels, GRAPH_T, grape::EmptyType> {
 public:
  using label_id_t = typename GRAPH_T::label_id_t;
  using vertex_id_t = typename GRAPH_T::vertex_id_t;
  using adj_list_array_t = typename GRAPH_T::template adj_list_array_t<>;
  using iterator =
      MultiLabelDstEdgeSetIter<num_labels, GRAPH_T, grape::EmptyType>;
  using flat_t =
      FlatEdgeSet<vertex_id_t, label_id_t, num_labels, grape::EmptyType>;
  using index_ele_tuple_t = typename iterator::index_ele_tuple_t;
  using self_type_t =
      MultiLabelDstEdgeSet<num_labels, GRAPH_T, grape::EmptyType>;
  static constexpr bool is_multi_label = false;
  static constexpr bool is_collection = false;
  static constexpr bool is_edge_set = true;
  static constexpr bool is_multi_dst_label = true;

  MultiLabelDstEdgeSet(std::vector<vertex_id_t>&& src_vertices,
                       std::array<adj_list_array_t, num_labels>&& adj_lists,
                       label_id_t edge_label, label_id_t src_label,
                       std::array<label_id_t, num_labels> other_label,
                       Direction dir)
      : src_vertices_(std::move(src_vertices)),
        adj_lists_(std::move(adj_lists)),
        edge_label_(edge_label),
        src_label_(src_label),
        other_label_(other_label),
        dir_(dir) {
    size_ = 0;
    for (auto i = 0; i < num_labels; ++i) {
      for (auto j = 0; j < src_vertices_.size(); ++j) {
        size_ += adj_lists_[i].get(j).size();
      }
    }
  }

  iterator begin() const { return iterator(src_vertices_, adj_lists_, 0); }
  iterator end() const {
    return iterator(src_vertices_, adj_lists_, src_vertices_.size());
  }

  size_t Size() const { return size_; }

  // get vertices of only one kind label
  template <typename EXPR, size_t num_query_labels,
            typename std::enable_if<(IsTruePredicate<EXPR>::value) &&
                                    (num_query_labels == 1)>::type* = nullptr>
  std::pair<RowVertexSet<label_id_t, vertex_id_t, grape::EmptyType>,
            std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<label_id_t, num_query_labels>& labels,
              EXPR& expr) const {
    if (dir_ == Direction::In) {
      CHECK(v_opt == VOpt::Start || v_opt == VOpt::Other);
    } else if (dir_ == Direction::Out) {
      CHECK(v_opt == VOpt::End || v_opt == VOpt::Other);
    }
    std::vector<offset_t> offsets;
    std::vector<vertex_id_t> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    std::array<bool, num_labels> is_valid;
    {
      std::unordered_set<label_id_t> tmp_set{labels.begin(), labels.end()};
      for (auto i = 0; i < num_labels; ++i) {
        if (tmp_set.find(other_label_[i]) != tmp_set.end()) {
          is_valid[i] = true;
        } else {
          is_valid[i] = false;
        }
      }
    }
    auto iter = iterator(src_vertices_, adj_lists_, 0, is_valid);
    auto end = iterator(src_vertices_, adj_lists_, src_vertices_.size());
    while (iter != end) {
      vids.emplace_back(iter.GetDst());
      offsets.emplace_back(vids.size());
      ++iter;
    }
    auto set = MakeDefaultRowVertexSet(std::move(vids), labels[0]);
    return std::make_pair(std::move(set), std::move(offsets));
  }

  // get vertices of two kind labels.
  template <typename EXPR, size_t num_query_labels,
            typename std::enable_if<(IsTruePredicate<EXPR>::value) &&
                                    (num_query_labels == 2)>::type* = nullptr>
  std::pair<TwoLabelVertexSet<vertex_id_t, label_id_t, grape::EmptyType>,
            std::vector<size_t>>
  GetVertices(VOpt v_opt, std::array<label_id_t, num_query_labels>& labels,
              EXPR& expr) const {
    LOG(INFO) << "Get vertices from edgeset " << Size()
              << "with labels: " << gs::to_string(labels);
    if (dir_ == Direction::In) {
      CHECK(v_opt == VOpt::Start || v_opt == VOpt::Other);
    } else if (dir_ == Direction::Out) {
      CHECK(v_opt == VOpt::End || v_opt == VOpt::Other);
    }
    std::vector<offset_t> offsets;
    std::vector<vertex_id_t> vids;
    offsets.reserve(Size());
    offsets.emplace_back(0);
    std::array<bool, num_labels> is_valid;
    {
      std::unordered_set<label_id_t> tmp_set{labels.begin(), labels.end()};
      for (auto i = 0; i < num_labels; ++i) {
        if (tmp_set.find(other_label_[i]) != tmp_set.end()) {
          LOG(INFO) << "ind : " << i << ",valid";
          is_valid[i] = true;
        } else {
          is_valid[i] = false;
        }
      }
    }
    auto iter = iterator(src_vertices_, adj_lists_, 0, is_valid);
    auto end = iterator(src_vertices_, adj_lists_, src_vertices_.size());
    grape::Bitset bitset;
    // make sure correct
    bitset.init(size_);
    while (iter != end) {
      vids.emplace_back(iter.GetDst());
      offsets.emplace_back(vids.size());
      auto label_ind = iter.GetLabelInd();
      if (label_ind == 0) {
        bitset.set_bit(label_ind);
      }
      ++iter;
    }
    LOG(INFO) << "vids size: " << vids.size();
    auto copied_label_ids = labels;
    auto set = make_two_label_set(std::move(vids), std::move(copied_label_ids),
                                  std::move(bitset));
    return std::make_pair(std::move(set), std::move(offsets));
  }

  template <size_t col_ind, typename... index_ele_tuple_t_>
  flat_t Flat(
      std::vector<std::tuple<index_ele_tuple_t_...>>& index_ele_tuple) const {
    std::vector<std::tuple<vertex_id_t, vertex_id_t, grape::EmptyType>> res;
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

 private:
  std::vector<vertex_id_t> src_vertices_;
  std::array<adj_list_array_t, num_labels> adj_lists_;
  label_id_t edge_label_, src_label_;
  std::array<label_id_t, num_labels> other_label_;
  size_t size_;
  Direction dir_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_DS_EDGE_MULTISET_LABEL_DST_EDGE_SET_H_