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

#ifndef ENGINES_HQPS_DB_STRUCTURES_MULTI_EDGE_SET_UNTYPED_EDGE_SET_H_
#define ENGINES_HQPS_DB_STRUCTURES_MULTI_EDGE_SET_UNTYPED_EDGE_SET_H_

#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/general_vertex_set.h"
#include "flex/utils/arrow_utils.h"
#include "grape/grape.h"

namespace gs {

template <typename VID_T, typename LabelT, typename CSR_ITER>
class UnTypedEdgeSetIter {
 public:
  using vid_t = VID_T;
  using label_t = LabelT;
  using edge_iter_t = CSR_ITER;

  using ele_tuple_t = std::tuple<vid_t, vid_t, Any>;
  using index_ele_tuple_t = std::tuple<size_t, vid_t, vid_t, Any>;
  using self_type_t = UnTypedEdgeSetIter<vid_t, label_t, edge_iter_t>;

  UnTypedEdgeSetIter(const std::vector<vid_t>& src_v,
                     std::vector<std::vector<edge_iter_t>>&& adj_lists,
                     size_t ind)
      : src_vertices_(src_v),
        adj_lists_(std::move(adj_lists)),
        vid_ind_(ind),
        iter_ind_(0),
        cur_ind_(0) {
    LOG(INFO) << "UnTypedEdgeSetIter init,size: " << adj_lists_.size()
              << ", vertices size: " << src_vertices_.size();
    if (vid_ind_ != src_vertices_.size()) {
      while (vid_ind_ < src_vertices_.size()) {
        auto& edge_iter_vec = adj_lists_[vid_ind_];
        while (iter_ind_ < edge_iter_vec.size()) {
          cur_iter_ = edge_iter_vec[iter_ind_];
          if (cur_iter_.IsValid()) {
            break;
          }
          VLOG(10) << "Skip invalid edge with " << vid_ind_
                   << ", iter_ind : " << iter_ind_;
          iter_ind_ += 1;
        }
        if (cur_iter_.IsValid()) {
          break;
        }
        vid_ind_ += 1;
        iter_ind_ = 0;
      }
      VLOG(10) << "Init start pointer to " << vid_ind_
               << ", iter_ind : " << iter_ind_;
    }
  }

  inline LabelT GetEdgeLabel() const { return cur_iter_.GetEdgeLabel(); }

  inline VID_T GetSrc() const { return src_vertices_[vid_ind_]; }
  inline VID_T GetDst() const { return cur_iter_.GetDstId(); }

  inline label_t GetDstLabel() const { return cur_iter_.GetDstLabel(); }
  inline label_t GetSrcLabel() const { return cur_iter_.GetSrcLabel(); }

  inline Any GetData() const { return cur_iter_.GetData(); }
  inline const std::vector<std::string>& GetPropNames() const {
    return cur_iter_.GetPropNames();
  }

  inline ele_tuple_t GetElement() const {
    return std::make_tuple(GetSrc(), GetDst(), GetData());
  }

  // Make sure this okay.
  inline index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(cur_ind_, GetSrc(), GetDst(), GetData());
  }

  inline const self_type_t& operator++() {
    ++cur_ind_;
    cur_iter_.Next();
    if (cur_iter_.IsValid()) {
      return *this;
    } else {
      // if cur_iter is out of range, probe for next.
      probe_for_next();
    }
    return *this;
  }

  inline bool operator==(const self_type_t& rhs) const {
    return vid_ind_ == rhs.vid_ind_;
  }

  inline bool operator!=(const self_type_t& rhs) const {
    return !(*this == rhs);
  }

  inline bool operator<(const self_type_t& rhs) const {
    return vid_ind_ < rhs.vid_ind_;
  }

  inline const self_type_t& operator*() const { return *this; }

  inline const self_type_t* operator->() const { return this; }

 private:
  // try to find next valid edge
  inline void probe_for_next() {
    // assume cur_iter_ is invalid
    VLOG(10) << "Probe for next edge with " << vid_ind_
             << ", iter_ind : " << iter_ind_;
    CHECK(!cur_iter_.IsValid());
    iter_ind_ += 1;
    while (vid_ind_ < src_vertices_.size()) {
      auto& edge_iter_vec = adj_lists_[vid_ind_];
      VLOG(10) << "Probe for next edge with " << vid_ind_
               << ", iter_ind : " << iter_ind_
               << ", edge_iter_vec size : " << edge_iter_vec.size();
      while (iter_ind_ < edge_iter_vec.size()) {
        cur_iter_ = edge_iter_vec[iter_ind_];
        if (cur_iter_.IsValid()) {
          return;
        }
        iter_ind_ += 1;
      }
      vid_ind_ += 1;
      iter_ind_ = 0;
    }
  }

  const std::vector<vid_t>& src_vertices_;
  std::vector<std::vector<edge_iter_t>> adj_lists_;
  size_t vid_ind_, iter_ind_;
  size_t cur_ind_;
  edge_iter_t cur_iter_;
};

// Define a EdgeSet which is able to store any kind of edges. Edges can have
// different number and type of properties.
template <typename VID_T, typename LabelT, typename SUB_GRAPH_T>
class UnTypedEdgeSet {
 public:
  using vid_t = VID_T;
  using label_t = LabelT;
  using sub_graph_t = SUB_GRAPH_T;
  using edge_iter_t = typename sub_graph_t::iterator;

  using iterator = UnTypedEdgeSetIter<VID_T, LabelT, edge_iter_t>;
  using index_ele_tuple_t = typename iterator::index_ele_tuple_t;
  using ele_tuple_t = typename iterator::ele_tuple_t;
  using self_type_t = UnTypedEdgeSet<vid_t, label_t, sub_graph_t>;
  using flat_t = UnTypedEdgeSet;
  using data_tuple_t = std::tuple<vid_t, vid_t, Any>;

  static constexpr bool is_edge_set = true;

  UnTypedEdgeSet(
      const std::vector<vid_t>& src_v,
      const std::vector<uint8_t>& label_indices,
      const std::vector<label_t>& labels,
      std::unordered_map<label_t, std::vector<sub_graph_t>>&& adj_lists,
      const Direction& direction)
      : src_vertices_(src_v),
        label_indices_(label_indices),
        src_labels_(labels),
        adj_lists_(std::move(adj_lists)),
        size_(0),
        direction_(direction) {
    sanity_check();
  }

  iterator begin() const {
    // generate a vector of visitable edge iterators.
    auto tmp = generate_iters();
    return iterator(src_vertices_, std::move(tmp), 0);
  }

  iterator end() const {
    std::vector<std::vector<edge_iter_t>> edge_iter_vecs;
    edge_iter_vecs.resize(src_vertices_.size());
    return iterator(src_vertices_, std::move(edge_iter_vecs),
                    src_vertices_.size());
  }

  std::vector<LabelKey> GetLabelVec() const {
    std::vector<LabelKey> res;
    res.reserve(Size());
    VLOG(1) << "GetLabelVec for UntypedEdgeSet, size: " << Size();
    for (size_t i = 0; i < src_vertices_.size(); ++i) {
      auto label_ind = label_indices_[i];
      auto label = src_labels_[label_ind];
      if (adj_lists_.find(label) != adj_lists_.end()) {
        auto& sub_graphs = adj_lists_.at(label);
        for (auto& sub_graph : sub_graphs) {
          auto edge_iters = sub_graph.get_edges(src_vertices_[i]);
          auto edge_label = sub_graph.GetEdgeLabel();
          for (size_t j = 0; j < edge_iters.Size(); ++j) {
            res.emplace_back(edge_label);
          }
        }
      }
    }
    return res;
  }

  size_t Size() const {
    if (size_ == 0) {
      auto iter_vec = generate_iters();
      for (auto iter1 : iter_vec) {
        for (auto iter : iter1) {
          size_ += iter.Size();
        }
      }
    }
    return size_;
  }

  // fill properties for the given edges.
  template <typename PropT>
  std::vector<PropT> getProperties(const PropNameArray<PropT>& prop_names,
                                   const std::vector<offset_t>& repeat_array) {
    std::vector<PropT> tmp_prop_vec;
    size_t sum = 0;
    {
      for (auto v : repeat_array) {
        sum += v;
      }
      tmp_prop_vec.reserve(sum);
    }
    VLOG(10) << "getting property for " << Size() << " edges ";
    auto iter = begin();
    auto end_iter = end();
    size_t cur_ind = 0;
    while (iter != end_iter) {
      auto edata = iter.GetData();
      PropT prop;
      if (edata.type == TypeConverter<PropT>::property_type()) {
        ConvertAny<PropT>::to(edata, prop);
      }
      CHECK(cur_ind < sum) << "cur: " << cur_ind << ", sum: " << sum;
      for (size_t i = 0; i < repeat_array[cur_ind]; ++i) {
        tmp_prop_vec.emplace_back(prop);
      }
      cur_ind += 1;
      ++iter;
    }
    return tmp_prop_vec;
  }

  // get label indices
  const std::vector<uint8_t>& GetLabelIndices() const { return label_indices_; }

  template <size_t num_labels>
  std::pair<GeneralVertexSet<vid_t, label_t, grape::EmptyType>,
            std::vector<offset_t>>
  GetVertices(const GetVOpt<label_t, num_labels, Filter<TruePredicate>>&
                  get_v_opt) const {
    auto v_opt = get_v_opt.v_opt_;
    auto v_labels = get_v_opt.v_labels_;
    auto v_labels_vec = array_to_vec(v_labels);
    // if v_labels_vec is null or empty, return all vertices
    if ((v_opt == VOpt::Start && direction_ == Direction::Out) ||
        (v_opt == VOpt::End && direction_ == Direction::In)) {
      if (v_labels_vec.empty()) {
        v_labels_vec = src_labels_;
      }
      return getSrcVertices(v_labels_vec);
    } else if ((v_opt == VOpt::Start && direction_ == Direction::In) ||
               (v_opt == VOpt::End && direction_ == Direction::Out) ||
               (v_opt == VOpt::Other)) {
      auto dst_label_set = get_dst_label_set();
      if (v_labels_vec.empty()) {
        v_labels_vec =
            std::vector<label_t>(dst_label_set.begin(), dst_label_set.end());
      }
      return getDstVertices(v_labels_vec);
    } else {
      LOG(FATAL) << "Not supported for " << gs::to_string(v_opt);
    }
  }

  std::pair<GeneralVertexSet<vid_t, label_t, grape::EmptyType>,
            std::vector<offset_t>>
  getSrcVertices(const std::vector<label_t>& req_labels) const {
    std::vector<vid_t> ret;
    std::vector<offset_t> offset;
    ret.reserve(Size());
    offset.reserve(Size());
    std::vector<label_t> res_label_vec;
    std::unordered_map<label_t, size_t> label_to_ind;
    std::tie(res_label_vec, label_to_ind) =
        preprocess_getting_labels(req_labels, src_labels_);

    std::vector<grape::Bitset> bitsets(res_label_vec.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].init(Size());
    }
    size_t cur_cnt = 0;
    for (auto iter : *this) {
      offset.emplace_back(cur_cnt);
      auto label = iter.GetSrcLabel();
      if (label_to_ind.find(label) != label_to_ind.end()) {
        auto ind = label_to_ind[label];
        auto vid = iter.GetSrc();
        ret.emplace_back(vid);
        CHECK(ind < res_label_vec.size());
        bitsets[ind].set_bit(cur_cnt);
        cur_cnt += 1;
      }
    }
    CHECK(cur_cnt <= Size());
    offset.emplace_back(cur_cnt);
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].resize(cur_cnt);
    }
    LOG(INFO) << "After resize from " << Size() << " to " << cur_cnt;
    auto general_set =
        make_general_set(std::move(ret), res_label_vec, std::move(bitsets));
    return std::make_pair(std::move(general_set), std::move(offset));
  }

  std::pair<GeneralVertexSet<vid_t, label_t, grape::EmptyType>,
            std::vector<offset_t>>
  getDstVertices(const std::vector<label_t>& req_labels) const {
    std::vector<vid_t> ret;
    std::vector<offset_t> offset;
    ret.reserve(Size());
    offset.reserve(Size());
    std::vector<label_t> res_label_vec;
    std::unordered_map<label_t, size_t> label_to_ind;

    label_t max_label_id = 0;
    std::vector<label_t> all_cur_labels;
    {
      auto dst_label_set = get_dst_label_set();
      for (auto iter : dst_label_set) {
        all_cur_labels.emplace_back(iter);
      }
    }
    std::tie(res_label_vec, label_to_ind) =
        preprocess_getting_labels(req_labels, all_cur_labels);

    std::vector<grape::Bitset> bitsets(res_label_vec.size());
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].init(Size());
    }
    {
      for (auto l : res_label_vec) {
        max_label_id = std::max(max_label_id, l);
      }
    }
    std::vector<int32_t> label_to_ind_vec;
    label_to_ind_vec.resize(max_label_id + 1, -1);
    for (size_t i = 0; i < res_label_vec.size(); ++i) {
      label_to_ind_vec[res_label_vec[i]] = label_to_ind[res_label_vec[i]];
    }
    size_t cur_cnt = 0;
    for (auto iter : *this) {
      offset.emplace_back(cur_cnt);
      auto label = iter.GetDstLabel();
      auto ind = label_to_ind_vec[label];
      if (ind != -1) {
        auto ind = label_to_ind[label];
        auto vid = iter.GetDst();
        ret.emplace_back(vid);
        bitsets[ind].set_bit(cur_cnt);
        cur_cnt += 1;
      }
    }
    CHECK(cur_cnt <= Size());
    offset.emplace_back(cur_cnt);
    for (size_t i = 0; i < bitsets.size(); ++i) {
      bitsets[i].resize(cur_cnt);
    }
    LOG(INFO) << "After resize from " << Size() << " to " << cur_cnt;
    auto general_set =
        make_general_set(std::move(ret), res_label_vec, std::move(bitsets));
    return std::make_pair(std::move(general_set), std::move(offset));
  }

  void Repeat(std::vector<offset_t>& cur_offset,
              std::vector<offset_t>& repeat_vec) {
    LOG(FATAL) << "not implemented, and should not be called";
  }

  template <int tag_id, int Fs,
            typename std::enable_if<Fs == -1>::type* = nullptr>
  auto ProjectWithRepeatArray(const std::vector<size_t>& repeat_array,
                              KeyAlias<tag_id, Fs>& key_alias) const {
    LOG(INFO) << "Project UntypedEdgeSet to FlatEdgeSet";
    using dst_ele_tuple_t = std::tuple<VID_T, VID_T, Any>;
    CHECK(repeat_array.size() == Size());
    size_t real_size = 0;
    for (auto v : repeat_array) {
      real_size += v;
    }
    std::vector<dst_ele_tuple_t> dst_eles;
    dst_eles.reserve(real_size);
    auto edge_label_triplets = get_edge_triplets();
    auto edge_iters = generate_iters();
    std::vector<uint8_t> label_triplet_indices;
    label_triplet_indices.reserve(real_size);
    std::vector<size_t> sizes;
    sizes.emplace_back(0);
    for (size_t i = 0; i < edge_label_triplets.size(); ++i) {
      sizes.emplace_back(sizes.back() + edge_label_triplets[i].size());
    }

    // 0,2,4
    size_t cur_ind = 0;
    for (size_t i = 0; i < src_vertices_.size(); ++i) {
      auto src_vid = src_vertices_[i];
      auto& cur_edge_iters = edge_iters[i];
      auto src_label_ind = label_indices_[i];
      auto src_label = src_labels_[src_label_ind];

      for (size_t j = 0; j < cur_edge_iters.size(); ++j) {
        auto& cur_iter = cur_edge_iters[j];
        while (cur_iter.IsValid()) {
          auto dst_vid = cur_iter.GetDstId();
          auto data = cur_iter.GetData();
          for (auto k = 0; k < repeat_array[cur_ind]; ++k) {
            dst_eles.emplace_back(std::make_tuple(src_vid, dst_vid, data));
            label_triplet_indices.emplace_back(sizes[src_label_ind] + j);
          }
          cur_iter.Next();
          cur_ind += 1;
        }
      }
    }
    std::vector<std::array<LabelT, 3>> res_label_triplets;
    // put edge_label_triplets into res_label_triplets
    for (size_t i = 0; i < edge_label_triplets.size(); ++i) {
      auto& cur_triplets_vec = edge_label_triplets[i];
      for (size_t j = 0; j < cur_triplets_vec.size(); ++j) {
        res_label_triplets.emplace_back(cur_triplets_vec[j]);
      }
    }
    std::vector<std::vector<std::string>> prop_names = get_prop_names();
    CHECK(prop_names.size() == res_label_triplets.size());
    return FlatEdgeSet<vid_t, label_t, Any>(
        std::move(dst_eles), std::move(res_label_triplets), prop_names,
        std::move(label_triplet_indices), direction_);
  }

 private:
  std::pair<std::vector<label_t>, std::unordered_map<label_t, size_t>>
  preprocess_getting_labels(const std::vector<label_t>& req_labels,
                            const std::vector<label_t>& all_cur_labels) const {
    std::unordered_map<label_t, size_t> label_to_ind;
    std::vector<label_t> res_label_vec;
    for (size_t i = 0; i < all_cur_labels.size(); ++i) {
      if (std::find(req_labels.begin(), req_labels.end(), all_cur_labels[i]) !=
          req_labels.end()) {
        res_label_vec.emplace_back(all_cur_labels[i]);
        label_to_ind[all_cur_labels[i]] = res_label_vec.size() - 1;
      }
    }
    return std::make_pair(std::move(res_label_vec), std::move(label_to_ind));
  }

  std::unordered_set<label_t> get_dst_label_set() const {
    std::unordered_set<label_t> dst_label_set;
    {
      size_t sub_graph_cnt = 0;
      for (auto iter : adj_lists_) {
        auto& sub_graphs = iter.second;
        for (size_t i = 0; i < sub_graphs.size(); ++i) {
          dst_label_set.insert(sub_graphs[i].GetDstLabel());
          sub_graph_cnt += 1;
        }
      }
      LOG(INFO) << "Found " << dst_label_set.size() << " dst labels from "
                << sub_graph_cnt << " sub graphs";
    }
    return dst_label_set;
  }

  std::vector<std::vector<edge_iter_t>> generate_iters() const {
    std::vector<std::vector<edge_iter_t>> edge_iter_vecs;
    edge_iter_vecs.reserve(src_vertices_.size());
    for (size_t i = 0; i < src_vertices_.size(); ++i) {
      std::vector<edge_iter_t> edge_iter_vec;
      auto label_ind = label_indices_[i];
      auto label = src_labels_[label_ind];
      if (adj_lists_.find(label) != adj_lists_.end()) {
        auto& sub_graphs = adj_lists_.at(label);
        for (auto& sub_graph : sub_graphs) {
          edge_iter_vec.emplace_back(sub_graph.get_edges(src_vertices_[i]));
        }
      }
      edge_iter_vecs.emplace_back(std::move(edge_iter_vec));
    }
    LOG(INFO) << "Generate iters for " << src_vertices_.size()
              << " vertices, with " << edge_iter_vecs.size() << " iters";
    return edge_iter_vecs;
  }

  std::vector<std::vector<std::array<LabelT, 3>>> get_edge_triplets() const {
    std::vector<std::vector<std::array<LabelT, 3>>> ret;
    for (auto src_label_ind = 0; src_label_ind < src_labels_.size();
         ++src_label_ind) {
      auto src_label = src_labels_[src_label_ind];
      std::vector<std::array<LabelT, 3>> tmp;
      if (adj_lists_.find(src_label) != adj_lists_.end()) {
        auto& sub_graphs = adj_lists_.at(src_label);
        for (auto& sub_graph : sub_graphs) {
          tmp.emplace_back(std::array<LabelT, 3>({sub_graph.GetSrcLabel(),
                                                  sub_graph.GetDstLabel(),
                                                  sub_graph.GetEdgeLabel()}));
        }
      }
      ret.emplace_back(std::move(tmp));
    }
    return ret;
  }

  std::vector<std::vector<std::string>> get_prop_names() const {
    std::vector<std::vector<std::string>> ret;
    for (auto iter : adj_lists_) {
      auto& sub_graphs = iter.second;
      for (size_t i = 0; i < sub_graphs.size(); ++i) {
        auto& sub_graph = sub_graphs[i];
        ret.push_back(sub_graph.GetPropNames());
      }
    }
    return ret;
  }

  void sanity_check() {
    CHECK(src_vertices_.size() == label_indices_.size());
    for (auto v : label_indices_) {
      CHECK(v < src_labels_.size());
    }
    CHECK(src_labels_.size() == adj_lists_.size())
        << "src_labels_ : " << src_labels_.size() << ",size "
        << adj_lists_.size();
  }

  std::vector<vid_t> src_vertices_;  // Can contain multiple label of vertices
  std::vector<uint8_t> label_indices_;  // label_indices_[i] is the index of the
                                        // label of src_vertices_[i]
  std::vector<label_t> src_labels_;
  std::unordered_map<label_t, std::vector<sub_graph_t>>
      adj_lists_;        // match src_label to all triplet.
  mutable size_t size_;  // computed lazily
  Direction direction_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_DB_STRUCTURES_MULTI_EDGE_SET_UNTYPED_EDGE_SET_H_