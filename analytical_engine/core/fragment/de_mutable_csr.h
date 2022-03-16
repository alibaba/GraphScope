/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DE_MUTABLE_CSR_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DE_MUTABLE_CSR_H_

#include <limits>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "grape/graph/de_mutable_csr.h"

#include "core/object/dynamic.h"

namespace grape {

/**
 * @brief Dedupe mutable CSR specialized for DynamicFragment
 *
 * This is specialzed DeMutableCSR class definition for DynamicFragment which
 * is a wrapper of MutableCSR and provides batch add/remove/reserve edges
 * operations.
 *
 */
template <>
class DeMutableCSR<
    vineyard::property_graph_types::VID_TYPE,
    Nbr<vineyard::property_graph_types::VID_TYPE, gs::dynamic::Value>> {
 public:
  using vid_t = vineyard::property_graph_types::VID_TYPE;
  using edata_t = gs::dynamic::Value;
  using nbr_t = Nbr<vid_t, edata_t>;
  using edge_t = Edge<vid_t, edata_t>;
  using adj_list_t = AdjList<vid_t, edata_t>;

  static constexpr double dense_threshold = 0.003;

  DeMutableCSR() {
    dedup_ = false;
    enable_tail_ = false;
  }

  DeMutableCSR(vid_t from, vid_t to, bool dedup = false,
               bool enable_tail = false)
      : min_id_(from),
        max_id_(to),
        max_head_id_(from),
        min_tail_id_(to),
        dedup_(dedup),
        enable_tail_(enable_tail) {}

  void Init(vid_t from, vid_t to, bool dedup = false,
            bool enable_tail = false) {
    min_id_ = from;
    max_id_ = to;
    max_head_id_ = from;
    min_tail_id_ = to;
    dedup_ = dedup;
    enable_tail_ = enable_tail;
  }

  vid_t vertex_num() const {
    return (max_id_ - min_tail_id_) + (max_head_id_ - min_id_);
  }

  vid_t head_vertex_num() const { return (max_head_id_ - min_id_); }

  vid_t tail_vertex_num() const { return (max_id_ - min_tail_id_); }

  bool empty() const { return head_.empty() && tail_.empty(); }

  size_t edge_num() const { return head_.edge_num() + tail_.edge_num(); }

  int degree(vid_t i) const {
    return in_head(i) ? head_.degree(head_index(i))
                      : tail_.degree(tail_index(i));
  }

  void remove_vertex(vid_t i) {
    if (in_head(i)) {
      head_.remove_vertex(head_index(i));
    } else if (enable_tail_) {
      tail_.remove_vertex(tail_index(i));
    }
  }

  bool is_empty(vid_t i) const {
    return in_head(i) ? head_.is_empty(head_index(i))
                      : tail_.is_empty(tail_index(i));
  }

  inline bool in_head(vid_t i) const { return i < max_head_id_; }

  inline vid_t head_index(vid_t i) const { return i - min_id_; }

  inline vid_t tail_index(vid_t i) const { return max_id_ - i - 1; }

  inline vid_t get_index(vid_t i) const {
    return in_head(i) ? head_index(i) : tail_index(i);
  }

  nbr_t* get_begin(vid_t i) {
    return in_head(i) ? head_.get_begin(head_index(i))
                      : tail_.get_begin(tail_index(i));
  }

  const nbr_t* get_begin(vid_t i) const {
    return in_head(i) ? head_.get_begin(head_index(i))
                      : tail_.get_begin(tail_index(i));
  }

  nbr_t* get_end(vid_t i) {
    return in_head(i) ? head_.get_end(head_index(i))
                      : tail_.get_end(tail_index(i));
  }

  const nbr_t* get_end(vid_t i) const {
    return in_head(i) ? head_.get_end(head_index(i))
                      : tail_.get_end(tail_index(i));
  }

  void add_vertices(vid_t to_head, vid_t to_tail) {
    max_head_id_ += to_head;
    vid_t head_num = max_head_id_ - min_id_;
    head_.reserve_vertices(head_num);

    if (enable_tail_) {
      min_tail_id_ -= to_tail;
      vid_t tail_num = max_id_ - min_tail_id_;
      tail_.reserve_vertices(tail_num);
    }
  }

  void add_edges(const std::vector<edge_t>& edges) {
    double rate =
        static_cast<double>(edges.size()) / static_cast<double>(edge_num());
    if (rate < dense_threshold) {
      add_edges_sparse(edges);
    } else {
      add_edges_dense(edges);
    }
  }

  void add_forward_edges(const std::vector<edge_t>& edges) {
    double rate =
        static_cast<double>(edges.size()) / static_cast<double>(edge_num());
    if (rate < dense_threshold) {
      add_forward_edges_sparse(edges);
    } else {
      add_forward_edges_dense(edges);
    }
  }

  void add_reversed_edges(const std::vector<edge_t>& edges) {
    double rate =
        static_cast<double>(edges.size()) / static_cast<double>(edge_num());
    if (rate < dense_threshold) {
      add_reversed_edges_sparse(edges);
    } else {
      add_reversed_edges_dense(edges);
    }
  }

  void remove_edges(const std::vector<edge_t>& edges) {
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = max_id_ - min_tail_id_;
    std::vector<bool> head_modified(head_num, false),
        tail_modified(tail_num, false);
    if (dedup_) {
      for (auto& e : edges) {
        vid_t src = e.src;
        if (in_head(src)) {
          vid_t index = head_index(src);
          head_modified[index] =
              head_.remove_one_with_tomb(index, e.dst) || head_modified[index];
        } else if (enable_tail_) {
          vid_t index = tail_index(src);
          tail_modified[index] =
              tail_.remove_one_with_tomb(index, e.dst) || tail_modified[index];
        }
      }
    } else {
      for (auto& e : edges) {
        vid_t src = e.src;
        if (in_head(src)) {
          vid_t index = head_index(src);
          head_modified[index] =
              head_.remove_with_tomb(index, e.dst) || head_modified[index];
        } else if (enable_tail_) {
          vid_t index = tail_index(src);
          tail_modified[index] =
              tail_.remove_with_tomb(index, e.dst) || tail_modified[index];
        }
      }
    }
    for (vid_t i = 0; i < head_num; ++i) {
      if (head_modified[i]) {
        head_.remove_tombs(i);
      }
    }
    if (enable_tail_) {
      for (vid_t i = 0; i < tail_num; ++i) {
        if (tail_modified[i]) {
          tail_.remove_tombs(i);
        }
      }
    }
  }

  void remove_edges(const std::vector<std::pair<vid_t, vid_t>>& edges) {
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = max_id_ - min_tail_id_;
    std::vector<bool> head_modified(head_num, false),
        tail_modified(tail_num, false);
    static constexpr vid_t sentinel = std::numeric_limits<vid_t>::max();
    if (dedup_) {
      for (auto& e : edges) {
        if (e.first == sentinel) {
          continue;
        }
        vid_t src = e.first;
        if (in_head(src)) {
          vid_t index = head_index(src);
          head_modified[index] = head_.remove_one_with_tomb(index, e.second) ||
                                 head_modified[index];
        } else if (enable_tail_) {
          vid_t index = tail_index(src);
          tail_modified[index] = tail_.remove_one_with_tomb(index, e.second) ||
                                 tail_modified[index];
        }
      }
    } else {
      for (auto& e : edges) {
        if (e.first == sentinel) {
          continue;
        }
        vid_t src = e.first;
        if (in_head(src)) {
          vid_t index = head_index(src);
          head_modified[index] =
              head_.remove_with_tomb(index, e.second) || head_modified[index];
        } else if (enable_tail_) {
          vid_t index = tail_index(src);
          tail_modified[index] =
              tail_.remove_with_tomb(index, e.second) || tail_modified[index];
        }
      }
    }
    for (vid_t i = 0; i < head_num; ++i) {
      if (head_modified[i]) {
        head_.remove_tombs(i);
      }
    }
    if (enable_tail_) {
      for (vid_t i = 0; i < tail_num; ++i) {
        if (tail_modified[i]) {
          tail_.remove_tombs(i);
        }
      }
    }
  }

  void remove_reversed_edges(
      const std::vector<std::pair<vid_t, vid_t>>& edges) {
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = max_id_ - min_tail_id_;
    std::vector<bool> head_modified(head_num, false),
        tail_modified(tail_num, false);
    static constexpr vid_t sentinel = std::numeric_limits<vid_t>::max();
    if (dedup_) {
      for (auto& e : edges) {
        if (e.first == sentinel) {
          continue;
        }
        vid_t src = e.second;
        if (in_head(src)) {
          vid_t index = head_index(src);
          head_modified[index] = head_.remove_one_with_tomb(index, e.first) ||
                                 head_modified[index];
        } else if (enable_tail_) {
          vid_t index = tail_index(src);
          tail_modified[index] = tail_.remove_one_with_tomb(index, e.first) ||
                                 tail_modified[index];
        }
      }
    } else {
      for (auto& e : edges) {
        if (e.first == sentinel) {
          continue;
        }
        vid_t src = e.second;
        if (in_head(src)) {
          vid_t index = head_index(src);
          head_modified[index] =
              head_.remove_with_tomb(index, e.first) || head_modified[index];
        } else if (enable_tail_) {
          vid_t index = tail_index(src);
          tail_modified[index] =
              tail_.remove_with_tomb(index, e.first) || tail_modified[index];
        }
      }
    }
    for (vid_t i = 0; i < head_num; ++i) {
      if (head_modified[i]) {
        head_.remove_tombs(i);
      }
    }
    if (enable_tail_) {
      for (vid_t i = 0; i < tail_num; ++i) {
        if (tail_modified[i]) {
          tail_.remove_tombs(i);
        }
      }
    }
  }

  template <typename FUNC_T>
  void remove_if(const FUNC_T& func) {
    head_.remove_if(func);
    tail_.remove_if(func);
  }

  void update_edges(const std::vector<edge_t>& edges) {
    static constexpr vid_t sentinel = std::numeric_limits<vid_t>::max();
    if (dedup_) {
      for (auto& e : edges) {
        if (e.src == sentinel) {
          continue;
        }
        vid_t src = e.src;
        if (in_head(src)) {
          head_.update_one(head_index(src), e.dst, e.edata);
        } else if (enable_tail_) {
          tail_.update_one(tail_index(src), e.dst, e.edata);
        }
      }
    } else {
      for (auto& e : edges) {
        if (e.src == sentinel) {
          continue;
        }
        vid_t src = e.src;
        if (in_head(src)) {
          head_.update(head_index(src), e.dst, e.edata);
        } else if (enable_tail_) {
          tail_.update(tail_index(src), e.dst, e.edata);
        }
      }
    }
  }

  void update_reversed_edges(const std::vector<edge_t>& edges) {
    static constexpr vid_t sentinel = std::numeric_limits<vid_t>::max();
    if (dedup_) {
      for (auto& e : edges) {
        if (e.src == sentinel) {
          continue;
        }
        vid_t src = e.dst;
        if (in_head(src)) {
          head_.update_one(head_index(src), e.src, e.edata);
        } else if (enable_tail_) {
          tail_.update_one(tail_index(src), e.src, e.edata);
        }
      }
    } else {
      for (auto& e : edges) {
        if (e.src == sentinel) {
          continue;
        }
        vid_t src = e.dst;
        if (in_head(src)) {
          head_.update(head_index(src), e.src, e.edata);
        } else if (enable_tail_) {
          tail_.update(tail_index(src), e.src, e.edata);
        }
      }
    }
  }

  void reserve_forward_edges(const std::vector<edge_t>& edges) {
    double rate =
        static_cast<double>(edges.size()) / static_cast<double>(edge_num());
    if (rate < dense_threshold) {
      std::map<vid_t, int> head_degree_to_add, tail_degree_to_add;
      reserve_forward_edges_sparse(edges, head_degree_to_add,
                                   tail_degree_to_add);
    } else {
      std::vector<int> head_degree_to_add, tail_degree_to_add;
      reserve_forward_edges_dense(edges, head_degree_to_add,
                                  tail_degree_to_add);
    }
  }

  void reserve_reversed_edges(const std::vector<edge_t>& edges) {
    double rate =
        static_cast<double>(edges.size()) / static_cast<double>(edge_num());
    if (rate < dense_threshold) {
      std::map<vid_t, int> head_degree_to_add, tail_degree_to_add;
      reserve_reversed_edges_sparse(edges, head_degree_to_add,
                                    tail_degree_to_add);
    } else {
      std::vector<int> head_degree_to_add, tail_degree_to_add;
      reserve_reversed_edges_dense(edges, head_degree_to_add,
                                   tail_degree_to_add);
    }
  }

  void reserve_edges(const std::vector<edge_t>& edges) {
    double rate =
        static_cast<double>(edges.size()) / static_cast<double>(edge_num());
    if (rate < dense_threshold) {
      std::map<vid_t, int> head_degree_to_add, tail_degree_to_add;
      reserve_edges_sparse(edges, head_degree_to_add, tail_degree_to_add);
    } else {
      std::vector<int> head_degree_to_add, tail_degree_to_add;
      reserve_edges_dense(edges, head_degree_to_add, tail_degree_to_add);
    }
  }

  void add_edge(const edge_t& e) {
    if (in_head(e.src)) {
      head_.put_edge(head_index(e.src), nbr_t(e.dst, e.edata));
    } else if (enable_tail_) {
      tail_.put_edge(tail_index(e.src), nbr_t(e.dst, e.edata));
    }
  }

  void add_reversed_edge(const edge_t& e) {
    if (in_head(e.dst)) {
      head_.put_edge(head_index(e.dst), nbr_t(e.src, e.edata));
    } else if (enable_tail_) {
      tail_.put_edge(tail_index(e.dst), nbr_t(e.src, e.edata));
    }
  }

  template <typename IOADAPTOR_T>
  void Serialize(std::unique_ptr<IOADAPTOR_T>& writer) {
    InArchive ia;
    ia << min_id_ << max_id_ << max_head_id_ << min_tail_id_ << dedup_;
    CHECK(writer->WriteArchive(ia));
    head_.template Serialize<IOADAPTOR_T>(writer);
    tail_.template Serialize<IOADAPTOR_T>(writer);
  }

  template <typename IOADAPTOR_T>
  void Deserialize(std::unique_ptr<IOADAPTOR_T>& reader) {
    OutArchive oa;
    CHECK(reader->ReadArchive(oa));
    oa >> min_id_ >> max_id_ >> max_head_id_ >> min_tail_id_ >> dedup_;
    head_.template Deserialize<IOADAPTOR_T>(reader);
    tail_.template Deserialize<IOADAPTOR_T>(reader);
  }

  void clear_edges() {
    head_.clear_edges();
    tail_.clear_edges();
    return;
  }

  void copy_from(const DeMutableCSR<vid_t, nbr_t>& source_csr) {
    min_id_ = source_csr.min_id_;
    max_id_ = source_csr.max_id_;
    max_head_id_ = source_csr.max_head_id_;
    min_tail_id_ = source_csr.min_tail_id_;
    dedup_ = source_csr.dedup_;
    enable_tail_ = source_csr.enable_tail_;
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = max_id_ - min_tail_id_;

    head_.reserve_vertices(head_num);
    if (enable_tail_) {
      tail_.reserve_vertices(tail_num);
    }
    std::vector<int> head_degree_to_add(head_num, 0);
    for (vid_t i = 0; i < head_num; ++i) {
      head_degree_to_add[i] = source_csr.head_.degree(i);
    }
    head_.reserve_edges_dense(head_degree_to_add);
    if (enable_tail_) {
      std::vector<int> tail_degree_to_add(tail_num, 0);
      for (vid_t i = 0; i < tail_num; ++i) {
        tail_degree_to_add[i] = source_csr.tail_.degree(i);
      }
      tail_.reserve_edges_dense(tail_degree_to_add);
    }

    for (vid_t i = 0; i < head_num; ++i) {
      auto begin = source_csr.head_.get_begin(i);
      auto end = source_csr.head_.get_end(i);
      for (auto iter = begin; iter != end; ++iter) {
        head_.put_edge(i, *iter);
      }
    }
    if (enable_tail_) {
      for (vid_t i = 0; i < tail_num; ++i) {
        auto begin = source_csr.tail_.get_begin(i);
        auto end = source_csr.tail_.get_end(i);
        for (auto iter = begin; iter != end; ++iter) {
          tail_.put_edge(i, *iter);
        }
      }
    }
  }

  void dedup_or_sort_neighbors_dense(
      const std::vector<int>& head_degree_to_add,
      const std::vector<int>& tail_degree_to_add) {
    if (dedup_) {
      head_.dedup_neighbors_dense(head_degree_to_add);
      if (enable_tail_) {
        tail_.dedup_neighbors_dense(tail_degree_to_add);
      }
    } else {
      head_.sort_neighbors_dense(head_degree_to_add);
      if (enable_tail_) {
        tail_.sort_neighbors_dense(tail_degree_to_add);
      }
    }
  }

  void dedup_or_sort_neighbors_sparse(
      const std::map<vid_t, int>& head_degree_to_add,
      const std::map<vid_t, int>& tail_degree_to_add) {
    if (dedup_) {
      head_.dedup_neighbors_sparse(head_degree_to_add);
      if (enable_tail_) {
        tail_.dedup_neighbors_sparse(tail_degree_to_add);
      }
    } else {
      head_.sort_neighbors_sparse(head_degree_to_add);
      if (enable_tail_) {
        tail_.sort_neighbors_sparse(tail_degree_to_add);
      }
    }
  }

 private:
  void add_edges_dense(const std::vector<edge_t>& edges) {
    std::vector<int> head_degree_to_add, tail_degree_to_add;
    reserve_edges_dense(edges, head_degree_to_add, tail_degree_to_add);

    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        head_.put_edge(head_index(e.src), nbr_t(e.dst, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.src), nbr_t(e.dst, e.edata));
      }
      if (in_head(e.dst)) {
        head_.put_edge(head_index(e.dst), nbr_t(e.src, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.dst), nbr_t(e.src, e.edata));
      }
    }

    dedup_or_sort_neighbors_dense(head_degree_to_add, tail_degree_to_add);
  }

  void add_reversed_edges_dense(const std::vector<edge_t>& edges) {
    std::vector<int> head_degree_to_add, tail_degree_to_add;
    reserve_reversed_edges_dense(edges, head_degree_to_add, tail_degree_to_add);

    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.dst)) {
        head_.put_edge(head_index(e.dst), nbr_t(e.src, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.dst), nbr_t(e.src, e.edata));
      }
    }

    dedup_or_sort_neighbors_dense(head_degree_to_add, tail_degree_to_add);
  }

  void add_forward_edges_dense(const std::vector<edge_t>& edges) {
    std::vector<int> head_degree_to_add, tail_degree_to_add;
    reserve_forward_edges_dense(edges, head_degree_to_add, tail_degree_to_add);

    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        head_.put_edge(head_index(e.src), nbr_t(e.dst, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.src), nbr_t(e.dst, e.edata));
      }
    }

    dedup_or_sort_neighbors_dense(head_degree_to_add, tail_degree_to_add);
  }

  void add_edges_sparse(const std::vector<edge_t>& edges) {
    std::map<vid_t, int> head_degree_to_add, tail_degree_to_add;
    reserve_edges_sparse(edges, head_degree_to_add, tail_degree_to_add);

    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        head_.put_edge(head_index(e.src), nbr_t(e.dst, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.src), nbr_t(e.dst, e.edata));
      }
      if (in_head(e.dst)) {
        head_.put_edge(head_index(e.dst), nbr_t(e.src, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.dst), nbr_t(e.src, e.edata));
      }
    }

    dedup_or_sort_neighbors_sparse(head_degree_to_add, tail_degree_to_add);
  }

  void add_forward_edges_sparse(const std::vector<edge_t>& edges) {
    std::map<vid_t, int> head_degree_to_add, tail_degree_to_add;
    reserve_forward_edges_sparse(edges, head_degree_to_add, tail_degree_to_add);

    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        head_.put_edge(head_index(e.src), nbr_t(e.dst, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.src), nbr_t(e.dst, e.edata));
      }
    }

    dedup_or_sort_neighbors_sparse(head_degree_to_add, tail_degree_to_add);
  }

  void add_reversed_edges_sparse(const std::vector<edge_t>& edges) {
    std::map<vid_t, int> head_degree_to_add, tail_degree_to_add;
    reserve_reversed_edges_sparse(edges, head_degree_to_add,
                                  tail_degree_to_add);

    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.dst)) {
        head_.put_edge(head_index(e.dst), nbr_t(e.src, e.edata));
      } else if (enable_tail_) {
        tail_.put_edge(tail_index(e.dst), nbr_t(e.src, e.edata));
      }
    }

    dedup_or_sort_neighbors_sparse(head_degree_to_add, tail_degree_to_add);
  }

  void reserve_edges_dense(const std::vector<edge_t>& edges,
                           std::vector<int>& head_degree_to_add,
                           std::vector<int>& tail_degree_to_add) {
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = enable_tail_ ? max_id_ - min_tail_id_ : 0;

    head_degree_to_add.resize(head_num, 0);
    tail_degree_to_add.resize(tail_num, 0);
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        ++head_degree_to_add[head_index(e.src)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.src)];
      }
      if (in_head(e.dst)) {
        ++head_degree_to_add[head_index(e.dst)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.dst)];
      }
    }

    head_.reserve_edges_dense(head_degree_to_add);
    if (enable_tail_) {
      tail_.reserve_edges_dense(tail_degree_to_add);
    }
  }

  void reserve_forward_edges_dense(const std::vector<edge_t>& edges,
                                   std::vector<int>& head_degree_to_add,
                                   std::vector<int>& tail_degree_to_add) {
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = enable_tail_ ? max_id_ - min_tail_id_ : 0;

    head_degree_to_add.resize(head_num, 0);
    tail_degree_to_add.resize(tail_num, 0);
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        ++head_degree_to_add[head_index(e.src)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.src)];
      }
    }

    head_.reserve_edges_dense(head_degree_to_add);
    if (enable_tail_) {
      tail_.reserve_edges_dense(tail_degree_to_add);
    }
  }

  void reserve_reversed_edges_dense(const std::vector<edge_t>& edges,
                                    std::vector<int>& head_degree_to_add,
                                    std::vector<int>& tail_degree_to_add) {
    vid_t head_num = max_head_id_ - min_id_;
    vid_t tail_num = enable_tail_ ? max_id_ - min_tail_id_ : 0;

    head_degree_to_add.resize(head_num, 0);
    tail_degree_to_add.resize(tail_num, 0);
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.dst)) {
        ++head_degree_to_add[head_index(e.dst)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.dst)];
      }
    }

    head_.reserve_edges_dense(head_degree_to_add);
    if (enable_tail_) {
      tail_.reserve_edges_dense(tail_degree_to_add);
    }
  }

  void reserve_edges_sparse(const std::vector<edge_t>& edges,
                            std::map<vid_t, int>& head_degree_to_add,
                            std::map<vid_t, int>& tail_degree_to_add) {
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        ++head_degree_to_add[head_index(e.src)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.src)];
      }
      if (in_head(e.dst)) {
        ++head_degree_to_add[head_index(e.dst)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.dst)];
      }
    }

    head_.reserve_edges_sparse(head_degree_to_add);
    if (enable_tail_) {
      tail_.reserve_edges_sparse(tail_degree_to_add);
    }
  }

  void reserve_forward_edges_sparse(const std::vector<edge_t>& edges,
                                    std::map<vid_t, int>& head_degree_to_add,
                                    std::map<vid_t, int>& tail_degree_to_add) {
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.src)) {
        ++head_degree_to_add[head_index(e.src)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.src)];
      }
    }

    head_.reserve_edges_sparse(head_degree_to_add);
    if (enable_tail_) {
      tail_.reserve_edges_sparse(tail_degree_to_add);
    }
  }

  void reserve_reversed_edges_sparse(const std::vector<edge_t>& edges,
                                     std::map<vid_t, int>& head_degree_to_add,
                                     std::map<vid_t, int>& tail_degree_to_add) {
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    for (auto& e : edges) {
      if (e.src == invalid_vid) {
        continue;
      }
      if (in_head(e.dst)) {
        ++head_degree_to_add[head_index(e.dst)];
      } else if (enable_tail_) {
        ++tail_degree_to_add[tail_index(e.dst)];
      }
    }

    head_.reserve_edges_sparse(head_degree_to_add);
    if (enable_tail_) {
      tail_.reserve_edges_sparse(tail_degree_to_add);
    }
  }

  template <typename _vid_t, typename _NBR_T>
  friend class DeMutableCSRBuilder;

  vid_t min_id_;
  vid_t max_id_;

  vid_t max_head_id_;
  vid_t min_tail_id_;
  bool dedup_;
  bool enable_tail_;

  MutableCSR<vid_t, Nbr<vid_t, edata_t>> head_, tail_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DE_MUTABLE_CSR_H_
