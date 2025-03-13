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

#ifndef EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_UTILS_H_
#define EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_UTILS_H_

#include <algorithm>
#include <random>
#include <vector>

#include <grape/grape.h>

#ifdef USE_SIMD_SORT
#include "x86-simd-sort/avx512-32bit-qsort.hpp"
#include "x86-simd-sort/avx512-64bit-qsort.hpp"
#endif

namespace grape {

template <typename LABEL_T>
using LabelMapType = std::map<LABEL_T, int>;

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast(const ADJ_LIST_T& edges,
                                 const VERTEX_ARRAY_T& labels) {
  static thread_local std::vector<LABEL_T> local_labels;
  local_labels.clear();
  for (auto& e : edges) {
    local_labels.emplace_back(labels[e.get_neighbor()]);
  }
#ifdef USE_SIMD_SORT
  avx512_qsort(local_labels.data(), local_labels.size());
#else
  std::sort(local_labels.begin(), local_labels.end());
#endif

  LABEL_T curr_label = local_labels[0];
  int curr_count = 1;
  LABEL_T best_label = LABEL_T{};
  int best_count = 0;
  int label_num = local_labels.size();

  for (int i = 1; i < label_num; ++i) {
    if (local_labels[i] != local_labels[i - 1]) {
      if (curr_count > best_count) {
        best_label = curr_label;
        best_count = curr_count;
      }
      curr_label = local_labels[i];
      curr_count = 1;
    } else {
      ++curr_count;
    }
  }

  if (curr_count > best_count) {
    return curr_label;
  } else {
    return best_label;
  }
}

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_jump(const ADJ_LIST_T& edges,
                                      const VERTEX_ARRAY_T& labels) {
  static thread_local std::vector<LABEL_T> local_labels;
  local_labels.clear();
  for (auto& e : edges) {
    local_labels.emplace_back(labels[e.get_neighbor()]);
  }
#ifdef USE_SIMD_SORT
  avx512_qsort(local_labels.data(), local_labels.size());
#else
  std::sort(local_labels.begin(), local_labels.end());
#endif

  LABEL_T curr_label = local_labels[0];
  int curr = 1;
  int label_num = local_labels.size();

  while ((curr != label_num) && (local_labels[curr] == curr_label)) {
    ++curr;
  }

  LABEL_T best_label = curr_label;
  int best_count = curr;

  while ((curr + best_count) < label_num) {
    curr_label = local_labels[curr];
    int next = curr + best_count;
    if (local_labels[next] == curr_label) {
      int mid = (curr + label_num) / 2;
      if (local_labels[mid] == curr_label) {
        return curr_label;
      }
      do {
        ++next;
      } while (next != label_num && (local_labels[next] == curr_label));
      best_count = (next - curr);
      best_label = curr_label;
      curr = next;
    } else {
      curr = next;
      curr_label = local_labels[next];
      while (local_labels[curr - 1] == curr_label) {
        --curr;
      }
    }
  }

  return best_label;
}

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_sparse(const ADJ_LIST_T& edges,
                                        const VERTEX_ARRAY_T& labels) {
  static thread_local LabelMapType<LABEL_T> labels_map;
  labels_map.clear();
  for (auto& e : edges) {
    ++labels_map[labels[e.get_neighbor()]];
  }
  LABEL_T ret{};
  int max_count = 0;
  for (auto& pair : labels_map) {
    if (pair.second > max_count ||
        (pair.second == max_count && ret > pair.first)) {
      ret = pair.first;
      max_count = pair.second;
    }
  }
  return ret;
}

template <typename T, typename CNT_T = int>
class LabelHashMap {
 public:
  LabelHashMap() {}
  ~LabelHashMap() {}

  void resize(size_t n) {
    entries_.resize(n);
    counts_.resize(n, 0);
  }

  void emplace(const T& val) {
    size_t len = entries_.size();
    size_t index = val % len;
    while (true) {
      if (counts_[index] == 0) {
        counts_[index] = 1;
        entries_[index] = val;
        index_.push_back(index);
        break;
      } else if (entries_[index] == val) {
        ++counts_[index];
        break;
      }
      index = (index + 1) % len;
    }
  }

  T get_most_frequent_label() {
    T ret = std::numeric_limits<T>::max();
    CNT_T freq = 0;
    if (index_.size() <= entries_.size() / 2) {
      for (auto ind : index_) {
        if (counts_[ind] > freq) {
          freq = counts_[ind];
          ret = entries_[ind];
        } else if (counts_[ind] == freq && entries_[ind] < ret) {
          ret = entries_[ind];
        }
        counts_[ind] = 0;
      }
    } else {
      CNT_T num_entries = entries_.size();
      for (CNT_T i = 0; i != num_entries; ++i) {
        if (counts_[i] > freq) {
          freq = counts_[i];
          ret = entries_[i];
        } else if (counts_[i] == freq && entries_[i] < ret) {
          ret = entries_[i];
        }
        counts_[i] = 0;
      }
    }
    index_.clear();
    return ret;
  }

 private:
  std::vector<T> entries_;
  std::vector<CNT_T> counts_;
  std::vector<CNT_T> index_;
};

template <typename LABEL_T, typename VERTEX_ARRAY_T, typename ADJ_LIST_T>
inline LABEL_T update_label_fast_dense(const ADJ_LIST_T& edges,
                                       const VERTEX_ARRAY_T& labels) {
  static thread_local LabelHashMap<LABEL_T> labels_map;
  labels_map.resize(edges.Size());
  for (auto& e : edges) {
    labels_map.emplace(labels[e.get_neighbor()]);
  }
  return labels_map.get_most_frequent_label();
}

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CDLP_CDLP_UTILS_H_
