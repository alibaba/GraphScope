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

#ifndef FLEX_UTILS_TOP_N_GENERATOR_H_
#define FLEX_UTILS_TOP_N_GENERATOR_H_

#include <queue>
#include <vector>

namespace gs {

template <typename T>
struct TopNUnit {
  TopNUnit(const T& val_, size_t idx_) : val(val_), idx(idx_) {}
  T val;
  size_t idx;
};

template <typename T>
struct TopNAscCmp {
  using elem_t = TopNUnit<T>;
  inline bool operator()(const elem_t& lhs, const elem_t& rhs) const {
    return lhs.val < rhs.val;
  }

  inline bool operator()(const T& lhs, const T& rhs) const { return lhs < rhs; }
};

template <typename T>
struct TopNDescCmp {
  using elem_t = TopNUnit<T>;
  inline bool operator()(const elem_t& lhs, const elem_t& rhs) const {
    return rhs.val < lhs.val;
  }

  inline bool operator()(const T& lhs, const T& rhs) const { return rhs < lhs; }
};

// CMP_T lhs > rhs is desc
// CMP_T lhs < rhs is asc
template <typename T, typename CMP_T>
class TopNGenerator {
  using unit_t = TopNUnit<T>;

 public:
  TopNGenerator(size_t n) : n_(n), pq_(CMP_T()) {}

  inline void push(const T& val, size_t idx) {
    if (pq_.empty()) {
      pq_.emplace(val, idx);
      return;
    }
    if (pq_.top().val == val) {
      replicated_indices_.push_back(idx);
    } else if (CMP_T()(pq_.top().val, val)) {
      if (pq_.size() + replicated_indices_.size() < n_) {
        for (auto i : replicated_indices_) {
          pq_.emplace(pq_.top().val, i);
        }
        replicated_indices_.clear();
        pq_.emplace(val, idx);
      }
    } else {
      if (pq_.size() < n_) {
        pq_.emplace(val, idx);
      } else {
        pq_.pop();
        replicated_indices_.clear();

        pq_.emplace(val, idx);
        auto cur = std::move(pq_.top());
        pq_.pop();

        while (!pq_.empty() && pq_.top().val == cur.val) {
          replicated_indices_.push_back(pq_.top().idx);
          pq_.pop();
        }
        pq_.push(cur);
      }
    }
  }

  void generate_indices(std::vector<size_t>& indices) {
    indices = std::move(replicated_indices_);
    replicated_indices_.clear();
    while (!pq_.empty()) {
      indices.push_back(pq_.top().idx);
      pq_.pop();
    }
  }

  void generate_pairs(std::vector<T>& values, std::vector<size_t>& indices) {
    indices = std::move(replicated_indices_);
    replicated_indices_.clear();
    values.clear();
    values.resize(indices.size(), pq_.top().val);
    while (!pq_.empty()) {
      values.push_back(pq_.top().val);
      indices.push_back(pq_.top().idx);
      pq_.pop();
    }
  }

 private:
  size_t n_;
  std::priority_queue<unit_t, std::vector<unit_t>, CMP_T> pq_;
  std::vector<size_t> replicated_indices_;
};

template <typename T, typename CMP_T>
class InplaceTopNGenerator {
  using unit_t = TopNUnit<T>;

 public:
  InplaceTopNGenerator(size_t n) : n_(n) {}

  void generate_indices(const std::vector<T>& input,
                        std::vector<size_t>& indices) {
    size_t size = input.size();
    std::priority_queue<unit_t, std::vector<unit_t>, CMP_T> pq(CMP_T{});
    for (size_t i = 0; i < size; ++i) {
      if (pq.size() < n_) {
        pq.emplace(input[i], i);
      } else if (CMP_T()(input[i], pq.top().val)) {
        pq.pop();
        pq.emplace(input[i], i);
      }
    }

    T top_val = pq.top().val;
    pq.pop();
    for (size_t i = 0; i < size; ++i) {
      if (input[i] == top_val) {
        indices.push_back(i);
      }
    }
    while (!pq.empty() && pq.top().val == top_val) {
      pq.pop();
    }
    while (!pq.empty()) {
      indices.push_back(pq.top().idx);
      pq.pop();
    }
  }

 private:
  size_t n_;
};

}  // namespace gs

#endif  // FLEX_UTILS_TOP_N_GENERATOR_H_