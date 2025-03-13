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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_OPT_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_OPT_CONTEXT_H_

#include <grape/grape.h>

#include <iomanip>
#include <limits>
#include <vector>

#include <grape/config.h>

namespace grape {

namespace lcc_opt_impl {

template <typename T>
class ref_vector {
 public:
  ref_vector() : ptr_(nullptr), num_(0) {}
  ref_vector(T* ptr, size_t num) : ptr_(ptr), num_(num) {}
  ~ref_vector() {}

  size_t size() const { return num_; }

  const T& operator[](size_t idx) const { return ptr_[idx]; }

  bool empty() const { return num_ == 0; }

  T* begin() { return ptr_; }
  T* end() { return (ptr_ + num_); }

  const T* begin() const { return ptr_; }
  const T* end() const { return (ptr_ + num_); }

  const T* data() const { return ptr_; }
  T* data() { return ptr_; }

 private:
  T* ptr_;
  size_t num_;
};

template <typename T>
class memory_pool : public ::grape::Allocator<T> {
 public:
  memory_pool() : cur_begin_(nullptr), cur_end_(nullptr), cur_limit_(nullptr) {}
  ~memory_pool() {
    for (auto& vec : allocated_) {
      this->deallocate(vec.data(), vec.size());
    }
  }

  void reserve(size_t cap) {
    CHECK_EQ(cur_begin_, cur_end_);
    size_t remaining = cur_limit_ - cur_end_;
    if (remaining < cap) {
      size_t min_cap =
          std::max(cap, (40 * 1024 * 1024 + sizeof(T) - 1) / sizeof(T));
      T* new_buf = this->allocate(min_cap);
      allocated_.emplace_back(new_buf, min_cap);
      cur_begin_ = cur_end_ = new_buf;
      cur_limit_ = new_buf + min_cap;
    }
  }

  void push_back(const T& val) { *(cur_end_++) = val; }

  const T& operator[](size_t idx) const { return cur_begin_[idx]; }
  T& operator[](size_t idx) { return cur_begin_[idx]; }

  size_t size() const { return cur_end_ - cur_begin_; }
  size_t capacity() const { return cur_limit_ - cur_begin_; }

  void resize(size_t sz) {
    CHECK_LE(sz, capacity());
    cur_end_ = cur_begin_ + sz;
  }

  T* begin() { return cur_begin_; }
  T* end() { return cur_end_; }

  const T* begin() const { return cur_begin_; }
  const T* end() const { return cur_end_; }

#ifndef USE_BMISS_STTNI_INTERSECT
  ref_vector<T> finish() {
    ref_vector<T> ret = ref_vector<T>(cur_begin_, cur_end_ - cur_begin_);
    cur_begin_ = cur_end_;
    return ret;
  }
#else
  T* align_to(T* input, size_t bytes = 16) {
    size_t ptr_val = reinterpret_cast<size_t>(input);
    ptr_val = (ptr_val + bytes - 1) / bytes * bytes;
    return reinterpret_cast<T*>(ptr_val);
  }

  ref_vector<T> finish() {
    ref_vector<T> ret = ref_vector<T>(cur_begin_, cur_end_ - cur_begin_);
    cur_begin_ = align_to(cur_end_);
    cur_end_ = cur_begin_;
    return ret;
  }
#endif

 private:
  T* cur_begin_;
  T* cur_end_;
  T* cur_limit_;
  std::vector<ref_vector<T>> allocated_;
};

}  // namespace lcc_opt_impl

/**
 * @brief Context for the parallel version of LCCOpt.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T, typename COUNT_T>
class LCCOptContext : public VertexDataContext<FRAG_T, double> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using count_t = COUNT_T;

  explicit LCCOptContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, double>(fragment) {
    global_degree.Init(fragment.Vertices());
    complete_neighbor.Init(fragment.Vertices());
    tricnt.Init(fragment.Vertices());
  }

  void Init(ParallelMessageManagerOpt& messages) { tricnt.SetValue(0); }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (global_degree[v] == 0 || global_degree[v] == 1) {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << 0.0 << std::endl;
      } else {
        double re = 2.0 * (static_cast<count_t>(tricnt[v])) /
                    (static_cast<int64_t>(global_degree[v]) *
                     (static_cast<int64_t>(global_degree[v]) - 1));
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << re << std::endl;
      }
    }
  }

  typename FRAG_T::template vertex_array_t<int> global_degree;
  std::vector<lcc_opt_impl::memory_pool<vertex_t>> memory_pools;
  typename FRAG_T::template vertex_array_t<lcc_opt_impl::ref_vector<vertex_t>>
      complete_neighbor;

  typename FRAG_T::template vertex_array_t<count_t> tricnt;
  int stage = 0;

  size_t degree_x = 0;
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_OPT_CONTEXT_H_
