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

#ifndef STORAGES_RT_MUTABLE_GRAPH_CSR_MUTABLE_CSR_H_
#define STORAGES_RT_MUTABLE_GRAPH_CSR_MUTABLE_CSR_H_

#include <thread>

#include "grape/utils/concurrent_queue.h"

#include "flex/storages/rt_mutable_graph/csr/adj_list.h"
#include "flex/storages/rt_mutable_graph/csr/csr_base.h"
#include "flex/storages/rt_mutable_graph/csr/nbr.h"

namespace gs {

template <typename EDATA_T>
class MutableCsrConstEdgeIter : public CsrConstEdgeIterBase {
  using const_nbr_ptr_t = typename MutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

 public:
  explicit MutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~MutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const override { return (*cur_).get_neighbor(); }
  Any get_data() const override {
    return AnyConverter<EDATA_T>::to_any((*cur_).get_data());
  }
  timestamp_t get_timestamp() const override { return (*cur_).get_timestamp(); }

  void next() override { ++cur_; }
  CsrConstEdgeIterBase& operator+=(size_t offset) override {
    cur_ += offset;
    if (!(cur_ < end_)) {
      cur_ = end_;
    }
    return *this;
  }
  bool is_valid() const override { return cur_ != end_; }
  size_t size() const override { return end_ - cur_; }

 private:
  const_nbr_ptr_t cur_;
  const_nbr_ptr_t end_;
};

template <typename EDATA_T>
class MutableCsrEdgeIter : public CsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit MutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~MutableCsrEdgeIter() = default;

  vid_t get_neighbor() const override { return cur_->neighbor; }
  Any get_data() const override {
    return AnyConverter<EDATA_T>::to_any(cur_->data);
  }
  timestamp_t get_timestamp() const override { return cur_->timestamp.load(); }

  void set_data(const Any& value, timestamp_t ts) override {
    ConvertAny<EDATA_T>::to(value, cur_->data);
    cur_->timestamp.store(ts);
  }

  CsrEdgeIterBase& operator+=(size_t offset) override {
    if (cur_ + offset >= end_) {
      cur_ = end_;
    } else {
      cur_ += offset;
    }
    return *this;
  }

  void next() override { ++cur_; }
  bool is_valid() const override { return cur_ != end_; }
  size_t size() const override { return end_ - cur_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};

template <>
class MutableCsrEdgeIter<std::string_view> : public CsrEdgeIterBase {
  using nbr_ptr_t = typename MutableNbrSliceMut<std::string_view>::nbr_ptr_t;

 public:
  explicit MutableCsrEdgeIter(MutableNbrSliceMut<std::string_view> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~MutableCsrEdgeIter() = default;

  vid_t get_neighbor() const override { return cur_.get_neighbor(); }
  Any get_data() const override {
    return AnyConverter<std::string_view>::to_any(cur_.get_data());
  }
  timestamp_t get_timestamp() const override { return cur_.get_timestamp(); }

  void set_data(const Any& value, timestamp_t ts) override {
    cur_.set_data(value.AsStringView(), ts);
  }
  size_t get_index() const { return cur_.get_index(); }
  void set_timestamp(timestamp_t ts) { cur_.set_timestamp(ts); }

  CsrEdgeIterBase& operator+=(size_t offset) override {
    cur_ += offset;
    if (!(cur_ < end_)) {
      cur_ = end_;
    }
    return *this;
  }

  void next() override { ++cur_; }
  bool is_valid() const override { return cur_ != end_; }
  size_t size() const override { return end_.ptr_ - cur_.ptr_; }

 private:
  nbr_ptr_t cur_;
  nbr_ptr_t end_;
};

template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    reserve_ratio = std::max(reserve_ratio, 1.0);
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", true);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += (std::ceil(d * reserve_ratio));
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", true);
    nbr_list_.resize(edge_num);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      int cap = std::ceil(deg * reserve_ratio);
      adj_lists_[i].init(ptr, cap, 0);
      ptr += cap;
    }

    unsorted_since_ = 0;
    return edge_num;
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {
    size_t vnum = adj_lists_.size();
    for (size_t i = 0; i != vnum; ++i) {
      std::sort(adj_lists_[i].data(),
                adj_lists_[i].data() + adj_lists_[i].size(),
                [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.data < rhs.data;
                });
    }
    unsorted_since_ = ts;
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    mmap_array<int>* cap_list = &degree_list;
    if (snapshot_dir != "") {
      degree_list.open(snapshot_dir + "/" + name + ".deg", false);
      if (std::filesystem::exists(snapshot_dir + "/" + name + ".cap")) {
        cap_list = new mmap_array<int>();
        cap_list->open(snapshot_dir + "/" + name + ".cap", false);
      }
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", false);
      load_meta(snapshot_dir + "/" + name);
    }
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
    adj_lists_.open(work_dir + "/" + name + ".adj", true);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      int cap = (*cap_list)[i];
      adj_lists_[i].init(ptr, cap, degree);
      ptr += cap;
    }
    if (cap_list != &degree_list) {
      delete cap_list;
    }
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    mmap_array<int> degree_list;
    degree_list.open(prefix + ".deg", false);
    load_meta(prefix);
    mmap_array<int>* cap_list = &degree_list;
    if (std::filesystem::exists(prefix + ".cap")) {
      cap_list = new mmap_array<int>();
      cap_list->open(prefix + ".cap", false);
    }

    nbr_list_.open(prefix + ".nbr", false);

    adj_lists_.reset();
    v_cap = std::max(v_cap, degree_list.size());
    adj_lists_.resize(v_cap);
    locks_ = new grape::SpinLock[v_cap];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      int cap = (*cap_list)[i];
      adj_lists_[i].init(ptr, cap, degree);
      ptr += cap;
    }
    for (size_t i = degree_list.size(); i < v_cap; ++i) {
      adj_lists_[i].init(ptr, 0, 0);
    }

    if (cap_list != &degree_list) {
      delete cap_list;
    }
  }

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override {
    mmap_array<int> degree_list;
    degree_list.open(prefix + ".deg", false);
    load_meta(prefix);
    mmap_array<int>* cap_list = &degree_list;
    if (std::filesystem::exists(prefix + ".cap")) {
      cap_list = new mmap_array<int>();
      cap_list->open(prefix + ".cap", false);
    }

    nbr_list_.open_with_hugepages(prefix + ".nbr");

    adj_lists_.reset();
    v_cap = std::max(v_cap, degree_list.size());
    adj_lists_.open_with_hugepages("");
    adj_lists_.resize(v_cap);
    locks_ = new grape::SpinLock[v_cap];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      int cap = (*cap_list)[i];
      adj_lists_[i].init(ptr, cap, degree);
      ptr += cap;
    }
    for (size_t i = degree_list.size(); i < v_cap; ++i) {
      adj_lists_[i].init(ptr, 0, 0);
    }

    if (cap_list != &degree_list) {
      delete cap_list;
    }
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    dump_meta(new_snapshot_dir + "/" + name);
    mmap_array<int> degree_list;
    std::vector<int> cap_list;
    degree_list.open(new_snapshot_dir + "/" + name + ".deg", true);
    degree_list.resize(vnum);
    cap_list.resize(vnum);
    bool need_cap_list = false;
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      offset += adj_lists_[i].capacity();

      degree_list[i] = adj_lists_[i].size();
      cap_list[i] = adj_lists_[i].capacity();
      if (degree_list[i] != cap_list[i]) {
        need_cap_list = true;
      }
    }

    if (need_cap_list) {
      FILE* fcap_out =
          fopen((new_snapshot_dir + "/" + name + ".cap").c_str(), "wb");
      CHECK_EQ(fwrite(cap_list.data(), sizeof(int), cap_list.size(), fcap_out),
               cap_list.size());
      fflush(fcap_out);
      fclose(fcap_out);
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_snapshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        CHECK_EQ(fwrite(adj_lists_[i].data(), sizeof(nbr_t),
                        adj_lists_[i].capacity(), fout),
                 adj_lists_[i].capacity());
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void warmup(int thread_num) const override {
    size_t vnum = adj_lists_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    const size_t chunk = 4096;
    std::atomic<size_t> output(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);

          if (begin == end) {
            break;
          }

          while (begin < end) {
            auto adj_list = get_edges(begin);
            for (auto& nbr : adj_list) {
              ret += nbr.neighbor;
            }
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(NULL, 0, 0);
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }
  size_t size() const override { return adj_lists_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<MutableCsrConstEdgeIter<EDATA_T>>(get_edges(v));
  }
  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new MutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<MutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }

  slice_t get_edges(vid_t v) const override {
    return adj_lists_[v].get_edges();
  }

  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }

 private:
  void load_meta(const std::string& prefix) {
    std::string meta_file_path = prefix + ".meta";
    if (std::filesystem::exists(meta_file_path)) {
      FILE* meta_file_fd = fopen(meta_file_path.c_str(), "r");
      CHECK_EQ(fread(&unsorted_since_, sizeof(timestamp_t), 1, meta_file_fd),
               1);
      fclose(meta_file_fd);
    } else {
      unsorted_since_ = 0;
    }
  }

  void dump_meta(const std::string& prefix) const {
    std::string meta_file_path = prefix + ".meta";
    FILE* meta_file_fd = fopen((prefix + ".meta").c_str(), "wb");
    CHECK_EQ(fwrite(&unsorted_since_, sizeof(timestamp_t), 1, meta_file_fd), 1);
    fflush(meta_file_fd);
    fclose(meta_file_fd);
  }

  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
  timestamp_t unsorted_since_;
};

template <>
class MutableCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using adjlist_t = MutableAdjlist<std::string_view>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;

  MutableCsr(StringColumn& column) : column_(column), locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", true);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", true);
    nbr_list_.resize(edge_num);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      adj_lists_[i].init(ptr, deg, 0);
      ptr += deg;
    }
    return edge_num;
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t data,
                                 timestamp_t ts) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    if (snapshot_dir != "") {
      degree_list.open(snapshot_dir + "/" + name + ".deg", false);
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", false);
    }
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
    adj_lists_.open(work_dir + "/" + name + ".adj", true);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      adj_lists_[i].init(ptr, degree, degree);
      ptr += degree;
    }
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    mmap_array<int> degree_list;
    degree_list.open(prefix + ".deg", false);
    nbr_list_.open(prefix + ".nbr", false);
    adj_lists_.reset();
    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      adj_lists_[i].init(ptr, degree, degree);
      ptr += degree;
    }
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    mmap_array<int> degree_list;
    degree_list.open(new_snapshot_dir + "/" + name + ".deg", true);
    degree_list.resize(vnum);
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      degree_list[i] = adj_lists_[i].size();
      offset += degree_list[i];
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_snapshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        CHECK_EQ(fwrite(adj_lists_[i].data(), sizeof(nbr_t),
                        adj_lists_[i].size(), fout),
                 adj_lists_[i].size());
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void warmup(int thread_num) const override {
    size_t vnum = adj_lists_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    const size_t chunk = 4096;
    std::atomic<size_t> output(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);

          if (begin == end) {
            break;
          }

          while (begin < end) {
            auto adj_list = get_edges(begin);
            for (auto& nbr : adj_list) {
              ret += nbr.get_neighbor();
            }
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(NULL, 0, 0);
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<MutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new MutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }
  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<MutableCsrEdgeIter<std::string_view>>(
        get_edges_mut(v));
  }

  void put_edge(vid_t src, vid_t dst, size_t data, timestamp_t ts,
                Allocator& alloc) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {
    put_edge(src, dst, index, ts, alloc);
  }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges(column_);
  }

  mut_slice_t get_edges_mut(vid_t i) {
    return adj_lists_[i].get_edges_mut(column_);
  }

 private:
  StringColumn& column_;
  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", true);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
    return vnum;
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {}

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", true);
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open(prefix + ".snbr", false);
    if (nbr_list_.size() < v_cap) {
      size_t old_size = nbr_list_.size();
      nbr_list_.reset();
      nbr_list_.resize(v_cap);
      FILE* fin = fopen((prefix + ".snbr").c_str(), "r");
      CHECK_EQ(fread(nbr_list_.data(), sizeof(nbr_t), old_size, fin), old_size);
      fclose(fin);
      for (size_t k = old_size; k != v_cap; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    }
  }

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open_with_hugepages(prefix + ".snbr", v_cap);
    size_t old_size = nbr_list_.size();
    if (old_size < v_cap) {
      nbr_list_.resize(v_cap);
      for (size_t k = old_size; k != v_cap; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    }
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void warmup(int thread_num) const override {
    size_t vnum = nbr_list_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    std::atomic<size_t> output(0);
    const size_t chunk = 4096;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);
          if (begin == end) {
            break;
          }
          while (begin < end) {
            auto& nbr = nbr_list_[begin];
            ret += nbr.neighbor;
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<MutableCsrConstEdgeIter<EDATA_T>>(get_edges(v));
  }
  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new MutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<MutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  slice_t get_edges(vid_t v) const override {
    slice_t ret;
    ret.set_size(nbr_list_[v].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[v]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }

 private:
  mmap_array<nbr_t> nbr_list_;
};

template <>
class SingleMutableCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;

  SingleMutableCsr(StringColumn& column) : column_(column) {}
  ~SingleMutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", true);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
    return vnum;
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t data,
                                 timestamp_t ts) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {}

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", true);
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open(prefix + ".snbr", false);
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void warmup(int thread_num) const override {
    size_t vnum = nbr_list_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    std::atomic<size_t> output(0);
    const size_t chunk = 4096;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);
          if (begin == end) {
            break;
          }
          while (begin < end) {
            auto& nbr = nbr_list_[begin];
            ret += nbr.neighbor;
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<MutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new MutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }

  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<MutableCsrEdgeIter<std::string_view>>(
        get_edges_mut(v));
  }

  void put_edge(vid_t src, vid_t dst, size_t data, timestamp_t ts, Allocator&) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {
    put_edge(src, dst, index, ts, alloc);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret(column_);
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret(column_);
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  MutableNbr<std::string_view> get_edge(vid_t i) const {
    MutableNbr<std::string_view> nbr;
    nbr.neighbor = nbr_list_[i].neighbor;
    nbr.timestamp.store(nbr_list_[i].timestamp.load());
    nbr.data = column_.get_view(nbr_list_[i].data);
    return nbr;
  }

 private:
  StringColumn& column_;
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;

  EmptyCsr() = default;
  ~EmptyCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    return 0;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void open_in_memory(const std::string& prefix, size_t v_cap) override {}

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override {}

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {}

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator&) override {}

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<MutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new MutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<MutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {}

  timestamp_t unsorted_since() const override {
    return std::numeric_limits<timestamp_t>::max();
  }

  slice_t get_edges(vid_t v) const override { return slice_t::empty(); }
};

template <>
class EmptyCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using slice_t = MutableNbrSlice<std::string_view>;

  EmptyCsr(StringColumn& column) : column_(column) {}
  ~EmptyCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    return 0;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void open_in_memory(const std::string& prefix, size_t v_cap) override {}

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {}

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {}
  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t data,
                                 timestamp_t ts = 0) override {}
  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<MutableCsrConstEdgeIter<std::string_view>>(
        MutableNbrSlice<std::string_view>::empty(column_));
  }
  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new MutableCsrConstEdgeIter<std::string_view>(
        MutableNbrSlice<std::string_view>::empty(column_));
  }
  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<MutableCsrEdgeIter<std::string_view>>(
        MutableNbrSliceMut<std::string_view>::empty(column_));
  }

  slice_t get_edges(vid_t v) const override { return slice_t::empty(column_); }

  StringColumn& column_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_CSR_MUTABLE_CSR_H_
