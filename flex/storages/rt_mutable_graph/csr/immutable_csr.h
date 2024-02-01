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

#ifndef STORAGES_RT_MUTABLE_GRAPH_CSR_IMMUTABLE_CSR_H_
#define STORAGES_RT_MUTABLE_GRAPH_CSR_IMMUTABLE_CSR_H_

namespace gs {

template <typename EDATA_T>
class ImmutableCsrConstEdgeIter : public CsrConstEdgeIterBase {
  using const_nbr_ptr_t = typename ImmutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

 public:
  explicit ImmutableCsrConstEdgeIter(const ImmutableNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~ImmutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const override { return (*cur_).get_neighbor(); }
  Any get_data() const override {
    return AnyConverter<EDATA_T>::to_any((*cur_).get_data());
  }
  timestamp_t get_timestamp() const override { return 0; }

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
class ImmutableCsr : public TypedImmutableCsrBase<EDATA_T> {
 public:
  using nbr_t = ImmutableNbr<EDATA_T>;
  using slice_t = ImmutableNbrSlice<EDATA_T>;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", true);
    adj_lists_.resize(vnum);

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }

    nbr_list_.open(work_dir + "/" + name + ".nbr", true);
    nbr_list_.resize(edge_num);

    degree_list_.open(work_dir + "/" + name + ".deg", true);
    degree_list_.resize(vnum);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      if (deg != 0) {
        adj_lists_[i] = ptr;
      } else {
        adj_lists_[i] = NULL;
      }
      ptr += deg;

      degree_list_[i] = 0;
    }

    unsorted_since_ = 0;
    return edge_num;
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts) override {
    auto& nbr = adj_lists_[src][degree_list_[src]++];
    nbr.neighbor = dst;
    nbr.data = data;
  }

  void batch_sort_by_edge_data(timestamp_t ts) override {
    size_t vnum = adj_lists_.size();
    for (size_t i = 0; i != vnum; ++i) {
      std::sort(adj_lists_[i], adj_lists_[i] + degree_list_[i],
                [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.data < rhs.data;
                });
    }
    unsorted_since_ = ts;
  }

  timestamp_t unsorted_since() const override { return unsorted_since_; }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    // Changes made to the CSR will not be synchronized to the file
    // TODO(luoxiaojian): Implement the insert operation on ImmutableCsr.
    if (snapshot_dir != "") {
      degree_list_.open(snapshot_dir + "/" + name + ".deg", false);
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", false);
      load_meta(snapshot_dir + "/" + name);
    }

    adj_lists_.open(work_dir + "/" + name + ".adj", true);
    adj_lists_.resize(degree_list_.size());

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list_.size(); ++i) {
      int deg = degree_list_[i];
      adj_lists_[i] = ptr;
      ptr += deg;
    }
  }

  void open_in_memory(const std::string& prefix, size_t v_cap) override {
    degree_list_.open(prefix + ".deg", false);
    load_meta(prefix);
    nbr_list_.open(prefix + ".nbr", false);
    adj_lists_.reset();
    v_cap = std::max(v_cap, degree_list_.size());
    adj_lists_.resize(v_cap);
    size_t old_degree_size = degree_list_.size();
    degree_list_.resize(v_cap);

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < old_degree_size; ++i) {
      int deg = degree_list_[i];
      if (deg != 0) {
        adj_lists_[i] = ptr;
      } else {
        adj_lists_[i] = NULL;
      }
      ptr += deg;
    }
    for (size_t i = old_degree_size; i < degree_list_.size(); ++i) {
      degree_list_[i] = 0;
      adj_lists_[i] = NULL;
    }
  }

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override {
    degree_list_.open_with_hugepages(prefix + ".deg", v_cap);
    load_meta(prefix);
    nbr_list_.open_with_hugepages(prefix + ".nbr");
    adj_lists_.reset();
    v_cap = std::max(v_cap, degree_list_.size());
    adj_lists_.resize(v_cap);
    size_t old_degree_size = degree_list_.size();
    degree_list_.resize(v_cap);

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < old_degree_size; ++i) {
      int deg = degree_list_[i];
      if (deg != 0) {
        adj_lists_[i] = ptr;
      } else {
        adj_lists_[i] = NULL;
      }
      ptr += deg;
    }
    for (size_t i = old_degree_size; i < degree_list_.size(); ++i) {
      degree_list_[i] = 0;
      adj_lists_[i] = NULL;
    }
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    dump_meta(new_snapshot_dir + "/" + name);
    size_t vnum = adj_lists_.size();
    {
      FILE* fout =
          fopen((new_snapshot_dir + "/" + name + ".deg").c_str(), "wb");
      fwrite(degree_list_.data(), sizeof(int), vnum, fout);
      fflush(fout);
      fclose(fout);
    }
    {
      FILE* fout =
          fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t k = 0; k < vnum; ++k) {
        if (adj_lists_[k] != NULL && degree_list_[k] != 0) {
          fwrite(adj_lists_[k], sizeof(nbr_t), degree_list_[k], fout);
        }
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k] = NULL;
        degree_list_[k] = 0;
      }
    } else {
      adj_lists_.resize(vnum);
      degree_list_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<ImmutableCsrConstEdgeIter<EDATA_T>>(get_edges(v));
  }
  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new ImmutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return nullptr;
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    LOG(FATAL) << "Put single edge is not supported";
  }

  slice_t get_edges(vid_t v) const override {
    slice_t ret;
    ret.set_begin(adj_lists_[v]);
    ret.set_size(degree_list_[v]);
    return ret;
  }

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

  mmap_array<nbr_t*> adj_lists_;
  mmap_array<int> degree_list_;
  mmap_array<nbr_t> nbr_list_;
  timestamp_t unsorted_since_;
};

template <typename EDATA_T>
class SingleImmutableCsr : public TypedImmutableCsrBase<EDATA_T> {
 public:
  using nbr_t = ImmutableNbr<EDATA_T>;
  using slice_t = ImmutableNbrSlice<EDATA_T>;

  SingleImmutableCsr() {}
  ~SingleImmutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", true);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
    }
    return vnum;
  }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts) override {
    CHECK_EQ(nbr_list_[src].neighbor, std::numeric_limits<vid_t>::max());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
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
        nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
      }
    }
  }

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open_with_hugepages(prefix + ".snbr", v_cap);
    size_t old_size = nbr_list_.size();
    if (old_size < v_cap) {
      nbr_list_.resize(v_cap);
      for (size_t k = old_size; k != v_cap; ++k) {
        nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
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
        nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<ImmutableCsrConstEdgeIter<EDATA_T>>(get_edges(v));
  }

  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new ImmutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }

  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return nullptr;
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator&) override {
    CHECK_LT(src, nbr_list_.size());
    CHECK_EQ(nbr_list_[src].neighbor, std::numeric_limits<vid_t>::max());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(
        nbr_list_[i].neighbor == std::numeric_limits<vid_t>::max() ? 0 : 1);
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
class SingleImmutableCsr<std::string_view>
    : public TypedImmutableCsrBase<std::string_view> {
 public:
  using nbr_t = ImmutableNbr<size_t>;
  using slice_t = ImmutableNbrSlice<std::string_view>;

  SingleImmutableCsr(StringColumn& column) : column_(column) {}
  ~SingleImmutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree,
                    double reserve_ratio) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", true);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
    }
    return vnum;
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, size_t data,
                                 timestamp_t ts) override {
    CHECK_EQ(nbr_list_[src].neighbor, std::numeric_limits<vid_t>::max());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
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
        nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
      }
    }
  }

  void open_with_hugepages(const std::string& prefix, size_t v_cap) override {
    nbr_list_.open_with_hugepages(prefix + ".snbr", v_cap);
    size_t old_size = nbr_list_.size();
    if (old_size < v_cap) {
      nbr_list_.resize(v_cap);
      for (size_t k = old_size; k != v_cap; ++k) {
        nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
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
        nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  std::shared_ptr<CsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<ImmutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  CsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new ImmutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }

  std::shared_ptr<CsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return nullptr;
  }

  void put_edge_with_index(vid_t src, vid_t dst, size_t data, timestamp_t ts,
                           Allocator&) override {
    CHECK_LT(src, nbr_list_.size());
    CHECK_EQ(nbr_list_[src].neighbor, std::numeric_limits<vid_t>::max());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret(column_);
    ret.set_size(
        nbr_list_[i].neighbor == std::numeric_limits<vid_t>::max() ? 0 : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  ImmutableNbr<std::string_view> get_edge(vid_t i) const {
    ImmutableNbr<std::string_view> nbr;
    nbr.neighbor = nbr_list_[i].neighbor;
    nbr.data = column_.get_view(nbr_list_[i].data);
    return nbr;
  }

 private:
  StringColumn& column_;
  mmap_array<nbr_t> nbr_list_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_CSR_IMMUTABLE_CSR_H_