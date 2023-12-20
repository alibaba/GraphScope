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

#ifndef GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_IMMUTABLE_CSR_H_
#define GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_IMMUTABLE_CSR_H_

#include "flex/storages/rt_mutable_graph/csr/adj_list.h"
#include "flex/storages/rt_mutable_graph/csr/csr_base.h"
#include "flex/storages/rt_mutable_graph/csr/nbr.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

template <typename EDATA_T>
class ImmutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = ImmutableNbr<EDATA_T>;

  ImmutableCsr() = default;
  ~ImmutableCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(vnum);

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }

    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(edge_num);

    degree_list_.open(work_dir + "/" + name + ".deg", false);
    degree_list_.resize(vnum);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      adj_lists_[i] = ptr;
      ptr += deg;

      degree_list_[i] = 0;
    }
    return edge_num;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (snapshot_dir != "") {
      degree_list_.open(snapshot_dir + "/" + name + ".deg", true);
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    }
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(degree_list_.size());

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list_.size(); ++i) {
      int deg = degree_list_[i];
      adj_lists_[i] = ptr;
      ptr += deg;
    }
  }

  void warmup(int thread_num) const override {}

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    size_t vnum = degree_list_.size();
    size_t edge_num = nbr_list_.size();

    if (!degree_list_.filename().empty() &&
        std::filesystem::exists(degree_list_.filename())) {
      std::filesystem::create_hard_link(degree_list_.filename(),
                                        new_snapshot_dir + "/" + name + ".deg");
    } else {
      FILE* fp = fopen((new_snapshot_dir + "/" + name + ".deg").c_str(), "wb");
      fwrite(degree_list_.data(), sizeof(int), vnum, fp);
      fflush(fp);
      fclose(fp);
    }

    bool reuse_nbr_list_file = false;
    if (!nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      reuse_nbr_list_file = true;
      nbr_t* ptr = nbr_list_.data();
      for (size_t i = 0; i < vnum; ++i) {
        if (adj_lists_[i] != ptr) {
          reuse_nbr_list_file = false;
          break;
        }
        ptr += degree_list_[i];
      }
      if (ptr != nbr_list_.data() + edge_num) {
        reuse_nbr_list_file = false;
      }
    }
    if (reuse_nbr_list_file) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_snapshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fp = fopen((new_snapshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        int deg = degree_list_[i];
        fwrite(adj_lists_[i], sizeof(nbr_t), deg, fp);
      }
      fflush(fp);
      fclose(fp);
    }
  }

  void resize(vid_t vnum) override { LOG(FATAL) << "not support"; }

  size_t size() const override { return adj_lists_.size(); }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    auto& nbr = adj_lists_[src][degree_list_[src]++];
    nbr.neighbor = dst;
    nbr.data = data;
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  MutableNbrSlice<EDATA_T> get_edges(vid_t i) const override {
    LOG(FATAL) << "not support";
    return MutableNbrSlice<EDATA_T>::empty();
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    LOG(FATAL) << "not support";
    return nullptr;
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    LOG(FATAL) << "not support";
    return nullptr;
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    LOG(FATAL) << "not support";
    return nullptr;
  }

  const nbr_t* get_edges_begin(vid_t v) const override { return adj_lists_[v]; }
  const nbr_t* get_edges_end(vid_t v) const override {
    return adj_lists_[v] + degree_list_[v];
  }

 private:
  mmap_array<nbr_t*> adj_lists_;
  mmap_array<int> degree_list_;
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class SingleImmutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = ImmutableNbr<EDATA_T>;

  SingleImmutableCsr() = default;
  ~SingleImmutableCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
    nbr_list_.resize(vnum);

    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].neighbor = std::numeric_limits<vid_t>::max();
    }

    return vnum;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
  }

  void warmup(int thread_num) const override {}

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    assert(!nbr_list_.read_only());
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void resize(vid_t vnum) override { LOG(FATAL) << "not support"; }

  size_t size() const override { return nbr_list_.size(); }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    auto& nbr = nbr_list_[src];
    CHECK_EQ(nbr.neighbor, std::numeric_limits<vid_t>::max());
    nbr.neighbor = dst;
    nbr.data = data;
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  MutableNbrSlice<EDATA_T> get_edges(vid_t i) const override {
    LOG(FATAL) << "not support";
    return MutableNbrSlice<EDATA_T>::empty();
  }

  const nbr_t& get_edge(vid_t i) const {
    if (i >= nbr_list_.size()) {
      LOG(FATAL) << "i = " << i << ", size = " << nbr_list_.size();
    }
    return nbr_list_[i];
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    LOG(FATAL) << "not support";
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    LOG(FATAL) << "not support";
    return nullptr;
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    LOG(FATAL) << "not support";
    return nullptr;
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    LOG(FATAL) << "not support";
    return nullptr;
  }

  const nbr_t* get_edges_begin(vid_t v) const override { return &nbr_list_[v]; }
  const nbr_t* get_edges_end(vid_t v) const override {
    return nbr_list_[v].neighbor == std::numeric_limits<vid_t>::max()
               ? &nbr_list_[v]
               : (&nbr_list_[v] + 1);
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_IMMUTABLE_CSR_H_