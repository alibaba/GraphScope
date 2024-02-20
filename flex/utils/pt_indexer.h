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

#ifndef GRAPHSCOPE_GRAPH_PT_INDEXER_H_
#define GRAPHSCOPE_GRAPH_PT_INDEXER_H_

#ifdef USE_PTHASH
#include <thread>

#include "grape/util.h"

#include "flex/utils/property/column.h"

#include "flex/utils/pthash_utils/single_phf_view.h"
#include "murmurhash.h"
#include "pthash.hpp"

namespace gs {

struct murmurhash2_64 {
  typedef pthash::hash64 hash_type;

  // specialization for std::string
  static inline hash_type hash(std::string const& val, uint64_t seed) {
    return MurmurHash2_64(val.data(), val.size(), seed);
  }

  // specialization for uint64_t, int64_t, uint32_t
  template <typename EDATA_T>
  static inline hash_type hash(EDATA_T val, uint64_t seed) {
    return MurmurHash2_64(reinterpret_cast<char const*>(&val), sizeof(val),
                          seed);
  }

  // specialization for std::string
  static inline hash_type hash(const std::string_view& val, uint64_t seed) {
    return MurmurHash2_64(val.data(), val.size(), seed);
  }

  static inline hash_type hash(const Any& val, uint64_t seed) {
    if (val.type == PropertyType::kString) {
      return hash(val.AsStringView(), seed);
    } else if (val.type == PropertyType::kInt64) {
      return hash<int64_t>(val.AsInt64(), seed);
    } else if (val.type == PropertyType::kUInt64) {
      return hash<uint64_t>(val.AsUInt64(), seed);
    } else if (val.type == PropertyType::kInt32) {
      return hash<int32_t>(val.AsInt32(), seed);
    } else if (val.type == PropertyType::kUInt32) {
      return hash<uint32_t>(val.AsUInt32(), seed);
    } else {
      LOG(FATAL) << "Unexpected property type: " << val.type;
      return hash_type();
    }
  }
};

template <typename KEY_T, typename INDEX_T>
class PTIndexerBuilder;

template <typename INDEX_T>
class PTIndexer {
 public:
  PTIndexer() : keys_(nullptr), base_size_(0), concat_keys_(nullptr) {}
  ~PTIndexer() {
    if (keys_ != nullptr) {
      delete keys_;
    }
    if (concat_keys_ != nullptr) {
      delete concat_keys_;
    }
  }

  PTIndexer(PTIndexer&& rhs)
      : keys_(rhs.keys_),
        base_map_(rhs.base_map_),
        base_size_(rhs.base_size_),
        extra_indexer_(std::move(rhs.extra_indexer_)) {
    rhs.keys_ = nullptr;
    rhs.concat_keys_ = nullptr;
  }

  void warmup(int) const {}
  static std::string prefix() { return "pthash"; }

  void reserve(size_t capacity) {
    if (capacity > base_size_) {
      extra_indexer_.reserve(capacity - base_size_);
    }
  }

  size_t size() const { return base_size_ + extra_indexer_.size(); }
  size_t capacity() const { return base_size_ + extra_indexer_.capacity(); }
  PropertyType get_type() const { return keys_->type(); }

  INDEX_T get_index(const Any& key) const {
    assert(key.type == get_type());
    size_t index = base_map_(key.AsInt64());
    if (index < base_size_ && keys_->get(index) == key) {
      return index;
    } else {
      return extra_indexer_.get_index(key) + base_size_;
    }
  }

  bool get_index(const Any& oid, INDEX_T& ret) const {
    assert(oid.type == get_type());
    size_t index = base_map_(oid);
    if (index < base_size_ && keys_->get(index) == oid) {
      ret = index;
      return true;
    } else {
      if (extra_indexer_.get_index(oid, ret)) {
        ret += base_size_;
        return true;
      }
      return false;
    }
  }

  INDEX_T insert(const Any& oid) {
    assert(oid.type == get_type());
    size_t index = base_map_(oid);
    if (index < base_size_ && keys_->get(index) == oid) {
      return index;
    }
    return extra_indexer_.insert(oid) + base_size_;
  }

  Any get_key(const INDEX_T& index) const {
    return index < base_size_ ? keys_->get(index)
                              : extra_indexer_.get_key(index - base_size_);
  }

  void dump_meta(const std::string& filename) {
    grape::InArchive arc;
    arc << get_type() << base_size_;
    std::string meta_file_path = filename;
    FILE* fout = fopen(meta_file_path.c_str(), "wb");
    fwrite(arc.GetBuffer(), arc.GetSize(), 1, fout);
    fflush(fout);
    fclose(fout);
  }

  void dump(const std::string& name, const std::string& snapshot_dir) {
    dump_meta(snapshot_dir + "/" + name + ".meta");
    keys_->resize(base_size_);
    keys_->dump(snapshot_dir + "/" + name + ".base_map.keys");
    base_map_.Save(snapshot_dir + "/" + name + ".base_map");
    extra_indexer_.dump(name + ".extra_indexer", snapshot_dir);
  }

  void close() {
    keys_->close();
    extra_indexer_.close();
  }

  void init(const PropertyType& type) {
    if (keys_ != nullptr) {
      delete keys_;
    }
    keys_ = nullptr;
    if (type == PropertyType::kInt64) {
      keys_ = new TypedColumn<int64_t>(StorageStrategy::kMem);
    } else if (type == PropertyType::kInt32) {
      keys_ = new TypedColumn<int32_t>(StorageStrategy::kMem);
    } else if (type == PropertyType::kUInt64) {
      keys_ = new TypedColumn<uint64_t>(StorageStrategy::kMem);
    } else if (type == PropertyType::kUInt32) {
      keys_ = new TypedColumn<uint32_t>(StorageStrategy::kMem);
    } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
      keys_ = new StringColumn(StorageStrategy::kMem,
                               type.additional_type_info.max_length);
    } else {
      LOG(FATAL) << "Not support type [" << type << "] as pk type ..";
    }
  }

  void load_meta(const std::string& filename) {
    std::string meta_file_path = filename;
    size_t meta_file_size = std::filesystem::file_size(meta_file_path);
    std::vector<char> buf(meta_file_size);
    FILE* fin = fopen(meta_file_path.c_str(), "r");
    CHECK_EQ(fread(buf.data(), sizeof(char), meta_file_size, fin),
             meta_file_size);
    grape::OutArchive arc;
    arc.SetSlice(buf.data(), meta_file_size);
    PropertyType type;
    arc >> type >> base_size_;
    init(type);
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    load_meta(snapshot_dir + "/" + name + ".meta");
    base_map_.Open(snapshot_dir + "/" + name + ".base_map");
    keys_->open(name + ".base_map.keys", snapshot_dir, work_dir);
    extra_indexer_.open(name + ".extra_indexer", snapshot_dir, work_dir);

    extra_indexer_.reserve(base_size_ / 2);
  }

  void open_in_memory(const std::string& name) {
    load_meta(name + ".meta");
    base_map_.Open(name + ".base_map");
    keys_->open_in_memory(name + ".base_map.keys");
    extra_indexer_.open_in_memory(name + ".extra_indexer");
    extra_indexer_.reserve(base_size_ / 2);
  }

  void open_with_hugepages(const std::string& name, bool hugepage_table) {
    load_meta(name + ".meta");
    keys_->open_with_hugepages(name + ".keys", true);
    base_map_.Open(name + ".base_map");
    extra_indexer_.open_with_hugepages(name, hugepage_table);
    extra_indexer_.reserve(base_size_ / 2);
  }
  const ColumnBase& get_keys() const {
    if (concat_keys_ != nullptr) {
      delete concat_keys_;
    }
    if (keys_->type() == PropertyType::kInt64) {
      concat_keys_ = new ConcatColumn<int64_t>(
          dynamic_cast<const TypedColumn<int64_t>&>(*keys_),
          dynamic_cast<const TypedColumn<int64_t>&>(extra_indexer_.get_keys()));
    } else if (keys_->type() == PropertyType::kUInt64) {
      concat_keys_ = new ConcatColumn<uint64_t>(
          dynamic_cast<const TypedColumn<uint64_t>&>(*keys_),
          dynamic_cast<const TypedColumn<uint64_t>&>(
              extra_indexer_.get_keys()));
    } else if (keys_->type() == PropertyType::kInt32) {
      concat_keys_ = new ConcatColumn<int32_t>(
          dynamic_cast<const TypedColumn<int32_t>&>(*keys_),
          dynamic_cast<const TypedColumn<int32_t>&>(extra_indexer_.get_keys()));
    } else if (keys_->type() == PropertyType::kUInt32) {
      concat_keys_ = new ConcatColumn<uint32_t>(
          dynamic_cast<const TypedColumn<uint32_t>&>(*keys_),
          dynamic_cast<const TypedColumn<uint32_t>&>(
              extra_indexer_.get_keys()));
    } else {
      concat_keys_ = new ConcatColumn<std::string_view>(
          dynamic_cast<const TypedColumn<std::string_view>&>(*keys_),
          dynamic_cast<const TypedColumn<std::string_view>&>(
              extra_indexer_.get_keys()));
    }
    return *concat_keys_;
  }

 private:
  template <typename _KEY_T, typename _INDEX_T>
  friend class PTIndexerBuilder;

  ColumnBase* keys_;
  SinglePHFView<murmurhash2_64> base_map_;
  size_t base_size_;
  LFIndexer<INDEX_T> extra_indexer_;
  mutable ColumnBase* concat_keys_;
};

class mem_buffer_saver {
 public:
  mem_buffer_saver() = default;
  ~mem_buffer_saver() = default;

  template <typename T>
  void visit(T& val) {
    if constexpr (std::is_pod<T>::value) {
      char* ptr = reinterpret_cast<char*>(&val);
      buf_.insert(buf_.end(), ptr, ptr + sizeof(T));
    } else {
      val.visit(*this);
    }
  }

  template <typename T, typename Allocator>
  void visit(std::vector<T, Allocator>& vec) {
    if constexpr (std::is_pod<T>::value) {
      size_t n = vec.size();
      visit(n);
      char* ptr = reinterpret_cast<char*>(vec.data());
      buf_.insert(buf_.end(), ptr, ptr + sizeof(T) * n);
    } else {
      size_t n = vec.size();
      visit(n);
      for (auto& v : vec)
        visit(v);
    }
  }

  std::vector<char>& buffer() { return buf_; }

 private:
  std::vector<char> buf_;
};

template <typename KEY_T, typename INDEX_T>
class PTIndexerBuilder {
  typedef pthash::single_phf<murmurhash2_64, pthash::dictionary_dictionary,
                             true>
      pthash_type;

 public:
  PTIndexerBuilder() = default;
  ~PTIndexerBuilder() = default;

  void add_vertex(const KEY_T& key) { keys_.push_back(key); }

  void finish(const std::string& filename, const std::string& work_dir,
              PTIndexer<INDEX_T>& output) {
    double t = -grape::GetCurrentTime();
    pthash::build_configuration config;
    config.c = 7.0;
    config.alpha = 0.94;
    int thread_num = std::thread::hardware_concurrency();
    if (keys_.size() > 121242388) {
      config.num_threads = std::min(thread_num, 32);
    } else if (keys_.size() > 100) {
      config.num_threads = std::min(thread_num, 16);
    } else {
      config.num_threads = 1;
    }
    config.minimal_output = true;
    config.verbose_output = false;

    pthash_type phf;
    phf.build_in_internal_memory(keys_.begin(), keys_.size(), config);

    TypedColumn<KEY_T>* keys_column =
        new TypedColumn<KEY_T>(StorageStrategy::kMem);
    keys_column->resize(keys_.size());

    {
      std::vector<std::thread> threads;
      std::atomic<size_t> offset(0);
      size_t total = keys_.size();
      const size_t chunk = 4096;
      for (unsigned i = 0; i < std::thread::hardware_concurrency(); ++i) {
        threads.emplace_back([&]() {
          while (true) {
            size_t begin = offset.fetch_add(chunk);
            if (begin >= total) {
              break;
            }
            size_t end = std::min(begin + chunk, total);
            while (begin < end) {
              keys_column->set_value(phf(keys_[begin]), keys_[begin]);
              ++begin;
            }
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
    }

    if (output.keys_ != NULL) {
      delete output.keys_;
    }
    output.keys_ = keys_column;

    mem_buffer_saver saver;
    saver.visit(phf);

    output.base_size_ = keys_.size();
    output.base_map_.Init(saver.buffer());
    output.extra_indexer_.init(output.keys_->type());
    output.dump(filename, work_dir);
    output.open_in_memory(work_dir + "/" + filename);

    // output.extra_indexer_.set_keys(keys_column->slice(output.base_size_));
    t += grape::GetCurrentTime();
    LOG(INFO) << "construct pthash with " << config.num_threads
              << " threads: " << t << "s";
  }

 private:
  std::vector<KEY_T> keys_;
};

}  // namespace gs
#endif  // USE_PTHASH

#endif  // GRAPHSCOPE_GRAPH_PT_INDEXER_H_