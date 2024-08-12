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

#ifndef GRAPHSCOPE_PROPERTY_COLUMN_H_
#define GRAPHSCOPE_PROPERTY_COLUMN_H_

#include <string>
#include <string_view>
#include "grape/utils/concurrent_queue.h"

#include "flex/utils/mmap_array.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void open_in_memory(const std::string& name) = 0;

  virtual void open_with_hugepages(const std::string& name, bool force) = 0;

  virtual void close() = 0;

  virtual void touch(const std::string& filename) = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void copy_to_tmp(const std::string& cur_path,
                           const std::string& tmp_path) = 0;
  virtual void resize(size_t size) = 0;

  virtual PropertyType type() const = 0;

  virtual void set_any(size_t index, const Any& value) = 0;

  virtual Any get(size_t index) const = 0;

  virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  virtual StorageStrategy storage_strategy() const = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path)) {
      basic_buffer_.open(basic_path, false);
      basic_size_ = basic_buffer_.size();
    } else {
      basic_size_ = 0;
    }
    if (work_dir == "") {
      extra_size_ = 0;
    } else {
      extra_buffer_.open(work_dir + "/" + name, true);
      extra_size_ = extra_buffer_.size();
    }
  }

  void open_in_memory(const std::string& name) override {
    if (!name.empty() && std::filesystem::exists(name)) {
      basic_buffer_.open(name, false);
      basic_size_ = basic_buffer_.size();
    } else {
      basic_buffer_.reset();
      basic_size_ = 0;
    }
    extra_buffer_.reset();
    extra_size_ = 0;
  }

  void open_with_hugepages(const std::string& name, bool force) override {
    if (strategy_ == StorageStrategy::kMem || force) {
      if (!name.empty() && std::filesystem::exists(name)) {
        basic_buffer_.open_with_hugepages(name);
        basic_size_ = basic_buffer_.size();
      } else {
        basic_buffer_.reset();
        basic_buffer_.set_hugepage_prefered(true);
        basic_size_ = 0;
      }
      extra_buffer_.reset();
      extra_buffer_.set_hugepage_prefered(true);
      extra_size_ = 0;
    } else if (strategy_ == StorageStrategy::kDisk) {
      LOG(INFO) << "Open " << name << " with normal mmap pages";
      open_in_memory(name);
    }
  }

  void touch(const std::string& filename) override {
    mmap_array<T> tmp;
    tmp.open(filename, true);
    tmp.resize(basic_size_ + extra_size_);
    for (size_t k = 0; k < basic_size_; ++k) {
      tmp.set(k, basic_buffer_.get(k));
    }
    for (size_t k = 0; k < extra_size_; ++k) {
      tmp.set(k + basic_size_, extra_buffer_.get(k));
    }
    basic_size_ = 0;
    basic_buffer_.reset();
    extra_size_ = tmp.size();
    extra_buffer_.swap(tmp);
    tmp.reset();
  }

  void close() override {
    basic_buffer_.reset();
    extra_buffer_.reset();
  }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    mmap_array<T> tmp;
    if (!std::filesystem::exists(cur_path)) {
      return;
    }
    copy_file(cur_path, tmp_path);
    extra_size_ = basic_size_;
    basic_size_ = 0;
    tmp.open(tmp_path, true);
    basic_buffer_.reset();
    extra_buffer_.swap(tmp);
    tmp.reset();
  }

  void dump(const std::string& filename) override {
    if (basic_size_ != 0 && extra_size_ == 0) {
      basic_buffer_.dump(filename);
    } else if (basic_size_ == 0 && extra_size_ != 0) {
      extra_buffer_.dump(filename);
    } else {
      mmap_array<T> tmp;
      tmp.open(filename, true);
      for (size_t k = 0; k < basic_size_; ++k) {
        tmp.set(k, basic_buffer_.get(k));
      }
      for (size_t k = 0; k < extra_size_; ++k) {
        tmp.set(k + basic_size_, extra_buffer_.get(k));
      }
    }
  }

  size_t size() const override { return basic_size_ + extra_size_; }

  void resize(size_t size) override {
    if (size < basic_buffer_.size()) {
      basic_size_ = size;
      extra_size_ = 0;
    } else {
      basic_size_ = basic_buffer_.size();
      extra_size_ = size - basic_size_;
      extra_buffer_.resize(extra_size_);
    }
  }

  PropertyType type() const override { return AnyConverter<T>::type(); }

  void set_value(size_t index, const T& val) {
    if (index >= basic_size_ && index < basic_size_ + extra_size_) {
      extra_buffer_.set(index - basic_size_, val);
    } else if (index < basic_size_) {
      basic_buffer_.set(index, val);
    } else {
      throw std::runtime_error("Index out of range");
    }
  }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  T get_view(size_t index) const {
    return index < basic_size_ ? basic_buffer_.get(index)
                               : extra_buffer_.get(index - basic_size_);
  }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(get_view(index));
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<T>& basic_buffer() const { return basic_buffer_; }
  size_t basic_buffer_size() const { return basic_size_; }
  const mmap_array<T>& extra_buffer() const { return extra_buffer_; }
  size_t extra_buffer_size() const { return extra_size_; }

 private:
  mmap_array<T> basic_buffer_;
  size_t basic_size_;
  mmap_array<T> extra_buffer_;
  size_t extra_size_;
  StorageStrategy strategy_;
};

template <>
class TypedColumn<RecordView> : public ColumnBase {
 public:
  TypedColumn(const std::vector<PropertyType>& types) : types_(types) {
    if (types.size() == 0) {
      LOG(FATAL) << "RecordView column must have sub types.";
    }
  }

  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    LOG(FATAL) << "RecordView column does not support open.";
  }

  void open_in_memory(const std::string& name) override;

  void open_with_hugepages(const std::string& name, bool force) override {
    LOG(FATAL) << "RecordView column does not support open with hugepages.";
  }

  void touch(const std::string& filename) override {
    LOG(FATAL) << "RecordView column does not support touch.";
  }

  void dump(const std::string& filename) override {
    LOG(FATAL) << "RecordView column does not support dump.";
  }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    LOG(FATAL) << "RecordView column does not support copy_to_tmp.";
  }
  void close() override;

  size_t size() const override;
  void resize(size_t size) override;

  PropertyType type() const override { return PropertyType::kRecordView; }

  void set_any(size_t index, const Any& value) override;

  void set_value(size_t index, const RecordView& val);

  RecordView get_view(size_t index) const;

  Any get(size_t index) const override;

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    LOG(FATAL) << "RecordView column does not support ingest.";
  }

  StorageStrategy storage_strategy() const override {
    LOG(ERROR) << "RecordView column does not have storage strategy.";
    return StorageStrategy::kMem;
  }

  std::vector<PropertyType> sub_types() const { return types_; }

 private:
  std::vector<PropertyType> types_;
  std::shared_ptr<Table> table_;
};

using BoolColumn = TypedColumn<bool>;
using IntColumn = TypedColumn<int32_t>;
using UIntColumn = TypedColumn<uint32_t>;
using LongColumn = TypedColumn<int64_t>;
using ULongColumn = TypedColumn<uint64_t>;
using DateColumn = TypedColumn<Date>;
using DayColumn = TypedColumn<Day>;
using DoubleColumn = TypedColumn<double>;
using FloatColumn = TypedColumn<float>;
using RecordViewColumn = TypedColumn<RecordView>;

template <>
class TypedColumn<grape::EmptyType> : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void open_in_memory(const std::string& name) override {}
  void open_with_hugepages(const std::string& name, bool force) override {}
  void touch(const std::string& filename) override {}
  void dump(const std::string& filename) override {}
  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}

  PropertyType type() const override { return PropertyType::kEmpty; }

  void set_any(size_t index, const Any& value) override {}

  Any get(size_t index) const override { return Any(); }

  void ingest(uint32_t index, grape::OutArchive& arc) override {}

  StorageStrategy storage_strategy() const override { return strategy_; }

 private:
  StorageStrategy strategy_;
};
template <>
class TypedColumn<std::string_view> : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy,
              uint16_t width = PropertyType::STRING_DEFAULT_MAX_LENGTH)
      : strategy_(strategy), width_(width) {}
  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path + ".items")) {
      basic_buffer_.open(basic_path, false);
      basic_size_ = basic_buffer_.size();
      basic_pos_ = basic_buffer_.data_size();
    } else {
      basic_size_ = 0;
      basic_pos_ = 0;
    }
    if (work_dir == "") {
      extra_size_ = 0;
      pos_.store(0);
    } else {
      extra_buffer_.open(work_dir + "/" + name, true);
      extra_size_ = extra_buffer_.size();
      pos_.store(extra_buffer_.data_size());
    }
  }

  void open_in_memory(const std::string& prefix) override {
    basic_buffer_.open(prefix, false);
    basic_size_ = basic_buffer_.size();
    basic_pos_ = basic_buffer_.data_size();

    extra_buffer_.reset();
    extra_size_ = 0;
    pos_.store(0);
  }

  void open_with_hugepages(const std::string& prefix, bool force) override {
    if (strategy_ == StorageStrategy::kMem || force) {
      basic_buffer_.open_with_hugepages(prefix);
      basic_size_ = basic_buffer_.size();
      basic_pos_ = basic_buffer_.data_size();

      extra_buffer_.reset();
      extra_buffer_.set_hugepage_prefered(true);
      extra_size_ = 0;
      pos_.store(0);
    } else if (strategy_ == StorageStrategy::kDisk) {
      LOG(INFO) << "Open " << prefix << " with normal mmap pages";
      open_in_memory(prefix);
    }
  }

  void touch(const std::string& filename) override {
    mmap_array<std::string_view> tmp;
    tmp.open(filename, true);
    tmp.resize(basic_size_ + extra_size_, (basic_size_ + extra_size_) * width_);
    size_t offset = 0;
    for (size_t k = 0; k < basic_size_; ++k) {
      std::string_view val = basic_buffer_.get(k);
      tmp.set(k, offset, val);
      offset += val.size();
    }
    for (size_t k = 0; k < extra_size_; ++k) {
      std::string_view val = extra_buffer_.get(k);
      tmp.set(k + basic_size_, offset, val);
      offset += val.size();
    }

    basic_size_ = 0;
    basic_pos_ = 0;
    basic_buffer_.reset();
    extra_size_ = tmp.size();
    extra_buffer_.swap(tmp);
    tmp.reset();

    pos_.store(offset);
  }

  void close() override {
    basic_buffer_.reset();
    extra_buffer_.reset();
  }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    mmap_array<std::string_view> tmp;
    if (!std::filesystem::exists(cur_path + ".data")) {
      return;
    }
    copy_file(cur_path + ".data", tmp_path + ".data");
    copy_file(cur_path + ".items", tmp_path + ".items");

    extra_size_ = basic_size_ + extra_size_;
    basic_size_ = 0;
    basic_pos_ = 0;
    basic_buffer_.reset();
    tmp.open(tmp_path, true);
    extra_buffer_.swap(tmp);
    tmp.reset();
    pos_.store(extra_buffer_.data_size());
  }

  void dump(const std::string& filename) override {
    if (basic_size_ != 0 && extra_size_ == 0) {
      basic_buffer_.resize(basic_size_, basic_pos_.load());
      basic_buffer_.dump(filename);
    } else if (basic_size_ == 0 && extra_size_ != 0) {
      extra_buffer_.resize(extra_size_, pos_.load());
      extra_buffer_.dump(filename);
    } else {
      mmap_array<std::string_view> tmp;
      tmp.open(filename, true);
      tmp.resize(basic_size_ + extra_size_,
                 (basic_size_ + extra_size_) * width_);
      size_t offset = 0;
      for (size_t k = 0; k < basic_size_; ++k) {
        std::string_view val = basic_buffer_.get(k);
        tmp.set(k, offset, val);
        offset += val.size();
      }
      for (size_t k = 0; k < extra_size_; ++k) {
        std::string_view val = extra_buffer_.get(k);
        tmp.set(k + basic_size_, offset, extra_buffer_.get(k));
        offset += val.size();
      }
      tmp.resize(basic_size_ + extra_size_, offset);
      tmp.reset();
    }
  }

  size_t size() const override { return basic_size_ + extra_size_; }

  void resize(size_t size) override {
    if (size < basic_buffer_.size()) {
      basic_size_ = size;
      extra_size_ = 0;
    } else {
      basic_size_ = basic_buffer_.size();
      extra_size_ = size - basic_size_;
      if (basic_buffer_.size() != 0) {
        size_t basic_avg_width =
            (basic_buffer_.data_size() + basic_buffer_.size() - 1) /
            basic_buffer_.size();
        //  extra_size_ * basic_avg_width may be smaller than pos_.load()
        extra_buffer_.resize(
            extra_size_, std::max(extra_size_ * basic_avg_width, pos_.load()));
      } else {
        extra_buffer_.resize(extra_size_,
                             std::max(extra_size_ * width_, pos_.load()));
      }
    }
    // resize `data` of basic_buffer
    {
      size_t pos = basic_pos_.load();
      pos = pos + (pos + 4) / 5;
      basic_buffer_.resize(basic_size_, pos);
    }
  }

  PropertyType type() const override { return PropertyType::Varchar(width_); }

  void set_value(size_t idx, const std::string_view& val) {
    if (idx >= basic_size_ && idx < basic_size_ + extra_size_) {
      size_t offset = pos_.fetch_add(val.size());
      extra_buffer_.set(idx - basic_size_, offset, val);
    } else if (idx < basic_size_) {
      size_t offset = basic_pos_.fetch_add(val.size());
      basic_buffer_.set(idx, offset, val);
    } else {
      LOG(FATAL) << "Index out of range";
    }
  }

  void set_any(size_t idx, const Any& value) override {
    set_value(idx, value.AsStringView());
  }

  // make sure there is enough space for the value
  void set_value_with_check(size_t idx, const std::string_view& value) {
    if (idx >= basic_size_ && idx < basic_size_ + extra_size_) {
      size_t offset = pos_.fetch_add(value.size());
      if (pos_.load() > extra_buffer_.data_size()) {
        extra_buffer_.resize(extra_buffer_.size(), pos_.load());
      }
      extra_buffer_.set(idx - basic_size_, offset, value);
    } else if (idx < basic_size_) {
      size_t offset = basic_pos_.fetch_add(value.size());
      if (basic_pos_.load() > basic_buffer_.data_size()) {
        basic_buffer_.resize(basic_buffer_.size(), basic_pos_.load());
      }
      basic_buffer_.set(idx, offset, value);
    } else {
      LOG(FATAL) << "Index out of range";
    }
  }

  std::string_view get_view(size_t idx) const {
    return idx < basic_size_ ? basic_buffer_.get(idx)
                             : extra_buffer_.get(idx - basic_size_);
  }

  Any get(size_t idx) const override {
    return AnyConverter<std::string_view>::to_any(get_view(idx));
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    std::string_view val;
    arc >> val;
    set_value(index, val);
  }

  const mmap_array<std::string_view>& basic_buffer() const {
    return basic_buffer_;
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  size_t basic_buffer_size() const { return basic_size_; }

  const mmap_array<std::string_view>& extra_buffer() const {
    return extra_buffer_;
  }

  size_t extra_buffer_size() const { return extra_size_; }

 private:
  mmap_array<std::string_view> basic_buffer_;
  size_t basic_size_;
  mmap_array<std::string_view> extra_buffer_;
  size_t extra_size_;
  std::atomic<size_t> pos_;
  std::atomic<size_t> basic_pos_;
  StorageStrategy strategy_;
  uint16_t width_;
};

using StringColumn = TypedColumn<std::string_view>;
template <typename INDEX_T>
class LFIndexer;

template <typename INDEX_T>
class StringMapColumn : public ColumnBase {
 public:
  StringMapColumn(StorageStrategy strategy)
      : index_col_(strategy), meta_map_(nullptr) {
    meta_map_ = new LFIndexer<INDEX_T>();
    meta_map_->init(
        PropertyType::Varchar(PropertyType::STRING_DEFAULT_MAX_LENGTH));
  }

  ~StringMapColumn() {
    if (meta_map_) {
      meta_map_->close();
      delete meta_map_;
    }
    index_col_.close();
  }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    meta_map_->copy_to_tmp(cur_path + ".map_meta", tmp_path + ".map_meta");
    index_col_.copy_to_tmp(cur_path, tmp_path);
  }
  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override;
  void open_in_memory(const std::string& name) override;
  void open_with_hugepages(const std::string& name, bool force) override;
  void dump(const std::string& filename) override;

  void touch(const std::string& filename) override {
    index_col_.touch(filename);
  }

  void close() override {
    if (meta_map_ != nullptr) {
      meta_map_->close();
    }
    index_col_.close();
  }

  size_t size() const override { return index_col_.size(); }
  void resize(size_t size) override { index_col_.resize(size); }

  PropertyType type() const override { return PropertyType::kStringMap; }

  void set_value(size_t idx, const std::string_view& val);

  void set_any(size_t idx, const Any& value) override {
    set_value(idx, value.AsStringView());
  }

  std::string_view get_view(size_t idx) const;

  Any get(size_t idx) const override {
    return AnyConverter<std::string_view>::to_any(get_view(idx));
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    std::string_view val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override {
    return index_col_.storage_strategy();
  }

  const TypedColumn<INDEX_T>& get_index_col() const { return index_col_; }
  const LFIndexer<INDEX_T>& get_meta_map() const { return *meta_map_; }

 private:
  TypedColumn<INDEX_T> index_col_;
  LFIndexer<INDEX_T>* meta_map_;
  grape::SpinLock lock_;
};

template <typename INDEX_T>
void StringMapColumn<INDEX_T>::open(const std::string& name,
                                    const std::string& snapshot_dir,
                                    const std::string& work_dir) {
  index_col_.open(name, snapshot_dir, work_dir);
  meta_map_->open(name + ".map_meta", snapshot_dir, work_dir);
  meta_map_->reserve(std::numeric_limits<INDEX_T>::max());
}

template <typename INDEX_T>
void StringMapColumn<INDEX_T>::open_in_memory(const std::string& name) {
  index_col_.open_in_memory(name);
  meta_map_->open_in_memory(name + ".map_meta");
  meta_map_->reserve(std::numeric_limits<INDEX_T>::max());
}

template <typename INDEX_T>
void StringMapColumn<INDEX_T>::open_with_hugepages(const std::string& name,
                                                   bool force) {
  index_col_.open_with_hugepages(name, force);
  meta_map_->open_with_hugepages(name + ".map_meta", true);
  meta_map_->reserve(std::numeric_limits<INDEX_T>::max());
}

template <typename INDEX_T>
void StringMapColumn<INDEX_T>::dump(const std::string& filename) {
  index_col_.dump(filename);
  meta_map_->dump(filename + ".map_meta", "");
}

template <typename INDEX_T>
std::string_view StringMapColumn<INDEX_T>::get_view(size_t idx) const {
  INDEX_T ind = index_col_.get_view(idx);
  return meta_map_->get_key(ind).AsStringView();
}

template <typename INDEX_T>
void StringMapColumn<INDEX_T>::set_value(size_t idx,
                                         const std::string_view& val) {
  INDEX_T lid;
  if (!meta_map_->get_index(val, lid)) {
    lock_.lock();
    if (!meta_map_->get_index(val, lid)) {
      lid = meta_map_->insert(val);
    }
    lock_.unlock();
  }
  index_col_.set_value(idx, lid);
}

using DefaultStringMapColumn = StringMapColumn<uint8_t>;

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem,
    const std::vector<PropertyType>& sub_types = {});

#ifdef USE_PTHASH
template <typename EDATA_T>
class ConcatColumn : public ColumnBase {
 public:
  ~ConcatColumn() {}

  ConcatColumn(const TypedColumn<EDATA_T>& basic_column,
               const TypedColumn<EDATA_T>& extra_column)
      : basic_column_(basic_column),
        extra_column_(extra_column),
        basic_size_(basic_column.size()) {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    LOG(FATAL) << "not implemented";
  }

  void open_in_memory(const std::string& name) {
    LOG(FATAL) << "not implemented";
  }

  void open_with_hugepages(const std::string& name, bool force) {
    LOG(FATAL) << "not implemented";
  }

  void close() { LOG(FATAL) << "not implemented"; }

  EDATA_T get_view(size_t index) const {
    return index < basic_size_ ? basic_column_.get(index)
                               : extra_column_.get(index - basic_size_);
  }

  void touch(const std::string& filename) { LOG(FATAL) << "not implemented"; }

  virtual void dump(const std::string& filename) {
    LOG(FATAL) << "not implemented";
  }

  size_t size() const { return basic_size_ + extra_column_.size(); }

  void copy_to_tmp(const std::string& cur_path, const std::string& tmp_path) {
    LOG(FATAL) << "not implemented";
  }
  void resize(size_t size) { LOG(FATAL) << "not implemented"; }

  PropertyType type() const { return AnyConverter<EDATA_T>::type(); }

  void set_any(size_t index, const Any& value) {
    LOG(FATAL) << "not implemented";
  }

  Any get(size_t index) const {
    if (index < basic_size_) {
      return basic_column_.get(index);
    } else {
      return extra_column_.get(index - basic_size_);
    }
  }

  void ingest(uint32_t index, grape::OutArchive& arc) {
    LOG(FATAL) << "not implemented";
  }

  StorageStrategy storage_strategy() const {
    return basic_column_.storage_strategy();
  }

 private:
  const TypedColumn<EDATA_T>& basic_column_;
  const TypedColumn<EDATA_T>& extra_column_;
  size_t basic_size_;
};
#endif

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  virtual ~RefColumnBase() {}
  virtual Any get(size_t index) const = 0;
};

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  TypedRefColumn(const mmap_array<T>& buffer, StorageStrategy strategy)
      : basic_buffer(buffer),
        basic_size(0),
        extra_buffer(buffer),
        extra_size(buffer.size()),
        strategy_(strategy) {}
  TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(column.basic_buffer()),
        basic_size(column.basic_buffer_size()),
        extra_buffer(column.extra_buffer()),
        extra_size(column.extra_buffer_size()),
        strategy_(column.storage_strategy()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    return index < basic_size ? basic_buffer.get(index)
                              : extra_buffer.get(index - basic_size);
  }

  size_t size() const { return basic_size + extra_size; }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(get_view(index));
  }

 private:
  const mmap_array<T>& basic_buffer;
  size_t basic_size;
  const mmap_array<T>& extra_buffer;
  size_t extra_size;

  StorageStrategy strategy_;
};

template <>
class TypedRefColumn<LabelKey> : public RefColumnBase {
 public:
  TypedRefColumn(LabelKey label_key) : label_key_(label_key) {}

  ~TypedRefColumn() {}

  inline LabelKey get_view(size_t index) const { return label_key_; }

  Any get(size_t index) const override {
    LOG(ERROR) << "LabelKeyColumn does not support get() to Any";
    return Any();
  }

 private:
  LabelKey label_key_;
};

template <>
class TypedRefColumn<GlobalId> : public RefColumnBase {
 public:
  using label_t = typename LabelKey::label_data_type;
  TypedRefColumn(label_t label_key) : label_key_(label_key) {}

  ~TypedRefColumn() {}

  inline GlobalId get_view(size_t index) const {
    return GlobalId(label_key_, index);
  }

  Any get(size_t index) const override {
    LOG(ERROR) << "GlobalId Column does not support get() to Any";
    return Any();
  }

 private:
  label_t label_key_;
};

// Create a reference column from a ColumnBase that contains a const reference
// to the actual column storage, offering a column-based store interface for
// vertex properties.
std::shared_ptr<RefColumnBase> CreateRefColumn(
    std::shared_ptr<ColumnBase> column);

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
