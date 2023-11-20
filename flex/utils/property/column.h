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

#include "flex/utils/mmap_array.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void close() = 0;

  virtual void touch(const std::string& filename) = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void copy_to_tmp(const std::string& cur_path,
                           const std::string& tmp_path) = 0;
  // virtual void clear() = 0;

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
      basic_buffer_.open(basic_path, true);
      basic_size_ = basic_buffer_.size();
    } else {
      basic_size_ = 0;
    }
    if (work_dir == "") {
      extra_size_ = 0;
    } else {
      extra_buffer_.open(work_dir + "/" + name, false);
      extra_size_ = extra_buffer_.size();
    }
  }
  void touch(const std::string& filename) override {
    mmap_array<T> tmp;
    tmp.open(filename, false);
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
    tmp.open(tmp_path, false);
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
      tmp.open(filename, false);
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

  PropertyType type() const override { return AnyConverter<T>::type; }

  void set_value(size_t index, const T& val) {
    assert(index >= basic_size_ && index < basic_size_ + extra_size_);
    extra_buffer_.set(index - basic_size_, val);
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

using BoolColumn = TypedColumn<bool>;
using IntColumn = TypedColumn<int32_t>;
using UIntColumn = TypedColumn<uint32_t>;
using LongColumn = TypedColumn<int64_t>;
using ULongColumn = TypedColumn<uint64_t>;
using DateColumn = TypedColumn<Date>;
using DoubleColumn = TypedColumn<double>;
using FloatColumn = TypedColumn<float>;

template <>
class TypedColumn<std::string_view> : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy, size_t width = 64) : width_(width) {}
  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path + ".items")) {
      basic_buffer_.open(basic_path, true);
      basic_size_ = basic_buffer_.size();
    } else {
      basic_size_ = 0;
    }
    if (work_dir == "") {
      extra_size_ = 0;
      pos_.store(0);
    } else {
      extra_buffer_.open(work_dir + "/" + name, false);
      extra_size_ = extra_buffer_.size();

      pos_.store(extra_buffer_.data_size());
    }
  }

  void touch(const std::string& filename) override {
    mmap_array<std::string_view> tmp;
    tmp.open(filename, false);
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
    basic_buffer_.reset();
    tmp.open(tmp_path, false);
    extra_buffer_.swap(tmp);
    tmp.reset();
    pos_.store(extra_buffer_.data_size());
  }

  void dump(const std::string& filename) override {
    if (basic_size_ != 0 && extra_size_ == 0) {
      basic_buffer_.dump(filename);
    } else if (basic_size_ == 0 && extra_size_ != 0) {
      extra_buffer_.resize(extra_size_, pos_.load());
      extra_buffer_.dump(filename);
    } else {
      mmap_array<std::string_view> tmp;
      tmp.open(filename, false);
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
      extra_buffer_.resize(extra_size_, extra_size_ * width_);
    }
  }

  PropertyType type() const override {
    return AnyConverter<std::string_view>::type;
  }

  void set_value(size_t idx, const std::string_view& val) {
    assert(idx >= basic_size_ && idx < basic_size_ + extra_size_);
    size_t offset = pos_.fetch_add(val.size());
    extra_buffer_.set(idx - basic_size_, offset, val);
  }

  void set_any(size_t idx, const Any& value) override {
    set_value(idx, AnyConverter<std::string_view>::from_any(value));
  }

  std::string_view get_view(size_t idx) const {
    // LOG(INFO) << basic_size_ << " basic size " << idx << "\n";
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
  StorageStrategy strategy_;
  size_t width_;
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
    meta_map_->init(PropertyType::kString);
  }

  ~StringMapColumn() {
    if (meta_map_) {
      meta_map_->close();
      delete meta_map_;
    }
    index_col_.close();
  }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override;
  void dump(const std::string& filename) override;

  void touch(const std::string& filename) override {
    // LOG(INFO) << "filename: " << filename << "\n";
    index_col_.touch(filename);
    // meta_map_->touch(filename);
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
    set_value(idx, AnyConverter<std::string_view>::from_any(value));
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
  LFIndexer<INDEX_T>* meta_map_;
  TypedColumn<INDEX_T> index_col_;
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
    lid = meta_map_->insert(val);
  }
  index_col_.set_value(idx, lid);
}

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem);

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

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
