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

  virtual void init(size_t max_vnum) = 0;

  virtual PropertyType type() const = 0;

  virtual void set_any(size_t index, const Any& value) = 0;

  virtual Any get(size_t index) const = 0;

  virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  virtual void Serialize(const std::string& filename, size_t size) = 0;

  virtual void Deserialize(const std::string& filename) = 0;

  virtual StorageStrategy storage_strategy() const = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() {}

  void init(size_t max_size) override { buffer_.resize(max_size); }

  void set_value(size_t index, const T& val) { buffer_.insert(index, val); }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  T get_view(size_t index) const { return buffer_[index]; }

  PropertyType type() const override { return AnyConverter<T>::type; }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(buffer_[index]);
  }

  void Serialize(const std::string& path, size_t size) override {
    buffer_.dump_to_file(path, size);
  }

  void Deserialize(const std::string& path) override {
    buffer_.open_for_read(path);
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<T>& buffer() const { return buffer_; }
  mmap_array<T>& buffer() { return buffer_; }

 private:
  mmap_array<T> buffer_;
  StorageStrategy strategy_;
};

using IntColumn = TypedColumn<int>;
using LongColumn = TypedColumn<int64_t>;
using DateColumn = TypedColumn<Date>;
using StringColumn = TypedColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem);

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
