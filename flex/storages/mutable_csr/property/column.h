#ifndef GRAPHSCOPE_PROPERTY_COLUMN_H_
#define GRAPHSCOPE_PROPERTY_COLUMN_H_

#include <string>
#include <string_view>

#include "grape/serialization/out_archive.h"
#include "flex/storages/mutable_csr/property/types.h"
#include "flex/utils/mmap_array.h"
#include "flex/storages/mutable_csr/types.h"

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

  virtual size_t size() const = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
public:
  using value_type = T;

  TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() {}

  void init(size_t max_size) override { buffer_.resize(max_size); }

  void set_value(size_t index, const T& val) { buffer_.insert(index, val); }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  inline T get_view(size_t index) const { return buffer_[index]; }

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

  size_t size() const override { return buffer_.size(); }

 private:
  mmap_array<T> buffer_;
  StorageStrategy strategy_;
};

using IntColumn = TypedColumn<int>;
using LongColumn = TypedColumn<int64_t>;
using DateColumn = TypedColumn<Date>;
using BrowserColumn = TypedColumn<Browser>;
using IpAddrColumn = TypedColumn<IpAddr>;
using GenderColumn = TypedColumn<Gender>;
using StringColumn = TypedColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem);

class RefColumnBase {
 public:
  virtual ~RefColumnBase() {}
};

class LabelRefColumn : public RefColumnBase {
 public:
  LabelRefColumn(uint8_t label_id) : label_id_(label_id) {}
  ~LabelRefColumn() {}

  inline uint8_t get_view(size_t index) const { return label_id_; }

 private:
  uint8_t label_id_;
};

template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  TypedRefColumn(const mmap_array<T>& buffer, StorageStrategy strategy) : buffer_(buffer), strategy_(strategy) {}
  TypedRefColumn(const TypedColumn<T>& column) : buffer_(column.buffer()), strategy_(column.storage_strategy()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const { return buffer_[index]; }

 private:
  const mmap_array<T>& buffer_;
  StorageStrategy strategy_;
};

std::shared_ptr<RefColumnBase> CreateRefColumn(std::shared_ptr<ColumnBase> column);

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
