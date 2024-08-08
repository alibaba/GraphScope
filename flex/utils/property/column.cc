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

#include "flex/utils/property/column.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"

#include "grape/serialization/out_archive.h"

namespace gs {

template <typename T>
class TypedEmptyColumn : public ColumnBase {
 public:
  TypedEmptyColumn() {}
  ~TypedEmptyColumn() {}

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

  PropertyType type() const override { return AnyConverter<T>::type(); }

  void set_value(size_t index, const T& val) {}

  void set_any(size_t index, const Any& value) override {}

  T get_view(size_t index) const { T{}; }

  Any get(size_t index) const override { return Any(); }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
  }

  StorageStrategy storage_strategy() const override {
    return StorageStrategy::kNone;
  }
};

template <>
class TypedEmptyColumn<std::string_view> : public ColumnBase {
 public:
  TypedEmptyColumn(
      int32_t max_length = PropertyType::STRING_DEFAULT_MAX_LENGTH) {}
  ~TypedEmptyColumn() {}

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

  PropertyType type() const override { return PropertyType::kStringView; }

  void set_value(size_t index, const std::string_view& val) {}

  void set_any(size_t index, const Any& value) override {}

  std::string_view get_view(size_t index) const { return std::string_view{}; }

  Any get(size_t index) const override { return Any(); }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    std::string_view val;
    arc >> val;
  }

  StorageStrategy storage_strategy() const override {
    return StorageStrategy::kNone;
  }
};

using IntEmptyColumn = TypedEmptyColumn<int32_t>;
using UIntEmptyColumn = TypedEmptyColumn<uint32_t>;
using LongEmptyColumn = TypedEmptyColumn<int64_t>;
using ULongEmptyColumn = TypedEmptyColumn<uint64_t>;
using DateEmptyColumn = TypedEmptyColumn<Date>;
using DayEmptyColumn = TypedEmptyColumn<Day>;
using BoolEmptyColumn = TypedEmptyColumn<bool>;
using FloatEmptyColumn = TypedEmptyColumn<float>;
using DoubleEmptyColumn = TypedEmptyColumn<double>;
using StringEmptyColumn = TypedEmptyColumn<std::string_view>;
using RecordViewEmptyColumn = TypedEmptyColumn<RecordView>;

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy,
    const std::vector<PropertyType>& sub_types) {
  if (strategy == StorageStrategy::kNone) {
    if (type == PropertyType::kBool) {
      return std::make_shared<BoolEmptyColumn>();
    } else if (type == PropertyType::kInt32) {
      return std::make_shared<IntEmptyColumn>();
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<LongEmptyColumn>();
    } else if (type == PropertyType::kUInt32) {
      return std::make_shared<UIntEmptyColumn>();
    } else if (type == PropertyType::kUInt64) {
      return std::make_shared<ULongEmptyColumn>();
    } else if (type == PropertyType::kDouble) {
      return std::make_shared<DoubleEmptyColumn>();
    } else if (type == PropertyType::kFloat) {
      return std::make_shared<FloatEmptyColumn>();
    } else if (type == PropertyType::kDate) {
      return std::make_shared<DateEmptyColumn>();
    } else if (type == PropertyType::kDay) {
      return std::make_shared<DayEmptyColumn>();
    } else if (type == PropertyType::kStringMap) {
      return std::make_shared<StringEmptyColumn>();
    } else if (type == PropertyType::kStringView) {
      return std::make_shared<StringEmptyColumn>(
          gs::PropertyType::STRING_DEFAULT_MAX_LENGTH);
    } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
      return std::make_shared<StringEmptyColumn>(
          type.additional_type_info.max_length);
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type.type_enum);
      return nullptr;
    }
  } else {
    if (type == PropertyType::kEmpty) {
      return std::make_shared<TypedColumn<grape::EmptyType>>(strategy);
    } else if (type == PropertyType::kBool) {
      return std::make_shared<BoolColumn>(strategy);
    } else if (type == PropertyType::kInt32) {
      return std::make_shared<IntColumn>(strategy);
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<LongColumn>(strategy);
    } else if (type == PropertyType::kUInt32) {
      return std::make_shared<UIntColumn>(strategy);
    } else if (type == PropertyType::kUInt64) {
      return std::make_shared<ULongColumn>(strategy);
    } else if (type == PropertyType::kDouble) {
      return std::make_shared<DoubleColumn>(strategy);
    } else if (type == PropertyType::kFloat) {
      return std::make_shared<FloatColumn>(strategy);
    } else if (type == PropertyType::kDate) {
      return std::make_shared<DateColumn>(strategy);
    } else if (type == PropertyType::kDay) {
      return std::make_shared<DayColumn>(strategy);
    } else if (type == PropertyType::kStringMap) {
      return std::make_shared<DefaultStringMapColumn>(strategy);
    } else if (type == PropertyType::kStringView) {
      return std::make_shared<StringColumn>(strategy);
    } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
      return std::make_shared<StringColumn>(
          strategy, type.additional_type_info.max_length);
    } else if (type.type_enum == impl::PropertyTypeImpl::kRecordView) {
      return std::make_shared<RecordViewColumn>(sub_types);
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type.type_enum);
      return nullptr;
    }
  }
}

std::shared_ptr<RefColumnBase> CreateRefColumn(
    std::shared_ptr<ColumnBase> column) {
  auto type = column->type();
  if (type == PropertyType::kBool) {
    return std::make_shared<TypedRefColumn<bool>>(
        *std::dynamic_pointer_cast<TypedColumn<bool>>(column));
  } else if (type == PropertyType::kDay) {
    return std::make_shared<TypedRefColumn<Day>>(
        *std::dynamic_pointer_cast<TypedColumn<Day>>(column));
  } else if (type == PropertyType::kDate) {
    return std::make_shared<TypedRefColumn<Date>>(
        *std::dynamic_pointer_cast<TypedColumn<Date>>(column));
  } else if (type == PropertyType::kUInt8) {
    return std::make_shared<TypedRefColumn<uint8_t>>(
        *std::dynamic_pointer_cast<TypedColumn<uint8_t>>(column));
  } else if (type == PropertyType::kUInt16) {
    return std::make_shared<TypedRefColumn<uint16_t>>(
        *std::dynamic_pointer_cast<TypedColumn<uint16_t>>(column));
  } else if (type == PropertyType::kInt32) {
    return std::make_shared<TypedRefColumn<int32_t>>(
        *std::dynamic_pointer_cast<TypedColumn<int32_t>>(column));
  } else if (type == PropertyType::kInt64) {
    return std::make_shared<TypedRefColumn<int64_t>>(
        *std::dynamic_pointer_cast<TypedColumn<int64_t>>(column));
  } else if (type == PropertyType::kUInt32) {
    return std::make_shared<TypedRefColumn<uint32_t>>(
        *std::dynamic_pointer_cast<TypedColumn<uint32_t>>(column));
  } else if (type == PropertyType::kUInt64) {
    return std::make_shared<TypedRefColumn<uint64_t>>(
        *std::dynamic_pointer_cast<TypedColumn<uint64_t>>(column));
  } else if (type == PropertyType::kStringView || type.IsVarchar()) {
    return std::make_shared<TypedRefColumn<std::string_view>>(
        *std::dynamic_pointer_cast<TypedColumn<std::string_view>>(column));
  } else if (type == PropertyType::kFloat) {
    return std::make_shared<TypedRefColumn<float>>(
        *std::dynamic_pointer_cast<TypedColumn<float>>(column));
  } else if (type == PropertyType::kDouble) {
    return std::make_shared<TypedRefColumn<double>>(
        *std::dynamic_pointer_cast<TypedColumn<double>>(column));
  } else {
    LOG(FATAL) << "unexpected type to create column, "
               << static_cast<int>(type.type_enum);
    return nullptr;
  }
}

void TypedColumn<RecordView>::open_in_memory(const std::string& name) {
  table_ = std::make_shared<Table>();
  std::vector<std::string> col_names;
  for (size_t i = 0; i < types_.size(); ++i) {
    col_names.emplace_back("col_" + std::to_string(i));
  }
  table_->open_in_memory(name, "", col_names, types_, {});
}

size_t TypedColumn<RecordView>::size() const { return table_->row_num(); }

void TypedColumn<RecordView>::resize(size_t size) { table_->resize(size); }

void TypedColumn<RecordView>::close() { table_->close(); }

void TypedColumn<RecordView>::set_any(size_t index, const Any& value) {
  auto rv = value.AsRecordView();
  set_value(index, rv);
}

void TypedColumn<RecordView>::set_value(size_t index, const RecordView& val) {
  std::vector<Any> vec;
  auto& cols = table_->columns();
  for (size_t i = 0; i < val.size(); ++i) {
    if (cols[i]->type() == PropertyType::kStringView) {
      (dynamic_cast<TypedColumn<std::string_view>*>(cols[i].get()))
          ->set_value_with_check(index, val[i].AsStringView());
    } else {
      cols[i]->set_any(index, val[i]);
    }
  }
}

Any TypedColumn<RecordView>::get(size_t index) const {
  return Any(RecordView(index, table_.get()));
}

RecordView TypedColumn<RecordView>::get_view(size_t index) const {
  return RecordView(index, table_.get());
}

}  // namespace gs
