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
#include "flex/utils/property/types.h"

#include "grape/serialization/out_archive.h"

namespace gs {

template <typename T>
class TypedEmptyColumn : public ColumnBase {
 public:
  TypedEmptyColumn() {}
  ~TypedEmptyColumn() {}

  void init(size_t max_size) override {}

  void set_value(size_t index, const T& val) {}

  void set_any(size_t index, const Any& value) override {}

  T get_view(size_t index) const { T{}; }

  PropertyType type() const override { return AnyConverter<T>::type; }

  Any get(size_t index) const override { return Any(); }

  size_t size() const override { return 0; }

  void clear() override {}

  void resize(size_t) override {}

  void Serialize(const std::string& path, size_t size) override {}

  void Deserialize(const std::string& path) override {}

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
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
using BoolEmptyColumn = TypedEmptyColumn<bool>;
using FloatEmptyColumn = TypedEmptyColumn<float>;
using DoubleEmptyColumn = TypedEmptyColumn<double>;
using StringEmptyColumn = TypedEmptyColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(PropertyType type,
                                         StorageStrategy strategy) {
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
    } else if (type == PropertyType::kString) {
      return std::make_shared<StringEmptyColumn>();
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type);
      return nullptr;
    }
  } else {
    if (type == PropertyType::kBool) {
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
    } else if (type == PropertyType::kString) {
      return std::make_shared<StringColumn>(strategy);
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type);
      return nullptr;
    }
  }
}

}  // namespace gs
