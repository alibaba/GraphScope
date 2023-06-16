#include "flex/storages/mutable_csr/property/column.h"

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

  void Serialize(const std::string& path, size_t size) override {}

  void Deserialize(const std::string& path) override {}

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
  }

  StorageStrategy storage_strategy() const override {
    return StorageStrategy::kNone;
  }

  size_t size() const override { return 0; }
};

using IntEmptyColumn = TypedEmptyColumn<int>;
using LongEmptyColumn = TypedEmptyColumn<int64_t>;
using DateEmptyColumn = TypedEmptyColumn<Date>;
using BrowserEmptyColumn = TypedEmptyColumn<Browser>;
using IpAddrEmptyColumn = TypedEmptyColumn<IpAddr>;
using GenderEmptyColumn = TypedEmptyColumn<Gender>;

std::shared_ptr<ColumnBase> CreateColumn(PropertyType type,
                                         StorageStrategy strategy) {
  if (strategy == StorageStrategy::kNone) {
    if (type == PropertyType::kInt32) {
      return std::make_shared<IntEmptyColumn>();
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<LongEmptyColumn>();
    } else if (type == PropertyType::kDate) {
      return std::make_shared<DateEmptyColumn>();
    } else if (type == PropertyType::kString) {
      return std::make_shared<TypedEmptyColumn<std::string_view>>();
    } else if (type == PropertyType::kBrowser) {
      return std::make_shared<BrowserEmptyColumn>();
    } else if (type == PropertyType::kIpAddr) {
      return std::make_shared<IpAddrEmptyColumn>();
    } else if (type == PropertyType::kGender) {
      return std::make_shared<GenderEmptyColumn>();
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type);
      return nullptr;
    }
  } else {
    if (type == PropertyType::kInt32) {
      return std::make_shared<IntColumn>(strategy);
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<LongColumn>(strategy);
    } else if (type == PropertyType::kDate) {
      return std::make_shared<DateColumn>(strategy);
    } else if (type == PropertyType::kString) {
      return std::make_shared<StringColumn>(strategy);
    } else if (type == PropertyType::kBrowser) {
      return std::make_shared<BrowserColumn>(strategy);
    } else if (type == PropertyType::kIpAddr) {
      return std::make_shared<IpAddrColumn>(strategy);
    } else if (type == PropertyType::kGender) {
      return std::make_shared<GenderColumn>(strategy);
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type);
      return nullptr;
    }
  }
}

std::shared_ptr<RefColumnBase> CreateRefColumn(std::shared_ptr<ColumnBase> column) {
  auto type = column->type();
  if (type == PropertyType::kInt32) {
    return std::make_shared<TypedRefColumn<int>>(*std::dynamic_pointer_cast<TypedColumn<int>>(column));
  } else if (type == PropertyType::kInt64) {
    return std::make_shared<TypedRefColumn<int64_t>>(*std::dynamic_pointer_cast<TypedColumn<int64_t>>(column));
  } else if (type == PropertyType::kDate) {
    return std::make_shared<TypedRefColumn<Date>>(*std::dynamic_pointer_cast<TypedColumn<Date>>(column));
  } else if (type == PropertyType::kString) {
    return std::make_shared<TypedRefColumn<std::string_view>>(*std::dynamic_pointer_cast<TypedColumn<std::string_view>>(column));
  } else if (type == PropertyType::kBrowser) {
    return std::make_shared<TypedRefColumn<Browser>>(*std::dynamic_pointer_cast<TypedColumn<Browser>>(column));
  } else if (type == PropertyType::kIpAddr) {
    return std::make_shared<TypedRefColumn<IpAddr>>(*std::dynamic_pointer_cast<TypedColumn<IpAddr>>(column));
  } else if (type == PropertyType::kGender) {
    return std::make_shared<TypedRefColumn<Gender>>(*std::dynamic_pointer_cast<TypedColumn<Gender>>(column));
  } else {
    LOG(FATAL) << "unexpected type to create column, "
               << static_cast<int>(type);
    return nullptr;
  }
}

}  // namespace gs
