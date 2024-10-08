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

#ifndef RUNTIME_COMMON_COLUMNS_VALUE_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_VALUE_COLUMNS_H_

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

#include <vector>

namespace gs {

namespace runtime {

template <typename T>
class ValueColumnBuilder;

template <typename T>
class OptionalValueColumnBuilder;

template <typename T>
class IValueColumn : public IContextColumn {
 public:
  IValueColumn() = default;
  virtual ~IValueColumn() = default;
  virtual T get_value(size_t idx) const = 0;
};
template <typename T>
class ValueColumn : public IValueColumn<T> {
 public:
  ValueColumn() = default;
  ~ValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ValueColumn<" + TypedConverter<T>::name() + ">[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override { return ContextColumnType::kValue; }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::make_shared<ValueColumnBuilder<T>>();
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder() const override {
    return std::dynamic_pointer_cast<IOptionalContextColumnBuilder>(
        std::make_shared<OptionalValueColumnBuilder<T>>());
  }
  std::shared_ptr<IContextColumn> dup() const override;

  std::shared_ptr<IContextColumn> shuffle(const std::vector<size_t>& offsets) const override;

  RTAnyType elem_type() const override { return TypedConverter<T>::type(); }
  RTAny get_elem(size_t idx) const override { return TypedConverter<T>::from_typed(data_[idx]); }

  T get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, bool> or std::is_same_v<T, Tuple>) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else {
      return new SigColumn<T>(data_);
    }
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(), [this](size_t a, size_t b) {
      // data_[a] == data_[b]
      if (!(data_[a] < data_[b]) && !(data_[b] < data_[a])) {
        return a < b;
      }
      return data_[a] < data_[b];
    });
    for (size_t i = 0; i < data_.size(); ++i) {
      if (i == 0 || ((data_[origin_offsets[i]] < data_[origin_offsets[i - 1]]) ||
                     (data_[origin_offsets[i - 1]] < data_[origin_offsets[i]]))) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

  std::shared_ptr<IContextColumn> union_col(std::shared_ptr<IContextColumn> other) const override;

 private:
  template <typename _T>
  friend class ValueColumnBuilder;
  std::vector<T> data_;
};

template <>
class ValueColumn<std::string_view> : public IValueColumn<std::string_view> {
 public:
  ValueColumn() = default;
  ~ValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ValueColumn<" + TypedConverter<std::string_view>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override { return ContextColumnType::kValue; }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<ValueColumnBuilder<std::string_view>>());
  }

  RTAnyType elem_type() const override { return RTAnyType::kStringValue; }
  std::shared_ptr<IContextColumn> shuffle(const std::vector<size_t>& offsets) const override;
  std::shared_ptr<IContextColumn> dup() const override;

  RTAny get_elem(size_t idx) const override { return RTAny::from_string(data_[idx]); }

  std::string_view get_value(size_t idx) const override { return std::string_view(data_[idx]); }

  ISigColumn* generate_signature() const override { return new SigColumn<std::string_view>(data_); }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(), [this](size_t a, size_t b) {
      if (data_[a] == data_[b]) {
        return a < b;
      }
      return data_[a] < data_[b];
    });
    for (size_t i = 0; i < data_.size(); ++i) {
      if (i == 0 || data_[origin_offsets[i]] != data_[origin_offsets[i - 1]]) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

 private:
  friend class ValueColumnBuilder<std::string_view>;
  std::vector<std::string> data_;
};

template <typename T>
class ValueColumnBuilder : public IContextColumnBuilder {
 public:
  ValueColumnBuilder() = default;
  ~ValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    data_.push_back(TypedConverter<T>::to_typed(val));
  }

  void push_back_opt(const T& val) { data_.push_back(val); }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ValueColumn<T>>();
    ret->data_.swap(data_);
    return ret;
  }

 private:
  std::vector<T> data_;
};

template <typename T>
class ListValueColumnBuilder;

template <typename T>
class ListValueColumn : public IValueColumn<List> {
 public:
  ListValueColumn() = default;
  ~ListValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ListValueColumn[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override { return ContextColumnType::kValue; }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    auto builder = std::make_shared<ListValueColumnBuilder<T>>();
    builder->set_list_impls(list_impls_);
    builder->set_list_data(list_data_);
    return builder;
  }

  std::shared_ptr<IContextColumn> dup() const override;
  std::shared_ptr<IContextColumn> shuffle(const std::vector<size_t>& offsets) const override;
  RTAnyType elem_type() const override {
    auto type = RTAnyType::kList;
    return type;
  }
  RTAny get_elem(size_t idx) const override { return RTAny::from_list(data_[idx]); }

  List get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(), [this](size_t a, size_t b) {
      // data_[a] == data_[b]
      if (!(data_[a] < data_[b]) && !(data_[b] < data_[a])) {
        return a < b;
      }
      return data_[a] < data_[b];
    });
    for (size_t i = 0; i < data_.size(); ++i) {
      if (i == 0 || ((data_[origin_offsets[i]] < data_[origin_offsets[i - 1]]) ||
                     (data_[origin_offsets[i - 1]] < data_[origin_offsets[i]]))) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

 private:
  template <typename _T>
  friend class ListValueColumnBuilder;
  std::vector<List> data_;
  std::vector<std::shared_ptr<ListImplBase>> list_impls_;
  std::vector<std::shared_ptr<T>> list_data_;
};

template <typename T>
class ListValueColumnBuilder : public IContextColumnBuilder {
 public:
  ListValueColumnBuilder() = default;
  ~ListValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    CHECK(val.type() == RTAnyType::kList);
    data_.emplace_back(val.as_list());
  }

  void push_back_opt(const List& val) { data_.emplace_back(val); }

  void set_list_impls(const std::vector<std::shared_ptr<ListImplBase>>& list_impls) {
    list_impls_ = list_impls;
  }

  void set_list_data(const std::vector<std::shared_ptr<T>>& list_data) { list_data_ = list_data; }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ListValueColumn<T>>();
    ret->data_.swap(data_);
    ret->list_impls_.swap(list_impls_);
    ret->list_data_.swap(list_data_);

    return ret;
  }

 private:
  std::vector<List> data_;
  std::vector<std::shared_ptr<ListImplBase>> list_impls_;
  std::vector<std::shared_ptr<T>> list_data_;
};
template <typename T>
std::shared_ptr<IContextColumn> ListValueColumn<T>::dup() const {
  ListValueColumnBuilder<T> builder;
  for (const auto& list : data_) {
    builder.push_back_opt(list);
  }
  builder.set_list_data(list_data_);
  builder.set_list_impls(list_impls_);
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ListValueColumn<T>::shuffle(
    const std::vector<size_t>& offsets) const {
  ListValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  builder.set_list_data(list_data_);
  builder.set_list_impls(list_impls_);
  return builder.finish();
}

class MapValueColumnBuilder;

class MapValueColumn : public IValueColumn<Map> {
 public:
  MapValueColumn() = default;
  ~MapValueColumn() = default;

  size_t size() const override { return values_.size(); }

  std::string column_info() const override {
    return "MapValueColumn[" + std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override { return ContextColumnType::kValue; }

  std::shared_ptr<IContextColumnBuilder> builder() const override;

  std::shared_ptr<IContextColumn> dup() const override;

  std::shared_ptr<IContextColumn> shuffle(const std::vector<size_t>& offsets) const override;

  RTAnyType elem_type() const override {
    auto type = RTAnyType::kMap;
    return type;
  }

  RTAny get_elem(size_t idx) const override {
    auto map_impl = MapImpl::make_map_impl(&keys_, &values_[idx]);
    auto map = Map::make_map(map_impl);

    return RTAny::from_map(map);
  }

  Map get_value(size_t idx) const override {
    auto map_impl = MapImpl::make_map_impl(&keys_, &values_[idx]);
    return Map::make_map(map_impl);
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

 private:
  friend class MapValueColumnBuilder;
  std::vector<std::string> keys_;
  std::vector<std::vector<RTAny>> values_;
};

class MapValueColumnBuilder : public IContextColumnBuilder {
 public:
  MapValueColumnBuilder() = default;
  ~MapValueColumnBuilder() = default;

  void reserve(size_t size) override { values_.reserve(size); }

  void push_back_elem(const RTAny& val) override {
    CHECK(val.type() == RTAnyType::kMap);
    auto map = val.as_map();
    std::vector<RTAny> values;
    const auto& [_, vals] = map.key_vals();
    for (const auto& v : *vals) {
      values.push_back(v);
    }
    values_.push_back(std::move(values));
  }

  void push_back_opt(const std::vector<RTAny>& values) { values_.push_back(values); }

  void set_keys(const std::vector<std::string>& keys) { keys_ = keys; }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<MapValueColumn>();
    ret->keys_.swap(keys_);
    ret->values_.swap(values_);
    return ret;
  }

 private:
  friend class MapValueColumn;
  std::vector<std::string> keys_;
  std::vector<std::vector<RTAny>> values_;
};

template <typename T>
class OptionalValueColumn : public IValueColumn<T> {
 public:
  OptionalValueColumn() = default;
  ~OptionalValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "OptionalValueColumn<" + TypedConverter<T>::name() + ">[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override { return ContextColumnType::kOptionalValue; }

  std::shared_ptr<IContextColumn> dup() const override {
    OptionalValueColumnBuilder<T> builder;
    for (size_t i = 0; i < data_.size(); ++i) {
      builder.push_back_opt(data_[i], valid_[i]);
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> shuffle(const std::vector<size_t>& offsets) const override {
    OptionalValueColumnBuilder<T> builder;
    builder.reserve(offsets.size());
    for (auto offset : offsets) {
      builder.push_back_opt(data_[offset], valid_[offset]);
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<OptionalValueColumnBuilder<T>>());
  }

  RTAnyType elem_type() const override {
    auto type = TypedConverter<T>::type();
    type.null_able_ = true;
    return type;
  }
  RTAny get_elem(size_t idx) const override { return TypedConverter<T>::from_typed(data_[idx]); }

  T get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, bool> or std::is_same_v<T, Tuple>) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else {
      return new SigColumn<T>(data_);
    }
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(), [this](size_t a, size_t b) {
      // data_[a] == data_[b]
      if (!(data_[a] < data_[b]) && !(data_[b] < data_[a])) {
        return a < b;
      }
      return data_[a] < data_[b];
    });
    for (size_t i = 0; i < data_.size(); ++i) {
      if (i == 0 || ((data_[origin_offsets[i]] < data_[origin_offsets[i - 1]]) ||
                     (data_[origin_offsets[i - 1]] < data_[origin_offsets[i]]))) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

  bool has_value(size_t idx) const override { return valid_[idx]; }
  bool is_optional() const override { return true; }

 private:
  template <typename _T>
  friend class OptionalValueColumnBuilder;
  std::vector<T> data_;
  std::vector<bool> valid_;
};

template <>
class OptionalValueColumn<std::string_view> : public IValueColumn<std::string_view> {
 public:
  OptionalValueColumn() = default;
  ~OptionalValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "OptionalValueColumn<" + TypedConverter<std::string_view>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override { return ContextColumnType::kOptionalValue; }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<OptionalValueColumnBuilder<std::string_view>>());
  }

  std::shared_ptr<IContextColumn> dup() const override;

  std::shared_ptr<IContextColumn> shuffle(const std::vector<size_t>& offsets) const override;
  RTAnyType elem_type() const override {
    auto type = RTAnyType::kStringValue;
    type.null_able_ = true;
    return type;
  }
  RTAny get_elem(size_t idx) const override { return RTAny::from_string(data_[idx]); }

  std::string_view get_value(size_t idx) const override { return std::string_view(data_[idx]); }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(), [this](size_t a, size_t b) {
      if (data_[a] == data_[b]) {
        return a < b;
      }
      return data_[a] < data_[b];
    });
    for (size_t i = 0; i < data_.size(); ++i) {
      if (i == 0 || data_[origin_offsets[i]] != data_[origin_offsets[i - 1]]) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

  bool has_value(size_t idx) const override { return valid_[idx]; }
  bool is_optional() const override { return true; }

 private:
  friend class OptionalValueColumnBuilder<std::string_view>;
  std::vector<std::string> data_;
  std::vector<bool> valid_;
};

template <typename T>
class OptionalValueColumnBuilder : public IOptionalContextColumnBuilder {
 public:
  OptionalValueColumnBuilder() = default;
  ~OptionalValueColumnBuilder() = default;

  void reserve(size_t size) override {
    data_.reserve(size);
    valid_.reserve(size);
  }

  void push_back_elem(const RTAny& val) override {
    data_.push_back(TypedConverter<T>::to_typed(val));
    valid_.push_back(true);
  }

  void push_back_opt(const T& val, bool valid) {
    data_.push_back(val);
    valid_.push_back(valid);
  }

  void push_back_null() override {
    data_.emplace_back(T());
    valid_.push_back(false);
  }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<OptionalValueColumn<T>>();
    ret->data_.swap(data_);
    ret->valid_.swap(valid_);
    return std::dynamic_pointer_cast<IContextColumn>(ret);
  }

 private:
  std::vector<T> data_;
  std::vector<bool> valid_;
};

template <>
class OptionalValueColumnBuilder<std::string_view> : public IOptionalContextColumnBuilder {
 public:
  OptionalValueColumnBuilder() = default;
  ~OptionalValueColumnBuilder() = default;

  void reserve(size_t size) override {
    data_.reserve(size);
    valid_.reserve(size);
  }

  void push_back_elem(const RTAny& val) override {
    data_.push_back(std::string(val.as_string()));
    valid_.push_back(true);
  }

  void push_back_opt(const std::string& val, bool valid) {
    data_.push_back(val);
    valid_.push_back(valid);
  }

  void push_back_null() override {
    data_.emplace_back();
    valid_.push_back(false);
  }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<OptionalValueColumn<std::string_view>>();
    ret->data_.swap(data_);
    ret->valid_.swap(valid_);
    return std::dynamic_pointer_cast<IContextColumn>(ret);
  }

 private:
  std::vector<std::string> data_;
  std::vector<bool> valid_;
};

template <>
class ValueColumnBuilder<std::string_view> : public IContextColumnBuilder {
 public:
  ValueColumnBuilder() = default;
  ~ValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  void push_back_elem(const RTAny& val) override { data_.push_back(std::string(val.as_string())); }

  void push_back_opt(const std::string& val) { data_.push_back(val); }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ValueColumn<std::string_view>>();
    ret->data_.swap(data_);
    return ret;
  }

 private:
  std::vector<std::string> data_;
};

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::shuffle(const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::dup() const {
  ValueColumnBuilder<T> builder;
  for (auto v : data_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::union_col(
    std::shared_ptr<IContextColumn> other) const {
  ValueColumnBuilder<T> builder;
  for (auto v : data_) {
    builder.push_back_opt(v);
  }
  const ValueColumn<T>& rhs = *std::dynamic_pointer_cast<ValueColumn<T>>(other);
  for (auto v : rhs.data_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_VALUE_COLUMNS_H_