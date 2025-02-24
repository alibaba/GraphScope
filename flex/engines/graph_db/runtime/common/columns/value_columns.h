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

#include "flex/engines/graph_db/runtime/common/columns/columns_utils.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
#include "flex/utils/top_n_generator.h"

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
  ValueColumn() {}
  ~ValueColumn() = default;

  inline size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ValueColumn<" + TypedConverter<T>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline RTAnyType elem_type() const override {
    return TypedConverter<T>::type();
  }
  inline RTAny get_elem(size_t idx) const override {
    return TypedConverter<T>::from_typed(data_[idx]);
  }

  inline T get_value(size_t idx) const override { return data_[idx]; }

  inline const std::vector<T>& data() const { return data_; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, bool> or std::is_same_v<T, Tuple> or
                  std::is_same_v<T, Map> or std::is_same_v<T, Set> or
                  std::is_same_v<T, Relation>) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else {
      return new SigColumn<T>(data_);
    }
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override;

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override;

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

  std::shared_ptr<Arena> get_arena() const override { return arena_; }

 private:
  template <typename _T>
  friend class ValueColumnBuilder;
  std::vector<T> data_;
  std::shared_ptr<Arena> arena_;
};

template <typename T>
class ValueColumnBuilder : public IContextColumnBuilder {
 public:
  ValueColumnBuilder() = default;
  ~ValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    data_.push_back(TypedConverter<T>::to_typed(val));
  }

  inline void push_back_opt(const T& val) { data_.push_back(val); }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>& arena) override {
    auto ret = std::make_shared<ValueColumn<T>>();
    ret->set_arena(arena);
    ret->data_.swap(data_);
    return ret;
  }

 private:
  std::vector<T> data_;
};

class ListValueColumnBuilder;

class ListValueColumnBase : public IValueColumn<List> {
 public:
  virtual std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
  unfold() const = 0;
};

class ListValueColumn : public ListValueColumnBase {
 public:
  ListValueColumn(RTAnyType type) : elem_type_(type) {}
  ~ListValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ListValueColumn[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  RTAnyType elem_type() const override {
    auto type = RTAnyType::kList;
    return type;
  }
  RTAny get_elem(size_t idx) const override {
    return RTAny::from_list(data_[idx]);
  }

  List get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  template <typename T>
  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>> unfold_impl()
      const {
    std::vector<size_t> offsets;
    auto builder = std::make_shared<ValueColumnBuilder<T>>();
    size_t i = 0;
    for (const auto& list : data_) {
      for (size_t j = 0; j < list.size(); ++j) {
        auto elem = list.get(j);
        builder->push_back_elem(elem);
        offsets.push_back(i);
      }
      ++i;
    }

    if constexpr (std::is_same_v<T, std::string_view> ||
                  std::is_same_v<T, Tuple> || std::is_same_v<T, Map> ||
                  std::is_same_v<T, Set>) {
      // TODO: we shouldn't use the same arena as the original column.
      // The ownership of list elements should be released.
      return {builder->finish(this->get_arena()), offsets};
    } else {
      return {builder->finish(nullptr), offsets};
    }
  }

  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>> unfold()
      const override {
    if (elem_type_ == RTAnyType::kVertex) {
      std::vector<size_t> offsets;
      auto builder = std::make_shared<MLVertexColumnBuilder>();
      size_t i = 0;
      for (const auto& list : data_) {
        for (size_t j = 0; j < list.size(); ++j) {
          auto elem = list.get(j);
          builder->push_back_elem(elem);
          offsets.push_back(i);
        }
        ++i;
      }
      return {builder->finish(nullptr), offsets};
    } else if (elem_type_ == RTAnyType::kI64Value) {
      return unfold_impl<int64_t>();
    } else if (elem_type_ == RTAnyType::kF64Value) {
      return unfold_impl<double>();
    } else if (elem_type_ == RTAnyType::kI32Value) {
      return unfold_impl<int32_t>();
    } else if (elem_type_ == RTAnyType::kDate32) {
      return unfold_impl<Day>();
    } else if (elem_type_ == RTAnyType::kTimestamp) {
      return unfold_impl<Date>();
    } else if (elem_type_ == RTAnyType::kStringValue) {
      return unfold_impl<std::string_view>();
    } else if (elem_type_ == RTAnyType::kTuple) {
      return unfold_impl<Tuple>();
    } else if (elem_type_ == RTAnyType::kRelation) {
      return unfold_impl<Relation>();
    } else if (elem_type_ == RTAnyType::kMap) {
      return unfold_impl<Map>();
    } else {
      LOG(FATAL) << "not implemented for " << this->column_info() << " "
                 << static_cast<int>(elem_type_);
      return {nullptr, std::vector<size_t>()};
    }
  }

  std::shared_ptr<Arena> get_arena() const override { return arena_; }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

 private:
  friend class ListValueColumnBuilder;
  RTAnyType elem_type_;
  std::vector<List> data_;

  std::shared_ptr<Arena> arena_;
};

class ListValueColumnBuilder : public IContextColumnBuilder {
 public:
  ListValueColumnBuilder(RTAnyType type) : type_(type) {}
  ~ListValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    assert(val.type() == RTAnyType::kList);
    data_.emplace_back(val.as_list());
  }

  void push_back_opt(const List& val) { data_.emplace_back(val); }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>& ptr) override {
    auto ret = std::make_shared<ListValueColumn>(type_);
    ret->data_.swap(data_);
    ret->set_arena(ptr);
    return ret;
  }

 private:
  RTAnyType type_;
  std::vector<List> data_;
};

template <typename T>
class OptionalValueColumn : public IValueColumn<T> {
 public:
  OptionalValueColumn() = default;
  ~OptionalValueColumn() = default;

  inline size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "OptionalValueColumn<" + TypedConverter<T>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kOptionalValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    OptionalValueColumnBuilder<T> builder;
    builder.reserve(offsets.size());
    for (auto offset : offsets) {
      builder.push_back_opt(data_[offset], valid_[offset]);
    }
    return builder.finish(this->get_arena());
  }

  inline RTAnyType elem_type() const override {
    auto type = TypedConverter<T>::type();
    return type;
  }
  inline RTAny get_elem(size_t idx) const override {
    return TypedConverter<T>::from_typed(data_[idx]);
  }

  inline T get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, bool> or std::is_same_v<T, Tuple> or
                  std::is_same_v<T, Map> or std::is_same_v<T, Set> or
                  std::is_same_v<T, Relation>) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else {
      return new SigColumn<T>(data_);
    }
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  bool has_value(size_t idx) const override { return valid_[idx]; }
  bool is_optional() const override { return true; }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }
  std::shared_ptr<Arena> get_arena() const override { return arena_; }

 private:
  template <typename _T>
  friend class OptionalValueColumnBuilder;
  std::vector<T> data_;
  std::vector<bool> valid_;
  std::shared_ptr<Arena> arena_;
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

  inline void push_back_elem(const RTAny& val) override {
    data_.push_back(TypedConverter<T>::to_typed(val));
    valid_.push_back(true);
  }

  inline void push_back_opt(const T& val, bool valid) {
    data_.push_back(val);
    valid_.push_back(valid);
  }

  inline void push_back_null() override {
    data_.emplace_back(T());
    valid_.push_back(false);
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>& arena) override {
    auto ret = std::make_shared<OptionalValueColumn<T>>();
    ret->data_.swap(data_);
    ret->valid_.swap(valid_);
    ret->set_arena(arena);
    return std::dynamic_pointer_cast<IContextColumn>(ret);
  }

 private:
  std::vector<T> data_;
  std::vector<bool> valid_;
};

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish(this->get_arena());
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset], true);
    }
  }
  return builder.finish(this->get_arena());
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
  auto arena1 = this->get_arena();
  auto arena2 = other->get_arena();
  auto arena = std::make_shared<Arena>();
  if (arena1 != nullptr) {
    arena->emplace_back(std::make_unique<ArenaRef>(arena1));
  }
  if (arena2 != nullptr) {
    arena->emplace_back(std::make_unique<ArenaRef>(arena2));
  }
  return builder.finish(arena);
}

template <typename T>
bool ValueColumn<T>::order_by_limit(bool asc, size_t limit,
                                    std::vector<size_t>& offsets) const {
  size_t size = data_.size();
  if (size == 0) {
    return false;
  }
  if (asc) {
    TopNGenerator<T, TopNAscCmp<T>> generator(limit);
    for (size_t i = 0; i < size; ++i) {
      generator.push(data_[i], i);
    }
    generator.generate_indices(offsets);
  } else {
    TopNGenerator<T, TopNDescCmp<T>> generator(limit);
    for (size_t i = 0; i < size; ++i) {
      generator.push(data_[i], i);
    }
    generator.generate_indices(offsets);
  }
  return true;
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_VALUE_COLUMNS_H_