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

#ifndef RUNTIME_COMMON_COLUMNS_I_CONTEXT_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_I_CONTEXT_COLUMNS_H_

#include <memory>
#include <string>

#include "flex/engines/graph_db/runtime/common/rt_any.h"

#include "glog/logging.h"

namespace gs {

namespace runtime {

enum class ContextColumnType {
  kVertex,
  kEdge,
  kValue,
  kPath,
  kOptionalValue,
};

class ISigColumn {
 public:
  ISigColumn() = default;
  virtual ~ISigColumn() = default;
  virtual size_t get_sig(size_t idx) const = 0;
};

template <typename T>
class SigColumn : public ISigColumn {
 public:
  SigColumn(const std::vector<T>& data) : data_(data.data()) {}
  ~SigColumn() = default;
  size_t get_sig(size_t idx) const override {
    return static_cast<size_t>(data_[idx]);
  }

 private:
  const T* data_;
};

template <>
class SigColumn<Date> : public ISigColumn {
 public:
  SigColumn(const std::vector<Date>& data) : data_(data.data()) {}
  ~SigColumn() = default;
  size_t get_sig(size_t idx) const override {
    return static_cast<size_t>(data_[idx].milli_second);
  }

 private:
  const Date* data_;
};

template <>
class SigColumn<std::pair<label_t, vid_t>> : public ISigColumn {
 public:
  SigColumn(const std::vector<std::pair<label_t, vid_t>>& data)
      : data_(data.data()) {}
  ~SigColumn() = default;
  size_t get_sig(size_t idx) const override {
    const auto& v = data_[idx];
    size_t ret = v.first;
    ret <<= 32;
    ret += v.second;
    return ret;
  }

 private:
  const std::pair<label_t, vid_t>* data_;
};

template <>
class SigColumn<std::string_view> : public ISigColumn {
 public:
  SigColumn(const std::vector<std::string>& data) {
    std::unordered_map<std::string, size_t> table;
    sig_list_.reserve(data.size());
    for (auto& str : data) {
      auto iter = table.find(str);
      if (iter == table.end()) {
        size_t idx = table.size();
        table.emplace(str, idx);
        sig_list_.push_back(idx);
      } else {
        sig_list_.push_back(iter->second);
      }
    }
  }
  ~SigColumn() = default;
  size_t get_sig(size_t idx) const override { return sig_list_[idx]; }

 private:
  std::vector<size_t> sig_list_;
};

template <>
class SigColumn<std::set<std::string>> : public ISigColumn {
 public:
  SigColumn(const std::vector<std::set<std::string>>& data) {
    std::map<std::set<std::string>, size_t> table;
    sig_list_.reserve(data.size());
    for (auto& str : data) {
      auto iter = table.find(str);
      if (iter == table.end()) {
        size_t idx = table.size();
        table.emplace(str, idx);
        sig_list_.push_back(idx);
      } else {
        sig_list_.push_back(iter->second);
      }
    }
  }
  ~SigColumn() = default;
  size_t get_sig(size_t idx) const override { return sig_list_[idx]; }

 private:
  std::vector<size_t> sig_list_;
};

template <>
class SigColumn<std::vector<vid_t>> : public ISigColumn {
 public:
  SigColumn(const std::vector<std::vector<vid_t>>& data) {
    std::map<std::vector<vid_t>, size_t> table;
    sig_list_.reserve(data.size());
    for (auto& str : data) {
      auto iter = table.find(str);
      if (iter == table.end()) {
        size_t idx = table.size();
        table.emplace(str, idx);
        sig_list_.push_back(idx);
      } else {
        sig_list_.push_back(iter->second);
      }
    }
  }
  ~SigColumn() = default;
  size_t get_sig(size_t idx) const override { return sig_list_[idx]; }

 private:
  std::vector<size_t> sig_list_;
};

class IContextColumnBuilder;
class IOptionalContextColumnBuilder;

class IContextColumn {
 public:
  IContextColumn() = default;
  virtual ~IContextColumn() = default;

  virtual size_t size() const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return 0;
  }

  virtual std::shared_ptr<IContextColumn> dup() const = 0;

  virtual std::string column_info() const = 0;
  virtual ContextColumnType column_type() const = 0;

  virtual RTAnyType elem_type() const = 0;

  virtual std::shared_ptr<IContextColumnBuilder> builder() const = 0;

  virtual std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual RTAny get_elem(size_t idx) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return RTAny();
  }

  virtual bool has_value(size_t idx) const { return true; }

  virtual bool is_optional() const { return false; }

  virtual ISigColumn* generate_signature() const {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  virtual void generate_dedup_offset(std::vector<size_t>& offsets) const {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }
};

class IContextColumnBuilder {
 public:
  IContextColumnBuilder() = default;
  virtual ~IContextColumnBuilder() = default;

  virtual void reserve(size_t size) = 0;
  virtual void push_back_elem(const RTAny& val) = 0;

  virtual std::shared_ptr<IContextColumn> finish() = 0;
};

class IOptionalContextColumnBuilder : public IContextColumnBuilder {
 public:
  IOptionalContextColumnBuilder() = default;
  virtual ~IOptionalContextColumnBuilder() = default;

  virtual void push_back_null() = 0;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_I_CONTEXT_COLUMNS_H_