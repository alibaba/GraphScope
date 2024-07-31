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

#ifndef RUNTIME_COMMON_COLUMNS_PATH_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_PATH_COLUMNS_H_
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"

namespace gs {
namespace runtime {

class IPathColumn : public IContextColumn {
 public:
  IPathColumn() = default;
  virtual ~IPathColumn() = default;
  virtual size_t size() const = 0;
  virtual std::string column_info() const = 0;
  virtual ContextColumnType column_type() const = 0;
  virtual std::shared_ptr<IContextColumn> dup() const = 0;
  virtual std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const = 0;
  virtual RTAnyType elem_type() const = 0;
  virtual RTAny get_elem(size_t idx) const = 0;
  virtual const Path& get_path(size_t idx) const = 0;
  virtual ISigColumn* generate_signature() const = 0;
  virtual void generate_dedup_offset(std::vector<size_t>& offsets) const = 0;
};

class GeneralPathColumnBuilder;

class GeneralPathColumn : public IPathColumn {
 public:
  GeneralPathColumn() = default;
  ~GeneralPathColumn() {}
  size_t size() const override { return data_.size(); }
  std::string column_info() const override {
    return "GeneralPathColumn[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override {
    return ContextColumnType::kPath;
  }
  std::shared_ptr<IContextColumn> dup() const override;
  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  RTAnyType elem_type() const override { return RTAnyType::kPath; }
  RTAny get_elem(size_t idx) const override { return RTAny(data_[idx]); }
  const Path& get_path(size_t idx) const override { return data_[idx]; }
  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override;

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(),
              [this](size_t a, size_t b) {
                // data_[a] == data_[b]
                if (!(data_[a] < data_[b]) && !(data_[b] < data_[a])) {
                  return a < b;
                }
                return data_[a] < data_[b];
              });
    offsets.clear();
    offsets.push_back(origin_offsets[0]);
    for (size_t i = 1; i < origin_offsets.size(); ++i) {
      if (!(data_[origin_offsets[i]] == data_[origin_offsets[i - 1]])) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

  template <typename FUNC>
  void foreach_path(FUNC func) const {
    for (size_t i = 0; i < data_.size(); ++i) {
      const auto& path = data_[i];
      func(i, path);
    }
  }

  void set_path_impls(std::vector<std::shared_ptr<PathImpl>>&& path_impls) {
    path_impls_.swap(path_impls);
  }

 private:
  friend class GeneralPathColumnBuilder;
  std::vector<Path> data_;
  std::vector<std::shared_ptr<PathImpl>> path_impls_;
};

class GeneralPathColumnBuilder : public IContextColumnBuilder {
 public:
  GeneralPathColumnBuilder() = default;
  ~GeneralPathColumnBuilder() = default;
  void push_back_opt(const Path& p) { data_.push_back(p); }
  void push_back_elem(const RTAny& val) override {
    data_.push_back(val.as_path());
  }
  void reserve(size_t size) override { data_.reserve(size); }
  void set_path_impls(
      const std::vector<std::shared_ptr<PathImpl>>& path_impls) {
    path_impls_ = path_impls;
  }
  std::shared_ptr<IContextColumn> finish() override {
    auto col = std::make_shared<GeneralPathColumn>();
    col->data_.swap(data_);
    col->set_path_impls(std::move(path_impls_));
    path_impls_.clear();
    return col;
  }

 private:
  std::vector<Path> data_;
  std::vector<std::shared_ptr<PathImpl>> path_impls_;
};

}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_COMMON_COLUMNS_PATH_COLUMNS_H_