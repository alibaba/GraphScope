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
#ifndef ANALYTICAL_ENGINE_CORE_UTILS_TRIVIAL_TENSOR_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_TRIVIAL_TENSOR_H_

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "vineyard/common/util/arrow.h"

namespace arrow {
class LargeStringArray;
}

namespace gs {
/**
 * @brief trivial_tensor_t is a naive implementation of tensor data structure.
 * It seems that xtensor is too heavy to us currently, and impose a lot of
 * unnecessary dependencies.
 *
 * @tparam T the data type to hold by the tensor
 */
template <typename T>
struct trivial_tensor_t {
 public:
  trivial_tensor_t() : size_(0), data_(nullptr) {}

  ~trivial_tensor_t() {
    if (data_) {
      delete[] data_;
      data_ = nullptr;
    }
  }

  T* data() { return data_; }

  const T* data() const { return data_; }

  void fill(T const& value) { std::fill_n(data_, size_, value); }

  std::vector<size_t> shape() const { return shape_; }

  size_t size() const { return size_; }

  void resize(std::vector<size_t> const& shape) {
    size_t flat_size = shape.empty() ? 0 : 1;
    for (auto dim_size : shape) {
      flat_size *= dim_size;
    }
    this->shape_ = shape;
    if (flat_size != size_) {
      T* new_data = new T[flat_size];
      std::copy(data_, data_ + size_, new_data);
      size_ = flat_size;
      if (data_) {
        delete[] data_;
      }
      data_ = new_data;
    }
  }

 private:
  size_t size_;
  std::vector<size_t> shape_;
  T* data_;
};

template <>
struct trivial_tensor_t<std::string> {
 public:
  trivial_tensor_t() : size_(0), data_(nullptr) {}

  ~trivial_tensor_t() = default;

  std::shared_ptr<arrow::LargeStringArray>& data() { return data_; }

  const std::shared_ptr<arrow::LargeStringArray>& data() const { return data_; }

  void fill(const std::string& value) {
    arrow::LargeStringBuilder builder;
    CHECK_ARROW_ERROR(builder.Reserve(size_));
    for (size_t i = 0; i < size_; ++i) {
      CHECK_ARROW_ERROR(builder.Append(value));
    }
    CHECK_ARROW_ERROR(builder.Finish(&data_));
  }

  std::vector<size_t> shape() const { return shape_; }

  size_t size() const { return size_; }

  void resize(std::vector<size_t> const& shape) {
    size_t flat_size = shape.empty() ? 0 : 1;
    for (auto dim_size : shape) {
      flat_size *= dim_size;
    }
    this->shape_ = shape;
    size_ = flat_size;
  }

 private:
  size_t size_;
  std::vector<size_t> shape_;
  std::shared_ptr<arrow::LargeStringArray> data_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_TRIVIAL_TENSOR_H_
