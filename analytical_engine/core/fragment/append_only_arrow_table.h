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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_APPEND_ONLY_ARROW_TABLE_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_APPEND_ONLY_ARROW_TABLE_H_

#include <memory>
#include <string>
#include <vector>

#include "vineyard/basic/ds/arrow_utils.h"

#include "core/error.h"

namespace gs {
namespace append_only_arrow_table_impl {
template <typename T>
struct ValueGetter {
  static T get(const std::shared_ptr<arrow::ArrayBuilder>& builder) {
    __builtin_unreachable();
  }
};

template <>
struct ValueGetter<uint64_t> {
  static uint64_t get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                      int64_t idx) {
    return std::dynamic_pointer_cast<arrow::UInt64Builder>(builder)->GetValue(
        idx);
  }
};

template <>
struct ValueGetter<int64_t> {
  static int64_t get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                     int64_t idx) {
    return std::dynamic_pointer_cast<arrow::Int64Builder>(builder)->GetValue(
        idx);
  }
};

template <>
struct ValueGetter<uint32_t> {
  static uint32_t get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                      int64_t idx) {
    return std::dynamic_pointer_cast<arrow::UInt32Builder>(builder)->GetValue(
        idx);
  }
};

template <>
struct ValueGetter<int32_t> {
  static int32_t get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                     int64_t idx) {
    return std::dynamic_pointer_cast<arrow::Int32Builder>(builder)->GetValue(
        idx);
  }
};

template <>
struct ValueGetter<double> {
  static double get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                    int64_t idx) {
    return std::dynamic_pointer_cast<arrow::DoubleBuilder>(builder)->GetValue(
        idx);
  }
};

template <>
struct ValueGetter<float> {
  static float get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                   int64_t idx) {
    return std::dynamic_pointer_cast<arrow::FloatBuilder>(builder)->GetValue(
        idx);
  }
};

template <>
struct ValueGetter<std::string> {
  static std::string get(const std::shared_ptr<arrow::ArrayBuilder>& builder,
                         int64_t idx) {
    return std::dynamic_pointer_cast<arrow::LargeStringBuilder>(builder)
        ->GetView(idx)
        .to_string();
  }
};
}  // namespace append_only_arrow_table_impl

/**
 * @brief An arrow table composed by multiple arrow array builder
 */
class AppendOnlyArrowTable {
 public:
  bl::result<void> AppendValue(int col, uint64_t val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::UInt64Builder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(int col, int64_t val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::Int64Builder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(int col, uint32_t val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::UInt32Builder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(int col, int32_t val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::Int32Builder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(int col, double val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::DoubleBuilder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(int col, float val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::FloatBuilder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(int col, std::string& val) {
    auto builder =
        std::dynamic_pointer_cast<arrow::LargeStringBuilder>(builders_[col]);

    ARROW_OK_OR_RAISE(builder->Append(val));
    return {};
  }

  bl::result<void> AppendValue(const std::shared_ptr<arrow::Table>& table,
                               int row) {
    CHECK_LT(row, table->num_rows());
    createBuildersIfNeeded(table);

    for (int64_t i = 0; i < table->num_columns(); i++) {
      auto column = table->column(i);
      auto type = column->type();

      CHECK_EQ(column->num_chunks(), 1);

      auto chunk = column->chunk(0);

      if (type == arrow::uint64()) {
        auto array = std::dynamic_pointer_cast<arrow::UInt64Array>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::UInt64Builder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->Value(row)));
      } else if (type == arrow::int64()) {
        auto array = std::dynamic_pointer_cast<arrow::Int64Array>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::Int64Builder>(builders_[i]);
        ARROW_OK_OR_RAISE(builder->Append(array->Value(row)));
      } else if (type == arrow::uint32()) {
        auto array = std::dynamic_pointer_cast<arrow::UInt32Array>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::UInt32Builder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->Value(row)));
      } else if (type == arrow::int32()) {
        auto array = std::dynamic_pointer_cast<arrow::Int32Array>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::Int32Builder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->Value(row)));
      } else if (type == arrow::float64()) {
        auto array = std::dynamic_pointer_cast<arrow::DoubleArray>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::DoubleBuilder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->Value(row)));
      } else if (type == arrow::float32()) {
        auto array = std::dynamic_pointer_cast<arrow::FloatArray>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::FloatBuilder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->Value(row)));
      } else if (type == arrow::utf8()) {
        auto array = std::dynamic_pointer_cast<arrow::StringArray>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::StringBuilder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->GetString(row)));
      } else if (type == arrow::large_utf8()) {
        auto array = std::dynamic_pointer_cast<arrow::LargeStringArray>(chunk);
        auto builder =
            std::dynamic_pointer_cast<arrow::LargeStringBuilder>(builders_[i]);

        ARROW_OK_OR_RAISE(builder->Append(array->GetString(row)));
      } else {
        CHECK(false);
      }
    }
    return {};
  }

  template <typename T>
  T GetValue(int column_id, int row_id) {
    return append_only_arrow_table_impl::ValueGetter<T>::get(
        builders_[column_id], row_id);
  }

  int64_t size() {
    if (schema_->num_fields() == 0)
      return 0;
    return builders_[0]->length();
  }

  std::shared_ptr<arrow::Schema> schema() { return schema_; }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;

  bl::result<void> createBuildersIfNeeded(
      const std::shared_ptr<arrow::Table>& table) {
    if (schema_ == nullptr) {
      schema_ = table->schema();
      BOOST_LEAF_CHECK(createBuilders());
    } else {
      if (!table->schema()->Equals(schema_)) {
        auto msg = "Different schema compared with previous one. Prev: " +
                   schema_->ToString() +
                   " Curr: " + table->schema()->ToString();
        RETURN_GS_ERROR(vineyard::ErrorCode::kArrowError, msg);
      }
    }
    return {};
  }

  bl::result<void> createBuilders() {
    auto& fields = schema_->fields();
    for (const auto& field : fields) {
      auto type = field->type();

      if (type == arrow::uint64()) {
        builders_.push_back(std::make_shared<arrow::UInt64Builder>());
      } else if (type == arrow::int64()) {
        builders_.push_back(std::make_shared<arrow::Int64Builder>());
      } else if (type == arrow::uint32()) {
        builders_.push_back(std::make_shared<arrow::UInt32Builder>());
      } else if (type == arrow::int32()) {
        builders_.push_back(std::make_shared<arrow::Int32Builder>());
      } else if (type == arrow::float32()) {
        builders_.push_back(std::make_shared<arrow::FloatBuilder>());
      } else if (type == arrow::float64()) {
        builders_.push_back(std::make_shared<arrow::DoubleBuilder>());
      } else if (type == arrow::utf8()) {
        builders_.push_back(std::make_shared<arrow::StringBuilder>());
      } else if (type == arrow::large_utf8()) {
        builders_.push_back(std::make_shared<arrow::LargeStringBuilder>());
      } else {
        RETURN_GS_ERROR(vineyard::ErrorCode::kArrowError,
                        "Unsupported type: " + type->ToString());
      }
    }
    return {};
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_APPEND_ONLY_ARROW_TABLE_H_
