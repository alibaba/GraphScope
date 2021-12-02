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

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_CONVERT_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_CONVERT_UTILS_H_

#include <memory>
#include <string>

#include "folly/dynamic.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

namespace gs {

template <typename FRAGMENT_T>
struct PropertyConverter {
  inline static void NodeValue(const std::shared_ptr<FRAGMENT_T>& fragment,
                               const typename FRAGMENT_T::vertex_t& v,
                               const std::shared_ptr<arrow::DataType> data_type,
                               const std::string& prop_name, int prop_id,
                               folly::dynamic& ret) {
    switch (data_type->id()) {
    case arrow::Type::type::INT32: {
      ret.insert(prop_name, fragment->template GetData<int32_t>(v, prop_id));
      break;
    }
    case arrow::Type::type::INT64: {
      ret.insert(prop_name, fragment->template GetData<int64_t>(v, prop_id));
      break;
    }
    case arrow::Type::type::UINT32: {
      ret.insert(prop_name, fragment->template GetData<uint32_t>(v, prop_id));
      break;
    }
    case arrow::Type::type::UINT64: {
      ret.insert(prop_name, fragment->template GetData<uint64_t>(v, prop_id));
      break;
    }
    case arrow::Type::type::FLOAT: {
      ret.insert(prop_name, fragment->template GetData<float>(v, prop_id));
      break;
    }
    case arrow::Type::type::DOUBLE: {
      ret.insert(prop_name, fragment->template GetData<double>(v, prop_id));
      break;
    }
    case arrow::Type::type::STRING:
    case arrow::Type::type::LARGE_STRING: {
      ret.insert(prop_name,
                 fragment->template GetData<std::string>(v, prop_id));
      break;
    }
    default:
      // unsupported types in dynamic, ignore
      break;
    }
  }

  inline static void EdgeValue(const std::shared_ptr<arrow::Table>& data_table,
                               int64_t row_id, folly::dynamic& ret) {
    for (auto col_id = 0; col_id < data_table->num_columns(); col_id++) {
      auto column = data_table->column(col_id);
      auto type = data_table->column(col_id)->type();
      auto property_name = data_table->field(col_id)->name();
      switch (type->id()) {
      case arrow::Type::type::INT32: {
        auto array =
            std::dynamic_pointer_cast<arrow::Int32Array>(column->chunk(0));
        ret.insert(property_name, array->Value(row_id));
        break;
      }
      case arrow::Type::type::INT64: {
        auto array =
            std::dynamic_pointer_cast<arrow::Int64Array>(column->chunk(0));
        ret.insert(property_name, array->Value(row_id));
        break;
      }
      case arrow::Type::type::UINT32: {
        auto array =
            std::dynamic_pointer_cast<arrow::UInt32Array>(column->chunk(0));
        ret.insert(property_name, array->Value(row_id));
        break;
      }
      case arrow::Type::type::FLOAT: {
        auto array =
            std::dynamic_pointer_cast<arrow::FloatArray>(column->chunk(0));
        ret.insert(property_name, array->Value(row_id));
        break;
      }
      case arrow::Type::type::DOUBLE: {
        auto array =
            std::dynamic_pointer_cast<arrow::DoubleArray>(column->chunk(0));
        ret.insert(property_name, array->Value(row_id));
        break;
      }
      case arrow::Type::type::STRING: {
        auto array =
            std::dynamic_pointer_cast<arrow::StringArray>(column->chunk(0));
        ret.insert(property_name, array->GetString(row_id));
        break;
      }
      case arrow::Type::type::LARGE_STRING: {
        auto array = std::dynamic_pointer_cast<arrow::LargeStringArray>(
            column->chunk(0));
        ret.insert(property_name, array->GetString(row_id));
        break;
      }
      default:
        // unsupported types in dynamic, ignore
        break;
      }
    }
  }
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_CONVERT_UTILS_H_
