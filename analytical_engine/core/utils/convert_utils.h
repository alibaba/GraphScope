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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/object/dynamic.h"

namespace gs {

template <typename FRAGMENT_T>
struct PropertyConverter {
  inline static void NodeValue(
      const std::shared_ptr<FRAGMENT_T>& fragment,
      const typename FRAGMENT_T::vertex_t& v,
      const std::shared_ptr<arrow::DataType> data_type,
      const std::string& prop_name, int prop_id, rapidjson::Value& ret,
      dynamic::AllocatorT& allocator = dynamic::Value::allocator_) {
    switch (data_type->id()) {
    case arrow::Type::type::INT32: {
      rapidjson::Value v_(fragment->template GetData<int32_t>(v, prop_id));
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    case arrow::Type::type::INT64: {
      rapidjson::Value v_(fragment->template GetData<int64_t>(v, prop_id));
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    case arrow::Type::type::UINT32: {
      rapidjson::Value v_(fragment->template GetData<uint32_t>(v, prop_id));
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    case arrow::Type::type::UINT64: {
      rapidjson::Value v_(fragment->template GetData<uint64_t>(v, prop_id));
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    case arrow::Type::type::FLOAT: {
      rapidjson::Value v_(fragment->template GetData<float>(v, prop_id));
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    case arrow::Type::type::DOUBLE: {
      rapidjson::Value v_(fragment->template GetData<double>(v, prop_id));
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    case arrow::Type::type::STRING:
    case arrow::Type::type::LARGE_STRING: {
      rapidjson::Value v_(
          fragment->template GetData<std::string>(v, prop_id).c_str(),
          allocator);
      ret.AddMember(rapidjson::Value(prop_name, allocator).Move(), v_,
                    allocator);
      break;
    }
    default:
      // unsupported types in dynamic, ignore
      break;
    }
  }

  inline static void EdgeValue(
      const std::shared_ptr<arrow::Table>& data_table, int64_t row_id,
      rapidjson::Value& ret,
      dynamic::AllocatorT& allocator = dynamic::Value::allocator_) {
    for (auto col_id = 0; col_id < data_table->num_columns(); col_id++) {
      auto column = data_table->column(col_id);
      auto type = data_table->column(col_id)->type();
      auto property_name = data_table->field(col_id)->name();
      switch (type->id()) {
      case arrow::Type::type::INT32: {
        auto array =
            std::dynamic_pointer_cast<arrow::Int32Array>(column->chunk(0));
        rapidjson::Value v(array->Value(row_id));
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      case arrow::Type::type::INT64: {
        auto array =
            std::dynamic_pointer_cast<arrow::Int64Array>(column->chunk(0));
        rapidjson::Value v(array->Value(row_id));
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      case arrow::Type::type::UINT32: {
        auto array =
            std::dynamic_pointer_cast<arrow::UInt32Array>(column->chunk(0));
        rapidjson::Value v(array->Value(row_id));
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      case arrow::Type::type::FLOAT: {
        auto array =
            std::dynamic_pointer_cast<arrow::FloatArray>(column->chunk(0));
        rapidjson::Value v(array->Value(row_id));
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      case arrow::Type::type::DOUBLE: {
        auto array =
            std::dynamic_pointer_cast<arrow::DoubleArray>(column->chunk(0));
        rapidjson::Value v(array->Value(row_id));
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      case arrow::Type::type::STRING: {
        auto array =
            std::dynamic_pointer_cast<arrow::StringArray>(column->chunk(0));
        rapidjson::Value v(array->GetString(row_id).c_str(), allocator);
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      case arrow::Type::type::LARGE_STRING: {
        auto array = std::dynamic_pointer_cast<arrow::LargeStringArray>(
            column->chunk(0));
        rapidjson::Value v(array->GetString(row_id).c_str(), allocator);
        ret.AddMember(rapidjson::Value(property_name, allocator).Move(), v,
                      allocator);
        break;
      }
      default:
        // unsupported types in dynamic, ignore
        break;
      }
    }
  }
};

template <typename ITER_T, typename FUNC_T>
void parallel_for(const ITER_T& begin, const ITER_T& end, const FUNC_T& func,
                  uint32_t thread_num, size_t chunk = 1024) {
  std::vector<std::thread> threads(thread_num);
  std::atomic<size_t> cur(0);
  for (uint32_t i = 0; i < thread_num; ++i) {
    threads[i] = std::thread([&cur, chunk, &func, begin, end, i]() {
      while (true) {
        const ITER_T cur_beg = std::min(begin + cur.fetch_add(chunk), end);
        const ITER_T cur_end = std::min(cur_beg + chunk, end);
        if (cur_beg == cur_end) {
          break;
        }
        for (auto iter = cur_beg; iter != cur_end; ++iter) {
          func(i, *iter);
        }
      }
    });
  }
  for (auto& thrd : threads) {
    thrd.join();
  }
}
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_CONVERT_UTILS_H_
