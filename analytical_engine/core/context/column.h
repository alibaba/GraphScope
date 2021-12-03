
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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_COLUMN_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_COLUMN_H_

#include <memory>
#include <string>

#include "vineyard/basic/ds/arrow_utils.h"

#include "core/context/context_protocols.h"
namespace gs {

/**
 * @brief IColumn is a base class that represents a column, which is used in
 * the property context.
 */
class IColumn {
 public:
  IColumn() = default;
  virtual ~IColumn() = default;
  const std::string& name() const { return name_; }
  void set_name(const std::string& name) { name_ = name; }

  virtual ContextDataType type() const { return ContextDataType::kUndefined; }
  virtual std::shared_ptr<arrow::Array> ToArrowArray() const = 0;

 private:
  std::string name_;
};

/**
 * @brief Column is a templated implementation of IColumn. Each element in the
 * column must be associated with a vertex in the fragment.
 *
 * @tparam FRAG_T Fragment class
 * @tparam DATA_T Data type
 */
template <typename FRAG_T, typename DATA_T>
class Column : public IColumn {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using vertex_range_t = typename fragment_t::vertex_range_t;
  using vertex_array_t = typename fragment_t::template vertex_array_t<DATA_T>;
  static_assert(std::is_pod<DATA_T>::value ||
                    std::is_same<DATA_T, std::string>::value,
                "Unsupported data type in Column, expect POD value or string.");

  Column(const std::string& name, vertex_range_t range) {
    this->set_name(name);
    data_.Init(range);
  }

  ContextDataType type() const override {
    return ContextTypeToEnum<DATA_T>::value;
  }

  std::shared_ptr<arrow::Array> ToArrowArray() const override {
    auto range = data_.GetVertexRange();
    typename vineyard::ConvertToArrowType<DATA_T>::BuilderType builder;
    std::shared_ptr<typename vineyard::ConvertToArrowType<DATA_T>::ArrayType>
        ret;
    for (auto v : range) {
      CHECK_ARROW_ERROR(builder.Append(data_[v]));
    }
    CHECK_ARROW_ERROR(builder.Finish(&ret));
    return ret;
  }

  DATA_T& at(vertex_t v) { return data_[v]; }

  const DATA_T& at(vertex_t v) const { return data_[v]; }

  void set(vertex_t v, const DATA_T& value) { data_[v] = value; }

  vertex_array_t& data() { return data_; }

 private:
  vertex_array_t data_;
};

/**
 * @brief Create a IColumn object based on given data type.
 *
 * @tparam FRAG_T
 * @param name
 * @param range
 * @param type
 * @return std::shared_ptr<IColumn>
 */
template <typename FRAG_T>
std::shared_ptr<IColumn> CreateColumn(const std::string& name,
                                      typename FRAG_T::vertex_range_t range,
                                      ContextDataType type) {
  if (type == ContextDataType::kInt32) {
    return std::make_shared<Column<FRAG_T, int32_t>>(name, range);
  } else if (type == ContextDataType::kInt64) {
    return std::make_shared<Column<FRAG_T, int64_t>>(name, range);
  } else if (type == ContextDataType::kUInt32) {
    return std::make_shared<Column<FRAG_T, uint32_t>>(name, range);
  } else if (type == ContextDataType::kUInt64) {
    return std::make_shared<Column<FRAG_T, uint64_t>>(name, range);
  } else if (type == ContextDataType::kFloat) {
    return std::make_shared<Column<FRAG_T, float>>(name, range);
  } else if (type == ContextDataType::kDouble) {
    return std::make_shared<Column<FRAG_T, double>>(name, range);
  } else if (type == ContextDataType::kString) {
    return std::make_shared<Column<FRAG_T, std::string>>(name, range);
  } else {
    return nullptr;
  }
}
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_COLUMN_H_
