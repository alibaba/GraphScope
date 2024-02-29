/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_TRANSFORM_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_TRANSFORM_UTILS_H_

#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"

#include "grape/communication/communicator.h"
#include "vineyard/basic/ds/tensor.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#ifdef NETWORKX
#include "core/object/dynamic.h"
#endif
#include "core/context/column.h"
#include "core/utils/trait_utils.h"

namespace bl = boost::leaf;

namespace gs {

#ifdef NETWORKX
template <typename T>
struct is_dynamic {
  constexpr static bool value = std::is_same<T, dynamic::Value>::value;
};

template <typename FRAG_T>
struct oid_is_dynamic {
  constexpr static bool value =
      std::is_same<typename FRAG_T::oid_t, dynamic::Value>::value;
};
#else
template <typename T>
struct is_dynamic {
  constexpr static bool value = false;
};

template <typename FRAG_T>
struct oid_is_dynamic {
  constexpr static bool value = false;
};
#endif

template <typename T>
void output_column_impl(grape::OutArchive& arc, int64_t length,
                        std::ofstream& fout) {
  T val;
  for (int64_t i = 0; i < length; ++i) {
    arc >> val;
    fout << val << std::endl;
  }
}

inline void output_nd_array(grape::InArchive&& iarc,
                            const std::string& location) {
  std::ofstream fout;
  fout.open(location);

  grape::OutArchive oarc(std::move(iarc));
  int64_t ndim;
  oarc >> ndim;
  int64_t length1 = 1, length2;
  for (int64_t dim_i = 0; dim_i < ndim; ++dim_i) {
    int64_t x;
    oarc >> x;
    length1 *= x;
  }

  int type_val;
  oarc >> type_val >> length2;

  CHECK_EQ(length1, length2);

  if (type_val == 1) {
    output_column_impl<int32_t>(oarc, length1, fout);
  } else if (type_val == 2) {
    output_column_impl<double>(oarc, length1, fout);
  } else if (type_val == 3) {
    output_column_impl<int64_t>(oarc, length1, fout);
  }

  CHECK(oarc.Empty());
}

inline void output_dataframe(grape::InArchive&& iarc,
                             const std::string& prefix) {
  grape::OutArchive oarc(std::move(iarc));
  int64_t col_num, row_num;
  oarc >> col_num >> row_num;
  for (int64_t i = 0; i < col_num; ++i) {
    std::string col_name;
    int type_val;
    oarc >> col_name >> type_val;
    std::string path = prefix + "_col_" + std::to_string(i) + "_" + col_name;
    std::ofstream fout;
    fout.open(path);
    if (type_val == 1) {
      output_column_impl<int>(oarc, row_num, fout);
    } else if (type_val == 2) {
      output_column_impl<double>(oarc, row_num, fout);
    } else if (type_val == 3) {
      output_column_impl<int64_t>(oarc, row_num, fout);
    }
  }

  CHECK(oarc.Empty());
}

template <typename OID_T>
typename std::enable_if<!is_dynamic<OID_T>::value, OID_T>::type string_to_oid(
    const std::string& s_oid) {
  return boost::lexical_cast<OID_T>(s_oid);
}

#ifdef NETWORKX
template <typename OID_T>
typename std::enable_if<is_dynamic<OID_T>::value, OID_T>::type string_to_oid(
    const std::string& s_oid) {
  return dynamic::Value(s_oid);
}
#endif

template <typename FRAG_T>
typename std::enable_if<!oid_is_dynamic<FRAG_T>::value,
                        typename std::vector<typename FRAG_T::vertex_t>>::type
select_vertices_impl(const FRAG_T& frag,
                     const typename FRAG_T::vertex_range_t& iv,
                     const std::pair<std::string, std::string>& range) {
  using vertex_t = typename FRAG_T::vertex_t;
  using oid_t = typename FRAG_T::oid_t;

  std::vector<vertex_t> vertices;
  auto& begin = range.first;
  auto& end = range.second;

  if (begin.empty() && end.empty()) {
    for (auto& v : iv) {
      vertices.emplace_back(v);
    }
  } else if (begin.empty() && !end.empty()) {
    oid_t end_id = string_to_oid<oid_t>(end);
    for (auto& v : iv) {
      if (frag.GetId(v) < end_id) {
        vertices.emplace_back(v);
      }
    }
  } else if (!begin.empty() && end.empty()) {
    oid_t begin_id = string_to_oid<oid_t>(begin);
    for (auto& v : iv) {
      if (frag.GetId(v) >= begin_id) {
        vertices.emplace_back(v);
      }
    }
  } else if (!begin.empty() && !end.empty()) {
    oid_t begin_id = string_to_oid<oid_t>(begin);
    oid_t end_id = string_to_oid<oid_t>(end);
    for (auto& v : iv) {
      oid_t id = frag.GetId(v);
      if (id >= begin_id && id < end_id) {
        vertices.emplace_back(v);
      }
    }
  }
  return vertices;
}

template <typename FRAG_T>
typename std::enable_if<oid_is_dynamic<FRAG_T>::value,
                        typename std::vector<typename FRAG_T::vertex_t>>::type
select_vertices_impl(const FRAG_T& frag,
                     const typename FRAG_T::vertex_range_t& iv,
                     const std::pair<std::string, std::string>& range) {
  using vertex_t = typename FRAG_T::vertex_t;
  using oid_t = typename FRAG_T::oid_t;

  std::vector<vertex_t> vertices;
  auto& begin = range.first;
  auto& end = range.second;

  if (begin.empty() && end.empty()) {
    for (auto& v : iv) {
      if (frag.IsAliveInnerVertex(v)) {
        vertices.emplace_back(v);
      }
    }
  } else if (begin.empty() && !end.empty()) {
    oid_t end_id = string_to_oid<oid_t>(end);
    for (auto& v : iv) {
      if (frag.GetId(v) < end_id && frag.IsAliveInnerVertex(v)) {
        vertices.emplace_back(v);
      }
    }
  } else if (!begin.empty() && end.empty()) {
    oid_t begin_id = string_to_oid<oid_t>(begin);
    for (auto& v : iv) {
      if (frag.GetId(v) >= begin_id && frag.IsAliveInnerVertex(v)) {
        vertices.emplace_back(v);
      }
    }
  } else if (!begin.empty() && !end.empty()) {
    oid_t begin_id = string_to_oid<oid_t>(begin);
    oid_t end_id = string_to_oid<oid_t>(end);
    for (auto& v : iv) {
      oid_t id = frag.GetId(v);
      if (id >= begin_id && id < end_id && frag.IsAliveInnerVertex(v)) {
        vertices.emplace_back(v);
      }
    }
  }
  return vertices;
}

inline void gather_archives(grape::InArchive& arc,
                            const grape::CommSpec& comm_spec, size_t from = 0) {
  if (comm_spec.fid() == 0) {
    int64_t local_length = 0;
    std::vector<int64_t> gathered_length(comm_spec.fnum(), 0);
    MPI_Gather(&local_length, 1, MPI_INT64_T, &gathered_length[0], 1,
               MPI_INT64_T, comm_spec.worker_id(), comm_spec.comm());
    int64_t total_length = 0;
    for (auto gl : gathered_length) {
      total_length += gl;
    }
    size_t old_length = arc.GetSize();
    arc.Resize(old_length + total_length);
    char* ptr = arc.GetBuffer() + static_cast<ptrdiff_t>(old_length);

    for (grape::fid_t i = 1; i < comm_spec.fnum(); ++i) {
      grape::sync_comm::recv_buffer<char>(
          ptr, static_cast<size_t>(gathered_length[i]),
          comm_spec.FragToWorker(i), 0, comm_spec.comm());
      ptr += static_cast<ptrdiff_t>(gathered_length[i]);
    }
  } else {
    auto local_length = static_cast<int64_t>(arc.GetSize() - from);
    MPI_Gather(&local_length, 1, MPI_INT64_T, NULL, 1, MPI_INT64_T,
               comm_spec.FragToWorker(0), comm_spec.comm());

    grape::sync_comm::send_buffer<char>(
        arc.GetBuffer() + static_cast<ptrdiff_t>(from),
        static_cast<size_t>(local_length), comm_spec.FragToWorker(0), 0,
        comm_spec.comm());
    arc.Resize(from);
  }
}

template <typename FRAG_T>
typename std::enable_if<
    !std::is_same<typename FRAG_T::vdata_t, grape::EmptyType>::value,
    bl::result<std::shared_ptr<arrow::Array>>>::type
vertex_data_to_arrow_array_impl(const FRAG_T& frag) {
  using vdata_t = typename FRAG_T::vdata_t;
  typename vineyard::ConvertToArrowType<vdata_t>::BuilderType builder;
  auto iv = frag.InnerVertices();

  for (auto& v : iv) {
    ARROW_OK_OR_RAISE(builder.Append(frag.GetData(v)));
  }
  std::shared_ptr<typename vineyard::ConvertToArrowType<vdata_t>::ArrayType>
      ret;
  ARROW_OK_OR_RAISE(builder.Finish(&ret));
  return std::shared_ptr<arrow::Array>(ret);
}

template <typename FRAG_T>
typename std::enable_if<
    std::is_same<typename FRAG_T::vdata_t, grape::EmptyType>::value,
    bl::result<std::shared_ptr<arrow::Array>>>::type
vertex_data_to_arrow_array_impl(const FRAG_T& frag) {
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                  "Can not transform empty type to arrow array");
}

template <typename FUNC_T,
          typename std::enable_if<
              !std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                            std::string>::value,
              void>::type* = nullptr>
typename std::enable_if<
    !std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                  grape::EmptyType>::value &&

        !is_dynamic<typename std::result_of<FUNC_T(size_t)>::type>::value,
    bl::result<std::shared_ptr<vineyard::ITensorBuilder>>>::type
build_vy_tensor_builder(vineyard::Client& client, size_t size, FUNC_T&& func,
                        int64_t part_idx) {
  using tensor_builder_t =
      vineyard::TensorBuilder<typename std::result_of<FUNC_T(size_t)>::type>;
  std::vector<int64_t> shape{static_cast<int64_t>(size)};
  std::shared_ptr<tensor_builder_t> tensor_builder;

  std::vector<int64_t> part{part_idx};
  tensor_builder = std::make_shared<tensor_builder_t>(client, shape, part);

  for (size_t i = 0; i < size; i++) {
    tensor_builder->data()[i] = func(i);
  }

  return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(tensor_builder);
}

template <typename FUNC_T,
          typename std::enable_if<
              std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                           std::string>::value,
              void>::type* = nullptr>
typename std::enable_if<
    !std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                  grape::EmptyType>::value &&

        !is_dynamic<typename std::result_of<FUNC_T(size_t)>::type>::value,
    bl::result<std::shared_ptr<vineyard::ITensorBuilder>>>::type
build_vy_tensor_builder(vineyard::Client& client, size_t size, FUNC_T&& func,
                        int64_t part_idx) {
  using tensor_builder_t =
      vineyard::TensorBuilder<typename std::result_of<FUNC_T(size_t)>::type>;
  std::vector<int64_t> shape{static_cast<int64_t>(size)};
  std::shared_ptr<tensor_builder_t> tensor_builder;

  std::vector<int64_t> part{part_idx};
  tensor_builder = std::make_shared<tensor_builder_t>(client, shape, part);

  for (size_t i = 0; i < size; i++) {
    std::string data = func(i);
    tensor_builder->Append(
        reinterpret_cast<typename tensor_builder_t::value_const_pointer_t>(
            data.data()),
        data.size());
  }

  return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(tensor_builder);
}

template <typename FUNC_T>
typename std::enable_if<
    std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                 grape::EmptyType>::value,
    bl::result<std::shared_ptr<vineyard::ITensorBuilder>>>::type
build_vy_tensor_builder(vineyard::Client& client, size_t size, FUNC_T&& func,
                        int64_t part_idx) {
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                  "Can not transform empty type to vineyard tensor builder");
}

template <typename FUNC_T>
typename std::enable_if<
    is_dynamic<typename std::result_of<FUNC_T(size_t)>::type>::value,
    bl::result<std::shared_ptr<vineyard::ITensorBuilder>>>::type
build_vy_tensor_builder(vineyard::Client& client, size_t size, FUNC_T&& func,
                        int64_t part_idx) {
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                  "Can not transform dynamic type to vineyard tensor builder");
}

template <typename FUNC_T>
typename std::enable_if<
    !std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                  grape::EmptyType>::value &&
        !is_dynamic<typename std::result_of<FUNC_T(size_t)>::type>::value,
    bl::result<vineyard::ObjectID>>::type
build_vy_tensor(vineyard::Client& client, size_t size, FUNC_T&& func,
                int64_t part_idx) {
  BOOST_LEAF_AUTO(base_builder,
                  build_vy_tensor_builder(client, size, func, part_idx));
  auto builder = std::dynamic_pointer_cast<
      vineyard::TensorBuilder<typename std::result_of<FUNC_T(size_t)>::type>>(
      base_builder);
  auto tensor = builder->Seal(client);

  VY_OK_OR_RAISE(tensor->Persist(client));
  return tensor->id();
}

template <typename FUNC_T>
typename std::enable_if<
    std::is_same<typename std::result_of<FUNC_T(size_t)>::type,
                 grape::EmptyType>::value,
    bl::result<vineyard::ObjectID>>::type
build_vy_tensor(vineyard::Client& client, size_t size, FUNC_T&& func,
                int64_t part_idx) {
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                  "Can not transform empty type");
}

template <typename FUNC_T>
typename std::enable_if<
    is_dynamic<typename std::result_of<FUNC_T(size_t)>::type>::value,
    bl::result<vineyard::ObjectID>>::type
build_vy_tensor(vineyard::Client& client, size_t size, FUNC_T&& func,
                int64_t part_idx) {
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                  "Can not transform dynamic type");
}

template <typename FRAG_T, typename DATA_T,
          typename std::enable_if<
              !std::is_same<std::string, DATA_T>::value>::type* = nullptr>
std::shared_ptr<vineyard::TensorBuilder<DATA_T>>
column_to_vy_tensor_builder_impl(
    vineyard::Client& client, const std::shared_ptr<IColumn>& column,
    const std::vector<typename FRAG_T::vertex_t>& vertices) {
  auto col = std::dynamic_pointer_cast<Column<FRAG_T, DATA_T>>(column);
  std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
  auto tensor_builder =
      std::make_unique<vineyard::TensorBuilder<DATA_T>>(client, shape);

  for (size_t i = 0; i < vertices.size(); i++) {
    tensor_builder->data()[i] = col->at(vertices[i]);
  }
  return tensor_builder;
}

template <typename FRAG_T, typename DATA_T,
          typename std::enable_if<
              std::is_same<std::string, DATA_T>::value>::type* = nullptr>
std::shared_ptr<vineyard::TensorBuilder<DATA_T>>
column_to_vy_tensor_builder_impl(
    vineyard::Client& client, const std::shared_ptr<IColumn>& column,
    const std::vector<typename FRAG_T::vertex_t>& vertices) {
  auto col = std::dynamic_pointer_cast<Column<FRAG_T, DATA_T>>(column);
  std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
  auto tensor_builder =
      std::make_unique<vineyard::TensorBuilder<DATA_T>>(client, shape);

  for (size_t i = 0; i < vertices.size(); i++) {
    const std::string data = col->at(vertices[i]);
    tensor_builder->Append(
        reinterpret_cast<
            typename vineyard::TensorBuilder<DATA_T>::value_const_pointer_t>(
            data.data()),
        data.size());
  }
  return tensor_builder;
}

template <typename FRAG_T>
bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
column_to_vy_tensor_builder(
    vineyard::Client& client, const std::shared_ptr<IColumn>& column,
    const std::vector<typename FRAG_T::vertex_t>& vertices) {
  std::shared_ptr<vineyard::ITensorBuilder> builder;

  switch (column->type()) {
  case ContextDataType::kInt32:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, int32_t>(client, column,
                                                                vertices);
    break;
  case ContextDataType::kBool:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, bool>(client, column,
                                                             vertices);
    break;
  case ContextDataType::kInt64:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, int64_t>(client, column,
                                                                vertices);
    break;
  case ContextDataType::kUInt32:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, uint32_t>(client, column,
                                                                 vertices);
    break;
  case ContextDataType::kUInt64:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, uint64_t>(client, column,
                                                                 vertices);
    break;
  case ContextDataType::kFloat:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, float>(client, column,
                                                              vertices);
    break;
  case ContextDataType::kDouble:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, double>(client, column,
                                                               vertices);
    break;
  case ContextDataType::kString:
    builder = column_to_vy_tensor_builder_impl<FRAG_T, std::string>(
        client, column, vertices);
    break;
  default:
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                    "Unsupported datatype");
  }
  return builder;
}

template <typename FRAG_T, typename DATA_T>
bl::result<vineyard::ObjectID> column_to_vy_tensor_impl(
    vineyard::Client& client, const std::shared_ptr<IColumn>& column,
    const std::vector<typename FRAG_T::vertex_t>& vertices) {
  auto tensor_builder = column_to_vy_tensor_builder_impl<FRAG_T, DATA_T>(
      client, column, vertices);
  auto tensor = tensor_builder->Seal(client);
  VY_OK_OR_RAISE(tensor->Persist(client));
  return tensor->id();
}

template <typename FRAG_T>
bl::result<vineyard::ObjectID> column_to_vy_tensor(
    vineyard::Client& client, const std::shared_ptr<IColumn>& column,
    const std::vector<typename FRAG_T::vertex_t>& vertices) {
  switch (column->type()) {
  case ContextDataType::kInt32:
    return column_to_vy_tensor_impl<FRAG_T, int32_t>(client, column, vertices);
  case ContextDataType::kBool:
    return column_to_vy_tensor_impl<FRAG_T, bool>(client, column, vertices);
  case ContextDataType::kInt64:
    return column_to_vy_tensor_impl<FRAG_T, int64_t>(client, column, vertices);
  case ContextDataType::kUInt32:
    return column_to_vy_tensor_impl<FRAG_T, uint32_t>(client, column, vertices);
  case ContextDataType::kUInt64:
    return column_to_vy_tensor_impl<FRAG_T, uint64_t>(client, column, vertices);
  case ContextDataType::kFloat:
    return column_to_vy_tensor_impl<FRAG_T, float>(client, column, vertices);
  case ContextDataType::kDouble:
    return column_to_vy_tensor_impl<FRAG_T, double>(client, column, vertices);
  case ContextDataType::kString:
    return column_to_vy_tensor_impl<FRAG_T, std::string>(client, column,
                                                         vertices);
  default:
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                    "Unsupported datatype");
  }
}

template <typename FRAG_T, typename DATA_T>
void serialize_context_property_impl(
    grape::InArchive& arc, const std::vector<typename FRAG_T::vertex_t>& range,
    const std::shared_ptr<IColumn>& base_column) {
  auto column = std::dynamic_pointer_cast<Column<FRAG_T, DATA_T>>(base_column);

  for (auto& v : range) {
    arc << column->at(v);
  }
}

template <typename FRAG_T>
bl::result<void> serialize_context_property(
    grape::InArchive& arc, const std::vector<typename FRAG_T::vertex_t>& range,
    std::shared_ptr<IColumn>& column) {
  switch (column->type()) {
  case ContextDataType::kInt32: {
    serialize_context_property_impl<FRAG_T, int32_t>(arc, range, column);
    break;
  }
  case ContextDataType::kUInt32: {
    serialize_context_property_impl<FRAG_T, uint32_t>(arc, range, column);
    break;
  }
  case ContextDataType::kInt64: {
    serialize_context_property_impl<FRAG_T, int64_t>(arc, range, column);
    break;
  }
  case ContextDataType::kUInt64: {
    serialize_context_property_impl<FRAG_T, uint64_t>(arc, range, column);
    break;
  }
  case ContextDataType::kFloat: {
    serialize_context_property_impl<FRAG_T, float>(arc, range, column);
    break;
  }
  case ContextDataType::kDouble: {
    serialize_context_property_impl<FRAG_T, double>(arc, range, column);
    break;
  }
  case ContextDataType::kString: {
    serialize_context_property_impl<FRAG_T, std::string>(arc, range, column);
    break;
  }
  case ContextDataType::kBool: {
    serialize_context_property_impl<FRAG_T, bool>(arc, range, column);
    break;
  }
  default:
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "column data type not supported...");
  }
  return {};
}

/**
 * @brief A transform utility for the kinds of fragments. This
 * utility provides a bunch of methods to serialize/transform context data and
 * the data in the fragment.
 * @tparam FRAG_T Labeled fragment class
 */
template <typename FRAG_T, typename Enable = void>
class TransformUtils {};

/**
 * @brief A transform utility for the labeled fragment, like ArrowFragment. This
 * utility provides a bunch of methods to serialize/transform context data and
 * the data in the fragment.
 * @tparam FRAG_T Labeled fragment class
 */
template <typename FRAG_T>
class TransformUtils<FRAG_T,
                     typename std::enable_if<
                         vineyard::is_property_fragment<FRAG_T>::value>::type> {
  using oid_t = typename FRAG_T::oid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using label_id_t = typename FRAG_T::label_id_t;
  using prop_id_t = typename FRAG_T::prop_id_t;

 public:
  explicit TransformUtils(const grape::CommSpec& comm_spec, const FRAG_T& frag)
      : comm_spec_(comm_spec), frag_(frag) {}

  bl::result<int> GetOidTypeId() { return vineyard::TypeToInt<oid_t>::value; }

  std::vector<vertex_t> SelectVertices(
      label_id_t label_id, const std::pair<std::string, std::string>& range) {
    auto iv = frag_.InnerVertices(label_id);

    return select_vertices_impl(frag_, iv, range);
  }

  void SerializeVertexId(const std::vector<vertex_t>& range,
                         grape::InArchive& arc) {
    for (auto& v : range) {
      arc << frag_.GetId(v);
    }
  }

  bl::result<void> SerializeVertexLabelId(const std::vector<vertex_t>& range,
                                          grape::InArchive& arc) {
    for (auto& v : range) {
      arc << frag_.vertex_label(v);
    }
    return {};
  }

  bl::result<std::shared_ptr<arrow::Array>> VertexIdToArrowArray(
      label_id_t label_id) {
    typename vineyard::ConvertToArrowType<oid_t>::BuilderType builder;
    auto iv = frag_.InnerVertices(label_id);
    for (auto& v : iv) {
      ARROW_OK_OR_RAISE(builder.Append(frag_.GetId(v)));
    }
    std::shared_ptr<typename vineyard::ConvertToArrowType<oid_t>::ArrayType>
        col;
    ARROW_OK_OR_RAISE(builder.Finish(&col));
    return std::dynamic_pointer_cast<arrow::Array>(col);
  }

  template <typename oid_t,
            typename std::enable_if<!std::is_same<oid_t, std::string>::value,
                                    void>::type* = nullptr>
  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexIdToVYTensorBuilder(vineyard::Client& client,
                            const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    std::vector<int64_t> part_idx{comm_spec_.fid()};
    auto tensor_builder = std::make_shared<vineyard::TensorBuilder<oid_t>>(
        client, shape, part_idx);

    for (size_t i = 0; i < vertices.size(); i++) {
      tensor_builder->data()[i] = frag_.GetId(vertices[i]);
    }
    return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(tensor_builder);
  }

  template <typename oid_t,
            typename std::enable_if<std::is_same<oid_t, std::string>::value,
                                    void>::type* = nullptr>
  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexIdToVYTensorBuilder(vineyard::Client& client,
                            const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    std::vector<int64_t> part_idx{comm_spec_.fid()};
    auto tensor_builder = std::make_shared<vineyard::TensorBuilder<oid_t>>(
        client, shape, part_idx);

    for (size_t i = 0; i < vertices.size(); i++) {
      tensor_builder->Append(frag_.GetInternalId(vertices[i]));
    }
    return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(tensor_builder);
  }

  bl::result<vineyard::ObjectID> VertexIdToVYTensor(
      vineyard::Client& client,
      const std::vector<typename FRAG_T::vertex_t>& vertices) {
    BOOST_LEAF_AUTO(base_builder,
                    VertexIdToVYTensorBuilder<oid_t>(client, vertices));
    auto builder = std::dynamic_pointer_cast<
        vineyard::TensorBuilder<typename FRAG_T::oid_t>>(base_builder);
    auto tensor = builder->Seal(client);
    VY_OK_OR_RAISE(tensor->Persist(client));
    return tensor->id();
  }

  bl::result<void> SerializeVertexProperty(const std::vector<vertex_t>& range,
                                           label_id_t label_id,
                                           prop_id_t prop_id,
                                           grape::InArchive& arc) {
    auto type = frag_.vertex_property_type(label_id, prop_id);

    if (type->Equals(arrow::int32())) {
      serializeVertexPropertyImpl<int32_t>(arc, range, prop_id);
    } else if (type->Equals(arrow::int64())) {
      serializeVertexPropertyImpl<int64_t>(arc, range, prop_id);
    } else if (type->Equals(arrow::uint32())) {
      serializeVertexPropertyImpl<uint32_t>(arc, range, prop_id);
    } else if (type->Equals(arrow::uint64())) {
      serializeVertexPropertyImpl<int64_t>(arc, range, prop_id);
    } else if (type->Equals(arrow::float32())) {
      serializeVertexPropertyImpl<float>(arc, range, prop_id);
    } else if (type->Equals(arrow::float64())) {
      serializeVertexPropertyImpl<double>(arc, range, prop_id);
    } else if (type->Equals(arrow::large_utf8())) {
      serializeVertexPropertyImpl<std::string>(arc, range, prop_id);
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                      "property type not support - " + type->ToString());
    }

    return {};
  }

  std::shared_ptr<arrow::Array> VertexPropertyToArrowArray(label_id_t label_id,
                                                           prop_id_t prop_id) {
    auto table = frag_.vertex_data_table(label_id);
    return table->column(prop_id)->chunk(0);
  }

  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexPropertyToVYTensorBuilder(vineyard::Client& client, label_id_t label_id,
                                  prop_id_t prop_id,
                                  const std::vector<vertex_t>& vertices) {
    auto type = frag_.vertex_property_type(label_id, prop_id);

    if (type->Equals(arrow::int32())) {
      return vertex_property_to_vy_tensor_builder_impl<int32_t>(client, prop_id,
                                                                vertices);
    } else if (type->Equals(arrow::int64())) {
      return vertex_property_to_vy_tensor_builder_impl<int64_t>(client, prop_id,
                                                                vertices);
    } else if (type->Equals(arrow::uint32())) {
      return vertex_property_to_vy_tensor_builder_impl<uint32_t>(
          client, prop_id, vertices);
    } else if (type->Equals(arrow::uint64())) {
      return vertex_property_to_vy_tensor_builder_impl<uint64_t>(
          client, prop_id, vertices);
    } else if (type->Equals(arrow::float32())) {
      return vertex_property_to_vy_tensor_builder_impl<float>(client, prop_id,
                                                              vertices);
    } else if (type->Equals(arrow::float64())) {
      return vertex_property_to_vy_tensor_builder_impl<double>(client, prop_id,
                                                               vertices);
    } else if (type->Equals(arrow::utf8()) ||
               type->Equals(arrow::large_utf8())) {
      return vertex_property_to_vy_tensor_builder_impl<std::string>(
          client, prop_id, vertices);
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                      "property type not support - " + type->ToString());
    }
  }

  bl::result<vineyard::ObjectID> VertexPropertyToVYTensor(
      vineyard::Client& client, typename FRAG_T::label_id_t label_id,
      typename FRAG_T::prop_id_t prop_id,
      const std::vector<typename FRAG_T::vertex_t>& vertices) {
    auto type = frag_.vertex_property_type(label_id, prop_id);

    if (type->Equals(arrow::int32())) {
      return vertex_property_to_vy_tensor_impl<int32_t>(client, prop_id,
                                                        vertices);
    } else if (type->Equals(arrow::int64())) {
      return vertex_property_to_vy_tensor_impl<int64_t>(client, prop_id,
                                                        vertices);
    } else if (type->Equals(arrow::uint32())) {
      return vertex_property_to_vy_tensor_impl<uint32_t>(client, prop_id,
                                                         vertices);
    } else if (type->Equals(arrow::uint64())) {
      return vertex_property_to_vy_tensor_impl<uint64_t>(client, prop_id,
                                                         vertices);
    } else if (type->Equals(arrow::float32())) {
      return vertex_property_to_vy_tensor_impl<float>(client, prop_id,
                                                      vertices);
    } else if (type->Equals(arrow::float64())) {
      return vertex_property_to_vy_tensor_impl<double>(client, prop_id,
                                                       vertices);
    } else if (type->Equals(arrow::utf8()) ||
               type->Equals(arrow::large_utf8())) {
      return vertex_property_to_vy_tensor_impl<std::string>(client, prop_id,
                                                            vertices);
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                      "property type not support - " + type->ToString());
    }
  }

 private:
  template <typename DATA_T>
  void serializeVertexPropertyImpl(
      grape::InArchive& arc,
      const std::vector<typename FRAG_T::vertex_t>& range,
      typename FRAG_T::prop_id_t prop_id) {
    for (auto& v : range) {
      arc << frag_.template GetData<DATA_T>(v, prop_id);
    }
  }

  template <typename DATA_T,
            typename std::enable_if<!std::is_same<DATA_T, std::string>::value,
                                    void>::type* = nullptr>
  std::shared_ptr<vineyard::ITensorBuilder>
  vertex_property_to_vy_tensor_builder_impl(
      vineyard::Client& client, typename FRAG_T::prop_id_t prop_id,
      const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    auto tensor_builder =
        std::make_shared<vineyard::TensorBuilder<DATA_T>>(client, shape);

    for (size_t i = 0; i < vertices.size(); i++) {
      tensor_builder->data()[i] =
          frag_.template GetData<DATA_T>(vertices[i], prop_id);
    }
    return tensor_builder;
  }

  template <typename DATA_T,
            typename std::enable_if<std::is_same<DATA_T, std::string>::value,
                                    void>::type* = nullptr>
  std::shared_ptr<vineyard::ITensorBuilder>
  vertex_property_to_vy_tensor_builder_impl(
      vineyard::Client& client, typename FRAG_T::prop_id_t prop_id,
      const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    auto tensor_builder =
        std::make_shared<vineyard::TensorBuilder<DATA_T>>(client, shape);

    for (size_t i = 0; i < vertices.size(); i++) {
      const std::string data =
          frag_.template GetData<DATA_T>(vertices[i], prop_id);
      tensor_builder->Append(
          reinterpret_cast<
              typename vineyard::TensorBuilder<DATA_T>::value_const_pointer_t>(
              data.data()),
          data.size());
    }
    return tensor_builder;
  }

  template <typename DATA_T>
  bl::result<vineyard::ObjectID> vertex_property_to_vy_tensor_impl(
      vineyard::Client& client, typename FRAG_T::prop_id_t prop_id,
      const std::vector<typename FRAG_T::vertex_t>& vertices) {
    auto tensor_builder =
        std::dynamic_pointer_cast<vineyard::TensorBuilder<DATA_T>>(
            vertex_property_to_vy_tensor_builder_impl<DATA_T>(client, prop_id,
                                                              vertices));
    auto tensor = tensor_builder->Seal(client);
    VY_OK_OR_RAISE(tensor->Persist(client));
    return tensor->id();
  }

  grape::CommSpec comm_spec_;
  const FRAG_T& frag_;
};

/**
 * @brief A transform utils for the non-labeled fragment but the type of oid is
 * not dynamic, like ArrowProjectedFragment or ArrowFlattenedFragment. This
 * utility provides a bunch of methods to serialize/transform context data and
 * the data in the fragment.
 * @tparam FRAG_T Non-labeled fragment class
 */
template <typename FRAG_T>
class TransformUtils<
    FRAG_T,
    typename std::enable_if<!vineyard::is_property_fragment<FRAG_T>::value &&
                            !oid_is_dynamic<FRAG_T>::value>::type> {
  using oid_t = typename FRAG_T::oid_t;
  using vertex_t = typename FRAG_T::vertex_t;

 public:
  explicit TransformUtils(const grape::CommSpec& comm_spec, const FRAG_T& frag)
      : comm_spec_(comm_spec), frag_(frag) {}

  bl::result<int> GetOidTypeId() { return vineyard::TypeToInt<oid_t>::value; }

  std::vector<vertex_t> SelectVertices(
      const std::pair<std::string, std::string>& range) {
    auto iv = frag_.InnerVertices();

    return select_vertices_impl(frag_, iv, range);
  }

  void SerializeVertexId(const std::vector<vertex_t>& range,
                         grape::InArchive& arc) {
    for (auto& v : range) {
      arc << frag_.GetId(v);
    }
  }

  bl::result<void> SerializeVertexLabelId(const std::vector<vertex_t>& range,
                                          grape::InArchive& arc) {
    for (auto& v : range) {
      int label_id = 0;
      vineyard::static_if<is_flattened_fragment<FRAG_T>::value>(
          [&](auto& frag, auto& v, auto& label_id) {
            label_id = frag.vertex_label(v);
          })(frag_, v, label_id);
      arc << label_id;
    }
    return {};
  }

  bl::result<std::shared_ptr<arrow::Array>> VertexIdToArrowArray() {
    typename vineyard::ConvertToArrowType<oid_t>::BuilderType builder;
    auto inner_vertices = frag_.InnerVertices();

    for (auto& v : inner_vertices) {
      ARROW_OK_OR_RAISE(builder.Append(frag_.GetId(v)));
    }
    std::shared_ptr<typename vineyard::ConvertToArrowType<oid_t>::ArrayType>
        ret;
    ARROW_OK_OR_RAISE(builder.Finish(&ret));
    return std::dynamic_pointer_cast<arrow::Array>(ret);
  }

  template <typename oid_t,
            typename std::enable_if<!std::is_same<std::string, oid_t>::value,
                                    void>::type* = nullptr>
  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexIdToVYTensorBuilder(vineyard::Client& client,
                            const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    std::vector<int64_t> part_idx{comm_spec_.fid()};
    auto tensor_builder = std::make_shared<vineyard::TensorBuilder<oid_t>>(
        client, shape, part_idx);

    for (size_t i = 0; i < vertices.size(); i++) {
      tensor_builder->data()[i] = frag_.GetId(vertices[i]);
    }
    return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(tensor_builder);
  }

  template <typename oid_t,
            typename std::enable_if<std::is_same<std::string, oid_t>::value,
                                    void>::type* = nullptr>
  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexIdToVYTensorBuilder(vineyard::Client& client,
                            const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    std::vector<int64_t> part_idx{comm_spec_.fid()};
    auto tensor_builder = std::make_shared<vineyard::TensorBuilder<oid_t>>(
        client, shape, part_idx);

    for (size_t i = 0; i < vertices.size(); i++) {
      tensor_builder->Append(frag_.GetInternalId(vertices[i]));
    }
    return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(tensor_builder);
  }

  bl::result<vineyard::ObjectID> VertexIdToVYTensor(
      vineyard::Client& client,
      const std::vector<typename FRAG_T::vertex_t>& vertices) {
    BOOST_LEAF_AUTO(base_builder,
                    VertexIdToVYTensorBuilder<oid_t>(client, vertices));
    auto builder = std::dynamic_pointer_cast<
        vineyard::TensorBuilder<typename FRAG_T::oid_t>>(base_builder);
    auto tensor = builder->Seal(client);
    VY_OK_OR_RAISE(tensor->Persist(client));
    return tensor->id();
  }

  void SerializeVertexData(const std::vector<vertex_t>& range,
                           grape::InArchive& arc) {
    for (auto& v : range) {
      arc << frag_.GetData(v);
    }
  }

  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexDataToVYTensorBuilder(vineyard::Client& client,
                              const std::vector<vertex_t>& vertices) {
    auto f = [this, &vertices](size_t i) { return frag_.GetData(vertices[i]); };

    return build_vy_tensor_builder(client, vertices.size(), f,
                                   comm_spec_.fid());
  }

  bl::result<vineyard::ObjectID> VertexDataToVYTensor(
      vineyard::Client& client, const std::vector<vertex_t>& vertices) {
    auto f = [this, &vertices](size_t i) { return frag_.GetData(vertices[i]); };

    return build_vy_tensor(client, vertices.size(), f, comm_spec_.fid());
  }

  bl::result<std::shared_ptr<arrow::Array>> VertexDataToArrowArray() {
    return vertex_data_to_arrow_array_impl(frag_);
  }

 private:
  grape::CommSpec comm_spec_;
  const FRAG_T& frag_;
};

#ifdef NETWORKX
/**
 * A transform utils for the non-labeled fragment but the type of oid is
 * dynamic, like DynamicFragment, DynamicProjectedFragment. This utility
 * provides a bunch of methods to serialize/transform context data and the data
 * in the fragment.
 * @tparam FRAG_T Non-labeled fragment class, but with dynamic::Value oid
 */
template <typename FRAG_T>
class TransformUtils<
    FRAG_T,
    typename std::enable_if<!vineyard::is_property_fragment<FRAG_T>::value &&
                            oid_is_dynamic<FRAG_T>::value>::type> {
  using vertex_t = typename FRAG_T::vertex_t;

 public:
  explicit TransformUtils(const grape::CommSpec& comm_spec, const FRAG_T& frag)
      : comm_spec_(comm_spec), frag_(frag) {}

  /**
   * N.B. This function must be invoked on every worker
   * @param comm_spec
   * @return
   */
  bl::result<int> GetOidTypeId() {
    dynamic::Type oid_type = dynamic::Type::kNullType;
    auto vm_ptr = frag_.GetVertexMap();
    if (frag_.GetInnerVerticesNum() > 0) {
      for (auto& v : frag_.InnerVertices()) {
        if (frag_.IsAliveInnerVertex(v)) {
          dynamic::Value oid;
          vm_ptr->GetOid(frag_.fid(), v.GetValue(), oid);
          oid_type = dynamic::GetType(oid);
          break;
        }
      }
    }

    grape::Communicator comm;
    comm.InitCommunicator(comm_spec_.comm());
    std::vector<dynamic::Type> gather_type;
    comm.AllGather(oid_type, gather_type);
    for (auto& t : gather_type) {
      if (oid_type != t) {
        std::stringstream ss;
        ss << "Exist different oid type between fragments";
        RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError, ss.str());
      }
    }

    if (oid_type == dynamic::Type::kInt32Type) {
      return vineyard::TypeToInt<int32_t>::value;
    } else if (oid_type == dynamic::Type::kInt64Type) {
      return vineyard::TypeToInt<int64_t>::value;
    } else if (oid_type == dynamic::Type::kStringType) {
      return vineyard::TypeToInt<std::string>::value;
    } else if (oid_type == dynamic::Type::kNullType) {
      return vineyard::TypeToInt<void>::value;
    }
    return -1;
  }

  std::vector<vertex_t> SelectVertices(
      const std::pair<std::string, std::string>& range) {
    auto iv = frag_.InnerVertices();

    return select_vertices_impl(frag_, iv, range);
  }

  void SerializeVertexId(const std::vector<vertex_t>& range,
                         grape::InArchive& arc) {
    for (auto& v : range) {
      arc << frag_.GetId(v);
    }
  }

  bl::result<void> SerializeVertexLabelId(const std::vector<vertex_t>& range,
                                          grape::InArchive& arc) {
    // N.B. one should not select label_id with DynamicFragment or
    // DynamicProjected
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kUnsupportedOperationError,
        "vlabel_id selector only support on ArrowFlattenedFragment.");
  }

  bl::result<std::shared_ptr<arrow::Array>> VertexIdToArrowArray() {
    auto inner_vertices = frag_.InnerVertices();

    BOOST_LEAF_AUTO(oid_type, GetOidTypeId());

    if (oid_type == vineyard::TypeToInt<int32_t>::value) {
      typename vineyard::ConvertToArrowType<int32_t>::BuilderType builder;
      for (auto& v : inner_vertices) {
        ARROW_OK_OR_RAISE(builder.Append(frag_.GetId(v).GetInt()));
      }
      std::shared_ptr<typename vineyard::ConvertToArrowType<int32_t>::ArrayType>
          ret;
      ARROW_OK_OR_RAISE(builder.Finish(&ret));
      return std::dynamic_pointer_cast<arrow::Array>(ret);
    } else if (oid_type == vineyard::TypeToInt<int64_t>::value) {
      typename vineyard::ConvertToArrowType<int64_t>::BuilderType builder;
      for (auto& v : inner_vertices) {
        ARROW_OK_OR_RAISE(builder.Append(frag_.GetId(v).GetInt64()));
      }
      std::shared_ptr<typename vineyard::ConvertToArrowType<int64_t>::ArrayType>
          ret;
      ARROW_OK_OR_RAISE(builder.Finish(&ret));
      return std::dynamic_pointer_cast<arrow::Array>(ret);
    } else if (oid_type == vineyard::TypeToInt<std::string>::value) {
      typename vineyard::ConvertToArrowType<std::string>::BuilderType builder;
      for (auto v : inner_vertices) {
        ARROW_OK_OR_RAISE(builder.Append(frag_.GetId(v).GetString()));
      }
      std::shared_ptr<
          typename vineyard::ConvertToArrowType<std::string>::ArrayType>
          ret;
      ARROW_OK_OR_RAISE(builder.Finish(&ret));
      return std::dynamic_pointer_cast<arrow::Array>(ret);
    }
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                    "Unsupported oid type");
  }

  template <typename oid_t>  // not used, to make the template specializations
                             // has the same API
  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexIdToVYTensorBuilder(vineyard::Client& client,
                            const std::vector<vertex_t>& vertices) {
    std::vector<int64_t> shape{static_cast<int64_t>(vertices.size())};
    std::vector<int64_t> part_idx{comm_spec_.fid()};

    BOOST_LEAF_AUTO(oid_type, GetOidTypeId());

    if (oid_type == vineyard::TypeToInt<int32_t>::value) {
      auto tensor_builder = std::make_shared<vineyard::TensorBuilder<int32_t>>(
          client, shape, part_idx);
      for (size_t i = 0; i < vertices.size(); i++) {
        tensor_builder->data()[i] = frag_.GetId(vertices[i]).GetInt();
      }
      return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(
          tensor_builder);
    } else if (oid_type == vineyard::TypeToInt<int64_t>::value) {
      auto tensor_builder = std::make_shared<vineyard::TensorBuilder<int64_t>>(
          client, shape, part_idx);
      for (size_t i = 0; i < vertices.size(); i++) {
        tensor_builder->data()[i] = frag_.GetId(vertices[i]).GetInt64();
      }
      return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(
          tensor_builder);
    } else if (oid_type == vineyard::TypeToInt<std::string>::value) {
      auto tensor_builder =
          std::make_shared<vineyard::TensorBuilder<std::string>>(client, shape,
                                                                 part_idx);
      for (size_t i = 0; i < vertices.size(); i++) {
        auto const value = frag_.GetId(vertices[i]);
        tensor_builder->Append(
            reinterpret_cast<typename vineyard::TensorBuilder<
                std::string>::value_const_pointer_t>(value.GetString()),
            value.GetStringLength());
      }
      return std::dynamic_pointer_cast<vineyard::ITensorBuilder>(
          tensor_builder);
    }

    RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                    "Unsupported oid type");
  }

  bl::result<vineyard::ObjectID> VertexIdToVYTensor(
      vineyard::Client& client, const std::vector<vertex_t>& vertices) {
    BOOST_LEAF_AUTO(base_builder, VertexIdToVYTensorBuilder<void /* dummy */>(
                                      client, vertices));
    BOOST_LEAF_AUTO(oid_type, GetOidTypeId());

    if (oid_type == vineyard::TypeToInt<int32_t>::value) {
      auto builder =
          std::dynamic_pointer_cast<vineyard::TensorBuilder<int32_t>>(
              base_builder);
      auto tensor = builder->Seal(client);
      VY_OK_OR_RAISE(tensor->Persist(client));
      return tensor->id();
    } else if (oid_type == vineyard::TypeToInt<int64_t>::value) {
      auto builder =
          std::dynamic_pointer_cast<vineyard::TensorBuilder<int64_t>>(
              base_builder);
      auto tensor = builder->Seal(client);
      VY_OK_OR_RAISE(tensor->Persist(client));
      return tensor->id();
    } else if (oid_type == vineyard::TypeToInt<std::string>::value) {
      auto builder =
          std::dynamic_pointer_cast<vineyard::TensorBuilder<std::string>>(
              base_builder);
      auto tensor = builder->Seal(client);
      VY_OK_OR_RAISE(tensor->Persist(client));
      return tensor->id();
    }

    RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                    "Unsupported oid type");
  }

  void SerializeVertexData(const std::vector<vertex_t>& range,
                           grape::InArchive& arc) {
    for (auto& v : range) {
      arc << frag_.GetData(v);
    }
  }

  bl::result<std::shared_ptr<vineyard::ITensorBuilder>>
  VertexDataToVYTensorBuilder(vineyard::Client& client,
                              const std::vector<vertex_t>& vertices) {
    auto f = [this, &vertices](size_t i) { return frag_.GetData(vertices[i]); };

    return build_vy_tensor_builder(client, vertices.size(), f,
                                   comm_spec_.fid());
  }

  bl::result<vineyard::ObjectID> VertexDataToVYTensor(
      vineyard::Client& client, const std::vector<vertex_t>& vertices) {
    auto f = [this, &vertices](size_t i) { return frag_.GetData(vertices[i]); };

    return build_vy_tensor(client, vertices.size(), f, comm_spec_.fid());
  }

  bl::result<std::shared_ptr<arrow::Array>> VertexDataToArrowArray() {
    return vertex_data_to_arrow_array_impl(frag_);
  }

 private:
  grape::CommSpec comm_spec_;
  const FRAG_T& frag_;
};
#endif
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_UTILS_TRANSFORM_UTILS_H_
