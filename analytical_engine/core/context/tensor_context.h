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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_TENSOR_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_TENSOR_CONTEXT_H_

#include <glog/logging.h>
#include <mpi.h>

#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "grape/app/context_base.h"
#include "grape/serialization/in_archive.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/dataframe.h"
#include "vineyard/basic/ds/tensor.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/uuid.h"

#ifdef NETWORKX
#include "core/object/dynamic.h"
#endif
#include "core/config.h"
#include "core/context/context_protocols.h"
#include "core/context/i_context.h"
#include "core/context/selector.h"
#include "core/context/tensor_dataframe_builder.h"
#include "core/error.h"
#include "core/utils/mpi_utils.h"
#include "core/utils/transform_utils.h"
#include "core/utils/trivial_tensor.h"

#define CONTEXT_TYPE_TENSOR "tensor"

namespace bl = boost::leaf;

namespace grape {

template <typename T>
inline InArchive& operator<<(InArchive& in_archive,
                             const gs::trivial_tensor_t<T>& tensor) {
  size_t size = tensor.size();
  if (size > 0) {
    in_archive.AddBytes(tensor.data(), size * sizeof(T));
  }
  return in_archive;
}

inline InArchive& operator<<(InArchive& in_archive,
                             const gs::trivial_tensor_t<std::string>& tensor) {
  size_t size = tensor.size();
  if (size > 0) {
    for (size_t i = 0; i < tensor.size(); ++i) {
      in_archive << tensor.data()->GetView(i);
    }
  }
  return in_archive;
}

#ifdef NETWORKX
inline InArchive& operator<<(
    InArchive& in_archive,
    const gs::trivial_tensor_t<gs::dynamic::Value>& tensor) {
  size_t size = tensor.size();
  if (size > 0) {
    auto type = gs::dynamic::GetType(tensor.data()[0]);
    CHECK(type == gs::dynamic::Type::kInt32Type ||
          type == gs::dynamic::Type::kInt64Type ||
          type == gs::dynamic::Type::kDoubleType ||
          type == gs::dynamic::Type::kStringType);
    for (size_t i = 0; i < tensor.size(); i++) {
      in_archive << tensor.data()[i];
    }
  }
  return in_archive;
}
#endif
}  // namespace grape

namespace gs {
class IFragmentWrapper;

template <typename T>
static bl::result<size_t> get_n_dim(const grape::CommSpec& comm_spec,
                                    const trivial_tensor_t<T>& tensor) {
  auto shape = tensor.shape();
  auto n_dim = shape.size();
  std::vector<size_t> n_dims;

  vineyard::GlobalAllGatherv<size_t>(n_dim, n_dims, comm_spec);

  // find out first n-dim of non empty shape
  n_dim = 0;
  for (auto e : n_dims) {
    if (e != 0) {
      n_dim = e;
      break;
    }
  }
  if (n_dim == 0) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Every tensor is 0-dim.");
  }

  for (auto e : n_dims) {
    if (e != 0 && e != n_dim) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,
                      "Dim count is not consistent.");
    }
  }
  return n_dim;
}

template <typename T>
static bl::result<std::vector<size_t>> get_non_empty_shape(
    const grape::CommSpec& comm_spec, const trivial_tensor_t<T>& tensor,
    uint32_t axis) {
  BOOST_LEAF_AUTO(n_dim, get_n_dim<T>(comm_spec, tensor));
  auto shape = tensor.shape();
  std::vector<std::vector<std::size_t>> shapes;
  vineyard::GlobalAllGatherv<std::vector<std::size_t>>(shape, shapes,
                                                       comm_spec);

  std::vector<size_t> first_shape;
  // find out first non-empty shape
  for (auto& sp : shapes) {
    if (!sp.empty()) {
      first_shape = sp;
      break;
    }
  }

  if (first_shape.empty()) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Every tensor is 0-dim.");
  }

  // for every dim except the dim to concat
  for (uint32_t i = 0; i < n_dim; i++) {
    if (i != axis) {
      for (auto& sp : shapes) {
        if (!sp.empty() && sp[i] != first_shape[i]) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                          "Incompatible dimension of tensors");
        }
      }
    }
  }
  return first_shape;
}

template <typename T>
static bl::result<size_t> get_n_column(const grape::CommSpec& comm_spec,
                                       const trivial_tensor_t<T>& tensor) {
  auto shape = tensor.shape();
  if (!shape.empty() && shape.size() != 2) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "This is not a 2-dim tensor.");
  }

  auto n_col = shape.empty() ? 0 : shape[1];
  std::vector<size_t> n_cols;

  vineyard::GlobalAllGatherv<size_t>(n_col, n_cols, comm_spec);

  for (auto e : n_cols) {
    if (e != 0) {
      n_col = e;
      break;
    }
  }

  if (n_col == 0) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Every tensor is empty.");
  }

  for (auto e : n_cols) {
    if (e != 0 && e != n_col) {
      std::stringstream ss;
      ss << "Number of column is not same. ";
      ss << "The column number of first non-empty is " << n_col;
      ss << ". But this one is " << e;
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError, ss.str());
    }
  }
  return n_col;
}

/**
 * @brief TensorContext is designed for holding a bunch of computation results.
 * The TensorContext should be used if the number of elements are
 * not related to the number of the vertex.
 *
 * @tparam FRAG_T
 * @tparam DATA_T
 */
template <typename FRAG_T, typename DATA_T>
class TensorContext : public grape::ContextBase {
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using tensor_t = trivial_tensor_t<DATA_T>;

 public:
  using data_t = DATA_T;

  explicit TensorContext(const fragment_t& fragment) : fragment_(fragment) {}

  const fragment_t& fragment() { return fragment_; }

  void assign(const std::vector<data_t>& data,
              const std::vector<size_t>& shape) {
    size_t size = 1;
    for (size_t dim_size : shape) {
      size *= dim_size;
    }
    CHECK_EQ(data.size(), size);

    set_shape(shape);
    memcpy(tensor_.data(), data.data(), sizeof(data_t) * data.size());
  }

  void assign(const data_t& data) { tensor_.fill(data); }

  void set_shape(std::vector<std::size_t> shape) {
    CHECK(!shape.empty());
    tensor_.resize(shape);
  }

  std::vector<size_t> shape() const { return tensor_.shape(); }

  inline tensor_t& tensor() { return tensor_; }

 private:
  const fragment_t& fragment_;
  tensor_t tensor_;
};

template <typename FRAG_T>
class TensorContext<FRAG_T, std::string> : public grape::ContextBase {
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using tensor_t = trivial_tensor_t<std::string>;

 public:
  using data_t = std::string;

  explicit TensorContext(const fragment_t& fragment) : fragment_(fragment) {}

  const fragment_t& fragment() { return fragment_; }

  void assign(const std::vector<data_t>& data,
              const std::vector<size_t>& shape) {
    size_t size = 1;
    for (size_t dim_size : shape) {
      size *= dim_size;
    }
    CHECK_EQ(data.size(), size);

    set_shape(shape);
    arrow::LargeStringBuilder builder;
    CHECK_ARROW_ERROR(builder.AppendValues(data));
    CHECK_ARROW_ERROR(builder.Finish(&(tensor_.data())));
  }

  void assign(const data_t& data) { tensor_.fill(data); }

  void set_shape(std::vector<std::size_t> shape) {
    CHECK(!shape.empty());
    tensor_.resize(shape);
  }

  std::vector<size_t> shape() const { return tensor_.shape(); }

  inline tensor_t& tensor() { return tensor_; }

 private:
  const fragment_t& fragment_;
  tensor_t tensor_;
};

/**
 * @brief TensorContextWrapper is the wrapper class for TensorContext.
 *
 * @tparam FRAG_T
 * @tparam DATA_T
 */
template <typename FRAG_T, typename DATA_T, typename = void>
class TensorContextWrapper : public ITensorContextWrapper {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using context_t = TensorContext<FRAG_T, DATA_T>;
  using data_t = DATA_T;

 public:
  explicit TensorContextWrapper(const std::string& id,
                                std::shared_ptr<IFragmentWrapper> frag_wrapper,
                                std::shared_ptr<context_t> ctx)
      : ITensorContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(ctx)) {}

  std::string context_type() override { return CONTEXT_TYPE_TENSOR; }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, uint32_t axis) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto arc = std::make_unique<grape::InArchive>();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (axis >= n_dim) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Invalid axis " + std::to_string(axis) +
                          ", n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(first_shape, get_non_empty_shape(comm_spec, tensor, axis));

    int64_t local_num = shape.empty() ? 0 : shape[axis], total_num;

    if (comm_spec.fid() == 0) {
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(n_dim);  // shape size
      first_shape[axis] = total_num;        // the shape after combined
      for (auto dim_size : first_shape) {
        *arc << static_cast<int64_t>(dim_size);
      }
      *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);

      size_t total_size = first_shape.empty() ? 0 : 1;
      for (auto e : first_shape) {
        total_size *= e;
      }
      *arc << static_cast<int64_t>(total_size);
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    auto old_size = arc->GetSize();

    *arc << tensor;
    gather_archives(*arc, comm_spec, old_size);
    return arc;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto arc = std::make_unique<grape::InArchive>();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (n_dim != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "This is not a 2-dims tensor, n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(n_col, get_n_column<data_t>(comm_spec, tensor));

    int64_t n_row = shape.empty() ? 0 : shape[0];
    int64_t total_n_row;

    if (comm_spec.worker_id() == grape::kCoordinatorRank) {
      MPI_Reduce(&n_row, &total_n_row, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(n_col);
      *arc << static_cast<int64_t>(total_n_row);
    } else {
      MPI_Reduce(&n_row, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
      if (comm_spec.worker_id() == grape::kCoordinatorRank) {
        *arc << "Col " + std::to_string(col_idx);  // Column name
        *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);
      }

      // Python side requires columnar data structure
      auto old_size = arc->GetSize();

      for (auto row_idx = 0; row_idx < n_row; row_idx++) {
        auto idx = row_idx * n_col + col_idx;
        *arc << tensor.data()[idx];
      }
      gather_archives(*arc, comm_spec, old_size);
    }
    return arc;
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      uint32_t axis) override {
    auto& frag = ctx_->fragment();
    auto& tensor = ctx_->tensor();
    auto local_shape = ctx_->shape();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (axis >= n_dim) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Invalid axis " + std::to_string(axis) +
                          ", n-dim: " + std::to_string(n_dim));
    }

    size_t local_num = local_shape.empty() ? 0 : local_shape[axis], total_num;

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    BOOST_LEAF_AUTO(first_shape, get_non_empty_shape(comm_spec, tensor, axis));

    first_shape[axis] = total_num;  // the shape after combined

    if (local_shape.empty()) {
      local_shape.resize(n_dim, 0);
    }

    std::vector<int64_t> partition_index;

    for (size_t i = 0; i < n_dim; i++) {
      partition_index.push_back(frag.fid());
    }

    std::vector<int64_t> vy_tensor_shape;
    for (auto e : local_shape) {
      vy_tensor_shape.push_back(static_cast<int64_t>(e));
    }
    vineyard::TensorBuilder<data_t> tensor_builder(client, vy_tensor_shape,
                                                   partition_index);

    for (size_t offset = 0; offset < tensor.size(); offset++) {
      tensor_builder.data()[offset] = tensor.data()[offset];
    }

    auto vy_tensor = std::dynamic_pointer_cast<vineyard::Tensor<data_t>>(
        tensor_builder.Seal(client));
    VY_OK_OR_RAISE(vy_tensor->Persist(client));

    std::vector<int64_t> global_shape;
    std::vector<int64_t> global_partition_shape;

    for (auto e : first_shape) {
      global_shape.push_back(static_cast<int64_t>(e));
      global_partition_shape.push_back(frag.fnum());
    }

    MPIGlobalTensorBuilder builder(client, comm_spec);
    builder.set_shape(global_shape);
    builder.set_partition_shape(global_partition_shape);
    builder.AddChunk(vy_tensor->id());

    return builder.Seal(client)->id();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto& frag = ctx_->fragment();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (n_dim != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "This is not a 2-dims tensor, n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(n_col, get_n_column<data_t>(comm_spec, tensor));

    size_t n_row = shape.empty() ? 0 : shape[0];

    vineyard::DataFrameBuilder df_builder(client);

    df_builder.set_partition_index(frag.fid(), 0);
    df_builder.set_row_batch_index(frag.fid());

    for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
      std::vector<int64_t> shape{static_cast<int64_t>(n_row)};
      auto tensor_builder =
          std::make_shared<vineyard::TensorBuilder<DATA_T>>(client, shape);

      for (size_t row_idx = 0; row_idx < n_row; row_idx++) {
        auto idx = row_idx * n_col + col_idx;
        tensor_builder->data()[row_idx] = tensor.data()[idx];
      }
      df_builder.AddColumn("Col " + std::to_string(col_idx), tensor_builder);
    }

    auto df = df_builder.Seal(client);
    VY_OK_OR_RAISE(df->Persist(client));
    auto df_chunk_id = df->id();

    MPIGlobalDataFrameBuilder builder(client, comm_spec);
    builder.set_partition_shape(frag.fnum(), n_col);
    builder.AddChunk(df_chunk_id);

    auto vy_obj = builder.Seal(client);

    return vy_obj->id();
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    auto& frag = ctx_->fragment();
    auto& tensor = ctx_->tensor();
    std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>
        arrow_arrays;
    for (auto& pair : selectors) {
      auto& col_name = pair.first;
      auto& selector = pair.second;
      std::shared_ptr<arrow::Array> arr;
      switch (selector.type()) {
      case SelectorType::kResult: {
        typename vineyard::ConvertToArrowType<data_t>::BuilderType builder;
        std::shared_ptr<
            typename vineyard::ConvertToArrowType<data_t>::ArrayType>
            arr_ptr;
        for (size_t offset = 0; offset < tensor.size(); offset++) {
          ARROW_OK_OR_RAISE(builder.Append(tensor.data()[offset]));
        }
        CHECK_ARROW_ERROR(builder.Finish(&arr_ptr));
        arr = std::dynamic_pointer_cast<arrow::Array>(arr_ptr);
        break;
      }
      default:
        RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                        "Unsupported operation, available selector type: "
                        "result. selector: " +
                            selector.str());
      }
      arrow_arrays.emplace_back(col_name, arr);
    }
    return arrow_arrays;
  }

 private:
  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};

template <typename FRAG_T>
class TensorContextWrapper<FRAG_T, std::string> : public ITensorContextWrapper {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using context_t = TensorContext<FRAG_T, std::string>;
  using data_t = std::string;

 public:
  explicit TensorContextWrapper(const std::string& id,
                                std::shared_ptr<IFragmentWrapper> frag_wrapper,
                                std::shared_ptr<context_t> ctx)
      : ITensorContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(ctx)) {}

  std::string context_type() override { return CONTEXT_TYPE_TENSOR; }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, uint32_t axis) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto arc = std::make_unique<grape::InArchive>();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (axis >= n_dim) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Invalid axis " + std::to_string(axis) +
                          ", n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(first_shape, get_non_empty_shape(comm_spec, tensor, axis));

    int64_t local_num = shape.empty() ? 0 : shape[axis], total_num;

    if (comm_spec.fid() == 0) {
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(n_dim);  // shape size
      first_shape[axis] = total_num;        // the shape after combined
      for (auto dim_size : first_shape) {
        *arc << static_cast<int64_t>(dim_size);
      }
      *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);

      size_t total_size = first_shape.empty() ? 0 : 1;
      for (auto e : first_shape) {
        total_size *= e;
      }
      *arc << static_cast<int64_t>(total_size);
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    auto old_size = arc->GetSize();

    *arc << tensor;
    gather_archives(*arc, comm_spec, old_size);
    return arc;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto arc = std::make_unique<grape::InArchive>();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (n_dim != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "This is not a 2-dims tensor, n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(n_col, get_n_column<data_t>(comm_spec, tensor));

    int64_t n_row = shape.empty() ? 0 : shape[0];
    int64_t total_n_row;

    if (comm_spec.worker_id() == grape::kCoordinatorRank) {
      MPI_Reduce(&n_row, &total_n_row, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(n_col);
      *arc << static_cast<int64_t>(total_n_row);
    } else {
      MPI_Reduce(&n_row, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
      if (comm_spec.worker_id() == grape::kCoordinatorRank) {
        *arc << "Col " + std::to_string(col_idx);  // Column name
        *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);
      }

      // Python side requires columnar data structure
      auto old_size = arc->GetSize();

      for (auto row_idx = 0; row_idx < n_row; row_idx++) {
        auto idx = row_idx * n_col + col_idx;
        *arc << tensor.data()->GetView(idx);
      }
      gather_archives(*arc, comm_spec, old_size);
    }
    return arc;
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      uint32_t axis) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Not implemented ToVineyardTensor for string type");
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Not implemented ToVineyardDataframe for string type");
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    auto& frag = ctx_->fragment();
    auto& tensor = ctx_->tensor();
    std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>
        arrow_arrays;
    for (auto& pair : selectors) {
      auto& col_name = pair.first;
      auto& selector = pair.second;
      std::shared_ptr<arrow::Array> arr;
      switch (selector.type()) {
      case SelectorType::kResult: {
        arr = std::dynamic_pointer_cast<arrow::Array>(tensor.data());
        break;
      }
      default:
        RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                        "Unsupported operation, available selector type: "
                        "result. selector: " +
                            selector.str());
      }
      arrow_arrays.emplace_back(col_name, arr);
    }
    return arrow_arrays;
  }

 private:
  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};

#ifdef NETWORKX
/**
 * @brief This is the specialized TensorContextWrapper for dynamic::Value type
 * of oid
 * @tparam FRAG_T
 * @tparam DATA_T
 */
template <typename FRAG_T, typename DATA_T>
class TensorContextWrapper<
    FRAG_T, DATA_T,
    typename std::enable_if<std::is_same<DATA_T, dynamic::Value>::value>::type>
    : public ITensorContextWrapper {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using context_t = TensorContext<FRAG_T, DATA_T>;
  using data_t = DATA_T;

 public:
  explicit TensorContextWrapper(const std::string& id,
                                std::shared_ptr<IFragmentWrapper> frag_wrapper,
                                std::shared_ptr<context_t> ctx)
      : ITensorContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(ctx)) {}

  std::string context_type() override { return CONTEXT_TYPE_TENSOR; }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, uint32_t axis) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto arc = std::make_unique<grape::InArchive>();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (axis >= n_dim) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Invalid axis " + std::to_string(axis) +
                          ", n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(first_shape, get_non_empty_shape(comm_spec, tensor, axis));
    BOOST_LEAF_AUTO(data_type, get_dynamic_type(comm_spec, tensor));

    int64_t local_num = shape.empty() ? 0 : shape[axis], total_num;

    if (comm_spec.fid() == 0) {
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(n_dim);  // shape size
      first_shape[axis] = total_num;        // shape after combined
      for (auto dim_size : first_shape) {
        *arc << static_cast<int64_t>(dim_size);
      }
      if (data_type == dynamic::Type::kInt32Type) {
        *arc << static_cast<int>(vineyard::TypeToInt<int32_t>::value);
      } else if (data_type == dynamic::Type::kInt64Type) {
        *arc << static_cast<int>(vineyard::TypeToInt<int64_t>::value);
      } else if (data_type == dynamic::Type::kDoubleType) {
        *arc << static_cast<int>(vineyard::TypeToInt<double>::value);
      } else if (data_type == dynamic::Type::kNullType) {
        *arc << static_cast<int>(vineyard::TypeToInt<void>::value);
      } else if (data_type == dynamic::Type::kStringType) {
        *arc << static_cast<int>(vineyard::TypeToInt<std::string>::value);
      } else {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                        "Only support int64, double");
      }
      size_t total_size = first_shape.empty() ? 0 : 1;
      for (auto e : first_shape) {
        total_size *= e;
      }
      *arc << static_cast<int64_t>(total_size);
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    auto old_size = arc->GetSize();
    *arc << tensor;
    gather_archives(*arc, comm_spec, old_size);
    return arc;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto arc = std::make_unique<grape::InArchive>();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (n_dim != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "This is not a 2-dims tensor, n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(n_col, get_n_column<data_t>(comm_spec, tensor));

    int64_t n_row = shape.empty() ? 0 : shape[0];
    int64_t total_n_row;

    if (comm_spec.worker_id() == grape::kCoordinatorRank) {
      MPI_Reduce(&n_row, &total_n_row, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(n_col);
      *arc << static_cast<int64_t>(total_n_row);
    } else {
      MPI_Reduce(&n_row, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    BOOST_LEAF_AUTO(data_type, get_dynamic_type(comm_spec, tensor));

    if (data_type == dynamic::Type::kInt64Type) {
      for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
        if (comm_spec.worker_id() == grape::kCoordinatorRank) {
          *arc << "Col " + std::to_string(col_idx);  // Column name
          *arc << static_cast<int>(vineyard::TypeToInt<int64_t>::value);
        }

        auto old_size = arc->GetSize();

        for (size_t row_idx = 0; row_idx < n_row; row_idx++) {
          auto idx = row_idx * n_col + col_idx;
          *arc << tensor.data()[idx].GetInt64();
        }
        gather_archives(*arc, comm_spec, old_size);
      }
    } else if (data_type == dynamic::Type::kDoubleType) {
      for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
        if (comm_spec.worker_id() == grape::kCoordinatorRank) {
          *arc << "Col " + std::to_string(col_idx);
          *arc << static_cast<int>(vineyard::TypeToInt<double>::value);
        }

        auto old_size = arc->GetSize();

        for (size_t row_idx = 0; row_idx < n_row; row_idx++) {
          auto idx = row_idx * n_col + col_idx;

          *arc << tensor.data()[idx].GetDouble();
        }
        gather_archives(*arc, comm_spec, old_size);
      }
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                      "Only support int64 or double");
    }
    return arc;
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      uint32_t axis) override {
    auto& frag = ctx_->fragment();
    auto& tensor = ctx_->tensor();
    auto local_shape = ctx_->shape();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (axis >= n_dim) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Invalid axis " + std::to_string(axis) +
                          ", n-dim: " + std::to_string(n_dim));
    }

    size_t local_num = local_shape.empty() ? 0 : local_shape[axis], total_num;

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    BOOST_LEAF_AUTO(first_shape, get_non_empty_shape(comm_spec, tensor, axis));
    BOOST_LEAF_AUTO(data_type, get_dynamic_type(comm_spec, tensor));

    first_shape[axis] = total_num;  // the shape after combined

    if (local_shape.empty()) {
      local_shape.resize(n_dim, 0);
    }

    std::vector<int64_t> partition_index;

    for (size_t i = 0; i < n_dim; i++) {
      partition_index.push_back(frag.fid());
    }

    std::vector<int64_t> vy_tensor_shape;

    for (auto e : local_shape) {
      vy_tensor_shape.push_back(static_cast<int64_t>(e));
    }

    vineyard::ObjectID tensor_chunk_id;

    if (data_type == dynamic::Type::kInt64Type) {
      vineyard::TensorBuilder<int64_t> tensor_builder(client, vy_tensor_shape,
                                                      partition_index);

      for (size_t offset = 0; offset < tensor.size(); offset++) {
        tensor_builder.data()[offset] = tensor.data()[offset].GetInt64();
      }

      auto vy_tensor = std::dynamic_pointer_cast<vineyard::Tensor<int64_t>>(
          tensor_builder.Seal(client));
      VY_OK_OR_RAISE(vy_tensor->Persist(client));
      tensor_chunk_id = vy_tensor->id();
    } else if (data_type == dynamic::Type::kDoubleType) {
      vineyard::TensorBuilder<double> tensor_builder(client, vy_tensor_shape,
                                                     partition_index);

      for (size_t offset = 0; offset < tensor.size(); offset++) {
        tensor_builder.data()[offset] = tensor.data()[offset].GetDouble();
      }

      auto vy_tensor = std::dynamic_pointer_cast<vineyard::Tensor<std::string>>(
          tensor_builder.Seal(client));
      VY_OK_OR_RAISE(vy_tensor->Persist(client));
      tensor_chunk_id = vy_tensor->id();
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                      "Only support int64 or double");
    }

    std::vector<int64_t> global_shape;
    std::vector<int64_t> global_partition_shape;

    for (auto e : first_shape) {
      global_shape.push_back(static_cast<int64_t>(e));
      global_partition_shape.push_back(frag.fnum());
    }

    MPIGlobalTensorBuilder builder(client, comm_spec);
    builder.set_shape(global_shape);
    builder.set_partition_shape(global_partition_shape);
    builder.AddChunk(tensor_chunk_id);

    return builder.Seal(client)->id();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client) override {
    auto shape = ctx_->shape();
    auto& tensor = ctx_->tensor();
    auto& frag = ctx_->fragment();

    BOOST_LEAF_AUTO(n_dim, get_n_dim<data_t>(comm_spec, tensor));

    if (n_dim != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "This is not a 2-dims tensor, n-dim: " + std::to_string(n_dim));
    }

    BOOST_LEAF_AUTO(n_col, get_n_column<data_t>(comm_spec, tensor));

    size_t n_row = shape.empty() ? 0 : shape[0];

    vineyard::DataFrameBuilder df_builder(client);

    df_builder.set_partition_index(frag.fid(), 0);
    df_builder.set_row_batch_index(frag.fid());

    BOOST_LEAF_AUTO(data_type, get_dynamic_type(comm_spec, tensor));

    if (data_type == dynamic::Type::kInt64Type) {
      for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
        std::vector<int64_t> shape{static_cast<int64_t>(n_row)};
        auto tensor_builder =
            std::make_shared<vineyard::TensorBuilder<int64_t>>(client, shape);

        for (auto row_idx = 0; row_idx < n_row; row_idx++) {
          auto idx = row_idx * n_col + col_idx;
          tensor_builder->data()[row_idx] = tensor.data()[idx].GetInt64();
        }
        df_builder.AddColumn("Col " + std::to_string(col_idx), tensor_builder);
      }
    } else if (data_type == dynamic::Type::kDoubleType) {
      for (size_t col_idx = 0; col_idx < n_col; col_idx++) {
        std::vector<int64_t> shape{static_cast<int64_t>(n_row)};
        auto tensor_builder =
            std::make_shared<vineyard::TensorBuilder<double>>(client, shape);

        for (auto row_idx = 0; row_idx < n_row; row_idx++) {
          auto idx = row_idx * n_col + col_idx;
          tensor_builder->data()[row_idx] = tensor.data()[idx].GetDouble();
        }
        df_builder.AddColumn("Col " + std::to_string(col_idx), tensor_builder);
      }
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                      "Only support int64 or double");
    }

    auto df = df_builder.Seal(client);
    VY_OK_OR_RAISE(df->Persist(client));
    auto df_chunk_id = df->id();

    MPIGlobalDataFrameBuilder builder(client, comm_spec);
    builder.set_partition_shape(frag.fnum(), n_col);
    builder.AddChunk(df_chunk_id);

    return builder.Seal(client)->id();
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Not implemented ToArrowArrays for dynamic type");
  }

 private:
  bl::result<dynamic::Type> get_dynamic_type(
      const grape::CommSpec& comm_spec,
      const trivial_tensor_t<dynamic::Value>& tensor) {
    int type = tensor.size() == 0 ? dynamic::Type::kNullType
                                  : dynamic::GetType(tensor.data()[0]);
    std::vector<int> types;
    vineyard::GlobalAllGatherv<int>(type, types, comm_spec);

    type = dynamic::Type::kNullType;
    for (auto e : types) {
      if (e != dynamic::Type::kNullType) {
        type = e;
        break;
      }
    }

    for (auto e : types) {
      if (e != dynamic::Type::kNullType && e != type) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,
                        "The types of dynamic::Value is not same.");
      }
    }
    return static_cast<dynamic::Type>(type);
  }

  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};
#endif

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_TENSOR_CONTEXT_H_
