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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_VERTEX_PROPERTY_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_VERTEX_PROPERTY_CONTEXT_H_

#include <mpi.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "grape/app/context_base.h"
#include "grape/serialization/in_archive.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/dataframe.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/uuid.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/utils/context_protocols.h"

#include "core/context/column.h"
#include "core/context/context_protocols.h"
#include "core/context/i_context.h"
#include "core/context/selector.h"
#include "core/context/tensor_dataframe_builder.h"
#include "core/error.h"
#include "core/utils/mpi_utils.h"
#include "core/utils/transform_utils.h"

#define CONTEXT_TYPE_VERTEX_PROPERTY "vertex_property"

namespace bl = boost::leaf;

namespace arrow {
class Array;
}

namespace gs {
class IFragmentWrapper;

/**
 * @brief VertexPropertyContext can hold any number of columns. The context is
 * designed for labeled fragment - ArrowFragment. Compared with
 * LabeledVertexDataContext, the data type and column count can be determined at
 * runtime.
 *
 * @tparam FRAG_T The fragment class (non-labeled fragment only)
 */
template <typename FRAG_T>
class VertexPropertyContext : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using oid_t = typename fragment_t::oid_t;

  explicit VertexPropertyContext(const fragment_t& fragment)
      : fragment_(fragment) {}

  const fragment_t& fragment() { return fragment_; }

  int64_t add_column(const std::string& name, ContextDataType type) {
    if (properties_map_.find(name) != properties_map_.end()) {
      return -1;
    }
    auto column =
        CreateColumn<fragment_t>(name, fragment_.InnerVertices(), type);
    properties_map_.emplace(name, column);
    auto ret = static_cast<int64_t>(vertex_properties_.size());
    vertex_properties_.emplace_back(column);
    return ret;
  }

  std::shared_ptr<IColumn> get_column(int64_t index) {
    if (static_cast<size_t>(index) >= vertex_properties_.size()) {
      return nullptr;
    }
    return vertex_properties_[index];
  }

  std::shared_ptr<IColumn> get_column(const std::string& name) {
    auto iter = properties_map_.find(name);
    if (iter == properties_map_.end()) {
      return nullptr;
    }
    return iter->second;
  }

  template <typename DATA_T>
  std::shared_ptr<Column<fragment_t, DATA_T>> get_typed_column(int64_t index) {
    if (static_cast<size_t>(index) >= vertex_properties_.size()) {
      return nullptr;
    }
    auto ret = vertex_properties_[index];
    if (ret->type() != ContextTypeToEnum<DATA_T>::value) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Column<fragment_t, DATA_T>>(ret);
  }

  template <typename DATA_T>
  std::shared_ptr<Column<fragment_t, DATA_T>> get_typed_column(
      const std::string& name) {
    auto iter = properties_map_.find(name);
    if (iter == properties_map_.end()) {
      return nullptr;
    }
    auto ret = iter->second;
    if (ret->type() != ContextTypeToEnum<DATA_T>::value) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Column<fragment_t, DATA_T>>(ret);
  }

  std::vector<std::shared_ptr<IColumn>>& vertex_properties() {
    return vertex_properties_;
  }

  const std::map<std::string, std::shared_ptr<IColumn>>& properties_map() {
    return properties_map_;
  }

 private:
  const fragment_t& fragment_;
  std::vector<std::shared_ptr<IColumn>> vertex_properties_;
  std::map<std::string, std::shared_ptr<IColumn>> properties_map_;
};

/**
 * @brief VertexPropertyContextWrapper is the wrapper class of
 * VertexPropertyContext for serializing the data.
 *
 * @tparam FRAG_T The fragment class (non-labeled fragment only)
 */
template <typename FRAG_T>
class VertexPropertyContextWrapper : public IVertexPropertyContextWrapper {
  using fragment_t = FRAG_T;
  using vdata_t = typename fragment_t::vdata_t;
  using oid_t = typename fragment_t::oid_t;
  using context_t = VertexPropertyContext<fragment_t>;

 public:
  VertexPropertyContextWrapper(const std::string& id,
                               std::shared_ptr<IFragmentWrapper> frag_wrapper,
                               std::shared_ptr<context_t> context)
      : IVertexPropertyContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(context)) {}

  std::string context_type() override { return CONTEXT_TYPE_VERTEX_PROPERTY; }

  std::string schema() override {
    std::ostringstream os;
    auto property_map = ctx_->properties_map();
    for (auto& pair : property_map) {
      os << pair.first + ",";
    }
    return os.str();
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    size_t old_size;
    int64_t total_num;
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    auto local_num = static_cast<int64_t>(vertices.size());
    auto arc = std::make_unique<grape::InArchive>();

    if (comm_spec.fid() == 0) {
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(1);  // shape size
      *arc << total_num;
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    switch (selector.type()) {
    case SelectorType::kVertexId: {
      BOOST_LEAF_AUTO(type_id, trans_utils.GetOidTypeId());
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(type_id);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      trans_utils.SerializeVertexId(vertices, *arc);
      break;
    }
    case SelectorType::kVertexData: {
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(vineyard::TypeToInt<vdata_t>::value);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      trans_utils.SerializeVertexData(vertices, *arc);
      break;
    }
    case SelectorType::kResult: {
      auto prop_name = selector.property_name();
      auto& properties_map = ctx_->properties_map();

      if (properties_map.find(prop_name) == properties_map.end()) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Property " + prop_name + " not found in context.");
      }
      auto column = properties_map.at(prop_name);
      if (comm_spec.fid() == 0) {
        *arc << ContextDataTypeToInt(column->type());
        *arc << total_num;
      }
      old_size = arc->GetSize();
      BOOST_LEAF_CHECK(
          serialize_context_property<FRAG_T>(*arc, vertices, column));
      break;
    }
    default:
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kUnsupportedOperationError,
          "Unsupported operation, available selector type: vid,vdata "
          "and result. selector: " +
              selector.str());
    }

    gather_archives(*arc, comm_spec, old_size);
    return arc;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    auto local_num = static_cast<int64_t>(vertices.size());
    auto arc = std::make_unique<grape::InArchive>();

    if (comm_spec.fid() == 0) {
      int64_t total_num;
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(selectors.size());
      *arc << total_num;
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }
    for (auto& pair : selectors) {
      auto& col_name = pair.first;
      auto& selector = pair.second;

      if (comm_spec.fid() == 0) {
        *arc << col_name;
      }

      size_t old_size;

      switch (selector.type()) {
      case SelectorType::kVertexId: {
        BOOST_LEAF_AUTO(type_id, trans_utils.GetOidTypeId());
        if (comm_spec.fid() == 0) {
          *arc << static_cast<int>(type_id);
        }
        old_size = arc->GetSize();
        trans_utils.SerializeVertexId(vertices, *arc);
        break;
      }
      case SelectorType::kVertexData: {
        if (comm_spec.fid() == 0) {
          *arc << static_cast<int>(vineyard::TypeToInt<vdata_t>::value);
        }
        old_size = arc->GetSize();
        trans_utils.SerializeVertexData(vertices, *arc);
        break;
      }
      case SelectorType::kResult: {
        auto prop_name = selector.property_name();
        auto& properties_map = ctx_->properties_map();

        if (properties_map.find(prop_name) == properties_map.end()) {
          RETURN_GS_ERROR(
              vineyard::ErrorCode::kInvalidValueError,
              "Property " + prop_name + " can not found in context.");
        }
        auto column = properties_map.at(prop_name);
        if (comm_spec.fid() == 0) {
          *arc << ContextDataTypeToInt(column->type());
        }
        old_size = arc->GetSize();
        BOOST_LEAF_CHECK(
            serialize_context_property<FRAG_T>(*arc, vertices, column));
        break;
      }
      default:
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kUnsupportedOperationError,
            "Unsupported operation, available selector type: vid,vdata "
            "and result. selector: " +
                selector.str());
      }
      gather_archives(*arc, comm_spec, old_size);
    }
    return arc;
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    size_t local_num = vertices.size(), total_num;

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    vineyard::ObjectID tensor_chunk_id;

    switch (selector.type()) {
    case SelectorType::kVertexId: {
      BOOST_LEAF_ASSIGN(tensor_chunk_id,
                        trans_utils.VertexIdToVYTensor(client, vertices));
      break;
    }
    case SelectorType::kVertexData: {
      BOOST_LEAF_ASSIGN(tensor_chunk_id,
                        trans_utils.VertexDataToVYTensor(client, vertices));
      break;
    }
    case SelectorType::kResult: {
      auto prop_name = selector.property_name();
      auto& properties_map = ctx_->properties_map();

      if (properties_map.find(prop_name) == properties_map.end()) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Property " + prop_name + " can not found in context.");
      }
      auto column = properties_map.at(prop_name);

      BOOST_LEAF_ASSIGN(tensor_chunk_id,
                        column_to_vy_tensor<FRAG_T>(client, column, vertices));
      break;
    }
    default:
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kUnsupportedOperationError,
          "Unsupported operation, available selector type: vid,vdata "
          "and result. selector: " +
              selector.str());
    }

    MPIGlobalTensorBuilder builder(client, comm_spec);
    builder.set_shape({static_cast<int64_t>(total_num)});
    builder.set_partition_shape({static_cast<int64_t>(frag.fnum())});
    builder.AddChunk(tensor_chunk_id);
    auto vy_obj = builder.Seal(client);

    return vy_obj->id();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    size_t local_num = vertices.size(), total_num;
    std::vector<int64_t> shape{static_cast<int64_t>(local_num)};
    vineyard::DataFrameBuilder df_builder(client);

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    df_builder.set_partition_index(frag.fid(), 0);
    df_builder.set_row_batch_index(frag.fid());

    for (auto& e : selectors) {
      auto& col_name = e.first;
      auto& selector = e.second;

      switch (selector.type()) {
      case SelectorType::kVertexId: {
        BOOST_LEAF_AUTO(tensor_builder,
                        trans_utils.template VertexIdToVYTensorBuilder<oid_t>(
                            client, vertices));
        df_builder.AddColumn(col_name, tensor_builder);
        break;
      }
      case SelectorType::kVertexData: {
        BOOST_LEAF_AUTO(tensor_builder, trans_utils.VertexDataToVYTensorBuilder(
                                            client, vertices));
        df_builder.AddColumn(col_name, tensor_builder);
        break;
      }
      case SelectorType::kResult: {
        auto prop_name = selector.property_name();
        auto& properties_map = ctx_->properties_map();

        if (properties_map.find(prop_name) == properties_map.end()) {
          RETURN_GS_ERROR(
              vineyard::ErrorCode::kInvalidValueError,
              "Property " + prop_name + " can not found in context.");
        }
        auto column = properties_map.at(prop_name);

        BOOST_LEAF_AUTO(tensor_builder, column_to_vy_tensor_builder<FRAG_T>(
                                            client, column, vertices));
        df_builder.AddColumn(col_name, tensor_builder);
        break;
      }
      default:
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kUnsupportedOperationError,
            "Unsupported operation, available selector type: vid,vdata "
            "and result. selector: " +
                selector.str());
      }
    }

    auto df = df_builder.Seal(client);
    VY_OK_OR_RAISE(df->Persist(client));
    auto df_chunk_id = df->id();

    MPIGlobalDataFrameBuilder builder(client, comm_spec);
    builder.set_partition_shape(frag.fnum(), selectors.size());
    builder.AddChunk(df_chunk_id);

    auto vy_obj = builder.Seal(client);

    return vy_obj->id();
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>> ret;

    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);

    for (auto& pair : selectors) {
      auto col_name = pair.first;
      auto selector = pair.second;
      std::shared_ptr<arrow::Array> arr;

      switch (selector.type()) {
      case SelectorType::kVertexId: {
        BOOST_LEAF_ASSIGN(arr, trans_utils.VertexIdToArrowArray());
        break;
      }
      case SelectorType::kVertexData: {
        BOOST_LEAF_ASSIGN(arr, trans_utils.VertexDataToArrowArray());
        break;
      }
      case SelectorType::kResult: {
        auto prop_name = selector.property_name();
        auto properties_map = ctx_->properties_map();

        if (properties_map.find(prop_name) == properties_map.end()) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                          "Column: " + prop_name + " not found in context.");
        }
        arr = properties_map.at(prop_name)->ToArrowArray();
        break;
      }
      default:
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kUnsupportedOperationError,
            "Unsupported operation, available selector type: vid,vdata "
            "and result. selector: " +
                selector.str());
      }
      ret.emplace_back(col_name, arr);
    }
    return ret;
  }

 private:
  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_VERTEX_PROPERTY_CONTEXT_H_
