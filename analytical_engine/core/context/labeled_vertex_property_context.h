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
#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_LABELED_VERTEX_PROPERTY_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_LABELED_VERTEX_PROPERTY_CONTEXT_H_

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "grape/app/context_base.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#include "core/context/column.h"
#include "core/context/i_context.h"
#include "core/context/selector.h"
#include "core/context/tensor_dataframe_builder.h"
#include "core/utils/mpi_utils.h"
#include "core/utils/transform_utils.h"

#define CONTEXT_TYPE_LABELED_VERTEX_PROPERTY "labeled_vertex_property"

namespace bl = boost::leaf;

namespace gs {
class IFragmentWrapper;

/**
 * @brief LabeledVertexPropertyContext can hold any number of columns. The
 * context is designed for labeled fragment - ArrowFragment. Compared with
 * LabeledVertexDataContext, the data type and column count can be determined at
 * runtime.
 *
 * @tparam FRAG_T The fragment class (labeled fragment only)
 */
template <typename FRAG_T>
class LabeledVertexPropertyContext : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;

  explicit LabeledVertexPropertyContext(const fragment_t& fragment)
      : fragment_(fragment) {
    auto label_num = fragment.vertex_label_num();
    vertex_properties_.resize(label_num);
    properties_map_.resize(label_num);
  }

  const fragment_t& fragment() { return fragment_; }

  int64_t add_column(label_id_t label, const std::string& name,
                     ContextDataType type) {
    if (static_cast<size_t>(label) >= properties_map_.size()) {
      return -1;
    }
    auto& map = properties_map_[label];
    if (map.find(name) != map.end()) {
      return -1;
    }
    auto column =
        CreateColumn<fragment_t>(name, fragment_.InnerVertices(label), type);
    map.emplace(name, column);
    auto& vec = vertex_properties_[label];
    auto ret = static_cast<int64_t>(vec.size());
    vec.emplace_back(column);
    return ret;
  }

  std::shared_ptr<IColumn> get_column(label_id_t label, int64_t index) {
    if (label >= vertex_properties_.size()) {
      return nullptr;
    }
    auto& vec = vertex_properties_[label];
    if (static_cast<size_t>(index) > vec.size()) {
      return nullptr;
    }
    return vec[index];
  }

  std::shared_ptr<IColumn> get_column(label_id_t label,
                                      const std::string& name) {
    if (label >= properties_map_.size()) {
      return nullptr;
    }
    auto& map = properties_map_[label];
    auto iter = map.find(name);
    if (iter == map.end()) {
      return nullptr;
    }
    return iter->second;
  }

  template <typename DATA_T>
  std::shared_ptr<Column<fragment_t, DATA_T>> get_typed_column(label_id_t label,
                                                               int64_t index) {
    if (static_cast<size_t>(label) >= vertex_properties_.size()) {
      return nullptr;
    }
    auto& vec = vertex_properties_[label];
    if (static_cast<size_t>(index) > vec.size()) {
      return nullptr;
    }
    auto ret = vec[index];
    if (ret->type() != ContextTypeToEnum<DATA_T>::value) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Column<fragment_t, DATA_T>>(ret);
  }

  template <typename DATA_T>
  std::shared_ptr<Column<fragment_t, DATA_T>> get_typed_column(
      label_id_t label, const std::string& name) {
    if (label >= properties_map_.size()) {
      return nullptr;
    }
    auto& map = properties_map_[label];
    auto iter = map.find(name);
    if (iter == map.end()) {
      return nullptr;
    }
    auto ret = iter->second;
    if (ret->type() != ContextTypeToEnum<DATA_T>::value) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Column<fragment_t, DATA_T>>(ret);
  }

  std::vector<std::vector<std::shared_ptr<IColumn>>>& vertex_properties() {
    return vertex_properties_;
  }

  std::vector<std::map<std::string, std::shared_ptr<IColumn>>>&
  properties_map() {
    return properties_map_;
  }

 private:
  const fragment_t& fragment_;
  std::vector<std::vector<std::shared_ptr<IColumn>>> vertex_properties_;
  std::vector<std::map<std::string, std::shared_ptr<IColumn>>> properties_map_;
};

/**
 * @brief LabeledVertexPropertyContextWrapper is the wrapper class of
 * LabeledVertexPropertyContext for serializing the data.
 *
 * @tparam FRAG_T The fragment class (labeled fragment only)
 */
template <typename FRAG_T>
class LabeledVertexPropertyContextWrapper
    : public ILabeledVertexPropertyContextWrapper {
  using fragment_t = FRAG_T;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;
  using context_t = LabeledVertexPropertyContext<fragment_t>;
  static_assert(vineyard::is_property_fragment<FRAG_T>::value,
                "LabeledVertexPropertyContextWrapper is only available for "
                "property graph");

 public:
  LabeledVertexPropertyContextWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<context_t> context)
      : ILabeledVertexPropertyContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(context)) {}

  std::string context_type() override {
    return CONTEXT_TYPE_LABELED_VERTEX_PROPERTY;
  }

  std::string schema() override {
    auto frag = ctx_->fragment();
    auto label_num = frag.vertex_label_num();
    std::ostringstream os;
    for (int i = 0; i < label_num; ++i) {
      os << i << ":";
      auto property_map = ctx_->properties_map()[i];
      for (auto& pair : property_map) {
        os << pair.first + ",";
      }
      os << "\n";
    }
    return os.str();
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    auto label_id = selector.label_id();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(label_id, range);
    auto arc = std::make_unique<grape::InArchive>();
    auto local_num = static_cast<int64_t>(vertices.size());
    int64_t total_num;

    if (comm_spec.fid() == 0) {
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(1);
      *arc << total_num;
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }
    size_t old_size;

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
      auto prop_id = selector.property_id();
      auto graph_prop_num = frag.vertex_property_num(label_id);

      if (prop_id >= graph_prop_num) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Invalid property id: " + std::to_string(prop_id));
      }

      if (comm_spec.fid() == 0) {
        *arc << vineyard::ArrowDataTypeToInt(
            frag.vertex_property_type(label_id, prop_id));
        *arc << total_num;
      }
      old_size = arc->GetSize();
      BOOST_LEAF_CHECK(trans_utils.SerializeVertexProperty(vertices, label_id,
                                                           prop_id, *arc));
      break;
    }
    case SelectorType::kResult: {
      auto& property_map = ctx_->properties_map()[label_id];
      auto prop_name = selector.property_name();
      if (property_map.find(prop_name) == property_map.end()) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Property " + prop_name + " not found in context.");
      }
      auto column = property_map.at(prop_name);
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
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);

    BOOST_LEAF_AUTO(label_id, LabeledSelector::GetVertexLabelId(selectors));

    auto vertices = trans_utils.SelectVertices(label_id, range);
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
          *arc << vineyard::ArrowDataTypeToInt(
              frag.vertex_property_type(label_id, selector.property_id()));
        }
        old_size = arc->GetSize();
        BOOST_LEAF_CHECK(trans_utils.SerializeVertexProperty(
            vertices, label_id, selector.property_id(), *arc));
        break;
      }
      case SelectorType::kResult: {
        auto& property_map = ctx_->properties_map()[label_id];
        auto prop_name = selector.property_name();

        if (property_map.find(prop_name) == property_map.end()) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                          "Property " + prop_name + " not found in context.");
        }
        auto column = property_map.at(prop_name);
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
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto label_id = selector.label_id();
    auto prop_name = selector.property_name();
    auto vertices = trans_utils.SelectVertices(label_id, range);
    size_t local_num = vertices.size(), total_num;
    vineyard::ObjectID tensor_chunk_id;

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    switch (selector.type()) {
    case SelectorType::kVertexId: {
      BOOST_LEAF_ASSIGN(tensor_chunk_id,
                        trans_utils.VertexIdToVYTensor(client, vertices));
      break;
    }
    case SelectorType::kVertexData: {
      auto prop_id = selector.property_id();
      BOOST_LEAF_ASSIGN(tensor_chunk_id,
                        trans_utils.VertexPropertyToVYTensor(
                            client, label_id, prop_id, vertices));
      break;
    }
    case SelectorType::kResult: {
      auto& property_map = ctx_->properties_map()[label_id];

      if (property_map.find(prop_name) == property_map.end()) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Property " + prop_name + " not found in context.");
      }

      auto column = property_map.at(prop_name);

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
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);

    BOOST_LEAF_AUTO(label_id, LabeledSelector::GetVertexLabelId(selectors));

    auto vertices = trans_utils.SelectVertices(label_id, range);
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
        auto prop_id = selector.property_id();

        BOOST_LEAF_AUTO(tensor_builder,
                        trans_utils.VertexPropertyToVYTensorBuilder(
                            client, label_id, prop_id, vertices));
        df_builder.AddColumn(col_name, tensor_builder);
        break;
      }
      case SelectorType::kResult: {
        auto prop_name = selector.property_name();
        auto& property_map = ctx_->properties_map()[label_id];

        if (property_map.find(prop_name) == property_map.end()) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                          "Property " + prop_name + " not found in context.");
        }

        auto column = property_map.at(prop_name);

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

  bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::vector<std::pair<std::string, LabeledSelector>>&
                    selectors) override {
    std::map<label_id_t,
             std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
        ret;
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);

    for (auto& pair : selectors) {
      auto& col_name = pair.first;
      auto& selector = pair.second;
      auto label_id = selector.label_id();
      std::shared_ptr<arrow::Array> arr;

      switch (selector.type()) {
      case SelectorType::kVertexId: {
        BOOST_LEAF_ASSIGN(arr, trans_utils.VertexIdToArrowArray(label_id));
        break;
      }
      case SelectorType::kVertexData: {
        auto prop_id = selector.property_id();
        arr = trans_utils.VertexPropertyToArrowArray(label_id, prop_id);
        break;
      }
      case SelectorType::kResult: {
        auto prop_name = selector.property_name();
        auto& property_map = ctx_->properties_map()[label_id];

        if (property_map.find(prop_name) == property_map.end()) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                          "Property " + prop_name + " not found in context.");
        }
        arr = property_map.at(prop_name)->ToArrowArray();
        break;
      }
      default:
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kUnsupportedOperationError,
            "Unsupported operation, available selector type: vid,vdata "
            "and result. selector: " +
                selector.str());
      }
      ret[label_id].emplace_back(col_name, arr);
    }
    return ret;
  }

 private:
  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_LABELED_VERTEX_PROPERTY_CONTEXT_H_
