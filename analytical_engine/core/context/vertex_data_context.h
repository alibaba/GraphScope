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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_VERTEX_DATA_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_VERTEX_DATA_CONTEXT_H_

#include <mpi.h>

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#ifdef NETWORKX
#include "core/object/dynamic.h"
#endif

#include "grape/app/context_base.h"
#include "grape/app/vertex_data_context.h"
#include "grape/serialization/in_archive.h"
#include "grape/utils/vertex_array.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/dataframe.h"
#include "vineyard/client/client.h"
#include "vineyard/client/ds/i_object.h"
#include "vineyard/common/util/uuid.h"

#include "core/config.h"
#include "core/context/context_protocols.h"
#include "core/context/i_context.h"
#include "core/context/selector.h"
#include "core/context/tensor_dataframe_builder.h"
#include "core/error.h"
#include "core/server/rpc_utils.h"
#include "core/utils/mpi_utils.h"
#include "core/utils/transform_utils.h"
#include "proto/types.pb.h"

#define CONTEXT_TYPE_VERTEX_DATA "vertex_data"
#define CONTEXT_TYPE_LABELED_VERTEX_DATA "labeled_vertex_data"
#define CONTEXT_TTPE_DYNAMIC_VERTEX_DATA "dynamic_vertex_data"

namespace bl = boost::leaf;

namespace arrow {
class Array;
}

#ifdef NETWORKX
namespace grape {
template <typename FRAG_T>
class VertexDataContext<FRAG_T, gs::dynamic::Value> : public ContextBase {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using vertex_array_t =
      typename fragment_t::template vertex_array_t<gs::dynamic::Value>;

 public:
  using data_t = gs::dynamic::Value;

  explicit VertexDataContext(const fragment_t& fragment,
                             bool including_outer = false)
      : fragment_(fragment) {
    if (including_outer) {
      data_.Init(fragment.Vertices());
    } else {
      data_.Init(fragment.InnerVertices());
    }
  }

  const fragment_t& fragment() { return fragment_; }

  inline virtual vertex_array_t& data() { return data_; }

  virtual const gs::dynamic::Value& GetVertexResult(const vertex_t& v) {
    return data_[v];
  }

 private:
  const fragment_t& fragment_;
  vertex_array_t data_;
};
}  // namespace grape
#endif  // NETWORKX

namespace gs {
class IFragmentWrapper;

template <typename FRAG_T, typename DATA_T>
typename std::enable_if<!is_dynamic<DATA_T>::value,
                        bl::result<std::shared_ptr<arrow::Array>>>::type
context_data_to_arrow_array(
    const typename FRAG_T::vertex_range_t& vertices,
    const typename FRAG_T::template vertex_array_t<DATA_T>& data) {
  typename vineyard::ConvertToArrowType<DATA_T>::BuilderType builder;
  std::shared_ptr<typename vineyard::ConvertToArrowType<DATA_T>::ArrayType> arr;

  for (auto v : vertices) {
    ARROW_OK_OR_RAISE(builder.Append(data[v]));
  }
  CHECK_ARROW_ERROR(builder.Finish(&arr));
  return std::dynamic_pointer_cast<arrow::Array>(arr);
}

template <typename FRAG_T, typename DATA_T>
typename std::enable_if<is_dynamic<DATA_T>::value,
                        bl::result<std::shared_ptr<arrow::Array>>>::type
context_data_to_arrow_array(
    const typename FRAG_T::vertex_range_t& vertices,
    const typename FRAG_T::template vertex_array_t<DATA_T>& data) {
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                  "Can not transform dynamic type");
}

template <typename FRAG_T, typename COMPUTE_CONTEXT_T>
class PregelContext;
/**
 * @brief VertexDataContext for labeled fragment
 *
 * @tparam FRAG_T The fragment class (Labeled fragment only)
 * @tparam DATA_T The Data type hold by context
 * @tparam Enable
 */
template <typename FRAG_T, typename DATA_T>
class LabeledVertexDataContext : public grape::ContextBase {
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;
  using label_id_t = typename fragment_t::label_id_t;
  using vertex_array_t =
      grape::VertexArray<typename fragment_t::vertices_t, DATA_T>;

 public:
  using data_t = DATA_T;
  static_assert(std::is_pod<data_t>::value ||
                    std::is_same<data_t, std::string>::value,
                "Unsupported data type");

  explicit LabeledVertexDataContext(const fragment_t& fragment,
                                    bool including_outer = false)
      : fragment_(fragment) {
    auto v_label_num = fragment_.vertex_label_num();
    data_.resize(v_label_num);
    for (label_id_t i = 0; i < v_label_num; ++i) {
      if (including_outer) {
        data_[i].Init(fragment.Vertices(i));
      } else {
        data_[i].Init(fragment.InnerVertices(i));
      }
    }
  }

  const fragment_t& fragment() { return fragment_; }

  const data_t& GetValue(vertex_t v) const {
    label_id_t i = fragment_.vertex_label(v);
    int64_t offset = fragment_.vertex_offset(v);
    return data_[i][vertex_t{offset}];
  }

  std::vector<vertex_array_t>& data() { return data_; }

 private:
  const fragment_t& fragment_;
  std::vector<vertex_array_t> data_;
};

/**
 * @brief This is the wrapper class for VertexDataContext. A series of methods
 * are provided to transform the data hold by the context.
 *
 * @tparam FRAG_T The fragment class (Non-labeled fragment only)
 * @tparam DATA_T The Data type hold by context
 */
template <typename FRAG_T, typename DATA_T>
class VertexDataContextWrapper : public IVertexDataContextWrapper {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using context_t = grape::VertexDataContext<fragment_t, DATA_T>;
  using vdata_t = typename fragment_t::vdata_t;
  using data_t = DATA_T;

 public:
  explicit VertexDataContextWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<context_t> ctx)
      : IVertexDataContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(ctx)) {}

  std::string context_type() override { return CONTEXT_TYPE_VERTEX_DATA; }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    auto& data = ctx_->data();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    int64_t local_num = static_cast<int64_t>(vertices.size()), total_num;
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

    size_t old_size;

    switch (selector.type()) {
    case SelectorType::kVertexId: {
      // N.B. This method must be invoked on every worker!
      BOOST_LEAF_AUTO(type_id, trans_utils.GetOidTypeId());
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(type_id);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      trans_utils.SerializeVertexId(vertices, *arc);
      break;
    }
    case SelectorType::kVertexLabelId: {
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(vineyard::TypeToInt<int>::value);
      }
      old_size = arc->GetSize();
      BOOST_LEAF_CHECK(trans_utils.SerializeVertexLabelId(vertices, *arc));
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
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      for (auto v : vertices) {
        *arc << data[v];
      }
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
    return std::move(arc);
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    auto& data = ctx_->data();
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
      auto col_name = pair.first;
      auto selector = pair.second;

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
      case SelectorType::kVertexLabelId: {
        if (comm_spec.fid() == 0) {
          *arc << static_cast<int>(vineyard::TypeToInt<int>::value);
        }
        old_size = arc->GetSize();
        BOOST_LEAF_CHECK(trans_utils.SerializeVertexLabelId(vertices, *arc));
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
        if (comm_spec.fid() == 0) {
          *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);
        }
        old_size = arc->GetSize();
        for (auto v : vertices) {
          *arc << data[v];
        }
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
    return std::move(arc);
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    auto& data = ctx_->data();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    size_t local_num = vertices.size(), total_num;
    std::vector<int64_t> shape{static_cast<int64_t>(local_num)};

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
      auto f = [&data, &vertices](size_t i) {
        auto v = vertices[i];
        return data[v];
      };
      BOOST_LEAF_ASSIGN(
          tensor_chunk_id,
          build_vy_tensor(client, vertices.size(), f, comm_spec.fid()));
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
    auto value = builder.Seal(client);

    return value->id();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    auto& data = ctx_->data();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(range);
    size_t local_num = vertices.size(), total_num;
    std::vector<int64_t> shape{static_cast<int64_t>(local_num)};

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    vineyard::DataFrameBuilder df_builder(client);

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
        auto f = [&data, &vertices](size_t i) { return data[vertices[i]]; };
        BOOST_LEAF_AUTO(tensor_builder,
                        build_vy_tensor_builder(client, vertices.size(), f,
                                                comm_spec.fid()));
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
    auto& frag = ctx_->fragment();
    auto& data = ctx_->data();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>
        arrow_arrays;

    for (auto& pair : selectors) {
      auto& col_name = pair.first;
      auto& selector = pair.second;
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
        auto tmp = context_data_to_arrow_array<fragment_t, data_t>(
            frag.InnerVertices(), data);
        BOOST_LEAF_ASSIGN(arr, tmp);
        break;
      }
      default:
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kUnsupportedOperationError,
            "Unsupported operation, available selector type: vid,vdata "
            "and result. selector: " +
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
 * @brief This is dynamic::Value specialization of VertexDataContext.
 *
 * @tparam FRAG_T The fragment class (Non-labeled fragment only)
 */
template <typename FRAG_T>
class VertexDataContextWrapper<FRAG_T, dynamic::Value>
    : public IVertexDataContextWrapper {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using context_t = grape::VertexDataContext<fragment_t, dynamic::Value>;
  using vdata_t = typename fragment_t::vdata_t;
  using data_t = dynamic::Value;

 public:
  explicit VertexDataContextWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<context_t> ctx)
      : IVertexDataContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(ctx)) {}

  std::string context_type() override {
    return CONTEXT_TTPE_DYNAMIC_VERTEX_DATA;
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::string> GetContextData(const rpc::GSParams& params) override {
    BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
    oid_t oid;
    dynamic::Parse(node_in_json, oid);
    auto& frag = ctx_->fragment();
    if (frag.HasNode(oid)) {
      vertex_t v;
      frag.GetVertex(oid, v);
      return dynamic::Stringify(ctx_->GetVertexResult(v));
    }
    return std::string("");
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "DynamicVertexDataContext not support the operation.");
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "DynamicVertexDataContext not support the operation.");
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "DynamicVertexDataContext not support the operation.");
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "DynamicVertexDataContext not support the operation.");
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "DynamicVertexDataContext not support the operation.");
  }

 private:
  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};
#endif  // NETWORKX

/**
 * @brief This is the wrapper class for LabeledVertexDataContext. A series of
 * methods are provided to transform the data hold by the context.
 *
 * @tparam FRAG_T The fragment class (Labeled fragment only)
 * @tparam DATA_T The Data type hold by context
 */
template <typename FRAG_T, typename DATA_T>
class LabeledVertexDataContextWrapper
    : public ILabeledVertexDataContextWrapper {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using context_t = LabeledVertexDataContext<FRAG_T, DATA_T>;
  using data_t = DATA_T;

 public:
  explicit LabeledVertexDataContextWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<context_t> context)
      : ILabeledVertexDataContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(context)) {}

  std::string context_type() override {
    return CONTEXT_TYPE_LABELED_VERTEX_DATA;
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    size_t old_size;
    int64_t total_num;
    auto& frag = ctx_->fragment();
    auto label_id = selector.label_id();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(label_id, range);
    auto local_num = static_cast<int64_t>(vertices.size());
    auto arc = std::make_unique<grape::InArchive>();

    if (comm_spec.fid() == 0) {
      MPI_Reduce(&local_num, &total_num, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.worker_id(), comm_spec.comm());
      *arc << static_cast<int64_t>(1);  // # of dims
      *arc << total_num;
    } else {
      MPI_Reduce(&local_num, NULL, 1, MPI_INT64_T, MPI_SUM,
                 comm_spec.FragToWorker(0), comm_spec.comm());
    }

    switch (selector.type()) {
    case SelectorType::kVertexId: {
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(vineyard::TypeToInt<oid_t>::value);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      trans_utils.SerializeVertexId(vertices, *arc);
      break;
    }
    case SelectorType::kVertexData: {
      auto prop_id = selector.property_id();
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
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      serialize_context_data(*arc, label_id, vertices);
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
    return std::move(arc);
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();

    BOOST_LEAF_AUTO(label_id, LabeledSelector::GetVertexLabelId(selectors));

    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
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
        if (comm_spec.fid() == 0) {
          *arc << static_cast<int>(
              vineyard::TypeToInt<typename fragment_t::oid_t>::value);
        }
        old_size = arc->GetSize();
        trans_utils.SerializeVertexId(vertices, *arc);
        break;
      }
      case SelectorType::kVertexData: {
        auto prop_id = selector.property_id();

        if (comm_spec.fid() == 0) {
          *arc << vineyard::ArrowDataTypeToInt(
              frag.vertex_property_type(label_id, prop_id));
        }
        old_size = arc->GetSize();
        BOOST_LEAF_CHECK(trans_utils.SerializeVertexProperty(vertices, label_id,
                                                             prop_id, *arc));
        break;
      }
      case SelectorType::kResult: {
        if (comm_spec.fid() == 0) {
          *arc << static_cast<int>(vineyard::TypeToInt<data_t>::value);
        }
        old_size = arc->GetSize();
        serialize_context_data(*arc, label_id, vertices);
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

    return std::move(arc);
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    auto& frag = ctx_->fragment();
    auto label_id = selector.label_id();
    auto& data = ctx_->data()[label_id];
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(label_id, range);

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
      auto prop_id = selector.property_id();

      BOOST_LEAF_ASSIGN(tensor_chunk_id,
                        trans_utils.VertexPropertyToVYTensor(
                            client, label_id, prop_id, vertices));
      break;
    }
    case SelectorType::kResult: {
      auto f = [&data, &vertices](size_t i) { return data[vertices[i]]; };

      BOOST_LEAF_ASSIGN(
          tensor_chunk_id,
          build_vy_tensor(client, vertices.size(), f, comm_spec.fid()));
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
    BOOST_LEAF_AUTO(label_id, LabeledSelector::GetVertexLabelId(selectors));

    auto& frag = ctx_->fragment();
    auto& data = ctx_->data()[label_id];
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    auto vertices = trans_utils.SelectVertices(label_id, range);
    size_t local_num = vertices.size(), total_num;
    std::vector<int64_t> shape{static_cast<int64_t>(local_num)};

    MPI_Allreduce(&local_num, &total_num, 1, MPI_SIZE_T, MPI_SUM,
                  comm_spec.comm());

    vineyard::DataFrameBuilder df_builder(client);

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
        auto f = [&data, &vertices](size_t i) { return data[vertices[i]]; };
        BOOST_LEAF_AUTO(tensor_builder,
                        build_vy_tensor_builder(client, vertices.size(), f,
                                                comm_spec.fid()));
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
    auto& frag = ctx_->fragment();
    TransformUtils<FRAG_T> trans_utils(comm_spec, frag);
    std::map<label_id_t,
             std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
        ret;  // <label id, <column name, selector>>

    for (auto& pair : selectors) {
      auto& col_name = pair.first;
      auto& selector = pair.second;
      std::shared_ptr<arrow::Array> arr;
      auto label_id = selector.label_id();

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
        auto& data = ctx_->data()[label_id];

        if (!selector.property_name().empty()) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                          "Should not specify property name.");
        }
        auto tmp = context_data_to_arrow_array<fragment_t, data_t>(
            frag.InnerVertices(label_id), data);
        BOOST_LEAF_ASSIGN(arr, tmp);
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
  void serialize_context_data(grape::InArchive& arc, label_id_t label_id,
                              const std::vector<vertex_t>& vertices) {
    auto& ctx_data = ctx_->data();
    auto& labeled_data = ctx_data[label_id];

    for (auto v : vertices) {
      arc << labeled_data[v];
    }
  }

  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_VERTEX_DATA_CONTEXT_H_
