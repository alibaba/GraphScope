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

#ifndef ANALYTICAL_ENGINE_CORE_OBJECT_FRAGMENT_WRAPPER_H_
#define ANALYTICAL_ENGINE_CORE_OBJECT_FRAGMENT_WRAPPER_H_
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/utils/grape_utils.h"

#include "core/context/labeled_vertex_property_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"
#include "core/error.h"
#include "core/fragment/dynamic_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/object/gs_object.h"
#include "core/object/i_fragment_wrapper.h"
#include "core/utils/transform_utils.h"
#include "proto/attr_value.pb.h"
#include "proto/graph_def.pb.h"

namespace gs {
inline void set_graph_def(
    const std::shared_ptr<vineyard::ArrowFragmentBase>& fragment,
    rpc::GraphDef& graph_def) {
  auto& meta = fragment->meta();
  graph_def.set_graph_type(rpc::ARROW_PROPERTY);
  graph_def.set_directed(static_cast<bool>(meta.GetKeyValue<int>("directed")));

  auto* schema_def = graph_def.mutable_schema_def();
  schema_def->set_oid_type(
      vineyard::normalize_datatype(meta.GetKeyValue("oid_type")));
  schema_def->set_vid_type(
      vineyard::normalize_datatype(meta.GetKeyValue("vid_type")));
  schema_def->set_property_schema_json(meta.GetKeyValue("schema"));
}

/**
 * @brief This is a fragment wrapper, which means a series of methods are
 * provided to serialize/transform the data attached to the fragment. An
 * AddColumn method is provided to add properties to create a new fragment from
 * the original one.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class FragmentWrapper {};

/**
 * @brief A specialized FragmentWrapper for ArrowFragment.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T, typename VID_T>
class FragmentWrapper<vineyard::ArrowFragment<OID_T, VID_T>>
    : public ILabeledFragmentWrapper {
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T>;
  using label_id_t = typename fragment_t::label_id_t;

 public:
  FragmentWrapper(const std::string& id, rpc::GraphDef graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : ILabeledFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::ARROW_PROPERTY);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::GraphDef& graph_def() const override { return graph_def_; }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    auto& meta = fragment_->meta();
    auto* client = dynamic_cast<vineyard::Client*>(meta.GetClient());
    vineyard::ObjectMeta obj_meta;
    VINEYARD_CHECK_OK(client->GetMetaData(fragment_->id(), obj_meta));
    vineyard::ObjectID new_frag_id;
    VINEYARD_CHECK_OK(client->CreateMetaData(obj_meta, new_frag_id));
    BOOST_LEAF_AUTO(frag_group_id, vineyard::ConstructFragmentGroup(
                                       *client, new_frag_id, comm_spec));
    auto new_frag =
        std::static_pointer_cast<fragment_t>(client->GetObject(new_frag_id));
    auto dst_graph_def = graph_def_;

    dst_graph_def.set_key(dst_graph_name);
    dst_graph_def.set_vineyard_id(frag_group_id);

    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, dst_graph_def, new_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

  bl::result<std::shared_ptr<ILabeledFragmentWrapper>> AddColumn(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      std::shared_ptr<IContextWrapper>& ctx_wrapper,
      const std::string& s_selectors) override {
    const auto& context_type = ctx_wrapper->context_type();
    auto& meta = fragment_->meta();
    auto* client = dynamic_cast<vineyard::Client*>(meta.GetClient());

    if (context_type != CONTEXT_TYPE_VERTEX_DATA &&
        context_type != CONTEXT_TYPE_LABELED_VERTEX_DATA &&
        context_type != CONTEXT_TYPE_VERTEX_PROPERTY &&
        context_type != CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,
                      "Illegal context type: " + context_type);
    }

    auto frag_wrapper = ctx_wrapper->fragment_wrapper();
    auto graph_type = frag_wrapper->graph_def().graph_type();
    vineyard::ObjectID vm_id_from_ctx = 0;

    if (graph_type == rpc::ARROW_PROPERTY) {
      vm_id_from_ctx =
          std::static_pointer_cast<const vineyard::ArrowFragmentBase>(
              frag_wrapper->fragment())
              ->vertex_map_id();
    } else if (graph_type == rpc::ARROW_PROJECTED) {
      auto& proj_meta =
          std::static_pointer_cast<const ArrowProjectedFragmentBase>(
              frag_wrapper->fragment())
              ->meta();
      const auto& frag_meta = proj_meta.GetMemberMeta("arrow_fragment");

      vm_id_from_ctx =
          client->GetObject<vineyard::ArrowFragmentBase>(frag_meta.GetId())
              ->vertex_map_id();
    }

    if (vm_id_from_ctx != fragment_->vertex_map_id()) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "ctx holds a vertex map id = " + std::to_string(vm_id_from_ctx) +
              ", but the vertex map id of fragment is " +
              std::to_string(fragment_->vertex_map_id()));
    }

    std::map<label_id_t,
             std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
        columns;

    if (context_type == CONTEXT_TYPE_VERTEX_DATA) {
      auto vd_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexDataContextWrapper>(ctx_wrapper);
      auto& proj_meta =
          std::static_pointer_cast<const ArrowProjectedFragmentBase>(
              frag_wrapper->fragment())
              ->meta();
      auto v_label_id = proj_meta.GetKeyValue<label_id_t>("projected_v_label");

      BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
      BOOST_LEAF_AUTO(arrow_arrays,
                      vd_ctx_wrapper->ToArrowArrays(comm_spec, selectors));
      columns[v_label_id] = arrow_arrays;
    } else if (context_type == CONTEXT_TYPE_LABELED_VERTEX_DATA) {
      auto lvd_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              ctx_wrapper);
      BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
      BOOST_LEAF_ASSIGN(columns,
                        lvd_ctx_wrapper->ToArrowArrays(comm_spec, selectors));
    } else if (context_type == CONTEXT_TYPE_VERTEX_PROPERTY) {
      auto vp_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(ctx_wrapper);
      auto& proj_meta =
          std::static_pointer_cast<const ArrowProjectedFragmentBase>(
              frag_wrapper->fragment())
              ->meta();
      auto v_label_id = proj_meta.GetKeyValue<label_id_t>("projected_v_label");

      BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
      BOOST_LEAF_AUTO(arrow_arrays,
                      vp_ctx_wrapper->ToArrowArrays(comm_spec, selectors));
      columns[v_label_id] = arrow_arrays;
    } else if (context_type == CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      auto vp_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
              ctx_wrapper);

      BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
      BOOST_LEAF_ASSIGN(columns,
                        vp_ctx_wrapper->ToArrowArrays(comm_spec, selectors));
    }

    auto new_frag_id = fragment_->AddVertexColumns(*client, columns);

    BOOST_LEAF_AUTO(frag_group_id, vineyard::ConstructFragmentGroup(
                                       *client, new_frag_id, comm_spec));
    auto new_frag = client->GetObject<fragment_t>(new_frag_id);

    rpc::GraphDef new_graph_def;

    new_graph_def.set_key(dst_graph_name);
    new_graph_def.set_vineyard_id(frag_group_id);

    set_graph_def(new_frag, new_graph_def);

    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, new_graph_def, new_frag);
    return std::dynamic_pointer_cast<ILabeledFragmentWrapper>(wrapper);
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    TransformUtils<fragment_t> trans_utils(comm_spec, *fragment_);
    auto label_id = selector.label_id();
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
      auto oid_type = trans_utils.GetOidTypeId();
      if (comm_spec.fid() == 0) {
        *arc << static_cast<int>(oid_type);
        *arc << total_num;
      }
      old_size = arc->GetSize();
      trans_utils.SerializeVertexId(vertices, *arc);
      break;
    }
    case SelectorType::kVertexData: {
      auto prop_id = selector.property_id();
      auto graph_prop_num = fragment_->vertex_property_num(label_id);

      if (prop_id >= graph_prop_num) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Invalid property id: " + std::to_string(prop_id));
      }

      if (comm_spec.fid() == 0) {
        *arc << vineyard::ArrowDataTypeToInt(
            fragment_->vertex_property_type(label_id, prop_id));
        *arc << total_num;
      }
      old_size = arc->GetSize();
      BOOST_LEAF_CHECK(trans_utils.SerializeVertexProperty(vertices, label_id,
                                                           prop_id, *arc));
      break;
    }
    default:
      RETURN_GS_ERROR(vineyard::ErrorCode::kUnsupportedOperationError,
                      "Unsupported operation, available selector type: "
                      "vid,vdata selector: " +
                          selector.str());
    }
    gather_archives(*arc, comm_spec, old_size);

    return arc;
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    TransformUtils<fragment_t> trans_utils(comm_spec, *fragment_);

    BOOST_LEAF_AUTO(label_id, LabeledSelector::GetVertexLabelId(selectors));
    auto vertices = trans_utils.SelectVertices(label_id, range);
    auto arc = std::make_unique<grape::InArchive>();
    auto local_num = static_cast<int64_t>(vertices.size());

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
          *arc << static_cast<int>(vineyard::TypeToInt<OID_T>::value);
        }
        old_size = arc->GetSize();
        trans_utils.SerializeVertexId(vertices, *arc);
        break;
      }
      case SelectorType::kVertexData: {
        if (comm_spec.fid() == 0) {
          *arc << vineyard::ArrowDataTypeToInt(fragment_->vertex_property_type(
              label_id, selector.property_id()));
        }
        old_size = arc->GetSize();
        BOOST_LEAF_CHECK(trans_utils.SerializeVertexProperty(
            vertices, label_id, selector.property_id(), *arc));
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

 private:
  rpc::GraphDef graph_def_;
  std::shared_ptr<fragment_t> fragment_;
};

/**
 * @brief A specialized FragmentWrapper for ArrowProjectedFragment.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class FragmentWrapper<ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>
    : public IFragmentWrapper {
  using fragment_t = ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  FragmentWrapper(const std::string& id, rpc::GraphDef graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : IFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::ARROW_PROJECTED);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::GraphDef& graph_def() const override { return graph_def_; }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not copy ArrowProjectedFragment");
  }

 private:
  rpc::GraphDef graph_def_;
  std::shared_ptr<fragment_t> fragment_;
};

#ifdef EXPERIMENTAL_ON
/**
 * @brief A specialized FragmentWrapper for DynamicFragment.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <>
class FragmentWrapper<DynamicFragment> : public IFragmentWrapper {
  using fragment_t = DynamicFragment;

 public:
  FragmentWrapper(const std::string& id, rpc::GraphDef graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : IFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::DYNAMIC_PROPERTY);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::GraphDef& graph_def() const override { return graph_def_; }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    // copy vertex map
    auto ori_vm_ptr = fragment_->GetVertexMap();
    auto new_vm_ptr =
        std::make_shared<typename fragment_t::vertex_map_t>(comm_spec);
    new_vm_ptr->Init();
    std::vector<std::thread> copy_vm_threads(comm_spec.fnum());
    for (size_t fid = 0; fid < comm_spec.fnum(); ++fid) {
      copy_vm_threads[fid] = std::thread(
          [&](size_t fid) {
            typename fragment_t::oid_t oid;
            typename fragment_t::vid_t gid{};
            typename fragment_t::vid_t fvnum =
                ori_vm_ptr->GetInnerVertexSize(fid);
            for (typename fragment_t::vid_t lid = 0; lid < fvnum; lid++) {
              ori_vm_ptr->GetOid(fid, lid, oid);
              CHECK(new_vm_ptr->AddVertex(fid, oid, gid));
            }
          },
          fid);
    }
    for (auto& thrd : copy_vm_threads) {
      thrd.join();
    }
    // copy fragment
    auto dst_frag = std::make_shared<fragment_t>(new_vm_ptr);

    dst_frag->Copy(fragment_, copy_type);

    auto dst_graph_def = graph_def_;
    dst_graph_def.set_key(dst_graph_name);
    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, dst_graph_def, dst_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

 private:
  rpc::GraphDef graph_def_;
  std::shared_ptr<fragment_t> fragment_;
};

/**
 * @brief A specialized FragmentWrapper for DynamicProjectedFragment.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename VDATA_T, typename EDATA_T>
class FragmentWrapper<DynamicProjectedFragment<VDATA_T, EDATA_T>>
    : public IFragmentWrapper {
  using fragment_t = DynamicProjectedFragment<VDATA_T, EDATA_T>;

 public:
  FragmentWrapper(const std::string& id, rpc::GraphDef graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : IFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::DYNAMIC_PROJECTED);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::GraphDef& graph_def() const override { return graph_def_; }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not copy DynamicProjectedFragment");
  }

 private:
  rpc::GraphDef graph_def_;
  std::shared_ptr<fragment_t> fragment_;
};
#endif

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_FRAGMENT_WRAPPER_H_
