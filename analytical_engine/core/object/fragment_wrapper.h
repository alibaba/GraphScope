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
#include "vineyard/graph/fragment/graph_schema.h"
#include "vineyard/graph/utils/grape_utils.h"

#include "core/context/labeled_vertex_property_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"
#include "core/error.h"
#include "core/fragment/dynamic_fragment_view.h"
#include "core/fragment/dynamic_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/object/gs_object.h"
#include "core/object/i_fragment_wrapper.h"
#include "core/utils/transform_utils.h"
#include "proto/attr_value.pb.h"
#include "proto/graph_def.pb.h"

namespace gs {

gs::rpc::graph::DataTypePb PropertyTypeToPb(vineyard::PropertyType type) {
  if (arrow::boolean()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::BOOL;
  } else if (arrow::int16()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::SHORT;
  } else if (arrow::int32()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::INT;
  } else if (arrow::int64()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::LONG;
  } else if (arrow::uint32()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::UINT;
  } else if (arrow::uint64()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::ULONG;
  } else if (arrow::float32()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::FLOAT;
  } else if (arrow::float64()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::DOUBLE;
  } else if (arrow::utf8()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::STRING;
  } else if (arrow::large_utf8()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::STRING;
  } else if (arrow::large_list(arrow::int32())->Equals(type)) {
    return gs::rpc::graph::DataTypePb::INT_LIST;
  } else if (arrow::large_list(arrow::int64())->Equals(type)) {
    return gs::rpc::graph::DataTypePb::LONG_LIST;
  } else if (arrow::large_list(arrow::float32())->Equals(type)) {
    return gs::rpc::graph::DataTypePb::FLOAT_LIST;
  } else if (arrow::large_list(arrow::float64())->Equals(type)) {
    return gs::rpc::graph::DataTypePb::DOUBLE_LIST;
  } else if (arrow::large_list(arrow::large_utf8())->Equals(type)) {
    return gs::rpc::graph::DataTypePb::STRING_LIST;
  } else if (arrow::null()->Equals(type)) {
    return gs::rpc::graph::DataTypePb::NULLVALUE;
  }

  LOG(ERROR) << "Unsupported arrow type " << type->ToString();
  return gs::rpc::graph::DataTypePb::UNKNOWN;
}

gs::rpc::graph::DataTypePb PropertyTypeToPb(const std::string& type) {
  if (type == "bool") {
    return gs::rpc::graph::DataTypePb::BOOL;
  } else if (type == "short" || type == "int16" || type == "int16_t") {
    return gs::rpc::graph::DataTypePb::SHORT;
  } else if (type == "int" || type == "int32" || type == "int32_t") {
    return gs::rpc::graph::DataTypePb::INT;
  } else if (type == "long" || type == "int64" || type == "int64_t") {
    return gs::rpc::graph::DataTypePb::LONG;
  } else if (type == "uint" || type == "uint32" || type == "uint32_t") {
    return gs::rpc::graph::DataType::UINT;
  } else if (type == "ulong" || type == "uint64" || type == "uint64_t") {
    return gs::rpc::graph::DataType::ULONG;
  } else if (type == "float") {
    return gs::rpc::graph::DataTypePb::FLOAT;
  } else if (type == "double") {
    return gs::rpc::graph::DataTypePb::DOUBLE;
  } else if (type == "bytes") {
    return gs::rpc::graph::DataTypePb::BYTES;
  } else if (type == "string" || type == "str") {
    return gs::rpc::graph::DataTypePb::STRING;
  } else if (type == "int_list") {
    return gs::rpc::graph::DataTypePb::INT_LIST;
  } else if (type == "long_list") {
    return gs::rpc::graph::DataTypePb::LONG_LIST;
  } else if (type == "float_list") {
    return gs::rpc::graph::DataTypePb::FLOAT_LIST;
  } else if (type == "double_list") {
    return gs::rpc::graph::DataTypePb::DOUBLE_LIST;
  } else if (type == "string_list" || type == "str_list") {
    return gs::rpc::graph::DataTypePb::STRING_LIST;
  } else if (type == "grape::EmptyType" || type == "null") {
    return gs::rpc::graph::DataTypePb::NULLVALUE;
  }
  LOG(ERROR) << "Unsupported type " << type;
  return gs::rpc::graph::DataTypePb::UNKNOWN;
}

gs::rpc::graph::TypeEnumPb TypeToTypeEnum(const std::string& type) {
  if (type == "VERTEX") {
    return gs::rpc::graph::TypeEnumPb::VERTEX;
  } else if (type == "EDGE") {
    return gs::rpc::graph::TypeEnumPb::EDGE;
  }
  return gs::rpc::graph::TypeEnumPb::UNSPECIFIED;
}

void ToPropertyDef(const vineyard::Entry::PropertyDef& prop,
                   const std::vector<std::string>& primary_keys,
                   gs::rpc::graph::PropertyDefPb* prop_def) {
  prop_def->set_id(prop.id);
  prop_def->set_name(prop.name);
  prop_def->set_data_type(PropertyTypeToPb(prop.type));
  if (std::find(std::begin(primary_keys), std::end(primary_keys), prop.name) !=
      std::end(primary_keys)) {
    prop_def->set_pk(true);
  }
}

void ToTypeDef(const vineyard::Entry& entry,
               gs::rpc::graph::TypeDefPb* type_def) {
  type_def->set_label(entry.label);
  type_def->mutable_label_id()->set_id(entry.id);
  type_def->set_type_enum(TypeToTypeEnum(entry.type));
  auto properties = entry.properties();
  auto primary_keys = entry.primary_keys;
  for (const auto& prop : properties) {
    ToPropertyDef(prop, primary_keys, type_def->add_props());
  }
}

void ToEdgeKind(const std::string& label,
                const std::pair<std::string, std::string>& relation,
                gs::rpc::graph::EdgeKindPb* edge_kind) {
  edge_kind->set_edge_label(label);
  edge_kind->set_src_vertex_label(relation.first);
  edge_kind->set_dst_vertex_label(relation.second);
}

inline void set_graph_def(
    const std::shared_ptr<vineyard::ArrowFragmentBase>& fragment,
    rpc::graph::GraphDefPb& graph_def) {
  auto& meta = fragment->meta();
  const auto& schema = fragment->schema();
  graph_def.set_graph_type(rpc::graph::ARROW_PROPERTY);
  graph_def.set_directed(static_cast<bool>(meta.GetKeyValue<int>("directed")));
  auto v_entries = schema.vertex_entries();
  auto e_entries = schema.edge_entries();
  for (const auto& entry : v_entries) {
    ToTypeDef(entry, graph_def.add_type_defs());
  }
  for (const auto& entry : e_entries) {
    ToTypeDef(entry, graph_def.add_type_defs());
  }
  for (const auto& entry : e_entries) {
    for (const auto& rel : entry.relations) {
      ToEdgeKind(entry.label, rel, graph_def.add_edge_kinds());
    }
  }
  auto property_name_to_id = graph_def.mutable_property_name_to_id();
  for (const auto& pair : schema.GetPropertyNameToIDMapping()) {
    (*property_name_to_id)[pair.first] = pair.second;
  }

  gs::rpc::graph::VineyardInfoPb vy_info;
  if (graph_def.has_extension()) {
    graph_def.extension().UnpackTo(&vy_info);
  }
  vy_info.set_oid_type(PropertyTypeToPb(meta.GetKeyValue("oid_type")));
  vy_info.set_vid_type(PropertyTypeToPb(meta.GetKeyValue("vid_type")));
  vy_info.set_property_schema_json(meta.GetKeyValue("schema"));
  graph_def.mutable_extension()->PackFrom(vy_info);
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
  FragmentWrapper(const std::string& id, rpc::graph::GraphDefPb graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : ILabeledFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::graph::ARROW_PROPERTY);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::graph::GraphDefPb& graph_def() const override {
    return graph_def_;
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    auto& meta = fragment_->meta();
    auto* client = dynamic_cast<vineyard::Client*>(meta.GetClient());
    BOOST_LEAF_AUTO(frag_group_id, vineyard::ConstructFragmentGroup(
                                       *client, fragment_->id(), comm_spec));
    auto dst_graph_def = graph_def_;

    dst_graph_def.set_key(dst_graph_name);
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (dst_graph_def.has_extension()) {
      dst_graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_vineyard_id(frag_group_id);
    dst_graph_def.mutable_extension()->PackFrom(vy_info);

    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, dst_graph_def, fragment_);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

  bl::result<std::shared_ptr<ILabeledFragmentWrapper>> Project(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::map<int, std::vector<int>>& vertices,
      const std::map<int, std::vector<int>>& edges) override {
    auto& meta = fragment_->meta();
    auto* client = dynamic_cast<vineyard::Client*>(meta.GetClient());
    BOOST_LEAF_AUTO(new_frag_id, fragment_->Project(*client, vertices, edges));
    VINEYARD_CHECK_OK(client->Persist(new_frag_id));
    BOOST_LEAF_AUTO(frag_group_id, vineyard::ConstructFragmentGroup(
                                       *client, new_frag_id, comm_spec));
    auto new_frag = client->GetObject<fragment_t>(new_frag_id);

    rpc::graph::GraphDefPb new_graph_def;

    new_graph_def.set_key(dst_graph_name);

    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def_.has_extension()) {
      graph_def_.extension().UnpackTo(&vy_info);
    }
    vy_info.set_vineyard_id(frag_group_id);
    new_graph_def.mutable_extension()->PackFrom(vy_info);

    set_graph_def(new_frag, new_graph_def);

    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, new_graph_def, new_frag);
    return std::dynamic_pointer_cast<ILabeledFragmentWrapper>(wrapper);
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

    if (graph_type == rpc::graph::ARROW_PROPERTY) {
      vm_id_from_ctx =
          std::static_pointer_cast<const vineyard::ArrowFragmentBase>(
              frag_wrapper->fragment())
              ->vertex_map_id();
    } else if (graph_type == rpc::graph::ARROW_PROJECTED) {
      auto& proj_meta =
          std::static_pointer_cast<const ArrowProjectedFragmentBase>(
              frag_wrapper->fragment())
              ->meta();
      const auto& frag_meta = proj_meta.GetMemberMeta("arrow_fragment");

      vm_id_from_ctx =
          client->GetObject<vineyard::ArrowFragmentBase>(frag_meta.GetId())
              ->vertex_map_id();
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
    vineyard::ObjectMeta ctx_meta, cur_meta;
    VINEYARD_CHECK_OK(client->GetMetaData(vm_id_from_ctx, ctx_meta));
    VINEYARD_CHECK_OK(
        client->GetMetaData(fragment_->vertex_map_id(), cur_meta));
    auto ctx_fnum = ctx_meta.GetKeyValue<fid_t>("fnum");
    auto cur_fnum = cur_meta.GetKeyValue<fid_t>("fnum");
    if (ctx_fnum != cur_fnum) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Fragment number of context differ from the destination fragment");
    }

    for (const auto& pair : columns) {
      if (fragment_->schema().GetVertexLabelName(pair.first).empty()) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,
                        "Label id " + std::to_string(pair.first) +
                            " is invalid in the destination fragment");
      }
      for (fid_t i = 0; i < cur_fnum; ++i) {
        auto name =
            "o2g_" + std::to_string(i) + "_" + std::to_string(pair.first);
        auto id_in_ctx = ctx_meta.GetMemberMeta(name).GetId();
        auto id_in_cur = cur_meta.GetMemberMeta(name).GetId();
        if (id_in_ctx != id_in_cur) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,
                          "Vertex datastructure " + name +
                              "in context differ from vertex map of the "
                              "destination fragment");
        }
        name = "oid_arrays_" + std::to_string(i) + "_" +
               std::to_string(pair.first);
        id_in_ctx = ctx_meta.GetMemberMeta(name).GetId();
        id_in_cur = cur_meta.GetMemberMeta(name).GetId();
        if (id_in_ctx != id_in_cur) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,
                          "Vertex datastructure " + name +
                              "in context differ from vertex map of the "
                              "destionation fragment");
        }
      }
    }

    BOOST_LEAF_AUTO(new_frag_id, fragment_->AddVertexColumns(*client, columns));

    VINEYARD_CHECK_OK(client->Persist(new_frag_id));
    BOOST_LEAF_AUTO(frag_group_id, vineyard::ConstructFragmentGroup(
                                       *client, new_frag_id, comm_spec));
    auto new_frag = client->GetObject<fragment_t>(new_frag_id);

    rpc::graph::GraphDefPb new_graph_def;
    new_graph_def.set_key(dst_graph_name);
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def_.has_extension()) {
      graph_def_.extension().UnpackTo(&vy_info);
    }
    vy_info.set_vineyard_id(frag_group_id);
    new_graph_def.mutable_extension()->PackFrom(vy_info);

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

    return std::move(arc);
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
    return std::move(arc);
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not to directed ArrowFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToUnDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not to undirected ArrowFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CreateGraphView(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Cannot generate a graph view over an ArrowFragment.");
  }

 private:
  rpc::graph::GraphDefPb graph_def_;
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
  FragmentWrapper(const std::string& id, rpc::graph::GraphDefPb graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : IFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::graph::ARROW_PROJECTED);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::graph::GraphDefPb& graph_def() const override {
    return graph_def_;
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not copy ArrowProjectedFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not to directed DynamicProjectedFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToUnDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not to undirected DynamicProjectedFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CreateGraphView(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not view ArrowProjectedFragment");
  }

 private:
  rpc::graph::GraphDefPb graph_def_;
  std::shared_ptr<fragment_t> fragment_;
};

#ifdef NETWORKX
/**
 * @brief A specialized FragmentWrapper for DynamicFragment.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <>
class FragmentWrapper<DynamicFragment> : public IFragmentWrapper {
  using fragment_t = DynamicFragment;
  using fragment_view_t = DynamicFragmentView;

 public:
  FragmentWrapper(const std::string& id, rpc::graph::GraphDefPb graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : IFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::graph::DYNAMIC_PROPERTY);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::graph::GraphDefPb& graph_def() const override {
    return graph_def_;
  }

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

    dst_frag->CopyFrom(fragment_, copy_type);

    auto dst_graph_def = graph_def_;
    dst_graph_def.set_key(dst_graph_name);
    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, dst_graph_def, dst_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
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

    dst_frag->ToDirectedFrom(fragment_);

    auto dst_graph_def = graph_def_;
    dst_graph_def.set_key(dst_graph_name);
    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, dst_graph_def, dst_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToUnDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
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

    dst_frag->ToUnDirectedFrom(fragment_);

    auto dst_graph_def = graph_def_;
    dst_graph_def.set_key(dst_graph_name);
    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        dst_graph_name, dst_graph_def, dst_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CreateGraphView(
      const grape::CommSpec& comm_spec, const std::string& view_graph_id,
      const std::string& view_type) override {
    auto frag_view = std::make_shared<fragment_view_t>(
        fragment_.get(), parse_fragment_view_type(view_type));

    auto dst_graph_def = graph_def_;
    dst_graph_def.set_key(view_graph_id);
    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        view_graph_id, dst_graph_def, frag_view);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

 private:
  rpc::graph::GraphDefPb graph_def_;
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
  FragmentWrapper(const std::string& id, rpc::graph::GraphDefPb graph_def,
                  std::shared_ptr<fragment_t> fragment)
      : IFragmentWrapper(id),
        graph_def_(std::move(graph_def)),
        fragment_(std::move(fragment)) {
    CHECK_EQ(graph_def_.graph_type(), rpc::graph::DYNAMIC_PROJECTED);
  }

  std::shared_ptr<void> fragment() const override {
    return std::static_pointer_cast<void>(fragment_);
  }

  const rpc::graph::GraphDefPb& graph_def() const override {
    return graph_def_;
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CopyGraph(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not copy DynamicProjectedFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not to directed DynamicProjectedFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> ToUnDirected(
      const grape::CommSpec& comm_spec,
      const std::string& dst_graph_name) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Can not to undirected DynamicProjectedFragment");
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> CreateGraphView(
      const grape::CommSpec& comm_spec, const std::string& dst_graph_name,
      const std::string& copy_type) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "Cannot generate a graph view over an ArrowFragment.");
  }

 private:
  rpc::graph::GraphDefPb graph_def_;
  std::shared_ptr<fragment_t> fragment_;
};
#endif

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_FRAGMENT_WRAPPER_H_
