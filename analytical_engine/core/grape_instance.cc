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

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"

#include "vineyard/io/io/io_factory.h"

#ifdef ENABLE_JAVA_SDK
#include "core/context/java_pie_projected_context.h"
#include "core/context/java_pie_property_context.h"
#endif

#include "core/context/tensor_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/grape_instance.h"
#include "core/io/property_parser.h"
#include "core/launcher.h"
#include "core/object/app_entry.h"
#include "core/object/graph_utils.h"
#include "core/object/i_fragment_wrapper.h"
#include "core/object/projector.h"
#include "core/server/rpc_utils.h"
#include "proto/types.pb.h"

namespace gs {

GrapeInstance::GrapeInstance(const grape::CommSpec& comm_spec)
    : comm_spec_(comm_spec) {}

void GrapeInstance::Init(const std::string& vineyard_socket) {
  // force link vineyard_io library for graph/app compilation
  vineyard::IOFactory::Init();
  EnsureClient(client_, vineyard_socket);
  if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of grape-engine initialized.";
  }
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::loadGraph(
    const rpc::GSParams& params) {
  std::string graph_name = "graph_" + generateId();

  BOOST_LEAF_AUTO(graph_type,
                  params.Get<rpc::graph::GraphTypePb>(rpc::GRAPH_TYPE));

  switch (graph_type) {
  case rpc::graph::DYNAMIC_PROPERTY: {
#ifdef NETWORKX
    using fragment_t = DynamicFragment;
    using vertex_map_t = typename fragment_t::vertex_map_t;
    BOOST_LEAF_AUTO(directed, params.Get<bool>(rpc::DIRECTED));
    BOOST_LEAF_AUTO(distributed, params.Get<bool>(rpc::DISTRIBUTED));

    VLOG(1) << "Loading graph, graph name: " << graph_name
            << ", graph type: DynamicFragment, directed: " << directed
            << ", distributed: " << distributed;

    auto vm_ptr = std::shared_ptr<vertex_map_t>(new vertex_map_t(comm_spec_));
    vm_ptr->Init();

    auto fragment = std::make_shared<fragment_t>(vm_ptr);
    bool duplicated = !distributed;
    fragment->Init(comm_spec_.fid(), directed, duplicated);

    rpc::graph::GraphDefPb graph_def;

    graph_def.set_key(graph_name);
    graph_def.set_directed(directed);
    graph_def.set_graph_type(rpc::graph::DYNAMIC_PROPERTY);
    // dynamic graph doesn't have a vineyard id
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_vineyard_id(-1);

    vy_info.set_oid_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::oid_t>::Get())));
    vy_info.set_vid_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::vid_t>::Get())));
    vy_info.set_vdata_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::vdata_t>::Get())));
    vy_info.set_edata_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::edata_t>::Get())));
    vy_info.set_property_schema_json("{}");
    graph_def.mutable_extension()->PackFrom(vy_info);

    auto wrapper = std::make_shared<FragmentWrapper<fragment_t>>(
        graph_name, graph_def, fragment);

    BOOST_LEAF_CHECK(object_manager_.PutObject(wrapper));
    return graph_def;
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
  }
  case rpc::graph::ARROW_PROPERTY: {
    BOOST_LEAF_AUTO(type_sig, params.Get<std::string>(rpc::TYPE_SIGNATURE));

    VLOG(1) << "Loading graph, graph name: " << graph_name
            << ", graph type: ArrowFragment, type sig: " << type_sig;

    BOOST_LEAF_AUTO(graph_utils,
                    object_manager_.GetObject<PropertyGraphUtils>(type_sig));
    BOOST_LEAF_AUTO(wrapper, graph_utils->LoadGraph(comm_spec_, *client_,
                                                    graph_name, params));
    BOOST_LEAF_CHECK(object_manager_.PutObject(wrapper));

    return wrapper->graph_def();
  }
  default:
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kInvalidValueError,
        "Unsupported graph type " + rpc::graph::GraphTypePb_Name(graph_type));
  }
}

bl::result<void> GrapeInstance::unloadGraph(const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  if (params.HasKey(rpc::VINEYARD_ID)) {
    BOOST_LEAF_AUTO(frag_group_id, params.Get<int64_t>(rpc::VINEYARD_ID));
    bool exists = false;
    client_->Exists(frag_group_id, exists);
    if (exists) {
      auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
          client_->GetObject(frag_group_id));
      auto fid = comm_spec_.WorkerToFrag(comm_spec_.worker_id());
      auto frag_id = fg->Fragments().at(fid);
      VY_OK_OR_RAISE(client_->DelData(frag_id, false, true));
    }
    MPI_Barrier(comm_spec_.comm());
    if (exists) {
      if (comm_spec_.worker_id() == 0) {
        VINEYARD_SUPPRESS(client_->DelData(frag_group_id, false, true));
      }
    }
  }
  return object_manager_.RemoveObject(graph_name);
}

bl::result<std::string> GrapeInstance::loadApp(const rpc::GSParams& params) {
  std::string app_name = "app_" + generateId();

  BOOST_LEAF_AUTO(lib_path, params.Get<std::string>(rpc::APP_LIBRARY_PATH));

  auto app = std::make_shared<AppEntry>(app_name, lib_path);
  VLOG(1) << "Loading application, application name: " << app_name
          << " , library path: " << lib_path;
  BOOST_LEAF_CHECK(app->Init());
  BOOST_LEAF_CHECK(object_manager_.PutObject(app));
  return app_name;
}

bl::result<void> GrapeInstance::unloadApp(const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(app_name, params.Get<std::string>(rpc::APP_NAME));
  return object_manager_.RemoveObject(app_name);
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::projectGraph(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(project_infos, gs::ParseProjectPropertyGraph(params));
  BOOST_LEAF_AUTO(
      frag_wrapper,
      object_manager_.GetObject<ILabeledFragmentWrapper>(graph_name));

  if (frag_wrapper->graph_def().graph_type() != rpc::graph::ARROW_PROPERTY) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "projectGraph is only available for ArrowFragment");
  }

  std::string dst_graph_name = "graph_" + generateId();

  BOOST_LEAF_AUTO(new_frag_wrapper,
                  frag_wrapper->Project(comm_spec_, dst_graph_name,
                                        project_infos[0], project_infos[1]));
  BOOST_LEAF_CHECK(object_manager_.PutObject(new_frag_wrapper));
  return new_frag_wrapper->graph_def();
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::projectToSimple(
    const rpc::GSParams& params) {
  std::string projected_id = "graph_projected_" + generateId();
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(type_sig, params.Get<std::string>(rpc::TYPE_SIGNATURE));

  VLOG(1) << "Projecting graph, dst graph name: " << graph_name
          << ", type sig: " << type_sig;

  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  BOOST_LEAF_AUTO(projector, object_manager_.GetObject<Projector>(type_sig));
  BOOST_LEAF_AUTO(projected_wrapper,
                  projector->Project(wrapper, projected_id, params));
  BOOST_LEAF_CHECK(object_manager_.PutObject(projected_wrapper));

  return projected_wrapper->graph_def();
}

bl::result<std::string> GrapeInstance::query(const rpc::GSParams& params,
                                             const rpc::QueryArgs& query_args) {
  BOOST_LEAF_AUTO(app_name, params.Get<std::string>(rpc::APP_NAME));
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(app, object_manager_.GetObject<AppEntry>(app_name));
  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));

  auto fragment = wrapper->fragment();
  auto spec = grape::DefaultParallelEngineSpec();
  std::string context_key = "ctx_" + generateId();

  BOOST_LEAF_AUTO(worker, app->CreateWorker(fragment, comm_spec_, spec));
  BOOST_LEAF_AUTO(ctx_wrapper,
                  app->Query(worker.get(), query_args, context_key, wrapper));
  std::string context_type;
  std::string context_schema;
  if (ctx_wrapper != nullptr) {
    context_type = ctx_wrapper->context_type();
    VLOG(0) << "context type: " << context_type;
    context_schema = ctx_wrapper->schema();
    BOOST_LEAF_CHECK(object_manager_.PutObject(ctx_wrapper));
  }
  return toJson({{"context_type", context_type},
                 {"context_key", context_key},
                 {"context_schema", context_schema}});
}

bl::result<void> GrapeInstance::unloadContext(const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  return object_manager_.RemoveObject(context_key);
}

bl::result<std::string> GrapeInstance::reportGraph(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  return wrapper->ReportGraph(comm_spec_, params);
}

bl::result<void> GrapeInstance::modifyVertices(
    const rpc::GSParams& params, const std::vector<std::string>& vertices) {
#ifdef NETWORKX
  BOOST_LEAF_AUTO(modify_type, params.Get<rpc::ModifyType>(rpc::MODIFY_TYPE));
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  auto graph_type = wrapper->graph_def().graph_type();

  if (graph_type != rpc::graph::DYNAMIC_PROPERTY) {
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kInvalidValueError,
        "GraphType must be DYNAMIC_PROPERTY, the origin graph type is:  " +
            rpc::graph::GraphTypePb_Name(graph_type) +
            ", graph id: " + graph_name);
  }

  auto fragment =
      std::static_pointer_cast<DynamicFragment>(wrapper->fragment());
  fragment->ModifyVertices(vertices, modify_type);
  return {};
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif
}

bl::result<void> GrapeInstance::modifyEdges(
    const rpc::GSParams& params, const std::vector<std::string>& edges) {
#ifdef NETWORKX
  BOOST_LEAF_AUTO(modify_type, params.Get<rpc::ModifyType>(rpc::MODIFY_TYPE));
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  auto graph_type = wrapper->graph_def().graph_type();

  if (graph_type != rpc::graph::DYNAMIC_PROPERTY) {
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kInvalidValueError,
        "GraphType must be DYNAMIC_PROPERTY, the origin graph type is: " +
            std::to_string(graph_type) + ", graph name: " + graph_name);
  }

  auto fragment =
      std::static_pointer_cast<DynamicFragment>(wrapper->fragment());
  fragment->ModifyEdges(edges, modify_type);
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif  // NETWORKX
  return {};
}

bl::result<std::shared_ptr<grape::InArchive>> GrapeInstance::contextToNumpy(
    const rpc::GSParams& params) {
  std::pair<std::string, std::string> range;
  std::string s_selector;

  if (params.HasKey(rpc::VERTEX_RANGE)) {
    BOOST_LEAF_AUTO(range_in_json, params.Get<std::string>(rpc::VERTEX_RANGE));
    range = parseRange(range_in_json);
  }

  if (params.HasKey(rpc::SELECTOR)) {
    BOOST_LEAF_ASSIGN(s_selector, params.Get<std::string>(rpc::SELECTOR));
  }

  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  BOOST_LEAF_AUTO(base_ctx_wrapper,
                  object_manager_.GetObject<IContextWrapper>(context_key));

  auto ctx_type = base_ctx_wrapper->context_type();

  if (ctx_type == CONTEXT_TYPE_TENSOR) {
    auto wrapper =
        std::dynamic_pointer_cast<ITensorContextWrapper>(base_ctx_wrapper);
    BOOST_LEAF_AUTO(axis, params.Get<int64_t>(rpc::AXIS));

    return wrapper->ToNdArray(comm_spec_, axis);
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_DATA) {
    auto wrapper =
        std::dynamic_pointer_cast<IVertexDataContextWrapper>(base_ctx_wrapper);

    BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
    return wrapper->ToNdArray(comm_spec_, selector, range);
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_DATA) {
    auto wrapper = std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
        base_ctx_wrapper);

    BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
    return wrapper->ToNdArray(comm_spec_, selector, range);
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_PROPERTY) {
    auto wrapper = std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
        base_ctx_wrapper);

    BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
    return wrapper->ToNdArray(comm_spec_, selector, range);
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
    auto wrapper =
        std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
            base_ctx_wrapper);

    BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
    return wrapper->ToNdArray(comm_spec_, selector, range);
#ifdef ENABLE_JAVA_SDK
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROPERTY) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Unsupported java property context type: " + std::string(ctx_type));
    }
    auto wrapper = std::dynamic_pointer_cast<IJavaPIEPropertyContextWrapper>(
        base_ctx_wrapper);
    BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
    return wrapper->ToNdArray(comm_spec_, selector, range);
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROJECTED) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "Unsupported java projected context type: " + std::string(ctx_type));
    }
    auto wrapper = std::dynamic_pointer_cast<IJavaPIEProjectedContextWrapper>(
        base_ctx_wrapper);
    BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
    return wrapper->ToNdArray(comm_spec_, selector, range);
#endif
  }
  RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                  "Unsupported context type: " + std::string(ctx_type));
}

bl::result<std::string> GrapeInstance::getContextData(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  BOOST_LEAF_AUTO(base_ctx_wrapper,
                  object_manager_.GetObject<IContextWrapper>(context_key));

  auto wrapper =
      std::dynamic_pointer_cast<IVertexDataContextWrapper>(base_ctx_wrapper);
  return wrapper->GetContextData(params);
}

bl::result<std::shared_ptr<grape::InArchive>> GrapeInstance::contextToDataframe(
    const rpc::GSParams& params) {
  std::pair<std::string, std::string> range;
  std::string s_selectors;

  if (params.HasKey(rpc::VERTEX_RANGE)) {
    BOOST_LEAF_AUTO(range_in_json, params.Get<std::string>(rpc::VERTEX_RANGE));
    range = parseRange(range_in_json);
  }

  if (params.HasKey(rpc::SELECTOR)) {
    BOOST_LEAF_ASSIGN(s_selectors, params.Get<std::string>(rpc::SELECTOR));
  }

  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  BOOST_LEAF_AUTO(base_ctx_wrapper,
                  object_manager_.GetObject<IContextWrapper>(context_key));

  auto ctx_type = base_ctx_wrapper->context_type();

  if (ctx_type == CONTEXT_TYPE_TENSOR) {
    auto wrapper =
        std::dynamic_pointer_cast<ITensorContextWrapper>(base_ctx_wrapper);

    return wrapper->ToDataframe(comm_spec_);
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_DATA) {
    auto wrapper =
        std::dynamic_pointer_cast<IVertexDataContextWrapper>(base_ctx_wrapper);

    BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
    return wrapper->ToDataframe(comm_spec_, selectors, range);
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_DATA) {
    auto wrapper = std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
        base_ctx_wrapper);

    BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
    return wrapper->ToDataframe(comm_spec_, selectors, range);
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_PROPERTY) {
    auto wrapper = std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
        base_ctx_wrapper);

    BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
    return wrapper->ToDataframe(comm_spec_, selectors, range);
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
    auto wrapper =
        std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
            base_ctx_wrapper);

    BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
    return wrapper->ToDataframe(comm_spec_, selectors, range);
#ifdef ENABLE_JAVA_SDK
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROPERTY) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Unsupported java property context type: " + std::string(ctx_type));
    }
    auto wrapper = std::dynamic_pointer_cast<IJavaPIEPropertyContextWrapper>(
        base_ctx_wrapper);
    BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
    return wrapper->ToDataframe(comm_spec_, selectors, range);
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROJECTED) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidValueError,
          "Unsupported java projected context type: " + std::string(ctx_type));
    }
    auto wrapper = std::dynamic_pointer_cast<IJavaPIEProjectedContextWrapper>(
        base_ctx_wrapper);
    BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
    return wrapper->ToDataframe(comm_spec_, selectors, range);
#endif
  }
  RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                  "Unsupported context type: " + std::string(ctx_type));
}

bl::result<std::string> GrapeInstance::contextToVineyardTensor(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  BOOST_LEAF_AUTO(base_ctx_wrapper,
                  object_manager_.GetObject<IContextWrapper>(context_key));
  auto ctx_type = base_ctx_wrapper->context_type();
  std::pair<std::string, std::string> range;

  if (params.HasKey(rpc::VERTEX_RANGE)) {
    BOOST_LEAF_AUTO(range_in_json, params.Get<std::string>(rpc::VERTEX_RANGE));
    range = parseRange(range_in_json);
  }

  vineyard::ObjectID id;

  if (ctx_type == CONTEXT_TYPE_TENSOR) {
    auto wrapper =
        std::dynamic_pointer_cast<ITensorContextWrapper>(base_ctx_wrapper);
    BOOST_LEAF_AUTO(axis, params.Get<int64_t>(rpc::AXIS));
    BOOST_LEAF_ASSIGN(id,
                      wrapper->ToVineyardTensor(comm_spec_, *client_, axis));
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_DATA) {
    auto wrapper =
        std::dynamic_pointer_cast<IVertexDataContextWrapper>(base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
    BOOST_LEAF_ASSIGN(
        id, wrapper->ToVineyardTensor(comm_spec_, *client_, selector, range));
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_DATA) {
    auto wrapper = std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
        base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
    BOOST_LEAF_ASSIGN(
        id, wrapper->ToVineyardTensor(comm_spec_, *client_, selector, range));
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_PROPERTY) {
    auto wrapper = std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
        base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
    BOOST_LEAF_ASSIGN(
        id, wrapper->ToVineyardTensor(comm_spec_, *client_, selector, range));
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
    auto wrapper =
        std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
            base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
    BOOST_LEAF_ASSIGN(
        id, wrapper->ToVineyardTensor(comm_spec_, *client_, selector, range));
#ifdef ENABLE_JAVA_SDK
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROPERTY) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Unsupported java property context type: " + std::string(ctx_type));
    }
    auto wrapper = std::dynamic_pointer_cast<IJavaPIEPropertyContextWrapper>(
        base_ctx_wrapper);
    BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
    BOOST_LEAF_ASSIGN(
        id, wrapper->ToVineyardTensor(comm_spec_, *client_, selector, range));
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROJECTED) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Unsupported java projected context type: " + std::string(ctx_type));
    }
    auto wrapper = std::dynamic_pointer_cast<IJavaPIEProjectedContextWrapper>(
        base_ctx_wrapper);
    BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
    BOOST_LEAF_ASSIGN(
        id, wrapper->ToVineyardTensor(comm_spec_, *client_, selector, range));
#endif
  } else {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Unsupported context type: " + std::string(ctx_type));
  }

  auto s_id = vineyard::ObjectIDToString(id);

  client_->PutName(id, s_id);

  return toJson({{"object_id", s_id}});
}

bl::result<std::string> GrapeInstance::contextToVineyardDataFrame(
    const rpc::GSParams& params) {
  std::pair<std::string, std::string> range;

  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  BOOST_LEAF_AUTO(base_ctx_wrapper,
                  object_manager_.GetObject<IContextWrapper>(context_key));
  if (params.HasKey(rpc::VERTEX_RANGE)) {
    BOOST_LEAF_AUTO(range_in_json, params.Get<std::string>(rpc::VERTEX_RANGE));
    range = parseRange(range_in_json);
  }

  vineyard::ObjectID id;
  auto ctx_type = base_ctx_wrapper->context_type();

  if (ctx_type == CONTEXT_TYPE_TENSOR) {
    auto wrapper =
        std::dynamic_pointer_cast<ITensorContextWrapper>(base_ctx_wrapper);

    BOOST_LEAF_ASSIGN(id, wrapper->ToVineyardDataframe(comm_spec_, *client_));
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_DATA) {
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<IVertexDataContextWrapper>(base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
    BOOST_LEAF_ASSIGN(id, vd_ctx_wrapper->ToVineyardDataframe(
                              comm_spec_, *client_, selectors, range));
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_DATA) {
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
            base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
    BOOST_LEAF_ASSIGN(id, vd_ctx_wrapper->ToVineyardDataframe(
                              comm_spec_, *client_, selectors, range));
  } else if (ctx_type == CONTEXT_TYPE_VERTEX_PROPERTY) {
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
            base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
    BOOST_LEAF_ASSIGN(id, vd_ctx_wrapper->ToVineyardDataframe(
                              comm_spec_, *client_, selectors, range));
  } else if (ctx_type == CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
            base_ctx_wrapper);

    BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
    BOOST_LEAF_ASSIGN(id, vd_ctx_wrapper->ToVineyardDataframe(
                              comm_spec_, *client_, selectors, range));
#ifdef ENABLE_JAVA_SDK
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROPERTY) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Unsupported java property context type: " + std::string(ctx_type));
    }
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<IJavaPIEPropertyContextWrapper>(
            base_ctx_wrapper);
    BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));
    BOOST_LEAF_ASSIGN(id, vd_ctx_wrapper->ToVineyardDataframe(
                              comm_spec_, *client_, selectors, range));
  } else if (ctx_type.find(CONTEXT_TYPE_JAVA_PIE_PROJECTED) !=
             std::string::npos) {
    std::vector<std::string> outer_and_inner;
    boost::split(outer_and_inner, ctx_type, boost::is_any_of(":"));
    if (outer_and_inner.size() != 2) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kIllegalStateError,
          "Unsupported java projected context type: " + std::string(ctx_type));
    }
    auto vd_ctx_wrapper =
        std::dynamic_pointer_cast<IJavaPIEProjectedContextWrapper>(
            base_ctx_wrapper);
    BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
    BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(s_selectors));
    BOOST_LEAF_ASSIGN(id, vd_ctx_wrapper->ToVineyardDataframe(
                              comm_spec_, *client_, selectors, range));
#endif
  } else {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Unsupported context type: " + std::string(ctx_type));
  }

  auto s_id = vineyard::ObjectIDToString(id);

  client_->PutName(id, s_id);

  return toJson({{"object_id", s_id}});
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::addColumn(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
  BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
  BOOST_LEAF_AUTO(
      frag_wrapper,
      object_manager_.GetObject<ILabeledFragmentWrapper>(graph_name));

  if (frag_wrapper->graph_def().graph_type() != rpc::graph::ARROW_PROPERTY) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "AddColumn is only available for ArrowFragment");
  }
  BOOST_LEAF_AUTO(ctx_wrapper,
                  object_manager_.GetObject<IContextWrapper>(context_key));
  std::string dst_graph_name = "graph_" + generateId();

  BOOST_LEAF_AUTO(new_frag_wrapper,
                  frag_wrapper->AddColumn(comm_spec_, dst_graph_name,
                                          ctx_wrapper, s_selectors));
  BOOST_LEAF_CHECK(object_manager_.PutObject(new_frag_wrapper));
  return new_frag_wrapper->graph_def();
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::convertGraph(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(src_graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(dst_graph_type,
                  params.Get<rpc::graph::GraphTypePb>(rpc::DST_GRAPH_TYPE));
  BOOST_LEAF_AUTO(type_sig, params.Get<std::string>(rpc::TYPE_SIGNATURE));
  std::string dst_graph_name = "graph_" + generateId();

  VLOG(1) << "Converting graph, src graph name: " << src_graph_name
          << ", dst graph name: " << dst_graph_name << ", dst graph type: "
          << rpc::graph::GraphTypePb_Name(dst_graph_type)
          << ", type_sig: " << type_sig;

  BOOST_LEAF_AUTO(g_utils,
                  object_manager_.GetObject<PropertyGraphUtils>(type_sig));
  BOOST_LEAF_AUTO(src_frag_wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(src_graph_name));

  auto src_graph_type = src_frag_wrapper->graph_def().graph_type();

  if (src_graph_type == rpc::graph::ARROW_PROPERTY &&
      dst_graph_type == rpc::graph::DYNAMIC_PROPERTY) {
    BOOST_LEAF_AUTO(default_label_id,
                    params.Get<int64_t>(rpc::DEFAULT_LABEL_ID));
    BOOST_LEAF_AUTO(dst_graph_wrapper, g_utils->ToDynamicFragment(
                                           comm_spec_, src_frag_wrapper,
                                           dst_graph_name, default_label_id));
    BOOST_LEAF_CHECK(object_manager_.PutObject(dst_graph_wrapper));
    return dst_graph_wrapper->graph_def();
  } else if (src_graph_type == rpc::graph::DYNAMIC_PROPERTY &&
             dst_graph_type == rpc::graph::ARROW_PROPERTY) {
    BOOST_LEAF_AUTO(dst_graph_wrapper,
                    g_utils->ToArrowFragment(*client_, comm_spec_,
                                             src_frag_wrapper, dst_graph_name));
    BOOST_LEAF_CHECK(object_manager_.PutObject(dst_graph_wrapper));
    return dst_graph_wrapper->graph_def();
  }
  RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                  "Unsupported conversion direction from " +
                      rpc::graph::GraphTypePb_Name(src_graph_type) + " to " +
                      rpc::graph::GraphTypePb_Name(dst_graph_type));
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::copyGraph(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(src_graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(copy_type, params.Get<std::string>(rpc::COPY_TYPE));

  BOOST_LEAF_AUTO(src_wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(src_graph_name));
  std::string dst_graph_name = "graph_" + generateId();

  BOOST_LEAF_AUTO(dst_wrapper, src_wrapper->CopyGraph(
                                   comm_spec_, dst_graph_name, copy_type));
  BOOST_LEAF_CHECK(object_manager_.PutObject(dst_wrapper));
  return dst_wrapper->graph_def();
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::toDirected(
    const rpc::GSParams& params) {
#ifdef NETWORKX
  BOOST_LEAF_AUTO(src_graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  // BOOST_LEAF_AUTO(copy_type, params.Get<std::string>(rpc::COPY_TYPE));

  BOOST_LEAF_AUTO(src_wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(src_graph_name));
  std::string dst_graph_name = "graph_" + generateId();

  BOOST_LEAF_AUTO(dst_wrapper,
                  src_wrapper->ToDirected(comm_spec_, dst_graph_name));
  BOOST_LEAF_CHECK(object_manager_.PutObject(dst_wrapper));
  return dst_wrapper->graph_def();
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif  // NETWORKX
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::toUnDirected(
    const rpc::GSParams& params) {
#ifdef NETWORKX
  BOOST_LEAF_AUTO(src_graph_name, params.Get<std::string>(rpc::GRAPH_NAME));

  BOOST_LEAF_AUTO(src_wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(src_graph_name));
  std::string dst_graph_name = "graph_" + generateId();

  BOOST_LEAF_AUTO(dst_wrapper,
                  src_wrapper->ToUndirected(comm_spec_, dst_graph_name));
  BOOST_LEAF_CHECK(object_manager_.PutObject(dst_wrapper));
  return dst_wrapper->graph_def();
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif  // NETWORKX
}

#ifdef NETWORKX
bl::result<rpc::graph::GraphDefPb> GrapeInstance::induceSubGraph(
    const rpc::GSParams& params,
    const std::unordered_set<typename DynamicFragment::oid_t>& induced_vertices,
    const std::vector<std::pair<typename DynamicFragment::oid_t,
                                typename DynamicFragment::oid_t>>&
        induced_edges) {
  BOOST_LEAF_AUTO(src_graph_name, params.Get<std::string>(rpc::GRAPH_NAME));

  BOOST_LEAF_AUTO(src_wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(src_graph_name));
  std::string sub_graph_name = "graph_" + generateId();

  VLOG(1) << "Inducing subgraph from " << src_graph_name
          << ", graph name: " << sub_graph_name;

  auto fragment =
      std::static_pointer_cast<DynamicFragment>(src_wrapper->fragment());

  auto sub_vm_ptr =
      std::make_shared<typename DynamicFragment::vertex_map_t>(comm_spec_);
  sub_vm_ptr->Init();
  typename DynamicFragment::partitioner_t partitioner;
  partitioner.Init(fragment->fnum());
  typename DynamicFragment::vid_t gid;
  for (auto& v : induced_vertices) {
    auto fid = partitioner.GetPartitionId(v);
    if (fid == fragment->fid() && fragment->HasNode(v)) {
      sub_vm_ptr->AddVertex(fid, v, gid);
    }
  }
  sub_vm_ptr->Construct();

  auto sub_graph_def = src_wrapper->graph_def();
  sub_graph_def.set_key(sub_graph_name);
  auto sub_frag = std::make_shared<DynamicFragment>(sub_vm_ptr);
  sub_frag->InduceSubgraph(fragment, induced_vertices, induced_edges);

  auto wrapper = std::make_shared<FragmentWrapper<DynamicFragment>>(
      sub_graph_name, sub_graph_def, sub_frag);

  BOOST_LEAF_CHECK(object_manager_.PutObject(wrapper));
  return wrapper->graph_def();
}
#endif  // NETWORKX

bl::result<void> GrapeInstance::clearGraph(const rpc::GSParams& params) {
#ifdef NETWORKX
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  auto graph_type = wrapper->graph_def().graph_type();

  if (graph_type != rpc::graph::DYNAMIC_PROPERTY) {
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kInvalidValueError,
        "GraphType must be DYNAMIC_PROPERTY, the origin graph type is: " +
            rpc::graph::GraphTypePb_Name(graph_type) +
            ", graph id: " + graph_name);
  }

  auto vm_ptr = std::shared_ptr<DynamicFragment::vertex_map_t>(
      new DynamicFragment::vertex_map_t(comm_spec_));
  vm_ptr->Init();
  auto fragment =
      std::static_pointer_cast<DynamicFragment>(wrapper->fragment());
  fragment->ClearGraph(vm_ptr);
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif  // NETWORKX
  return {};
}

bl::result<void> GrapeInstance::clearEdges(const rpc::GSParams& params) {
#ifdef NETWORKX
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  auto graph_type = wrapper->graph_def().graph_type();

  if (graph_type != rpc::graph::DYNAMIC_PROPERTY) {
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kInvalidValueError,
        "GraphType must be DYNAMIC_PROPERTY, the origin graph type is: " +
            rpc::graph::GraphTypePb_Name(graph_type) +
            ", graph id: " + graph_name);
  }

  auto fragment =
      std::static_pointer_cast<DynamicFragment>(wrapper->fragment());
  fragment->ClearEdges();
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif  // NETWORKX
  return {};
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::createGraphView(
    const rpc::GSParams& params) {
#ifdef NETWORKX
  std::string view_id = "graph_view_" + generateId();
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(view_type, params.Get<std::string>(rpc::VIEW_TYPE));

  VLOG(1) << "Creating graph view, dst graph name: " << view_id
          << ", view type: " << view_type;

  BOOST_LEAF_AUTO(wrapper,
                  object_manager_.GetObject<IFragmentWrapper>(graph_name));
  BOOST_LEAF_AUTO(view_wrapper,
                  wrapper->CreateGraphView(comm_spec_, view_id, view_type));
  BOOST_LEAF_CHECK(object_manager_.PutObject(view_wrapper));

  return view_wrapper->graph_def();
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile it "
                  "with NETWORKX=ON");
#endif  // NETWORKX
}

bl::result<rpc::graph::GraphDefPb> GrapeInstance::addLabelsToGraph(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(
      src_wrapper,
      object_manager_.GetObject<ILabeledFragmentWrapper>(graph_name));
  if (src_wrapper->graph_def().graph_type() != rpc::graph::ARROW_PROPERTY) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "AddLabelsToGraph is only avaiable for ArrowFragment");
  }

  auto src_frag_id =
      std::static_pointer_cast<vineyard::Object>(src_wrapper->fragment())->id();
  BOOST_LEAF_AUTO(type_sig, params.Get<std::string>(rpc::TYPE_SIGNATURE));
  BOOST_LEAF_AUTO(graph_utils,
                  object_manager_.GetObject<PropertyGraphUtils>(type_sig));
  std::string dst_graph_name = "graph_" + generateId();
  BOOST_LEAF_AUTO(dst_wrapper, graph_utils->AddLabelsToGraph(
                                   src_frag_id, comm_spec_, *client_,
                                   dst_graph_name, params));
  BOOST_LEAF_CHECK(object_manager_.PutObject(dst_wrapper));

  return dst_wrapper->graph_def();
}

bl::result<std::shared_ptr<grape::InArchive>> GrapeInstance::graphToNumpy(
    const rpc::GSParams& params) {
  std::pair<std::string, std::string> range;

  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));
  BOOST_LEAF_AUTO(s_selector, params.Get<std::string>(rpc::SELECTOR));
  BOOST_LEAF_AUTO(
      wrapper, object_manager_.GetObject<ILabeledFragmentWrapper>(graph_name));

  if (params.HasKey(rpc::VERTEX_RANGE)) {
    BOOST_LEAF_AUTO(range_in_json, params.Get<std::string>(rpc::VERTEX_RANGE));
    range = parseRange(range_in_json);
  }
  BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));

  return wrapper->ToNdArray(comm_spec_, selector, range);
}

bl::result<std::shared_ptr<grape::InArchive>> GrapeInstance::graphToDataframe(
    const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_name, params.Get<std::string>(rpc::GRAPH_NAME));

  BOOST_LEAF_AUTO(
      wrapper, object_manager_.GetObject<ILabeledFragmentWrapper>(graph_name));

  std::pair<std::string, std::string> range;

  if (params.HasKey(rpc::VERTEX_RANGE)) {
    BOOST_LEAF_AUTO(range_in_json, params.Get<std::string>(rpc::VERTEX_RANGE));
    range = parseRange(range_in_json);
  }

  BOOST_LEAF_AUTO(s_selectors, params.Get<std::string>(rpc::SELECTOR));
  BOOST_LEAF_AUTO(selectors, LabeledSelector::ParseSelectors(s_selectors));

  return wrapper->ToDataframe(comm_spec_, selectors, range);
}

bl::result<void> GrapeInstance::registerGraphType(const rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_type,
                  params.Get<rpc::graph::GraphTypePb>(rpc::GRAPH_TYPE));
  BOOST_LEAF_AUTO(type_sig, params.Get<std::string>(rpc::TYPE_SIGNATURE));
  BOOST_LEAF_AUTO(lib_path, params.Get<std::string>(rpc::GRAPH_LIBRARY_PATH));

  VLOG(1) << "Registering Graph, graph type: "
          << rpc::graph::GraphTypePb_Name(graph_type)
          << ", Type sigature: " << type_sig << ", lib path: " << lib_path;

  if (object_manager_.HasObject(type_sig)) {
    VLOG(1) << "Graph already registered, signature is: " << type_sig;
    return {};
  }

  if (graph_type == rpc::graph::ARROW_PROPERTY) {
    auto utils = std::make_shared<PropertyGraphUtils>(type_sig, lib_path);
    BOOST_LEAF_CHECK(utils->Init());
    return object_manager_.PutObject(utils);
  } else if (graph_type == rpc::graph::ARROW_PROJECTED ||
             graph_type == rpc::graph::DYNAMIC_PROJECTED ||
             graph_type == rpc::graph::ARROW_FLATTENED) {
    auto projector = std::make_shared<Projector>(type_sig, lib_path);
    BOOST_LEAF_CHECK(projector->Init());
    return object_manager_.PutObject(projector);
  } else {
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kInvalidValueError,
        "Unsupported graph type: " + rpc::graph::GraphTypePb_Name(graph_type));
  }
}

bl::result<std::shared_ptr<DispatchResult>> GrapeInstance::OnReceive(
    const CommandDetail& cmd) {
  auto r = std::make_shared<DispatchResult>(comm_spec_.worker_id());
  rpc::GSParams params(cmd.params);

  switch (cmd.type) {
  case rpc::CREATE_GRAPH: {
    BOOST_LEAF_AUTO(graph_def, loadGraph(params));
    r->set_graph_def(graph_def);
    break;
  }
  case rpc::CREATE_APP: {
    // do nothing
    break;
  }
  case rpc::BIND_APP: {
    BOOST_LEAF_AUTO(app_name, loadApp(params));
    r->set_data(app_name);
    break;
  }
  case rpc::RUN_APP: {
    BOOST_LEAF_AUTO(context_key, query(params, cmd.query_args));
    r->set_data(context_key);
    break;
  }
  case rpc::UNLOAD_APP: {
    BOOST_LEAF_CHECK(unloadApp(params));
    break;
  }
  case rpc::UNLOAD_GRAPH: {
    BOOST_LEAF_CHECK(unloadGraph(params));
    break;
  }
  case rpc::UNLOAD_CONTEXT: {
    BOOST_LEAF_CHECK(unloadContext(params));
    break;
  }
  case rpc::REPORT_GRAPH: {
    BOOST_LEAF_AUTO(report_in_json, reportGraph(params));
    r->set_data(report_in_json,
                DispatchResult::AggregatePolicy::kPickFirstNonEmpty);
    break;
  }
  case rpc::PROJECT_GRAPH: {
    BOOST_LEAF_AUTO(graph_def, projectGraph(params));
    r->set_graph_def(graph_def);
    break;
  }
  case rpc::PROJECT_TO_SIMPLE: {
    BOOST_LEAF_AUTO(graph_def, projectToSimple(params));
    r->set_graph_def(graph_def);
    break;
  }
  case rpc::MODIFY_VERTICES: {
#ifdef NETWORKX
    std::vector<std::string> vertices_to_modify;
    int size = cmd.params.at(rpc::NODES).list().s_size();
    vertices_to_modify.reserve(size);
    for (int i = 0; i < size; ++i) {
      vertices_to_modify.push_back(cmd.params.at(rpc::NODES).list().s(i));
    }
    BOOST_LEAF_CHECK(modifyVertices(params, vertices_to_modify));
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::MODIFY_EDGES: {
#ifdef NETWORKX
    std::vector<std::string> edges_to_modify;
    int size = cmd.params.at(rpc::EDGES).list().s_size();
    edges_to_modify.reserve(size);
    for (int i = 0; i < size; ++i) {
      edges_to_modify.push_back(cmd.params.at(rpc::EDGES).list().s(i));
    }
    BOOST_LEAF_CHECK(modifyEdges(params, edges_to_modify));
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::TRANSFORM_GRAPH: {
#ifdef NETWORKX
    BOOST_LEAF_AUTO(graph_def, convertGraph(params));
    r->set_graph_def(graph_def);
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::COPY_GRAPH: {
    BOOST_LEAF_AUTO(graph_def, copyGraph(params));
    r->set_graph_def(graph_def);
    break;
  }
  case rpc::TO_DIRECTED: {
#ifdef NETWORKX
    BOOST_LEAF_AUTO(graph_def, toDirected(params));
    r->set_graph_def(graph_def);
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::TO_UNDIRECTED: {
#ifdef NETWORKX
    BOOST_LEAF_AUTO(graph_def, toUnDirected(params));
    r->set_graph_def(graph_def);
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::INDUCE_SUBGRAPH: {
#ifdef NETWORKX
    std::unordered_set<DynamicFragment::oid_t> induced_vertices;
    std::vector<std::pair<DynamicFragment::oid_t, DynamicFragment::oid_t>>
        induced_edges;
    auto line_parser_ptr = std::make_unique<DynamicLineParser>();
    if (params.HasKey(rpc::NODES)) {
      // induce subgraph from nodes.
      int size = cmd.params.at(rpc::NODES).list().s_size();
      induced_vertices.reserve(size);
      DynamicFragment::oid_t oid;
      DynamicFragment::vdata_t vdata;
      for (int i = 0; i < size; ++i) {
        line_parser_ptr->LineParserForVFile(
            cmd.params.at(rpc::NODES).list().s(i), oid, vdata);
        induced_vertices.insert(oid);
      }
    } else if (params.HasKey(rpc::EDGES)) {
      // induce subgraph from edges.
      int size = cmd.params.at(rpc::EDGES).list().s_size();
      induced_edges.reserve(size);
      DynamicFragment::oid_t u_oid, v_oid;
      DynamicFragment::edata_t edata;
      for (int i = 0; i < size; ++i) {
        line_parser_ptr->LineParserForEFile(
            cmd.params.at(rpc::EDGES).list().s(i), u_oid, v_oid, edata);
        induced_vertices.insert(u_oid);
        induced_vertices.insert(v_oid);
        induced_edges.emplace_back(u_oid, v_oid);
      }
    }
    BOOST_LEAF_AUTO(graph_def,
                    induceSubGraph(params, induced_vertices, induced_edges));
    r->set_graph_def(graph_def);
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::CLEAR_GRAPH: {
#ifdef NETWORKX
    BOOST_LEAF_CHECK(clearGraph(params));
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::CLEAR_EDGES: {
#ifdef NETWORKX
    BOOST_LEAF_CHECK(clearEdges(params));
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::VIEW_GRAPH: {
#ifdef NETWORKX
    BOOST_LEAF_AUTO(graph_def, createGraphView(params));
    r->set_graph_def(graph_def);
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
    break;
  }
  case rpc::ADD_LABELS: {
    BOOST_LEAF_AUTO(graph_def, addLabelsToGraph(params));
    r->set_graph_def(graph_def);
    break;
  }
  case rpc::CONTEXT_TO_NUMPY: {
    BOOST_LEAF_AUTO(arc, contextToNumpy(params));
    r->set_data(*arc, DispatchResult::AggregatePolicy::kPickFirst);
    break;
  }
  case rpc::CONTEXT_TO_DATAFRAME: {
    BOOST_LEAF_AUTO(arc, contextToDataframe(params));
    r->set_data(*arc, DispatchResult::AggregatePolicy::kPickFirst);
    break;
  }
  case rpc::TO_VINEYARD_TENSOR: {
    BOOST_LEAF_AUTO(vy_obj_id_in_json, contextToVineyardTensor(params));
    r->set_data(vy_obj_id_in_json);
    break;
  }
  case rpc::TO_VINEYARD_DATAFRAME: {
    BOOST_LEAF_AUTO(vy_obj_id_in_json, contextToVineyardDataFrame(params));
    r->set_data(vy_obj_id_in_json);
    break;
  }
  case rpc::GET_CONTEXT_DATA: {
    BOOST_LEAF_AUTO(context_json, getContextData(params));
    r->set_data(context_json,
                DispatchResult::AggregatePolicy::kPickFirstNonEmpty);
    break;
  }
  case rpc::ADD_COLUMN: {
    BOOST_LEAF_AUTO(graph_def, addColumn(params));
    r->set_graph_def(graph_def);
    break;
  }
  case rpc::GRAPH_TO_NUMPY: {
    BOOST_LEAF_AUTO(arc, graphToNumpy(params));
    r->set_data(*arc, DispatchResult::AggregatePolicy::kPickFirst);
    break;
  }
  case rpc::GRAPH_TO_DATAFRAME: {
    BOOST_LEAF_AUTO(arc, graphToDataframe(params));
    r->set_data(*arc, DispatchResult::AggregatePolicy::kPickFirst);
    break;
  }
  case rpc::REGISTER_GRAPH_TYPE: {
    BOOST_LEAF_CHECK(registerGraphType(params));
    break;
  }
  case rpc::GET_ENGINE_CONFIG: {
    EngineConfig conf;
#ifdef NETWORKX
    conf.networkx = "ON";
#else
    conf.networkx = "OFF";
#endif
    conf.vineyard_socket = client_->IPCSocket();
    conf.vineyard_rpc_endpoint = client_->RPCEndpoint();
    r->set_data(conf.ToJsonString(),
                DispatchResult::AggregatePolicy::kPickFirst);
    break;
  }
  default:
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Unsupported command type: " + std::to_string(cmd.type));
  }
  return r;
}

}  // namespace gs
