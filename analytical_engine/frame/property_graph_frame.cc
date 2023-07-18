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

#include "vineyard/client/client.h"
#include "vineyard/common/util/macros.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/loader/fragment_loader_utils.h"
#include "vineyard/graph/loader/gar_fragment_loader.h"
#include "vineyard/graph/writer/arrow_fragment_writer.h"

#include "core/config.h"
#include "core/error.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/io/property_parser.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/loader/arrow_to_dynamic_converter.h"
#include "core/loader/dynamic_to_arrow_converter.h"
#include "core/object/fragment_wrapper.h"
#include "core/server/rpc_utils.h"
#include "core/utils/fragment_traits.h"
#include "core/vertex_map/arrow_projected_vertex_map.h"
#include "proto/attr_value.pb.h"
#include "proto/graph_def.pb.h"

#if !defined(_GRAPH_TYPE)
#error Missing _GRAPH_TYPE
#endif

using oid_t = typename _GRAPH_TYPE::oid_t;
using vid_t = typename _GRAPH_TYPE::vid_t;
using vertex_map_t = typename _GRAPH_TYPE::vertex_map_t;
static constexpr bool compact_v = _GRAPH_TYPE::compact_v;

namespace bl = boost::leaf;
namespace detail {

__attribute__((visibility(
    "hidden"))) static bl::result<std::shared_ptr<gs::IFragmentWrapper>>
LoadGraph(const grape::CommSpec& comm_spec, vineyard::Client& client,
          const std::string& graph_name, const gs::rpc::GSParams& params) {
  BOOST_LEAF_AUTO(from_vineyard_id,
                  params.Get<bool>(gs::rpc::IS_FROM_VINEYARD_ID));

  if (from_vineyard_id) {
    vineyard::ObjectID frag_group_id = vineyard::InvalidObjectID();
    if (params.HasKey(gs::rpc::VINEYARD_ID)) {
      frag_group_id = params.Get<int64_t>(gs::rpc::VINEYARD_ID).value();
    } else if (params.HasKey(gs::rpc::VINEYARD_NAME)) {
      BOOST_LEAF_AUTO(frag_group_name,
                      params.Get<std::string>(gs::rpc::VINEYARD_NAME));
      auto status = client.GetName(frag_group_name, frag_group_id, false);
      if (!status.ok()) {
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kInvalidValueError,
            "Missing param: VINEYARD_NAME not found: " + status.ToString());
      }
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Missing param: VINEYARD_ID or VINEYARD_NAME");
    }
    auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
        client.GetObject(frag_group_id));
    auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
    auto frag_id = fg->Fragments().at(fid);
    auto frag =
        std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));

    BOOST_LEAF_AUTO(new_frag_group_id, vineyard::ConstructFragmentGroup(
                                           client, frag_id, comm_spec));
    gs::rpc::graph::GraphDefPb graph_def;

    graph_def.set_key(graph_name);
    graph_def.set_compact_edges(frag->compact_edges());
    graph_def.set_use_perfect_hash(frag->use_perfect_hash());
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_vineyard_id(new_frag_group_id);
    vy_info.clear_fragments();
    for (auto const& item : fg->Fragments()) {
      vy_info.add_fragments(item.second);
    }
    graph_def.mutable_extension()->PackFrom(vy_info);
    gs::set_graph_def(frag, graph_def);

    auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
        graph_name, graph_def, frag);
    return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
  } else {
    vineyard::ObjectID frag_group_id = vineyard::InvalidObjectID();
    bool generate_eid = false;
    bool retain_oid = false;
    bool from_gar = params.HasKey(gs::rpc::IS_FROM_GAR)
                        ? params.Get<bool>(gs::rpc::IS_FROM_GAR).value()
                        : false;
    if (from_gar) {
#ifdef ENABLE_GAR
      BOOST_LEAF_AUTO(graph_info_path,
                      params.Get<std::string>(gs::rpc::GRAPH_INFO_PATH));
      BOOST_LEAF_ASSIGN(generate_eid, params.Get<bool>(gs::rpc::GENERATE_EID));
      BOOST_LEAF_ASSIGN(retain_oid, params.Get<bool>(gs::rpc::RETAIN_OID));
      using loader_t =
          vineyard::gar_fragment_loader_t<oid_t, vid_t, vertex_map_t>;
      loader_t loader(client, comm_spec, graph_info_path);
      MPI_Barrier(comm_spec.comm());
      BOOST_LEAF_ASSIGN(frag_group_id, loader.LoadFragmentAsFragmentGroup());
#else
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "The vineyard is not compiled with GAR support");
#endif
    } else {
      BOOST_LEAF_AUTO(graph_info, gs::ParseCreatePropertyGraph(params));
      using loader_t = gs::arrow_fragment_loader_t<oid_t, vid_t, vertex_map_t>;
      loader_t loader(client, comm_spec, graph_info);

      MPI_Barrier(comm_spec.comm());
      {
        vineyard::json __dummy;
        VINEYARD_DISCARD(
            client.GetData(vineyard::InvalidObjectID(), __dummy, true, false));
      }

      BOOST_LEAF_ASSIGN(frag_group_id, loader.LoadFragmentAsFragmentGroup());
      generate_eid = graph_info->generate_eid;
      retain_oid = graph_info->retain_oid;
    }

    MPI_Barrier(comm_spec.comm());
    LOG_IF(INFO, comm_spec.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-SEAL-100";

    MPI_Barrier(comm_spec.comm());
    {
      vineyard::json __dummy;
      VINEYARD_DISCARD(
          client.GetData(vineyard::InvalidObjectID(), __dummy, true, false));
    }

    auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
        client.GetObject(frag_group_id));
    auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
    auto frag_id = fg->Fragments().at(fid);
    auto frag =
        std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));
    gs::rpc::graph::GraphDefPb graph_def;

    graph_def.set_key(graph_name);
    graph_def.set_compact_edges(frag->compact_edges());
    graph_def.set_use_perfect_hash(frag->use_perfect_hash());

    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_vineyard_id(frag_group_id);
    vy_info.clear_fragments();
    for (auto const& item : fg->Fragments()) {
      vy_info.add_fragments(item.second);
    }
    vy_info.set_generate_eid(generate_eid);
    vy_info.set_retain_oid(retain_oid);
    graph_def.mutable_extension()->PackFrom(vy_info);
    gs::set_graph_def(frag, graph_def);

    auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
        graph_name, graph_def, frag);
    return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
  }
}

__attribute__((visibility("hidden"))) static bl::result<void> ArchiveGraph(
    vineyard::ObjectID frag_group_id, const grape::CommSpec& comm_spec,
    vineyard::Client& client, const gs::rpc::GSParams& params) {
#ifdef ENABLE_GAR
  BOOST_LEAF_AUTO(graph_info_path,
                  params.Get<std::string>(gs::rpc::GRAPH_INFO_PATH));

  auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
      client.GetObject(frag_group_id));
  auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
  auto frag_id = fg->Fragments().at(fid);
  auto frag = std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));

  using archive_t = vineyard::ArrowFragmentWriter<_GRAPH_TYPE>;
  archive_t archive(frag, comm_spec, graph_info_path);
  archive.WriteFragment();

  return {};
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                  "The vineyard is not compiled with GAR support");
#endif
}

__attribute__((visibility("hidden")))
bl::result<std::shared_ptr<gs::IFragmentWrapper>>
ToArrowFragment(vineyard::Client& client, const grape::CommSpec& comm_spec,
                std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
                const std::string& dst_graph_name) {
#ifdef NETWORKX
  static_assert(std::is_same<vid_t, gs::DynamicFragment::vid_t>::value,
                "The type of ArrowFragment::vid_t does not match with the "
                "DynamicFragment::vid_t");

  if (wrapper_in->graph_def().graph_type() !=
      gs::rpc::graph::DYNAMIC_PROPERTY) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Source fragment it not DynamicFragment.");
  }
  auto dynamic_frag =
      std::static_pointer_cast<gs::DynamicFragment>(wrapper_in->fragment());

  gs::TransformUtils<gs::DynamicFragment> trans_utils(comm_spec, *dynamic_frag);
  BOOST_LEAF_AUTO(oid_type, trans_utils.GetOidTypeId());

  if (oid_type == vineyard::TypeToInt<int32_t>::value &&
      !std::is_same<oid_t, int32_t>::value &&
      !std::is_same<oid_t, int64_t>::value) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "The oid type of DynamicFragment is int32, but the "
                    "oid type of destination fragment is: " +
                        std::string(vineyard::type_name<oid_t>()));
  }

  if (oid_type == vineyard::TypeToInt<int64_t>::value &&
      !std::is_same<oid_t, int32_t>::value &&
      !std::is_same<oid_t, int64_t>::value) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "The oid type of DynamicFragment is int64, but the "
                    "oid type of destination fragment is: " +
                        std::string(vineyard::type_name<oid_t>()));
  }

  if (oid_type == vineyard::TypeToInt<std::string>::value &&
      !std::is_same<oid_t, std::string>::value) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                    "The oid type of DynamicFragment is string, but the "
                    "oid type of destination fragment is: " +
                        std::string(vineyard::type_name<oid_t>()));
  }

  gs::DynamicToArrowConverter<oid_t, vertex_map_t, compact_v> converter(
      comm_spec, client);
  BOOST_LEAF_AUTO(arrow_frag, converter.Convert(dynamic_frag));
  VINEYARD_CHECK_OK(client.Persist(arrow_frag->id()));
  BOOST_LEAF_AUTO(frag_group_id, vineyard::ConstructFragmentGroup(
                                     client, arrow_frag->id(), comm_spec));
  auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
      client.GetObject(frag_group_id));

  gs::rpc::graph::GraphDefPb graph_def;
  graph_def.set_key(dst_graph_name);
  graph_def.set_compact_edges(arrow_frag->compact_edges());
  graph_def.set_use_perfect_hash(arrow_frag->use_perfect_hash());
  gs::rpc::graph::VineyardInfoPb vy_info;
  if (graph_def.has_extension()) {
    graph_def.extension().UnpackTo(&vy_info);
  }
  vy_info.set_vineyard_id(frag_group_id);
  vy_info.clear_fragments();
  for (auto const& item : fg->Fragments()) {
    vy_info.add_fragments(item.second);
  }
  graph_def.mutable_extension()->PackFrom(vy_info);

  gs::set_graph_def(arrow_frag, graph_def);

  auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
      dst_graph_name, graph_def, arrow_frag);
  return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please "
                  "recompile it with NETWORKX=ON");
#endif
}

__attribute__((visibility(
    "hidden"))) static bl::result<std::shared_ptr<gs::IFragmentWrapper>>
ToDynamicFragment(const grape::CommSpec& comm_spec,
                  std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
                  const std::string& dst_graph_name, int default_label_id) {
#ifdef NETWORKX
  if (wrapper_in->graph_def().graph_type() != gs::rpc::graph::ARROW_PROPERTY) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Source fragment must be ArrowFragment.");
  }
  auto arrow_frag =
      std::static_pointer_cast<_GRAPH_TYPE>(wrapper_in->fragment());
  gs::ArrowToDynamicConverter<_GRAPH_TYPE> converter(comm_spec,
                                                     default_label_id);

  BOOST_LEAF_AUTO(dynamic_frag, converter.Convert(arrow_frag));

  gs::rpc::graph::GraphDefPb graph_def;

  graph_def.set_key(dst_graph_name);
  graph_def.set_directed(dynamic_frag->directed());
  graph_def.set_graph_type(gs::rpc::graph::DYNAMIC_PROPERTY);
  graph_def.set_compact_edges(false);
  graph_def.set_use_perfect_hash(false);
  gs::rpc::graph::MutableGraphInfoPb graph_info;
  if (graph_def.has_extension()) {
    graph_def.extension().UnpackTo(&graph_info);
  }
  graph_info.set_property_schema_json(
      gs::dynamic::Stringify(dynamic_frag->GetSchema()));
  graph_def.mutable_extension()->PackFrom(graph_info);

  auto wrapper = std::make_shared<gs::FragmentWrapper<gs::DynamicFragment>>(
      dst_graph_name, graph_def, dynamic_frag);
  return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
#else
  RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                  "GraphScope is built with NETWORKX=OFF, please recompile "
                  "it with NETWORKX=ON");
#endif
}

__attribute__((visibility(
    "hidden"))) static bl::result<std::shared_ptr<gs::IFragmentWrapper>>
AddLabelsToGraph(vineyard::ObjectID origin_frag_id,
                 const grape::CommSpec& comm_spec, vineyard::Client& client,
                 const std::string& graph_name,
                 const gs::rpc::GSParams& params) {
  BOOST_LEAF_AUTO(graph_info, gs::ParseCreatePropertyGraph(params));
  using loader_t = gs::arrow_fragment_loader_t<oid_t, vid_t, vertex_map_t>;
  loader_t loader(client, comm_spec, graph_info);

  BOOST_LEAF_AUTO(frag_group_id,
                  loader.AddLabelsToFragmentAsFragmentGroup(origin_frag_id));
  MPI_Barrier(comm_spec.comm());

  LOG_IF(INFO, comm_spec.worker_id() == 0)
      << "PROGRESS--GRAPH-LOADING-SEAL-100";

  auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
      client.GetObject(frag_group_id));
  auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
  auto frag_id = fg->Fragments().at(fid);
  auto frag = std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));
  gs::rpc::graph::GraphDefPb graph_def;

  graph_def.set_key(graph_name);
  graph_def.set_compact_edges(frag->compact_edges());
  graph_def.set_use_perfect_hash(frag->use_perfect_hash());

  gs::rpc::graph::VineyardInfoPb vy_info;
  if (graph_def.has_extension()) {
    graph_def.extension().UnpackTo(&vy_info);
  }
  vy_info.set_vineyard_id(frag_group_id);
  vy_info.clear_fragments();
  for (auto const& item : fg->Fragments()) {
    vy_info.add_fragments(item.second);
  }
  vy_info.set_generate_eid(graph_info->generate_eid);
  vy_info.set_retain_oid(graph_info->retain_oid);
  graph_def.mutable_extension()->PackFrom(vy_info);
  gs::set_graph_def(frag, graph_def);

  auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
      graph_name, graph_def, frag);
  return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
}
}  // namespace detail

/**
 * property_graph_frame.cc serves as a frame to be compiled with ArrowFragment.
 * LoadGraph, ArchiveGraph, ToArrowFragment, and ToDynamicFragment functions
 * are provided to proceed with corresponding operations. The frame only needs
 * one macro _GRAPH_TYPE to present which specialized ArrowFragment type will be
 * injected into the frame.
 */
extern "C" {

void LoadGraph(
    const grape::CommSpec& comm_spec, vineyard::Client& client,
    const std::string& graph_name, const gs::rpc::GSParams& params,
    bl::result<std::shared_ptr<gs::IFragmentWrapper>>& fragment_wrapper) {
  __FRAME_CATCH_AND_ASSIGN_GS_ERROR(
      fragment_wrapper,
      detail::LoadGraph(comm_spec, client, graph_name, params));
}

void ArchiveGraph(vineyard::ObjectID frag_id, const grape::CommSpec& comm_spec,
                  vineyard::Client& client, const gs::rpc::GSParams& params,
                  bl::result<void>& result_out) {
  result_out = detail::ArchiveGraph(frag_id, comm_spec, client, params);
}

void ToArrowFragment(
    vineyard::Client& client, const grape::CommSpec& comm_spec,
    std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
    const std::string& dst_graph_name,
    bl::result<std::shared_ptr<gs::IFragmentWrapper>>& wrapper_out) {
  __FRAME_CATCH_AND_ASSIGN_GS_ERROR(
      wrapper_out,
      detail::ToArrowFragment(client, comm_spec, wrapper_in, dst_graph_name));
}

void ToDynamicFragment(
    const grape::CommSpec& comm_spec,
    std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
    const std::string& dst_graph_name, int default_label_id,
    bl::result<std::shared_ptr<gs::IFragmentWrapper>>& wrapper_out) {
  __FRAME_CATCH_AND_ASSIGN_GS_ERROR(
      wrapper_out, detail::ToDynamicFragment(comm_spec, wrapper_in,
                                             dst_graph_name, default_label_id));
}

void AddLabelsToGraph(
    vineyard::ObjectID frag_id, const grape::CommSpec& comm_spec,
    vineyard::Client& client, const std::string& graph_name,
    const gs::rpc::GSParams& params,
    bl::result<std::shared_ptr<gs::IFragmentWrapper>>& fragment_wrapper) {
  __FRAME_CATCH_AND_ASSIGN_GS_ERROR(
      fragment_wrapper,
      detail::AddLabelsToGraph(frag_id, comm_spec, client, graph_name, params));
}

template class vineyard::BasicArrowVertexMapBuilder<
    typename vineyard::InternalType<oid_t>::type, vid_t>;
template class vineyard::ArrowVertexMap<
    typename vineyard::InternalType<oid_t>::type, vid_t>;
template class vineyard::ArrowVertexMapBuilder<
    typename vineyard::InternalType<oid_t>::type, vid_t>;
template class gs::ArrowProjectedVertexMap<
    typename vineyard::InternalType<oid_t>::type, vid_t>;

}  // extern "C"
