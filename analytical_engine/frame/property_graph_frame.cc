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

#include <map>
#include <memory>

#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/config.h"
#include "core/error.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/io/property_parser.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/loader/arrow_to_dynamic_converter.h"
#include "core/loader/dynamic_to_arrow_converter.h"
#include "core/object/fragment_wrapper.h"
#include "core/server/rpc_utils.h"
#include "core/vertex_map/arrow_projected_vertex_map.h"
#include "proto/attr_value.pb.h"
#include "proto/graph_def.pb.h"

#if !defined(_GRAPH_TYPE)
#error Missing _GRAPH_TYPE
#endif

/**
 * property_graph_frame.cc serves as a frame to be compiled with ArrowFragment.
 * LoadGraph, ToArrowFragment, and ToDynamicFragment functions are provided to
 * proceed with corresponding operations. The frame only needs one macro
 * _GRAPH_TYPE to present which specialized ArrowFragment type will be injected
 * into the frame.
 */
extern "C" {

void LoadGraph(
    const grape::CommSpec& comm_spec, vineyard::Client& client,
    const std::string& graph_name, const gs::rpc::GSParams& params,
    gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>>& fragment_wrapper) {
  using oid_t = typename _GRAPH_TYPE::oid_t;
  using vid_t = typename _GRAPH_TYPE::vid_t;

  fragment_wrapper = gs::bl::try_handle_some(
      [&]() -> gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>> {
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
              RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                              "Missing param: VINEYARD_NAME not found: " +
                                  status.ToString());
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
          gs::rpc::graph::VineyardInfoPb vy_info;
          if (graph_def.has_extension()) {
            graph_def.extension().UnpackTo(&vy_info);
          }
          vy_info.set_vineyard_id(new_frag_group_id);
          gs::set_graph_def(frag, graph_def);

          auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
              graph_name, graph_def, frag);
          return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
        } else {
          BOOST_LEAF_AUTO(graph_info, gs::ParseCreatePropertyGraph(params));
          gs::ArrowFragmentLoader<oid_t, vid_t> loader(client, comm_spec,
                                                       graph_info);

          MPI_Barrier(comm_spec.comm());
          {
            vineyard::json __dummy;
            VINEYARD_DISCARD(client.GetData(vineyard::InvalidObjectID(),
                                            __dummy, true, false));
          }

          BOOST_LEAF_AUTO(frag_group_id, loader.LoadFragmentAsFragmentGroup());
          MPI_Barrier(comm_spec.comm());

          LOG_IF(INFO, comm_spec.worker_id() == 0)
              << "PROGRESS--GRAPH-LOADING-SEAL-100";

          MPI_Barrier(comm_spec.comm());
          {
            vineyard::json __dummy;
            VINEYARD_DISCARD(client.GetData(vineyard::InvalidObjectID(),
                                            __dummy, true, false));
          }

          auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
              client.GetObject(frag_group_id));
          auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
          auto frag_id = fg->Fragments().at(fid);
          auto frag =
              std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));
          gs::rpc::graph::GraphDefPb graph_def;

          graph_def.set_key(graph_name);

          gs::rpc::graph::VineyardInfoPb vy_info;
          if (graph_def.has_extension()) {
            graph_def.extension().UnpackTo(&vy_info);
          }
          vy_info.set_vineyard_id(frag_group_id);
          vy_info.set_generate_eid(graph_info->generate_eid);
          graph_def.mutable_extension()->PackFrom(vy_info);
          gs::set_graph_def(frag, graph_def);

          auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
              graph_name, graph_def, frag);
          return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
        }
      });
}

void ToArrowFragment(
    vineyard::Client& client, const grape::CommSpec& comm_spec,
    std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
    const std::string& dst_graph_name,
    gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>>& wrapper_out) {
#ifdef NETWORKX
  using oid_t = typename _GRAPH_TYPE::oid_t;
  using vid_t = typename _GRAPH_TYPE::vid_t;
  static_assert(std::is_same<vid_t, gs::DynamicFragment::vid_t>::value,
                "The type of ArrowFragment::vid_t does not match with the "
                "DynamicFragment::vid_t");
#endif

  wrapper_out = gs::bl::try_handle_some(
      [&]() -> gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>> {
#ifdef NETWORKX
        if (wrapper_in->graph_def().graph_type() !=
            gs::rpc::graph::DYNAMIC_PROPERTY) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                          "Source fragment it not DynamicFragment.");
        }
        auto dynamic_frag = std::static_pointer_cast<gs::DynamicFragment>(
            wrapper_in->fragment());

        BOOST_LEAF_AUTO(oid_type, dynamic_frag->GetOidType(comm_spec));

        if (oid_type == folly::dynamic::Type::INT64 &&
            !std::is_same<oid_t, int32_t>::value &&
            !std::is_same<oid_t, int64_t>::value) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                          "The oid type of DynamicFragment is int64, but the "
                          "oid type of destination fragment is: " +
                              std::string(vineyard::TypeName<oid_t>::Get()));
        }

        if (oid_type == folly::dynamic::Type::STRING &&
            !std::is_same<oid_t, std::string>::value) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                          "The oid type of DynamicFragment is string, but the "
                          "oid type of destination fragment is: " +
                              std::string(vineyard::TypeName<oid_t>::Get()));
        }

        gs::DynamicToArrowConverter<oid_t> converter(comm_spec, client);
        BOOST_LEAF_AUTO(arrow_frag, converter.Convert(dynamic_frag));
        VINEYARD_CHECK_OK(client.Persist(arrow_frag->id()));
        BOOST_LEAF_AUTO(frag_group_id,
                        vineyard::ConstructFragmentGroup(
                            client, arrow_frag->id(), comm_spec));
        gs::rpc::graph::GraphDefPb graph_def;

        graph_def.set_key(dst_graph_name);
        gs::rpc::graph::VineyardInfoPb vy_info;
        if (graph_def.has_extension()) {
          graph_def.extension().UnpackTo(&vy_info);
        }
        vy_info.set_vineyard_id(frag_group_id);
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
      });
}

void ToDynamicFragment(
    const grape::CommSpec& comm_spec,
    std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
    const std::string& dst_graph_name, int default_label_id,
    gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>>& wrapper_out) {
  wrapper_out = gs::bl::try_handle_some([&]() -> gs::bl::result<std::shared_ptr<
                                                  gs::IFragmentWrapper>> {
#ifdef NETWORKX
    if (wrapper_in->graph_def().graph_type() !=
        gs::rpc::graph::ARROW_PROPERTY) {
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
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_oid_type(gs::PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::oid_t>::Get())));
    vy_info.set_vid_type(gs::PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::vid_t>::Get())));
    vy_info.set_vdata_type(gs::PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::vdata_t>::Get())));
    vy_info.set_edata_type(gs::PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename gs::DynamicFragment::edata_t>::Get())));
    vy_info.set_property_schema_json("{}");
    graph_def.mutable_extension()->PackFrom(vy_info);

    auto wrapper = std::make_shared<gs::FragmentWrapper<gs::DynamicFragment>>(
        dst_graph_name, graph_def, dynamic_frag);
    return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
#else
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "GraphScope is built with NETWORKX=OFF, please recompile "
                    "it with NETWORKX=ON");
#endif
  });
}

void AddLabelsToGraph(
    vineyard::ObjectID frag_id, const grape::CommSpec& comm_spec,
    vineyard::Client& client, const std::string& graph_name,
    const gs::rpc::GSParams& params,
    gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>>& fragment_wrapper) {
  using oid_t = typename _GRAPH_TYPE::oid_t;
  using vid_t = typename _GRAPH_TYPE::vid_t;

  fragment_wrapper = gs::bl::try_handle_some(
      [&]() -> gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>> {
        BOOST_LEAF_AUTO(graph_info, gs::ParseCreatePropertyGraph(params));
        gs::ArrowFragmentLoader<oid_t, vid_t> loader(client, comm_spec,
                                                     graph_info);

        BOOST_LEAF_AUTO(frag_group_id,
                        loader.AddLabelsToGraphAsFragmentGroup(frag_id));
        MPI_Barrier(comm_spec.comm());

        LOG_IF(INFO, comm_spec.worker_id() == 0)
            << "PROGRESS--GRAPH-LOADING-SEAL-100";

        auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
            client.GetObject(frag_group_id));
        auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
        auto frag_id = fg->Fragments().at(fid);
        auto frag =
            std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));
        gs::rpc::graph::GraphDefPb graph_def;

        graph_def.set_key(graph_name);

        gs::rpc::graph::VineyardInfoPb vy_info;
        if (graph_def.has_extension()) {
          graph_def.extension().UnpackTo(&vy_info);
        }
        vy_info.set_vineyard_id(frag_group_id);
        vy_info.set_generate_eid(graph_info->generate_eid);
        graph_def.mutable_extension()->PackFrom(vy_info);
        gs::set_graph_def(frag, graph_def);

        auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
            graph_name, graph_def, frag);
        return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
      });
}
template class vineyard::BasicArrowVertexMapBuilder<
    typename vineyard::InternalType<_GRAPH_TYPE::oid_t>::type,
    _GRAPH_TYPE::vid_t>;
template class vineyard::ArrowVertexMap<
    typename vineyard::InternalType<_GRAPH_TYPE::oid_t>::type,
    _GRAPH_TYPE::vid_t>;
template class vineyard::ArrowVertexMapBuilder<
    typename vineyard::InternalType<_GRAPH_TYPE::oid_t>::type,
    _GRAPH_TYPE::vid_t>;
template class gs::ArrowProjectedVertexMap<
    typename vineyard::InternalType<_GRAPH_TYPE::oid_t>::type,
    _GRAPH_TYPE::vid_t>;
}
