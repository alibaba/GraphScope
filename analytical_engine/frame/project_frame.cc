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
#include <string>

#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/fragment/dynamic_projected_fragment.h"
#include "core/object/fragment_wrapper.h"
#include "core/server/rpc_utils.h"
#include "proto/attr_value.pb.h"

#if !defined(_PROJECTED_GRAPH_TYPE)
#error "_PROJECTED_GRAPH_TYPE is undefined"
#endif

/**
 * project_frame.cc serves as a frame to be compiled with
 * ArrowProjectedFragment/DynamicProjectedFragment. The frame will be compiled
 * when the client issues a PROJECT_TO_SIMPLE request. Then, a library will be
 * produced based on the frame. The reason we need the frame is the template
 * parameters are unknown before the project request has arrived at the
 * analytical engine. A dynamic library is necessary to prevent hardcode data
 * type in the engine.
 */
namespace gs {

template <typename FRAG_T>
class ProjectSimpleFrame {};

template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ProjectSimpleFrame<
    gs::ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>> {
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T>;
  using projected_fragment_t =
      gs::ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  static bl::result<std::shared_ptr<IFragmentWrapper>> Project(
      std::shared_ptr<IFragmentWrapper>& input_wrapper,
      const std::string& projected_graph_name, const rpc::GSParams& params) {
    auto graph_type = input_wrapper->graph_def().graph_type();
    if (graph_type != rpc::graph::ARROW_PROPERTY) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "graph_type should be ARROW_PROPERTY, got " +
                          rpc::graph::GraphTypePb_Name(graph_type));
    }

    BOOST_LEAF_AUTO(v_label_id, params.Get<int64_t>(rpc::V_LABEL_ID));
    BOOST_LEAF_AUTO(e_label_id, params.Get<int64_t>(rpc::E_LABEL_ID));
    BOOST_LEAF_AUTO(v_prop_id, params.Get<int64_t>(rpc::V_PROP_ID));
    BOOST_LEAF_AUTO(e_prop_id, params.Get<int64_t>(rpc::E_PROP_ID));
    auto v_label = std::to_string(v_label_id);
    auto e_label = std::to_string(e_label_id);
    auto v_prop = std::to_string(v_prop_id);
    auto e_prop = std::to_string(e_prop_id);
    auto input_frag =
        std::static_pointer_cast<fragment_t>(input_wrapper->fragment());
    auto projected_frag = projected_fragment_t::Project(
        input_frag, v_label, v_prop, e_label, e_prop);

    rpc::graph::GraphDefPb graph_def;
    graph_def.set_key(projected_graph_name);
    graph_def.set_graph_type(rpc::graph::ARROW_PROJECTED);

    setGraphDef(projected_frag, v_label, e_label, v_prop, e_prop, graph_def);

    auto wrapper = std::make_shared<FragmentWrapper<projected_fragment_t>>(
        projected_graph_name, graph_def, projected_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }

 private:
  static void setGraphDef(std::shared_ptr<projected_fragment_t>& fragment,
                          std::string& v_label, std::string& e_label,
                          std::string& v_prop, std::string& e_prop,
                          rpc::graph::GraphDefPb& graph_def) {
    auto& meta = fragment->meta();
    const auto& parent_meta = meta.GetMemberMeta("arrow_fragment");

    graph_def.set_directed(
        static_cast<bool>(parent_meta.template GetKeyValue<int>("directed")));

    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_oid_type(PropertyTypeToPb(
        vineyard::normalize_datatype(parent_meta.GetKeyValue("oid_type"))));
    vy_info.set_vid_type(PropertyTypeToPb(
        vineyard::normalize_datatype(parent_meta.GetKeyValue("vid_type"))));

    std::string vdata_type, edata_type;
    if (v_prop != "-1") {
      std::string vdata_key = "vertex_property_type_" + v_label + "_" + v_prop;
      vdata_type =
          vineyard::normalize_datatype(parent_meta.GetKeyValue(vdata_key));
    } else {
      vdata_type = vineyard::normalize_datatype("empty");
    }
    vy_info.set_vdata_type(PropertyTypeToPb(vdata_type));

    if (e_prop != "-1") {
      std::string edata_key = "edge_property_type_" + e_label + "_" + e_prop;
      edata_type =
          vineyard::normalize_datatype(parent_meta.GetKeyValue(edata_key));
    } else {
      edata_type = vineyard::normalize_datatype("empty");
    }
    vy_info.set_edata_type(PropertyTypeToPb(edata_type));
    vy_info.set_property_schema_json("{}");
    graph_def.mutable_extension()->PackFrom(vy_info);
  }
};

#ifdef NETWORKX
template <typename VDATA_T, typename EDATA_T>
class ProjectSimpleFrame<gs::DynamicProjectedFragment<VDATA_T, EDATA_T>> {
  using fragment_t = gs::DynamicFragment;
  using projected_fragment_t = gs::DynamicProjectedFragment<VDATA_T, EDATA_T>;

 public:
  static bl::result<std::shared_ptr<IFragmentWrapper>> Project(
      std::shared_ptr<IFragmentWrapper>& input_wrapper,
      const std::string& projected_graph_name, const rpc::GSParams& params) {
    auto graph_type = input_wrapper->graph_def().graph_type();
    if (graph_type != rpc::graph::DYNAMIC_PROPERTY) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "graph_type should be DYNAMIC_PROPERTY, got " +
                          rpc::graph::GraphTypePb_Name(graph_type));
    }
    BOOST_LEAF_AUTO(v_prop_key, params.Get<std::string>(rpc::V_PROP_KEY));
    BOOST_LEAF_AUTO(e_prop_key, params.Get<std::string>(rpc::E_PROP_KEY));
    auto input_frag =
        std::static_pointer_cast<fragment_t>(input_wrapper->fragment());
    auto projected_frag =
        projected_fragment_t::Project(input_frag, v_prop_key, e_prop_key);

    rpc::graph::GraphDefPb graph_def;

    graph_def.set_key(projected_graph_name);
    graph_def.set_graph_type(rpc::graph::DYNAMIC_PROJECTED);
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_oid_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::oid_t>::Get())));
    vy_info.set_vid_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::vid_t>::Get())));
    vy_info.set_vdata_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::vdata_t>::Get())));
    vy_info.set_edata_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::edata_t>::Get())));
    graph_def.mutable_extension()->PackFrom(vy_info);
    auto wrapper = std::make_shared<FragmentWrapper<projected_fragment_t>>(
        projected_graph_name, graph_def, projected_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }
};

template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ProjectSimpleFrame<
    gs::ArrowFlattenedFragment<OID_T, VID_T, VDATA_T, EDATA_T>> {
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T>;
  using projected_fragment_t =
      gs::ArrowFlattenedFragment<OID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  static bl::result<std::shared_ptr<IFragmentWrapper>> Project(
      std::shared_ptr<IFragmentWrapper>& input_wrapper,
      const std::string& projected_graph_name, const rpc::GSParams& params) {
    auto graph_type = input_wrapper->graph_def().graph_type();
    if (graph_type != rpc::graph::ARROW_PROPERTY) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "graph_type should be ARROW_PROPERTY, got " +
                          rpc::graph::GraphTypePb_Name(graph_type));
    }

    BOOST_LEAF_AUTO(v_prop_key, params.Get<std::string>(rpc::V_PROP_KEY));
    BOOST_LEAF_AUTO(e_prop_key, params.Get<std::string>(rpc::E_PROP_KEY));
    auto input_frag =
        std::static_pointer_cast<fragment_t>(input_wrapper->fragment());
    auto projected_frag =
        projected_fragment_t::Project(input_frag, v_prop_key, e_prop_key);

    rpc::graph::GraphDefPb graph_def;

    graph_def.set_key(projected_graph_name);
    graph_def.set_graph_type(rpc::graph::ARROW_FLATTENED);
    gs::rpc::graph::VineyardInfoPb vy_info;
    if (graph_def.has_extension()) {
      graph_def.extension().UnpackTo(&vy_info);
    }
    vy_info.set_oid_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::oid_t>::Get())));
    vy_info.set_vid_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::vid_t>::Get())));
    vy_info.set_vdata_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::vdata_t>::Get())));
    vy_info.set_edata_type(PropertyTypeToPb(vineyard::normalize_datatype(
        vineyard::TypeName<typename projected_fragment_t::edata_t>::Get())));
    graph_def.mutable_extension()->PackFrom(vy_info);
    auto wrapper = std::make_shared<FragmentWrapper<projected_fragment_t>>(
        projected_graph_name, graph_def, projected_frag);
    return std::dynamic_pointer_cast<IFragmentWrapper>(wrapper);
  }
};
#endif
}  // namespace gs

extern "C" {
void Project(
    std::shared_ptr<gs::IFragmentWrapper>& wrapper_in,
    const std::string& projected_graph_name, const gs::rpc::GSParams& params,
    gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>>& wrapper_out) {
  wrapper_out = gs::ProjectSimpleFrame<_PROJECTED_GRAPH_TYPE>::Project(
      wrapper_in, projected_graph_name, params);
}

template class _PROJECTED_GRAPH_TYPE;
}
