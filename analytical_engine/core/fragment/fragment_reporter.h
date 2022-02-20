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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_FRAGMENT_REPORTER_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_FRAGMENT_REPORTER_H_

#ifdef NETWORKX

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "boost/lexical_cast.hpp"

#include "grape/communication/communicator.h"
#include "grape/worker/comm_spec.h"

#include "core/fragment/dynamic_fragment.h"
#include "core/server/rpc_utils.h"
#include "core/utils/convert_utils.h"
#include "proto/graphscope/proto/types.pb.h"

namespace gs {
/**
 * @brief DynamicFragmentReporter is used to query the vertex and edge
 * information of DynamicFragment.
 */
class DynamicFragmentReporter : public grape::Communicator {
  using fragment_t = DynamicFragment;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using adj_list_t = typename fragment_t::adj_list_t;

 public:
  explicit DynamicFragmentReporter(const grape::CommSpec& comm_spec)
      : comm_spec_(comm_spec) {
    InitCommunicator(comm_spec.comm());
  }

  bl::result<std::string> Report(std::shared_ptr<fragment_t>& fragment,
                                 const rpc::GSParams& params) {
    BOOST_LEAF_AUTO(report_type, params.Get<rpc::ReportType>(rpc::REPORT_TYPE));
    switch (report_type) {
    case rpc::NODE_NUM: {
      return std::to_string(reportNodeNum(fragment));
    }
    case rpc::EDGE_NUM: {
      return std::to_string(reportEdgeNum(fragment));
    }
    case rpc::SELFLOOPS_NUM: {
      return std::to_string(reportSelfloopsNum(fragment));
    }
    case rpc::HAS_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      dynamic::Parse(node_in_json, node_id);
      return std::to_string(hasNode(fragment, node_id));
    }

    case rpc::HAS_EDGE: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      dynamic::Value edge;
      dynamic::Parse(edge_in_json, edge);
      dynamic::Value src_id(edge[0]);
      dynamic::Value dst_id(edge[1]);
      return std::to_string(hasEdge(fragment, src_id, dst_id));
    }
    case rpc::NODE_DATA: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      dynamic::Parse(node_in_json, node_id);
      return getNodeData(fragment, node_id);
    }
    case rpc::EDGE_DATA: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      dynamic::Value edge;
      dynamic::Parse(edge_in_json, edge);
      dynamic::Value src_id(edge[0]);
      dynamic::Value dst_id(edge[1]);
      return getEdgeData(fragment, src_id, dst_id);
    }
    case rpc::NEIGHBORS_BY_NODE:
    case rpc::SUCCS_BY_NODE:
    case rpc::PREDS_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      dynamic::Parse(node_in_json, node_id);
      return getNeighbors(fragment, node_id, report_type);
    }
    case rpc::NODES_BY_LOC: {
      BOOST_LEAF_AUTO(fid, params.Get<int64_t>(rpc::FID));
      BOOST_LEAF_AUTO(lid, params.Get<int64_t>(rpc::LID));
      return batchGetNodes(fragment, fid, lid);
    }
    default:
      LOG(FATAL) << "Invalid report type";
    }
    return std::string("");
  }

 private:
  inline size_t reportNodeNum(std::shared_ptr<fragment_t>& fragment) {
    size_t frag_vnum = 0, total_vnum = 0;
    frag_vnum = fragment->GetInnerVerticesNum();
    Sum(frag_vnum, total_vnum);
    return total_vnum;
  }

  inline size_t reportEdgeNum(std::shared_ptr<fragment_t>& fragment) {
    size_t frag_enum = 0, total_enum = 0;
    frag_enum = fragment->GetEdgeNum();
    Sum(frag_enum, total_enum);
    return total_enum;
  }

  inline size_t reportSelfloopsNum(std::shared_ptr<fragment_t>& fragment) {
    size_t frag_selfloops_num = 0, total_selfloops_num = 0;
    frag_selfloops_num = fragment->selfloops_num();
    Sum(frag_selfloops_num, total_selfloops_num);
    return total_selfloops_num;
  }

  bool hasNode(std::shared_ptr<fragment_t>& fragment, const oid_t& node) {
    bool ret = false;
    bool existed_in_frag = fragment->HasNode(node);
    Sum(existed_in_frag, ret);
    return ret;
  }

  bool hasEdge(std::shared_ptr<fragment_t>& fragment, const oid_t& u,
               const oid_t& v) {
    bool ret = false;
    bool existed_in_frag = fragment->HasEdge(u, v);
    Sum(existed_in_frag, ret);
    return ret;
  }

  std::string getNodeData(std::shared_ptr<fragment_t>& fragment,
                          const oid_t& n) {
    vertex_t v;
    if (fragment->GetInnerVertex(n, v) && fragment->IsAliveInnerVertex(v)) {
      return dynamic::Stringify(fragment->GetData(v));
    }
    return std::string();
  }

  std::string getEdgeData(std::shared_ptr<fragment_t>& fragment, const oid_t& u,
                          const oid_t& v) {
    dynamic::Value ref_data;
    fragment->GetEdgeData(u, v, ref_data);
    return ref_data.IsNull() ? std::string() : dynamic::Stringify(ref_data);
  }

  std::string getNeighbors(std::shared_ptr<fragment_t>& fragment,
                           const oid_t& node,
                           const rpc::ReportType& report_type) {
    vertex_t v;
    dynamic::Value nbrs(rapidjson::kArrayType);
    if (fragment->GetInnerVertex(node, v)) {
      adj_list_t edges;
      dynamic::Value id_array(rapidjson::kArrayType);
      dynamic::Value data_array(rapidjson::kArrayType);
      report_type == rpc::PREDS_BY_NODE
          ? edges = fragment->GetIncomingAdjList(v)
          : edges = fragment->GetOutgoingAdjList(v);
      for (const auto& e : edges) {
        id_array.PushBack(fragment->GetId(e.neighbor));
        data_array.PushBack(e.data);
      }
      nbrs.PushBack(id_array).PushBack(data_array);
    }
    return nbrs.Empty() ? std::string() : dynamic::Stringify(nbrs);
  }

  std::string batchGetNodes(std::shared_ptr<fragment_t>& fragment, vid_t fid,
                            vid_t start_lid) {
    if (fragment->fid() == fid) {
      int cnt = 0;
      vertex_t v(start_lid);
      dynamic::Value nodes(rapidjson::kObjectType);
      dynamic::Value batch_nodes(rapidjson::kArrayType);
      auto end_value = fragment->InnerVertices().end_value();
      while (v.GetValue() < end_value && cnt < batch_num_) {
        if (fragment->IsInnerVertex(v) && fragment->IsAliveInnerVertex(v)) {
          batch_nodes.PushBack(fragment->GetId(v));
          ++cnt;
        }
        ++v;
      }
      // nodes["status"] store this batch_get_nodes operation status, if no node
      // to fetch, set false;  nodes["batch"] store the nodes.
      // nodes["next"] store the start vertex location of next batch_get_nodes
      // operation.
      dynamic::Value next(rapidjson::kArrayType);
      if (!batch_nodes.Empty()) {
        nodes.Insert("status", true);
        nodes.Insert("batch", batch_nodes);
        if (fragment->IsInnerVertex(v)) {
          next.PushBack(fid).PushBack(v.GetValue());
        } else {
          next.PushBack(fid + 1).PushBack(0);
        }
      } else {
        nodes.Insert("status", false);
        next.PushBack(fid + 1).PushBack(0);
      }
      nodes.Insert("next", next);
      return dynamic::Stringify(nodes);
    }
    return std::string();
  }

  grape::CommSpec comm_spec_;
  static const int batch_num_ = 100;
};

template <typename T>
T ExtractOidFromDynamic(dynamic::Value& node_id) {}

template <>
int64_t ExtractOidFromDynamic(dynamic::Value& node_id) {
  return node_id.GetInt64();
}

template <>
std::string ExtractOidFromDynamic(dynamic::Value& node_id) {
  return node_id.GetString();
}

template <typename FRAG_T>
class ArrowFragmentReporter {};

/**
 * @brief ArrowFragmentReporter is used to query the vertex and edge
 * information of ArrowFragment.
 */
template <typename OID_T, typename VID_T>
class ArrowFragmentReporter<vineyard::ArrowFragment<OID_T, VID_T>>
    : public grape::Communicator {
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T>;
  using label_id_t = typename fragment_t::label_id_t;
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vertex_t = typename fragment_t::vertex_t;
  using adj_list_t = typename fragment_t::adj_list_t;

 public:
  explicit ArrowFragmentReporter(const grape::CommSpec& comm_spec,
                                 label_id_t default_label_id)
      : comm_spec_(comm_spec), default_label_id_(default_label_id) {
    InitCommunicator(comm_spec.comm());
  }

  bl::result<std::string> Report(std::shared_ptr<fragment_t>& fragment,
                                 const rpc::GSParams& params) {
    BOOST_LEAF_AUTO(report_type, params.Get<rpc::ReportType>(rpc::REPORT_TYPE));
    switch (report_type) {
    case rpc::NODE_NUM: {
      return std::to_string(reportNodeNum(fragment));
    }
    case rpc::EDGE_NUM: {
      return std::to_string(reportEdgeNum(fragment));
    }
    case rpc::SELFLOOPS_NUM: {
      // TODO(acezen): support selfloops num for arrow fragment.
      return std::string();
    }
    case rpc::HAS_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);
      return std::to_string(hasNode(fragment, label_id, oid));
    }
    case rpc::HAS_EDGE: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      // the input edge format: ((u_label_id, u_oid), (v_label_id, v_oid))
      dynamic::Value edge;
      dynamic::Parse(edge_in_json, edge);
      label_id_t u_label_id = edge[0][0].GetInt64();
      label_id_t v_label_id = edge[1][0].GetInt64();
      dynamic::Value u_dy_oid(edge[0][1]);
      dynamic::Value v_dy_oid(edge[1][1]);
      auto u_oid = ExtractOidFromDynamic<oid_t>(u_dy_oid);
      auto v_oid = ExtractOidFromDynamic<oid_t>(v_dy_oid);
      return std::to_string(
          hasEdge(fragment, u_label_id, u_oid, v_label_id, v_oid));
    }
    case rpc::NODE_DATA: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);
      return getNodeData(fragment, label_id, oid);
    }
    case rpc::EDGE_DATA: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      // the input edge format: ((u_label_id, u_oid), (v_label_id, v_oid))
      dynamic::Value edge;
      dynamic::Parse(edge_in_json, edge);
      label_id_t u_label_id = edge[0][0].GetInt64();
      label_id_t v_label_id = edge[1][0].GetInt64();
      dynamic::Value u_dy_oid(edge[0][1]);
      dynamic::Value v_dy_oid(edge[1][1]);
      auto u_oid = ExtractOidFromDynamic<oid_t>(u_dy_oid);
      auto v_oid = ExtractOidFromDynamic<oid_t>(v_dy_oid);
      return getEdgeData(fragment, u_label_id, u_oid, v_label_id, v_oid);
    }
    case rpc::NEIGHBORS_BY_NODE:
    case rpc::SUCCS_BY_NODE:
    case rpc::PREDS_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);
      return getNeighbors(fragment, label_id, oid, report_type);
    }
    case rpc::NODES_BY_LOC: {
      BOOST_LEAF_AUTO(fid, params.Get<int64_t>(rpc::FID));
      BOOST_LEAF_AUTO(label_id, params.Get<int64_t>(rpc::V_LABEL_ID));
      BOOST_LEAF_AUTO(start, params.Get<int64_t>(rpc::LID));
      vid_t end = start + batch_num_;
      return batchGetNodes(fragment, fid, label_id, start, end);
    }
    default:
      CHECK(false);
    }
    return std::string();
  }

 private:
  inline size_t reportNodeNum(std::shared_ptr<fragment_t>& fragment) {
    return fragment->GetTotalNodesNum();
  }

  inline size_t reportEdgeNum(std::shared_ptr<fragment_t>& fragment) {
    size_t frag_enum = 0, total_enum = 0;
    frag_enum = fragment->GetEdgeNum();
    Sum(frag_enum, total_enum);
    return total_enum;
  }

  bool hasNode(std::shared_ptr<fragment_t>& fragment, label_id_t label_id,
               const oid_t& oid) {
    bool ret = false;
    vid_t gid;
    bool existed =
        fragment->GetVertexMap()->GetGid(fragment->fid(), label_id, oid, gid);
    Sum(existed, ret);
    return ret;
  }

  bool hasEdge(std::shared_ptr<fragment_t>& fragment, label_id_t u_label_id,
               const oid_t& u_oid, label_id_t v_label_id, const oid_t& v_oid) {
    bool ret = false, existed = false;
    vid_t u_gid, v_gid;
    vertex_t u, v;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), u_label_id, u_oid, u_gid) &&
        vm_ptr->GetGid(v_label_id, v_oid, v_gid) &&
        fragment->InnerVertexGid2Vertex(u_gid, u) &&
        fragment->Gid2Vertex(v_gid, v)) {
      for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
           e_label++) {
        auto oe = fragment->GetOutgoingAdjList(u, e_label);
        for (auto& e : oe) {
          if (v == e.neighbor()) {
            existed = true;
            break;
          }
        }
      }
    }

    Sum(existed, ret);
    return ret;
  }

  std::string getNodeData(std::shared_ptr<fragment_t>& fragment,
                          label_id_t label_id, const oid_t& n) {
    vid_t gid;
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), label_id, n, gid) &&
        fragment->InnerVertexGid2Vertex(gid, v)) {
      dynamic::Value ref_data(rapidjson::kObjectType);
      auto vertex_data = fragment->vertex_data_table(label_id);
      // N.B: th last column is id, we ignore it.
      for (auto col_id = 0; col_id < vertex_data->num_columns() - 1; col_id++) {
        auto prop_name = vertex_data->field(col_id)->name();
        auto type = vertex_data->column(col_id)->type();
        PropertyConverter<fragment_t>::NodeValue(fragment, v, type, prop_name,
                                                 col_id, ref_data);
      }
      return dynamic::Stringify(ref_data);
    }
    return std::string();
  }

  std::string getEdgeData(std::shared_ptr<fragment_t>& fragment,
                          label_id_t u_label_id, const oid_t& u_oid,
                          label_id_t v_label_id, const oid_t& v_oid) {
    dynamic::Value ref_data;
    vid_t u_gid, v_gid;
    vertex_t u, v;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), u_label_id, u_oid, u_gid) &&
        vm_ptr->GetGid(v_label_id, v_oid, v_gid) &&
        fragment->InnerVertexGid2Vertex(u_gid, u) &&
        fragment->Gid2Vertex(v_gid, v)) {
      for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
           e_label++) {
        auto oe = fragment->GetOutgoingAdjList(u, e_label);
        for (auto& e : oe) {
          if (v == e.neighbor()) {
            ref_data = dynamic::Value(rapidjson::kObjectType);
            auto edge_data = fragment->edge_data_table(e_label);
            PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(),
                                                     ref_data);
            return dynamic::Stringify(ref_data);
          }
        }
      }
    }
    return std::string();
  }

  std::string getNeighbors(std::shared_ptr<fragment_t>& fragment,
                           label_id_t label_id, const oid_t& n,
                           const rpc::ReportType& report_type) {
    vid_t gid;
    vertex_t v;
    dynamic::Value nbrs(rapidjson::kArrayType);
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), label_id, n, gid) &&
        fragment->InnerVertexGid2Vertex(gid, v)) {
      dynamic::Value id_array(rapidjson::kArrayType);
      dynamic::Value data_array(rapidjson::kArrayType);
      for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
           e_label++) {
        adj_list_t edges;
        auto edge_data = fragment->edge_data_table(e_label);
        report_type == rpc::PREDS_BY_NODE
            ? edges = fragment->GetIncomingAdjList(v, e_label)
            : edges = fragment->GetOutgoingAdjList(v, e_label);
        for (auto& e : edges) {
          auto n_label_id = fragment->vertex_label(e.neighbor());
          if (n_label_id == default_label_id_) {
            id_array.PushBack(fragment->GetId(e.neighbor()));
          } else {
            id_array.PushBack(
                dynamic::Value(rapidjson::kArrayType)
                    .PushBack(fragment->schema().GetVertexLabelName(n_label_id))
                    .PushBack(fragment->GetId(e.neighbor())));
          }
          dynamic::Value data(rapidjson::kObjectType);
          PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(),
                                                   data);
          data_array.PushBack(data);
        }
      }
      nbrs.PushBack(id_array).PushBack(data_array);
    }
    std::string ret = dynamic::Stringify(nbrs);
    return nbrs.Empty() ? std::string() : dynamic::Stringify(nbrs);
  }

  std::string batchGetNodes(std::shared_ptr<fragment_t>& fragment, vid_t fid,
                            label_id_t label_id, vid_t start, vid_t end) {
    if (fragment->fid() == fid) {
      dynamic::Value nodes(rapidjson::kObjectType);
      dynamic::Value batch_nodes(rapidjson::kArrayType);
      auto label_name = fragment->schema().GetVertexLabelName(label_id);
      for (auto v : fragment->InnerVerticesSlice(label_id, start, end)) {
        if (label_id == default_label_id_) {
          batch_nodes.PushBack(fragment->GetId(v));
        } else {
          batch_nodes.PushBack(dynamic::Value(rapidjson::kArrayType)
                                   .PushBack(label_name)
                                   .PushBack(fragment->GetId(v)));
        }
      }
      if (end >= fragment->GetInnerVerticesNum(label_id)) {
        // switch to next label
        ++label_id;
        start = 0;
      } else {
        start = end;
      }
      if (label_id >= fragment->vertex_label_num()) {
        // switch to next fragment
        ++fid;
        label_id = 0;
        start = 0;
      }
      // ob["status"] store this batch_get_nodes operation status, if no node
      // to fetch, set false;  ob["batch"] store the nodes
      if (batch_nodes.Empty()) {
        nodes.Insert("status", false);
      } else {
        nodes.Insert("status", true);
        nodes.Insert("batch", batch_nodes);
      }
      // the start vertex location of next batch_get_nodes operation.
      dynamic::Value next(rapidjson::kArrayType);
      next.PushBack(fid).PushBack(start).PushBack(label_id);
      nodes.Insert("next", next);
      return dynamic::Stringify(nodes);
    }
    return std::string();
  }

  grape::CommSpec comm_spec_;
  label_id_t default_label_id_;
  static const int batch_num_ = 100;
};
}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_FRAGMENT_REPORTER_H_
