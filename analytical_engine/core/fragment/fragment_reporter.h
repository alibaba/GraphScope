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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "glog/logging.h"
#include "grape/communication/communicator.h"
#include "grape/serialization/in_archive.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/property_graph_utils.h"

#include "core/fragment/dynamic_fragment.h"
#include "core/object/dynamic.h"
#include "core/server/rpc_utils.h"
#include "core/utils/convert_utils.h"
#include "core/utils/msgpack_utils.h"
#include "proto/types.pb.h"

namespace bl = boost::leaf;

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

  bl::result<std::unique_ptr<grape::InArchive>> Report(
      std::shared_ptr<fragment_t>& fragment, const rpc::GSParams& params) {
    BOOST_LEAF_AUTO(report_type, params.Get<rpc::ReportType>(rpc::REPORT_TYPE));
    auto in_archive = std::make_unique<grape::InArchive>();
    switch (report_type) {
    case rpc::NODE_NUM: {
      size_t frag_vnum = 0, total_vnum = 0;
      frag_vnum = fragment->GetInnerVerticesNum();
      Sum(frag_vnum, total_vnum);
      if (comm_spec_.fid() == 0) {
        *in_archive << total_vnum;
      }
      break;
    }
    case rpc::EDGE_NUM: {
      size_t frag_enum = 0, total_enum = 0;
      frag_enum = fragment->GetEdgeNum();
      Sum(frag_enum, total_enum);
      if (comm_spec_.fid() == 0) {
        *in_archive << total_enum;
      }
      break;
    }
    case rpc::SELFLOOPS_NUM: {
      size_t frag_selfloops_num = 0, total_selfloops_num = 0;
      frag_selfloops_num = fragment->selfloops_num();
      Sum(frag_selfloops_num, total_selfloops_num);
      if (comm_spec_.fid() == 0) {
        *in_archive << total_selfloops_num;
      }
      break;
    }
    case rpc::HAS_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      dynamic::Parse(node_in_json, node_id);
      bool result = false;
      Sum(fragment->HasNode(node_id), result);
      if (comm_spec_.fid() == 0) {
        *in_archive << result;
      }
      break;
    }
    case rpc::HAS_EDGE: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      dynamic::Value edge;
      dynamic::Parse(edge_in_json, edge);
      dynamic::Value src_id(edge[0]);
      dynamic::Value dst_id(edge[1]);
      bool result = false;
      Sum(fragment->HasEdge(src_id, dst_id), result);
      if (comm_spec_.fid() == 0) {
        *in_archive << result;
      }
      break;
    }
    case rpc::NODE_DATA: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      vertex_t v;
      dynamic::Parse(node_in_json, node_id);
      if (fragment->GetInnerVertex(node_id, v) &&
          fragment->IsAliveInnerVertex(v)) {
        msgpack::sbuffer sbuf;
        msgpack::pack(&sbuf, fragment->GetData(v));
        *in_archive << sbuf;
      }
      break;
    }
    case rpc::EDGE_DATA: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      dynamic::Value edge;
      dynamic::Parse(edge_in_json, edge);
      dynamic::Value src_id(edge[0]);
      dynamic::Value dst_id(edge[1]);
      dynamic::Value ref_data;
      if (fragment->GetEdgeData(src_id, dst_id, ref_data)) {
        *in_archive << ref_data;
      }
      break;
    }
    case rpc::SUCCS_BY_NODE:
    case rpc::PREDS_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      dynamic::Parse(node_in_json, node_id);
      vertex_t v;
      if (fragment->GetInnerVertex(node_id, v)) {
        getNeighborsList(fragment, v, report_type, *in_archive);
      }
      break;
    }
    case rpc::SUCC_ATTR_BY_NODE:
    case rpc::PRED_ATTR_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id;
      dynamic::Parse(node_in_json, node_id);
      vertex_t v;
      if (fragment->GetInnerVertex(node_id, v)) {
        getNeighborsAttrList(fragment, v, report_type, *in_archive);
      }
      break;
    }
    case rpc::NODE_ID_CACHE_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      if (fragment->IsInnerVertexGid(gid)) {
        getNodeIdCacheByGid(fragment, gid, *in_archive);
      }
      break;
    }
    case rpc::NODE_ATTR_CACHE_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      if (fragment->IsInnerVertexGid(gid)) {
        getNodeAttrCacheByGid(fragment, gid, *in_archive);
      }
      break;
    }
    case rpc::SUCC_BY_GID:
    case rpc::PRED_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      if (fragment->IsInnerVertexGid(gid)) {
        getNeighborCacheByGid(fragment, gid, report_type, *in_archive);
      }
      break;
    }
    case rpc::SUCC_ATTR_BY_GID:
    case rpc::PRED_ATTR_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      if (fragment->IsInnerVertexGid(gid)) {
        getNeighborAttrCacheByGid(fragment, gid, report_type, *in_archive);
      }
      break;
    }
    default:
      LOG(ERROR) << "Invalid report type";
    }
    return in_archive;
  }

 private:
  void getNeighborsList(std::shared_ptr<fragment_t>& fragment,
                        const vertex_t& v, const rpc::ReportType& report_type,
                        grape::InArchive& arc) {
    adj_list_t edges;
    report_type == rpc::PREDS_BY_NODE ? edges = fragment->GetIncomingAdjList(v)
                                      : edges = fragment->GetOutgoingAdjList(v);
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);
    packer.pack_array(edges.Size());
    for (const auto& e : edges) {
      packer.pack(fragment->GetId(e.neighbor));
    }
    arc << sbuf;
  }

  void getNeighborsAttrList(std::shared_ptr<fragment_t>& fragment,
                            const vertex_t& v,
                            const rpc::ReportType& report_type,
                            grape::InArchive& arc) {
    adj_list_t edges;
    dynamic::Value data_array(rapidjson::kArrayType);
    report_type == rpc::PRED_ATTR_BY_NODE
        ? edges = fragment->GetIncomingAdjList(v)
        : edges = fragment->GetOutgoingAdjList(v);
    for (const auto& e : edges) {
      data_array.PushBack(e.data);
    }
    arc << data_array;
  }

  void getNodeIdCacheByGid(std::shared_ptr<fragment_t>& fragment, vid_t gid,
                           grape::InArchive& arc) {
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    auto fid = fragment->fid();
    fragment->InnerVertexGid2Vertex(gid, v);
    dynamic::Value nodes_id(rapidjson::kArrayType);

    for (int cnt = 0;
         v.GetValue() < vm_ptr->GetInnerVertexSize(fid) && cnt < batch_num_;
         ++v) {
      if (fragment->IsAliveInnerVertex(v)) {
        nodes_id.PushBack(fragment->GetId(v));
        ++cnt;
      }
    }

    // archive the gid for next batch fetch, batch size and nodes id array.
    if (v.GetValue() < fragment->GetInnerVerticesNum()) {
      arc << vm_ptr->Lid2Gid(fragment->fid(), v.GetValue()) << nodes_id.Size();
    } else if (fid == fragment->fnum() - 1) {
      uint64_t next = 0;
      arc << next << nodes_id.Size();
    } else {
      arc << vm_ptr->Lid2Gid(fragment->fid() + 1, 0) << nodes_id.Size();
    }
    msgpack::sbuffer sbuf;
    msgpack::pack(&sbuf, nodes_id);
    arc << sbuf;
  }

  void getNodeAttrCacheByGid(std::shared_ptr<fragment_t>& fragment, vid_t gid,
                             grape::InArchive& arc) {
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    auto fid = fragment->fid();
    fragment->InnerVertexGid2Vertex(gid, v);
    dynamic::Value nodes_attr(rapidjson::kArrayType);

    for (int cnt = 0;
         v.GetValue() < vm_ptr->GetInnerVertexSize(fid) && cnt < batch_num_;
         ++v) {
      if (fragment->IsAliveInnerVertex(v)) {
        nodes_attr.PushBack(fragment->GetData(v));
        ++cnt;
      }
    }
    // archive the start gid and nodes attribute array.
    msgpack::sbuffer sbuf;
    msgpack::pack(&sbuf, nodes_attr);
    arc << gid << sbuf;
  }

  void getNeighborCacheByGid(std::shared_ptr<fragment_t>& fragment, vid_t gid,
                             const rpc::ReportType& report_type,
                             grape::InArchive& arc) {
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    auto fid = fragment->fid();
    fragment->InnerVertexGid2Vertex(gid, v);
    dynamic::Value adj_list(rapidjson::kArrayType);
    adj_list_t edges;

    for (int cnt = 0;
         v.GetValue() < vm_ptr->GetInnerVertexSize(fid) && cnt < batch_num_;
         ++v) {
      if (fragment->IsAliveInnerVertex(v)) {
        dynamic::Value neighbor_ids(rapidjson::kArrayType);
        report_type == rpc::PRED_BY_GID
            ? edges = fragment->GetIncomingAdjList(v)
            : edges = fragment->GetOutgoingAdjList(v);
        for (const auto& e : edges) {
          neighbor_ids.PushBack(fragment->GetId(e.get_neighbor()));
        }
        adj_list.PushBack(neighbor_ids);
        ++cnt;
      }
    }

    // archive the start gid and neighbors array.
    msgpack::sbuffer sbuf;
    msgpack::pack(&sbuf, adj_list);
    arc << gid << sbuf;
  }

  void getNeighborAttrCacheByGid(std::shared_ptr<fragment_t>& fragment,
                                 vid_t gid, const rpc::ReportType& report_type,
                                 grape::InArchive& arc) {
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    auto fid = fragment->fid();
    fragment->InnerVertexGid2Vertex(gid, v);
    dynamic::Value adj_list(rapidjson::kArrayType);
    adj_list_t edges;

    for (int cnt = 0;
         v.GetValue() < vm_ptr->GetInnerVertexSize(fid) && cnt < batch_num_;
         ++v) {
      if (fragment->IsAliveInnerVertex(v)) {
        dynamic::Value neighbor_attrs(rapidjson::kArrayType);
        report_type == rpc::PRED_ATTR_BY_GID
            ? edges = fragment->GetIncomingAdjList(v)
            : edges = fragment->GetOutgoingAdjList(v);
        for (const auto& e : edges) {
          neighbor_attrs.PushBack(e.data);
        }
        adj_list.PushBack(neighbor_attrs);
        ++cnt;
      }
    }

    // archive the start gid and edges attribute array.
    msgpack::sbuffer sbuf;
    msgpack::pack(&sbuf, adj_list);
    arc << gid << sbuf;
  }

  grape::CommSpec comm_spec_;
  static const int batch_num_ = 10000000;
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
template <typename OID_T, typename VID_T, typename VERTEX_MAP_T, bool COMPACT>
class ArrowFragmentReporter<
    vineyard::ArrowFragment<OID_T, VID_T, VERTEX_MAP_T, COMPACT>>
    : public grape::Communicator {
  using fragment_t =
      vineyard::ArrowFragment<OID_T, VID_T, VERTEX_MAP_T, COMPACT>;
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

  bl::result<std::unique_ptr<grape::InArchive>> Report(
      std::shared_ptr<fragment_t>& fragment, const rpc::GSParams& params) {
    BOOST_LEAF_AUTO(report_type, params.Get<rpc::ReportType>(rpc::REPORT_TYPE));
    auto in_archive = std::make_unique<grape::InArchive>();
    switch (report_type) {
    case rpc::NODE_NUM: {
      if (comm_spec_.fid() == 0) {
        *in_archive << fragment->GetTotalNodesNum();
      }
      break;
    }
    case rpc::EDGE_NUM: {
      size_t frag_enum = 0, total_enum = 0;
      frag_enum = fragment->GetEdgeNum();
      Sum(frag_enum, total_enum);
      if (comm_spec_.fid() == 0) {
        *in_archive << total_enum;
      }
      break;
    }
    case rpc::SELFLOOPS_NUM: {
      // TODO(acezen): support selfloops num for arrow fragment.
      break;
    }
    case rpc::HAS_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);

      bool result = false;
      vid_t gid;
      bool existed =
          fragment->GetVertexMap()->GetGid(fragment->fid(), label_id, oid, gid);
      Sum(existed, result);
      if (comm_spec_.fid() == 0) {
        *in_archive << result;
      }
      break;
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
      bool result = hasEdge(fragment, u_label_id, u_oid, v_label_id, v_oid);
      if (comm_spec_.fid() == 0) {
        *in_archive << result;
      }
      break;
    }
    case rpc::NODE_DATA: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);
      getNodeData(fragment, label_id, oid, *in_archive);
      break;
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
      getEdgeData(fragment, u_label_id, u_oid, v_label_id, v_oid, *in_archive);
      break;
    }
    case rpc::SUCCS_BY_NODE:
    case rpc::PREDS_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);
      getNeighborsList(fragment, label_id, oid, report_type, *in_archive);
      break;
    }
    case rpc::SUCC_ATTR_BY_NODE:
    case rpc::PRED_ATTR_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      dynamic::Value node;
      dynamic::Parse(node_in_json, node);
      label_id_t label_id = node[0].GetInt64();
      dynamic::Value dynamic_oid(node[1]);
      oid_t oid = ExtractOidFromDynamic<oid_t>(dynamic_oid);
      getNeighborsAttrList(fragment, label_id, oid, report_type, *in_archive);
      break;
    }
    case rpc::NODE_ID_CACHE_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      getNodeIdCacheByGid(fragment, gid, *in_archive);
      break;
    }
    case rpc::NODE_ATTR_CACHE_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      getNodeAttrCacheByGid(fragment, gid, *in_archive);
      break;
    }
    case rpc::PRED_BY_GID:
    case rpc::SUCC_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      getNeighborCacheByGid(fragment, gid, report_type, *in_archive);
      break;
    }
    case rpc::SUCC_ATTR_BY_GID:
    case rpc::PRED_ATTR_BY_GID: {
      BOOST_LEAF_AUTO(gid, params.Get<uint64_t>(rpc::GID));
      getNeighborAttrCacheByGid(fragment, gid, report_type, *in_archive);
      break;
    }
    default:
      CHECK(false);
    }
    return in_archive;
  }

 private:
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

  void getNodeData(std::shared_ptr<fragment_t>& fragment, label_id_t label_id,
                   const oid_t& n, grape::InArchive& arc) {
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
      msgpack::sbuffer sbuf;
      msgpack::pack(&sbuf, ref_data);
      arc << sbuf;
    }
  }

  void getEdgeData(std::shared_ptr<fragment_t>& fragment, label_id_t u_label_id,
                   const oid_t& u_oid, label_id_t v_label_id,
                   const oid_t& v_oid, grape::InArchive& arc) {
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
            dynamic::Value ref_data(rapidjson::kObjectType);
            auto edge_data = fragment->edge_data_table(e_label);
            PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(),
                                                     ref_data);
            arc << ref_data;
          }
        }
      }
    }
  }

  void getNeighborsList(std::shared_ptr<fragment_t>& fragment,
                        label_id_t label_id, const oid_t& n,
                        const rpc::ReportType& report_type,
                        grape::InArchive& arc) {
    vid_t gid;
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), label_id, n, gid) &&
        fragment->InnerVertexGid2Vertex(gid, v)) {
      dynamic::Value id_array(rapidjson::kArrayType);
      for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
           e_label++) {
        for (auto& e : report_type == rpc::PREDS_BY_NODE
                           ? fragment->GetIncomingAdjList(v, e_label)
                           : fragment->GetOutgoingAdjList(v, e_label)) {
          auto n_label_id = fragment->vertex_label(e.neighbor());
          if (n_label_id == default_label_id_) {
            id_array.PushBack(fragment->GetId(e.neighbor()));
          } else {
            id_array.PushBack(
                dynamic::Value(rapidjson::kArrayType)
                    .PushBack(fragment->schema().GetVertexLabelName(n_label_id))
                    .PushBack(fragment->GetId(e.neighbor())));
          }
        }
      }
      msgpack::sbuffer sbuf;
      msgpack::pack(&sbuf, id_array);
      arc << sbuf;
    }
  }

  void getNeighborsAttrList(std::shared_ptr<fragment_t>& fragment,
                            label_id_t label_id, const oid_t& n,
                            const rpc::ReportType& report_type,
                            grape::InArchive& arc) {
    vid_t gid;
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), label_id, n, gid) &&
        fragment->InnerVertexGid2Vertex(gid, v)) {
      dynamic::Value data_array(rapidjson::kArrayType);
      for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
           e_label++) {
        auto edge_data = fragment->edge_data_table(e_label);
        for (auto& e : report_type == rpc::PRED_ATTR_BY_NODE
                           ? fragment->GetIncomingAdjList(v, e_label)
                           : fragment->GetOutgoingAdjList(v, e_label)) {
          dynamic::Value data(rapidjson::kObjectType);
          PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(),
                                                   data);
          data_array.PushBack(data);
        }
      }
      arc << data_array;
    }
  }

  void getNodeIdCacheByGid(std::shared_ptr<fragment_t>& fragment, vid_t gid,
                           grape::InArchive& arc) {
    vineyard::IdParser<vid_t> id_parser;
    auto fid = fragment->fid();
    auto fnum = fragment->fnum();
    auto label_num = fragment->vertex_label_num();
    id_parser.Init(fnum, label_num);
    if (id_parser.GetFid(gid) == fid) {
      dynamic::Value nodes_id(rapidjson::kArrayType);
      vertex_t v;
      fragment->InnerVertexGid2Vertex(gid, v);
      auto label_id = id_parser.GetLabelId(v.GetValue());
      auto label_name = fragment->schema().GetVertexLabelName(label_id);
      for (int cnt = 0; cnt < batch_num_;) {
        if (id_parser.GetOffset(v.GetValue()) <
            static_cast<int64_t>(fragment->GetInnerVerticesNum(label_id))) {
          if (label_id == default_label_id_) {
            nodes_id.PushBack(fragment->GetId(v));
          } else {
            nodes_id.PushBack(dynamic::Value(rapidjson::kArrayType)
                                  .PushBack(label_name)
                                  .PushBack(fragment->GetId(v)));
          }
          v++;
          cnt++;
        } else if (label_id < label_num - 1) {
          label_id++;
          label_name = fragment->schema().GetVertexLabelName(label_id);
          auto gid_ = id_parser.GenerateId(fid, label_id, 0);
          fragment->InnerVertexGid2Vertex(gid_, v);
        } else {
          break;
        }
      }
      // archive the gid for next batch fetch, batch size and nodes id array.
      if (id_parser.GetOffset(v.GetValue()) <
          static_cast<int64_t>(fragment->GetInnerVerticesNum(label_id))) {
        arc << fragment->GetInnerVertexGid(v) << nodes_id.Size();
      } else if (label_id == label_num - 1) {
        if (fid == fnum - 1) {
          arc << id_parser.GenerateId(0, 0, 0) << nodes_id.Size();
        } else {
          arc << id_parser.GenerateId(fid + 1, 0, 0) << nodes_id.Size();
        }
      }
      msgpack::sbuffer sbuf;
      msgpack::pack(&sbuf, nodes_id);
      arc << sbuf;
    }
  }

  void getNodeAttrCacheByGid(std::shared_ptr<fragment_t>& fragment, vid_t gid,
                             grape::InArchive& arc) {
    vineyard::IdParser<vid_t> id_parser;
    auto fid = fragment->fid();
    auto fnum = fragment->fnum();
    auto label_num = fragment->vertex_label_num();
    id_parser.Init(fnum, label_num);
    if (id_parser.GetFid(gid) == fid) {
      dynamic::Value nodes_attr(rapidjson::kArrayType);
      vertex_t v;
      fragment->InnerVertexGid2Vertex(gid, v);
      auto label_id = id_parser.GetLabelId(v.GetValue());
      for (int cnt = 0; cnt < batch_num_;) {
        if (id_parser.GetOffset(v.GetValue()) <
            static_cast<int64_t>(fragment->GetInnerVerticesNum(label_id))) {
          dynamic::Value ref_data(rapidjson::kObjectType);
          auto vertex_data = fragment->vertex_data_table(label_id);
          // N.B: th last column is id, we ignore it.
          for (auto col_id = 0; col_id < vertex_data->num_columns() - 1;
               col_id++) {
            auto prop_name = vertex_data->field(col_id)->name();
            auto type = vertex_data->column(col_id)->type();
            PropertyConverter<fragment_t>::NodeValue(
                fragment, v, type, prop_name, col_id, ref_data);
          }
          nodes_attr.PushBack(ref_data);
          v++;
          cnt++;
        } else if (label_id < label_num - 1) {
          label_id++;
          auto gid_ = id_parser.GenerateId(fid, label_id, 0);
          fragment->InnerVertexGid2Vertex(gid_, v);
        } else {
          break;
        }
      }
      // archive the start gid and nodes attribute array.
      msgpack::sbuffer sbuf;
      msgpack::pack(&sbuf, nodes_attr);
      arc << gid << sbuf;
    }
  }

  void getNeighborCacheByGid(std::shared_ptr<fragment_t>& fragment, vid_t gid,
                             const rpc::ReportType& report_type,
                             grape::InArchive& arc) {
    vineyard::IdParser<vid_t> id_parser;
    auto fid = fragment->fid();
    auto fnum = fragment->fnum();
    auto label_num = fragment->vertex_label_num();
    id_parser.Init(fnum, label_num);
    if (id_parser.GetFid(gid) == fid) {
      dynamic::Value adj_list(rapidjson::kArrayType);
      vertex_t v;
      fragment->InnerVertexGid2Vertex(gid, v);
      auto label_id = id_parser.GetLabelId(v.GetValue());
      for (int cnt = 0; cnt < batch_num_;) {
        if (id_parser.GetOffset(v.GetValue()) <
            static_cast<int64_t>(fragment->GetInnerVerticesNum(label_id))) {
          dynamic::Value neighbor_ids(rapidjson::kArrayType);
          for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
               e_label++) {
            for (auto& e : report_type == rpc::PRED_BY_GID
                               ? fragment->GetIncomingAdjList(v, e_label)
                               : fragment->GetOutgoingAdjList(v, e_label)) {
              auto n_label_id = fragment->vertex_label(e.neighbor());
              if (n_label_id == default_label_id_) {
                neighbor_ids.PushBack(fragment->GetId(e.neighbor()));
              } else {
                neighbor_ids.PushBack(
                    dynamic::Value(rapidjson::kArrayType)
                        .PushBack(
                            fragment->schema().GetVertexLabelName(n_label_id))
                        .PushBack(fragment->GetId(e.neighbor())));
              }
            }
          }
          adj_list.PushBack(neighbor_ids);
          v++;
          cnt++;
        } else if (label_id < label_num - 1) {
          label_id++;
          auto gid_ = id_parser.GenerateId(fid, label_id, 0);
          fragment->InnerVertexGid2Vertex(gid_, v);
        } else {
          break;
        }
      }

      // archive the start gid and neighbors array.
      msgpack::sbuffer sbuf;
      msgpack::pack(&sbuf, adj_list);
      arc << gid << sbuf;
    }
  }

  void getNeighborAttrCacheByGid(std::shared_ptr<fragment_t>& fragment,
                                 vid_t gid, const rpc::ReportType& report_type,
                                 grape::InArchive& arc) {
    vineyard::IdParser<vid_t> id_parser;
    auto fid = fragment->fid();
    auto fnum = fragment->fnum();
    auto label_num = fragment->vertex_label_num();
    id_parser.Init(fnum, label_num);
    if (id_parser.GetFid(gid) == fid) {
      dynamic::Value adj_list(rapidjson::kArrayType);
      vertex_t v;
      fragment->InnerVertexGid2Vertex(gid, v);
      auto label_id = id_parser.GetLabelId(v.GetValue());
      for (int cnt = 0; cnt < batch_num_;) {
        if (id_parser.GetOffset(v.GetValue()) <
            static_cast<int64_t>(fragment->GetInnerVerticesNum(label_id))) {
          dynamic::Value neighbor_attrs(rapidjson::kArrayType);
          for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
               e_label++) {
            auto edge_data = fragment->edge_data_table(e_label);
            for (auto& e : report_type == rpc::PRED_ATTR_BY_GID
                               ? fragment->GetIncomingAdjList(v, e_label)
                               : fragment->GetOutgoingAdjList(v, e_label)) {
              dynamic::Value data(rapidjson::kObjectType);
              PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(),
                                                       data);
              neighbor_attrs.PushBack(data);
            }
          }
          adj_list.PushBack(neighbor_attrs);
          v++;
          cnt++;
        } else if (label_id < label_num - 1) {
          label_id++;
          auto gid_ = id_parser.GenerateId(fid, label_id, 0);
          fragment->InnerVertexGid2Vertex(gid_, v);
        } else {
          break;
        }
      }
      // archive the start gid and edges attributes array.
      msgpack::sbuffer sbuf;
      msgpack::pack(&sbuf, adj_list);
      arc << gid << sbuf;
    }
  }

  grape::CommSpec comm_spec_;
  label_id_t default_label_id_;
  static const int batch_num_ = 10000000;
};
}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_FRAGMENT_REPORTER_H_
