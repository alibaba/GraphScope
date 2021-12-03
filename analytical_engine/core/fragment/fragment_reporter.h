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
#include "folly/dynamic.h"
#include "folly/json.h"

#include "grape/communication/communicator.h"
#include "grape/worker/comm_spec.h"

#include "core/server/rpc_utils.h"
#include "core/utils/convert_utils.h"
#include "proto/types.pb.h"

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
    json_opts_.allow_non_string_keys = true;
    json_opts_.allow_nan_inf = true;
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
      oid_t node_id = folly::parseJson(node_in_json, json_opts_)[0];
      return std::to_string(hasNode(fragment, node_id));
    }
    case rpc::HAS_EDGE: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      oid_t edge = folly::parseJson(edge_in_json, json_opts_);
      auto& src_id = edge[0];
      auto& dst_id = edge[1];
      return std::to_string(hasEdge(fragment, src_id, dst_id));
    }
    case rpc::NODE_DATA: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id = folly::parseJson(node_in_json, json_opts_)[0];
      return getNodeData(fragment, node_id);
    }
    case rpc::EDGE_DATA: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      oid_t edge = folly::parseJson(edge_in_json, json_opts_);
      auto& src_id = edge[0];
      auto& dst_id = edge[1];
      return getEdgeData(fragment, src_id, dst_id);
    }
    case rpc::DEG_BY_NODE:
    case rpc::IN_DEG_BY_NODE:
    case rpc::OUT_DEG_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      BOOST_LEAF_AUTO(edge_key, params.Get<std::string>(rpc::EDGE_KEY));
      oid_t node_id = folly::parseJson(node_in_json, json_opts_)[0];
      return std::to_string(
          getDegree(fragment, node_id, report_type, edge_key));
    }
    case rpc::DEG_BY_LOC:
    case rpc::IN_DEG_BY_LOC:
    case rpc::OUT_DEG_BY_LOC: {
      BOOST_LEAF_AUTO(fid, params.Get<int64_t>(rpc::FID));
      BOOST_LEAF_AUTO(lid, params.Get<int64_t>(rpc::LID));
      BOOST_LEAF_AUTO(edge_key, params.Get<std::string>(rpc::EDGE_KEY));

      return batchGetDegree(fragment, fid, lid, report_type, edge_key);
    }
    case rpc::NEIGHBORS_BY_NODE:
    case rpc::SUCCS_BY_NODE:
    case rpc::PREDS_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      oid_t node_id = folly::parseJson(node_in_json, json_opts_)[0];
      return getNeighbors(fragment, node_id, report_type);
    }
    case rpc::NEIGHBORS_BY_LOC:
    case rpc::SUCCS_BY_LOC:
    case rpc::PREDS_BY_LOC: {
      BOOST_LEAF_AUTO(fid, params.Get<int64_t>(rpc::FID));
      BOOST_LEAF_AUTO(lid, params.Get<int64_t>(rpc::LID));
      return batchGetNeighbors(fragment, fid, lid, report_type);
    }
    case rpc::NODES_BY_LOC: {
      BOOST_LEAF_AUTO(fid, params.Get<int64_t>(rpc::FID));
      BOOST_LEAF_AUTO(lid, params.Get<int64_t>(rpc::LID));
      return batchGetNodes(fragment, fid, lid);
    }
    default:
      CHECK(false);
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
      return folly::toJson(fragment->GetData(v));
    }
    return std::string();
  }

  std::string getEdgeData(std::shared_ptr<fragment_t>& fragment, const oid_t& u,
                          const oid_t& v) {
    folly::dynamic ref_data;
    fragment->GetEdgeData(u, v, ref_data);
    return ref_data.isNull() ? std::string()
                             : folly::json::serialize(ref_data, json_opts_);
  }

  double getDegree(std::shared_ptr<fragment_t>& fragment, const oid_t& node,
                   const rpc::ReportType& type, const std::string& weight) {
    vertex_t v;
    double degree = 0, sum_degree = 0;
    if (fragment->GetInnerVertex(node, v) && fragment->IsAliveInnerVertex(v)) {
      degree = getGraphDegree(fragment, v, type, weight);
    }
    Sum(degree, sum_degree);
    return sum_degree;
  }

  std::string getNeighbors(std::shared_ptr<fragment_t>& fragment,
                           const oid_t& node,
                           const rpc::ReportType& report_type) {
    vertex_t v;
    folly::dynamic nbrs = folly::dynamic::array;
    if (fragment->GetInnerVertex(node, v) && fragment->IsAliveInnerVertex(v)) {
      adj_list_t edges;
      nbrs.resize(2, folly::dynamic::array);
      report_type == rpc::PREDS_BY_NODE
          ? edges = fragment->GetIncomingAdjList(v)
          : edges = fragment->GetOutgoingAdjList(v);
      for (auto& e : edges) {
        // nbrs[0] store the neighbors id array
        nbrs[0].push_back(fragment->GetId(e.neighbor()));
        // nbrs[1] store the neighbors data array
        nbrs[1].push_back(e.data());
      }
    }
    return nbrs.empty() ? std::string()
                        : folly::json::serialize(nbrs, json_opts_);
  }

  std::string batchGetNodes(std::shared_ptr<fragment_t>& fragment, vid_t fid,
                            vid_t start_lid) {
    if (fragment->fid() == fid) {
      int cnt = 0;
      vertex_t v(start_lid);
      folly::dynamic nodes = folly::dynamic::object;
      folly::dynamic batch_nodes = folly::dynamic::array;
      while (fragment->IsInnerVertex(v) && cnt < batch_num_) {
        if (fragment->IsAliveInnerVertex(v)) {
          folly::dynamic one_item = folly::dynamic::object;
          one_item.insert("id", fragment->GetId(v));
          one_item.insert("data", fragment->GetData(v));
          batch_nodes.push_back(one_item);
          ++cnt;
        }
        ++v;
      }
      // nodes["status"] store this batch_get_nodes operation status, if no node
      // to fetch, set false;  nodes["batch"] store the nodes.
      // nodes["next"] store the start vertex location of next batch_get_nodes
      // operation.
      if (!batch_nodes.empty()) {
        nodes["status"] = true;
        nodes["batch"] = batch_nodes;
        if (fragment->IsInnerVertex(v)) {
          nodes["next"] = folly::dynamic::array(fid, v.GetValue());
        } else {
          nodes["next"] = folly::dynamic::array(fid + 1, 0);
        }
      } else {
        nodes["status"] = false;
        nodes["next"] = folly::dynamic::array(fid + 1, 0);
      }
      return folly::json::serialize(nodes, json_opts_);
    }
    return std::string();
  }

  std::string batchGetDegree(std::shared_ptr<fragment_t>& fragment, vid_t fid,
                             vid_t start_lid, const rpc::ReportType& type,
                             const std::string& weight) {
    std::string ret;
    if (fragment->fid() == fid) {
      int cnt = 0;
      vertex_t v(start_lid);
      folly::dynamic ret_dy = folly::dynamic::object;
      folly::dynamic batch_degs = folly::dynamic::array;
      while (fragment->IsInnerVertex(v) && cnt < batch_num_) {
        if (fragment->IsAliveInnerVertex(v)) {
          folly::dynamic one_item = folly::dynamic::object;
          double degree = 0;
          one_item.insert("node", fragment->GetId(v));
          degree = getGraphDegree(fragment, v, type, weight);
          one_item["deg"] = degree;
          batch_degs.push_back(one_item);
          ++cnt;
        }
        ++v;
      }
      if (!batch_degs.empty()) {
        ret_dy["status"] = true;
        ret_dy["batch"] = batch_degs;
        if (fragment->IsInnerVertex(v)) {
          ret_dy["next"] = folly::dynamic::array(fid, v.GetValue());
        } else {
          ret_dy["next"] = folly::dynamic::array(fid + 1, 0);
        }
      } else {
        ret_dy["status"] = false;
        ret_dy["next"] = folly::dynamic::array(fid + 1, 0);
      }
      ret = folly::json::serialize(ret_dy, json_opts_);
    }
    return ret;
  }

  std::string batchGetNeighbors(std::shared_ptr<fragment_t>& fragment,
                                vid_t fid, vid_t start_lid,
                                const rpc::ReportType& type) {
    if (fragment->fid() == fid) {
      int cnt = 0;
      vertex_t v(start_lid);
      folly::dynamic ret_dy = folly::dynamic::object;
      folly::dynamic all_nbrs = folly::dynamic::array;
      while (fragment->IsInnerVertex(v) && cnt < batch_num_) {
        if (fragment->IsAliveInnerVertex(v)) {
          folly::dynamic one_item = folly::dynamic::object;
          one_item.insert("node", fragment->GetId(v));
          one_item["nbrs"] = folly::dynamic::object;
          if (type == rpc::NEIGHBORS_BY_LOC || type == rpc::SUCCS_BY_LOC) {
            auto oe = fragment->GetOutgoingAdjList(v);
            for (auto& e : oe) {
              one_item["nbrs"].insert(fragment->GetId(e.neighbor()), e.data());
            }
          }
          if (type == rpc::NEIGHBORS_BY_LOC || type == rpc::PREDS_BY_LOC) {
            auto ie = fragment->GetIncomingAdjList(v);
            for (auto& e : ie) {
              one_item["nbrs"].insert(fragment->GetId(e.neighbor()), e.data());
            }
          }
          all_nbrs.push_back(one_item);
          ++cnt;
        }
        ++v;
      }
      if (!all_nbrs.empty()) {
        ret_dy["status"] = true;
        ret_dy["batch"] = all_nbrs;
        if (fragment->IsInnerVertex(v)) {
          ret_dy["next"] = folly::dynamic::array(fid, v.GetValue());
        } else {
          ret_dy["next"] = folly::dynamic::array(fid + 1, 0);
        }
      } else {
        ret_dy["status"] = false;
        ret_dy["next"] = folly::dynamic::array(fid + 1, 0);
      }
      return folly::json::serialize(ret_dy, json_opts_);
    }
    return std::string();
  }

  double getGraphDegree(std::shared_ptr<fragment_t>& fragment, vertex_t& v,
                        const rpc::ReportType& type,
                        const std::string& weight) {
    double degree = 0;
    if (type == rpc::IN_DEG_BY_NODE || type == rpc::DEG_BY_NODE ||
        type == rpc::IN_DEG_BY_LOC || type == rpc::DEG_BY_LOC) {
      if (weight.empty()) {
        degree += static_cast<double>(fragment->GetLocalInDegree(v));
      } else {
        auto ie = fragment->GetIncomingAdjList(v);
        for (auto& e : ie) {
          degree += e.data().getDefault(weight, 1).asDouble();
        }
      }
    }
    if (type == rpc::OUT_DEG_BY_NODE || type == rpc::DEG_BY_NODE ||
        type == rpc::OUT_DEG_BY_LOC || type == rpc::DEG_BY_LOC) {
      if (weight.empty()) {
        degree += static_cast<double>(fragment->GetLocalOutDegree(v));
      } else {
        auto oe = fragment->GetOutgoingAdjList(v);
        for (auto& e : oe) {
          degree += e.data().getDefault(weight, 1).asDouble();
        }
      }
    }
    return degree;
  }

  grape::CommSpec comm_spec_;
  static const int batch_num_ = 100;
  folly::json::serialization_opts json_opts_;
};

template <typename T>
T ExtractOidFromDynamic(folly::dynamic node_id) {}

template <>
int64_t ExtractOidFromDynamic(folly::dynamic node_id) {
  return node_id.asInt();
}

template <>
std::string ExtractOidFromDynamic(folly::dynamic node_id) {
  return node_id.asString();
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
    json_opts_.allow_non_string_keys = true;
    json_opts_.allow_nan_inf = true;
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
      folly::dynamic node = folly::parseJson(node_in_json, json_opts_)[0];
      label_id_t label_id = node[0].asInt();
      oid_t oid = ExtractOidFromDynamic<oid_t>(node[1]);
      return std::to_string(hasNode(fragment, label_id, oid));
    }
    case rpc::HAS_EDGE: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      // the input edge format: ((u_label_id, u_oid), (v_label_id, v_oid))
      folly::dynamic edge = folly::parseJson(edge_in_json, json_opts_);
      label_id_t u_label_id = edge[0][0].asInt();
      label_id_t v_label_id = edge[1][0].asInt();
      auto u_oid = ExtractOidFromDynamic<oid_t>(edge[0][1]);
      auto v_oid = ExtractOidFromDynamic<oid_t>(edge[1][1]);
      return std::to_string(
          hasEdge(fragment, u_label_id, u_oid, v_label_id, v_oid));
    }
    case rpc::NODE_DATA: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      folly::dynamic node = folly::parseJson(node_in_json, json_opts_)[0];
      label_id_t label_id = node[0].asInt();
      oid_t oid = ExtractOidFromDynamic<oid_t>(node[1]);
      return getNodeData(fragment, label_id, oid);
    }
    case rpc::EDGE_DATA: {
      BOOST_LEAF_AUTO(edge_in_json, params.Get<std::string>(rpc::EDGE));
      // the input edge format: ((u_label_id, u_oid), (v_label_id, v_oid))
      folly::dynamic edge = folly::parseJson(edge_in_json, json_opts_);
      label_id_t u_label_id = edge[0][0].asInt();
      label_id_t v_label_id = edge[1][0].asInt();
      auto u_oid = ExtractOidFromDynamic<oid_t>(edge[0][1]);
      auto v_oid = ExtractOidFromDynamic<oid_t>(edge[1][1]);
      return getEdgeData(fragment, u_label_id, u_oid, v_label_id, v_oid);
    }
    case rpc::NEIGHBORS_BY_NODE:
    case rpc::SUCCS_BY_NODE:
    case rpc::PREDS_BY_NODE: {
      BOOST_LEAF_AUTO(node_in_json, params.Get<std::string>(rpc::NODE));
      // the input node format: (label_id, oid)
      folly::dynamic node = folly::parseJson(node_in_json, json_opts_)[0];
      label_id_t label_id = node[0].asInt();
      oid_t oid = ExtractOidFromDynamic<oid_t>(node[1]);
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
    folly::dynamic ref_data;
    vid_t gid;
    vertex_t v;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), label_id, n, gid) &&
        fragment->InnerVertexGid2Vertex(gid, v)) {
      ref_data = folly::dynamic::object;
      auto vertex_data = fragment->vertex_data_table(label_id);
      // N.B: th last column is id, we ignore it.
      for (auto col_id = 0; col_id < vertex_data->num_columns() - 1; col_id++) {
        auto prop_name = vertex_data->field(col_id)->name();
        auto type = vertex_data->column(col_id)->type();
        PropertyConverter<fragment_t>::NodeValue(fragment, v, type, prop_name,
                                                 col_id, ref_data);
      }
    }
    return ref_data.isNull() ? std::string() : folly::toJson(ref_data);
  }

  std::string getEdgeData(std::shared_ptr<fragment_t>& fragment,
                          label_id_t u_label_id, const oid_t& u_oid,
                          label_id_t v_label_id, const oid_t& v_oid) {
    folly::dynamic ref_data;
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
            ref_data = folly::dynamic::object;
            auto edge_data = fragment->edge_data_table(e_label);
            PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(),
                                                     ref_data);
          }
        }
      }
    }
    return ref_data.isNull() ? std::string() : folly::toJson(ref_data);
  }

  std::string getNeighbors(std::shared_ptr<fragment_t>& fragment,
                           label_id_t label_id, const oid_t& n,
                           const rpc::ReportType& report_type) {
    vid_t gid;
    vertex_t v;
    folly::dynamic nbrs = folly::dynamic::array;
    auto vm_ptr = fragment->GetVertexMap();
    if (vm_ptr->GetGid(fragment->fid(), label_id, n, gid) &&
        fragment->InnerVertexGid2Vertex(gid, v)) {
      nbrs.resize(2, folly::dynamic::array);
      for (label_id_t e_label = 0; e_label < fragment->edge_label_num();
           e_label++) {
        adj_list_t edges;
        auto edge_data = fragment->edge_data_table(e_label);
        if (report_type == rpc::PREDS_BY_NODE) {
          edges = fragment->GetIncomingAdjList(v, e_label);
        } else {
          edges = fragment->GetOutgoingAdjList(v, e_label);
        }
        for (auto& e : edges) {
          auto n_label_id = fragment->vertex_label(e.neighbor());
          // nbrs[0] store the neighbors id array
          if (n_label_id == default_label_id_) {
            nbrs[0].push_back(fragment->GetId(e.neighbor()));
          } else {
            nbrs[0].push_back(folly::dynamic::array(
                fragment->schema().GetVertexLabelName(n_label_id),
                fragment->GetId(e.neighbor())));
          }
          folly::dynamic ob = folly::dynamic::object;
          PropertyConverter<fragment_t>::EdgeValue(edge_data, e.edge_id(), ob);
          // nbrs[1] store the neighbors data array
          nbrs[1].push_back(ob);
        }
      }
    }
    return nbrs.empty() ? std::string() : folly::toJson(nbrs);
  }

  std::string batchGetNodes(std::shared_ptr<fragment_t>& fragment, vid_t fid,
                            label_id_t label_id, vid_t start, vid_t end) {
    folly::dynamic ob = folly::dynamic::object;
    if (fragment->fid() == fid) {
      folly::dynamic batch_nodes = folly::dynamic::array;
      auto label_name = fragment->schema().GetVertexLabelName(label_id);
      for (auto v : fragment->InnerVerticesSlice(label_id, start, end)) {
        folly::dynamic one_item = folly::dynamic::object;
        if (label_id == default_label_id_) {
          one_item["id"] = fragment->GetId(v);
        } else {
          one_item["id"] =
              folly::dynamic::array(label_name, fragment->GetId(v));
        }
        one_item["data"] = folly::dynamic::object;
        auto vertex_data = fragment->vertex_data_table(label_id);
        // N.B: th last column is id, we ignore it.
        for (auto col_id = 0; col_id < vertex_data->num_columns() - 1;
             col_id++) {
          auto prop_name = vertex_data->field(col_id)->name();
          auto type = vertex_data->column(col_id)->type();
          PropertyConverter<fragment_t>::NodeValue(fragment, v, type, prop_name,
                                                   col_id, one_item["data"]);
        }
        batch_nodes.push_back(one_item);
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
      if (batch_nodes.empty()) {
        ob["status"] = false;
      } else {
        ob["status"] = true;
        ob["batch"] = batch_nodes;
      }
      // the start vertex location of next batch_get_nodes operation.
      ob["next"] = folly::dynamic::array(fid, start, label_id);
    }
    return ob.empty() ? std::string() : folly::toJson(ob);
  }

  grape::CommSpec comm_spec_;
  label_id_t default_label_id_;
  folly::json::serialization_opts json_opts_;
  static const int batch_num_ = 100;
};
}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_FRAGMENT_REPORTER_H_
