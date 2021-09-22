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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_REPORTER_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_REPORTER_H_

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
#include "proto/types.pb.h"

namespace gs {
/**
 * @brief DynamicGraphReporter is used to query the vertex and edge information
 * of DynamicFragment.
 */
class DynamicGraphReporter : public grape::Communicator {
  using fragment_t = DynamicFragment;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;

 public:
  explicit DynamicGraphReporter(const grape::CommSpec& comm_spec)
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
    bool to_send = fragment->HasNode(node);
    Sum(to_send, ret);
    return ret;
  }

  bool hasEdge(std::shared_ptr<fragment_t>& fragment, const oid_t& u,
               const oid_t& v) {
    bool ret = false;
    bool to_send = fragment->HasEdge(u, v);
    Sum(to_send, ret);
    return ret;
  }

  std::string getNodeData(std::shared_ptr<fragment_t>& fragment,
                          const oid_t& n) {
    std::string ret;
    fragment->GetVertexData(n, ret);
    return ret;
  }

  std::string getEdgeData(std::shared_ptr<fragment_t>& fragment, const oid_t& u,
                          const oid_t& v) {
    std::string ret;
    fragment->GetEdgeData(u, v, ret);
    return ret;
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
                           const oid_t& node, const rpc::ReportType& type) {
    vertex_t v;
    std::string ret;
    folly::dynamic nbrs = folly::dynamic::array;
    nbrs.resize(2, folly::dynamic::array);
    if (fragment->GetInnerVertex(node, v) && fragment->IsAliveInnerVertex(v)) {
      if (type == rpc::NEIGHBORS_BY_NODE || type == rpc::SUCCS_BY_NODE) {
        auto oe = fragment->GetOutgoingAdjList(v);
        for (auto& e : oe) {
          nbrs[0].push_back(fragment->GetId(e.neighbor()));
          nbrs[1].push_back(e.data());
        }
      }
      if (type == rpc::NEIGHBORS_BY_NODE || type == rpc::PREDS_BY_NODE) {
        auto ie = fragment->GetIncomingAdjList(v);
        for (auto& e : ie) {
          nbrs[0].push_back(fragment->GetId(e.neighbor()));
          nbrs[1].push_back(e.data());
        }
      }
      ret = folly::json::serialize(nbrs, json_opts_);
    }
    return ret;
  }

  std::string batchGetNodes(std::shared_ptr<fragment_t>& fragment, vid_t fid,
                            vid_t start_lid) {
    std::string ret;
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
      ret = folly::json::serialize(nodes, json_opts_);
    }
    return ret;
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
    std::string ret;
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
      ret = folly::json::serialize(ret_dy, json_opts_);
    }
    return ret;
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
}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_REPORTER_H_
