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

#ifndef ANALYTICAL_ENGINE_CORE_GRAPE_INSTANCE_H_
#define ANALYTICAL_ENGINE_CORE_GRAPE_INSTANCE_H_

#include <glog/logging.h>
#include <mpi.h>

#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "boost/foreach.hpp"
#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "grape/communication/sync_comm.h"
#include "grape/config.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/graph/utils/grape_utils.h"

#include "core/object/object_manager.h"
#include "core/server/dispatcher.h"
#include "core/server/rpc_utils.h"
#include "proto/types.pb.h"

namespace bl = boost::leaf;

namespace grape {
class InArchive;
}  // namespace grape
namespace vineyard {
class Client;
}  // namespace vineyard

namespace gs {
class IContextWrapper;
namespace rpc {
namespace graph {
class GraphDefPb;
}  // namespace graph
}  // namespace rpc
struct CommandDetail;

/**
 * @brief EngineConfig contains configurations about the analytical engine, such
 * as networkx features in enabled or not, vineyard socket, and vineyard rpc
 * endpoint.
 */
struct EngineConfig {
  std::string networkx;
  std::string vineyard_socket;
  std::string vineyard_rpc_endpoint;
  std::string enable_java_sdk;

  std::string ToJsonString() const {
    boost::property_tree::ptree pt;
    pt.put("networkx", networkx);
    pt.put("vineyard_socket", vineyard_socket);
    pt.put("vineyard_rpc_endpoint", vineyard_rpc_endpoint);
    pt.put("enable_java_sdk", enable_java_sdk);
    std::stringstream ss;
    boost::property_tree::json_parser::write_json(ss, pt);
    return ss.str();
  }
};
/** @brief MPI management.
 *
 * This controller initials MPI communication world, assign a rank to each
 * process. According to the assigned rank, determines whether this process runs
 * as a coordinator or a worker. It also in charges of execute commands from
 * coordinator.
 */
class GrapeInstance : public Subscriber {
 public:
  explicit GrapeInstance(const grape::CommSpec& comm_spec);

  void Init(const std::string& vineyard_socket);

  bl::result<std::shared_ptr<DispatchResult>> OnReceive(
      std::shared_ptr<CommandDetail> cmd) override;

 private:
  bl::result<rpc::graph::GraphDefPb> loadGraph(const rpc::GSParams& params);

  bl::result<void> unloadGraph(const rpc::GSParams& params);

  bl::result<void> archiveGraph(const rpc::GSParams& params);

  bl::result<std::string> loadApp(const rpc::GSParams& params);

  bl::result<void> unloadApp(const rpc::GSParams& params);

  bl::result<std::string> query(const rpc::GSParams& params,
                                const rpc::QueryArgs& query_args);

  bl::result<void> unloadContext(const rpc::GSParams& params);

  bl::result<std::shared_ptr<grape::InArchive>> reportGraph(
      const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> projectGraph(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> projectToSimple(
      const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> modifyVertices(
      const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> modifyEdges(const rpc::GSParams& params);

  bl::result<void> clearEdges(const rpc::GSParams& params);

  bl::result<void> clearGraph(const rpc::GSParams& params);

  bl::result<std::shared_ptr<grape::InArchive>> contextToNumpy(
      const rpc::GSParams& params);

  bl::result<std::shared_ptr<grape::InArchive>> contextToDataframe(
      const rpc::GSParams& params);

  bl::result<std::string> contextToVineyardTensor(const rpc::GSParams& params);

  bl::result<std::string> contextToVineyardDataFrame(
      const rpc::GSParams& params);

  bl::result<void> outputContext(const rpc::GSParams& params);

  bl::result<std::string> output(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> addColumn(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> convertGraph(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> copyGraph(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> toDirected(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> toUnDirected(const rpc::GSParams& params);

  bl::result<rpc::graph::GraphDefPb> createGraphView(
      const rpc::GSParams& params);

#ifdef NETWORKX
  bl::result<rpc::graph::GraphDefPb> induceSubGraph(
      const rpc::GSParams& params);
#endif  // NETWORKX

  bl::result<rpc::graph::GraphDefPb> addLabelsToGraph(
      const rpc::GSParams& params);
  bl::result<std::string> getContextData(const rpc::GSParams& params);

  bl::result<std::shared_ptr<grape::InArchive>> graphToNumpy(
      const rpc::GSParams& params);

  bl::result<std::shared_ptr<grape::InArchive>> graphToDataframe(
      const rpc::GSParams& params);

  bl::result<void> registerGraphType(const rpc::GSParams& params);

  bl::result<void> getContextDetails(
      const rpc::GSParams& params, std::string* s_selector,
      std::pair<std::string, std::string>* range,
      std::shared_ptr<IContextWrapper>* wrapper) {
    if (params.HasKey(rpc::SELECTOR)) {
      BOOST_LEAF_ASSIGN(*s_selector, params.Get<std::string>(rpc::SELECTOR));
    }
    if (params.HasKey(rpc::VERTEX_RANGE)) {
      BOOST_LEAF_AUTO(range_in_json,
                      params.Get<std::string>(rpc::VERTEX_RANGE));
      *range = parseRange(range_in_json);
    }
    BOOST_LEAF_AUTO(context_key, params.Get<std::string>(rpc::CONTEXT_KEY));
    BOOST_LEAF_ASSIGN(*wrapper,
                      object_manager_.GetObject<IContextWrapper>(context_key));
    return {};
  }

  static std::string toJson(const std::map<std::string, std::string>& map) {
    boost::property_tree::ptree pt;

    for (auto& e : map) {
      pt.put(e.first, e.second);
    }

    std::stringstream ss;
    boost::property_tree::json_parser::write_json(ss, pt);
    return ss.str();
  }

  static std::pair<std::string, std::string> parseRange(
      const std::string& range) {
    // format: "{begin: a, end: b}" or "{begin: a}" or "{end: b}" or "{}"
    std::stringstream ss(range);
    boost::property_tree::ptree pt;
    std::string begin;
    std::string end;
    try {
      boost::property_tree::json_parser::read_json(ss, pt);
      BOOST_FOREACH  // NOLINT(whitespace/parens)
          (boost::property_tree::ptree::value_type & v, pt) {
        CHECK(v.second.empty());
        if (v.first == "begin") {
          begin = v.second.data();
        }
        if (v.first == "end") {
          end = v.second.data();
        }
      }
    } catch (boost::property_tree::ptree_error& e) {
      begin = "";
      end = "";
    }
    return std::make_pair(begin, end);
  }

  std::string generateId() {
    std::string id;

    if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
      id = vineyard::random_string(8);
    }
    grape::sync_comm::Bcast(id, grape::kCoordinatorRank, MPI_COMM_WORLD);
    return id;
  }

  grape::CommSpec comm_spec_;
  ObjectManager object_manager_;
  std::shared_ptr<vineyard::Client> client_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_GRAPE_INSTANCE_H_
