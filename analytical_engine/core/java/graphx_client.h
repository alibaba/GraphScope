/** Copyright 2022 Alibaba Group Holding Limited.
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_CLIENT_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_CLIENT_H_

#include <memory>
#include <string>

#include "core/java/graphx_loader.h"
#include "core/java/rdd_transfer_client.h"

namespace gs {

template <typename OID_T, typename VID_T, typename VDATA_T = std::string,
          typename EDATA_T = std::string>
class GraphXClient
    : public vineyard::BasicEVFragmentLoader<OID_T, VID_T,
                                             GraphXPartitioner<OID_T>> {
 public:
  GraphXClient(int listen_port, int part_cnt, vineyard::Client& client,
               const grape::CommSpec& comm_spec,
               const GraphXPartitioner<OID_T>& partitioner,
               bool directed = true, bool generate_eid = false,
               bool retain_oid = false)
      : vineyard::BasicEVFragmentLoader<OID_T, VID_T, GraphXPartitioner<OID_T>>(
            client, comm_spec, partitioner, directed, generate_eid,
            retain_oid) {
    listen_port_ = listen_port;
    part_cnt_ = part_cnt;
  }

  boost::leaf::result<vineyard::ObjectID> LoadFragment() {
    int request_port = listen_port_;
    std::string target_str = "localhost:" + std::to_string(request_port);
    RDDReaderClient node_client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

    node_client.RequestPartitionInfo();
    node_client.RequestArrItem();
    node_client.SendClose();

    std::shared_ptr<arrow::Table> vertex_table = node_client.get_vertex_table();
    LOG(INFO) << "Finish built vertex table";
    sleep(10);

    request_port += part_cnt_;
    target_str = "localhost:" + std::to_string(request_port);
    RDDReaderClient edge_client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

    edge_client.GetEdgeData();

    edge_client.RequestPartitionInfo();
    edge_client.RequestArrItem();
    edge_client.SendClose();
    std::shared_ptr<arrow::Table> edge_table = edge_client.get_edge_table();
    LOG(INFO) << "Finish built edge table";

    BOOST_LEAF_CHECK(this->AddVertexTable("v0", vertex_table));
    LOG(INFO) << "Fnish adding vertices";

    BOOST_LEAF_CHECK(this->ConstructVertices());
    LOG(INFO) << "Fnish Construct vertices";

    BOOST_LEAF_CHECK(this->AddEdgeTable("v0", "v0", "e0", edge_table));
    LOG(INFO) << "Fnish adding edges";

    BOOST_LEAF_CHECK(this->ConstructEdges());
    LOG(INFO) << "Fnish Construct edges";

    return this->ConstructFragment();
  }

 private:
  int part_cnt_;
  int listen_port_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_CLIENT_H_
