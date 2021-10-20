/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.coordinator.service;

import com.alibaba.maxgraph.coordinator.MetricCollector;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.cluster.management.ClusterApplierService;
import com.alibaba.maxgraph.common.cluster.management.ClusterState;
import com.alibaba.maxgraph.common.server.AbstractRpcServer;
import com.alibaba.maxgraph.coordinator.manager.ServerDataManager;
import com.alibaba.maxgraph.proto.*;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CoordinatorRpcServer extends AbstractRpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRpcServer.class);

    private MasterService masterService;
    private ClusterApplierService clusterApplierService;
    private ServerDataManager serverDataManager;
    private MetricCollector metricCollector;

    public CoordinatorRpcServer(ClusterApplierService clusterApplierService, MasterService masterService,
                                ServerDataManager serverDataManager, MetricCollector metricCollector) {
        this.clusterApplierService = clusterApplierService;
        this.masterService = masterService;
        this.serverDataManager = serverDataManager;
        this.metricCollector = metricCollector;
    }

    @Override
    public BindableService getService() {
        return new CoordinatorGrpcService();
    }

    void resendRequest(HeartbeartRequest request) {
        NodeInfo nodeInfo = request.getNodeInfo();
        RoleType role = nodeInfo.getNodeId().getRole();
        int serverID = nodeInfo.getServerId();
        serverDataManager.instanceInfo.onSimpleHeartBeat(role, serverID,
                new Endpoint(nodeInfo.getHost(), nodeInfo.getPort()), request.getLogDir());
    }

    class CoordinatorGrpcService extends CoordinatorGrpc.CoordinatorImplBase {

        @Override
        public void heartbeat(HeartbeartRequest request, StreamObserver<HeartbeartResponse> responseObserver) {
            HeartbeartResponse.Builder responseBuilder = HeartbeartResponse.newBuilder();

            NodeInfo nodeInfo = request.getNodeInfo();
            Endpoint endpoint = new Endpoint(nodeInfo.getHost(), nodeInfo.getPort());
            Pair<Boolean, Long> isLegalResult = serverDataManager.instanceInfo.checkAndUpdateAliveId(nodeInfo.getNodeId().getRole(),
                    nodeInfo.getServerId(), request.getAliveId(), endpoint);
            boolean isLegal = isLegalResult.getLeft();
            responseBuilder.setIsLegal(isLegal);
            responseBuilder.setAliveId(isLegalResult.getRight());
            LOG.info("serverId is {} isLegal is {} aliveId is {}", nodeInfo.getServerId(), isLegal, isLegalResult.getRight());

            if (isLegal) {
                resendRequest(request);
                metricCollector.updateMetrics(request.getNodeInfo().getServerId(), request.getMetricInfo(),
                        request.getNodeInfo().getNodeId().getId());
                // Refresh heart beat
                long heartbeatTime = System.currentTimeMillis();

                NodeStateProto nodeStateProto = request.getNodeStateProto();
                masterService.updateNodeState(nodeInfo, heartbeatTime, nodeStateProto);

                // Return new cluster version if needed
                long workerClusterStateVersion = request.getClusterStateVersion();
                ClusterState currentClusterState = clusterApplierService.lastAppliedState();
                responseBuilder.setClusterStateVersion(currentClusterState.version());

                if (currentClusterState.version() > workerClusterStateVersion) {
                    ClusterStateProto.Builder builder = ClusterStateProto.newBuilder(currentClusterState.toProto());
                    NodeID nodeId = nodeInfo.getNodeId();
                    builder.clearExpectedStateMap();
                    if (nodeId.getRole() == RoleType.EXECUTOR) {
                        builder.putExpectedStateMap(nodeId.getId(),
                                currentClusterState.getNodeExpectedStateMap().get(nodeInfo.getServerId()));
                    }
                    responseBuilder.setClusterState(builder.build());
                }
            } else {
                LOG.error("ingest-node is illegal, serverId is {}", nodeInfo.getServerId());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

    }
}
