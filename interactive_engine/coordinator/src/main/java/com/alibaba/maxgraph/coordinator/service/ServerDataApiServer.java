/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.coordinator.service;

import com.alibaba.maxgraph.common.DataStatus;
import com.alibaba.maxgraph.common.server.AbstractRpcServer;
import com.alibaba.maxgraph.common.util.RpcUtils;
import com.alibaba.maxgraph.coordinator.MetricCollector;
import com.alibaba.maxgraph.coordinator.manager.InstanceInfo;
import com.alibaba.maxgraph.coordinator.manager.ServerDataManager;
import com.alibaba.maxgraph.coordinator.manager.runtime.GroupStatus;
import com.alibaba.maxgraph.proto.*;
import com.alibaba.maxgraph.proto.InstanceInfoResp.Builder;
import com.alibaba.maxgraph.proto.ServerDataApiGrpc.ServerDataApiImplBase;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.google.common.collect.Iterators;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @date 2018-06-12 下午1:34
 */
public class ServerDataApiServer extends AbstractRpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDataApiServer.class);
    private ServerDataManager serverDataManager;
    private MasterService masterService;
    private MetricCollector metricCollector;

    public ServerDataApiServer(
            ServerDataManager serverDataManager,
            MasterService masterService,
            MetricCollector metricCollector) {
        this.serverDataManager = serverDataManager;
        this.masterService = masterService;
        this.metricCollector = metricCollector;
    }

    @Override
    public BindableService getService() {
        return new ServerDataService();
    }

    @Override
    public String getName() {
        return "ServerDataApiServer";
    }

    void resendRequest(ServerHBReq request) throws Exception {
        StoreStatus status = request.getStatus();
        if (status == StoreStatus.INITIALING) {
            LOG.debug("ignore initialing node");
            return;
        }
        long heartbeatTime = System.currentTimeMillis();

        int serverID = request.getId();
        int nodeId = serverID - 1;
        EndpointProto endpoint = request.getEndpoint();
        NodeInfo nodeInfo =
                NodeInfo.newBuilder()
                        .setNodeId(NodeID.newBuilder().setRole(RoleType.STORE_NODE).setId(nodeId))
                        .setHost(endpoint.getHost())
                        .setPort(endpoint.getPort())
                        .build();

        NodeStateProto.Builder nodeStateBuilder = NodeStateProto.newBuilder();
        for (Integer partition :
                serverDataManager.partitionManager.getServerAssignment(serverID).getPartitions()) {
            nodeStateBuilder.putNodeStateMap(
                    partition, StateList.newBuilder().addStates(ShardState.ONLINING_VALUE).build());
        }
        NodeStateProto nodeState = nodeStateBuilder.build();
        masterService.updateNodeState(nodeInfo, heartbeatTime, nodeState);
    }

    void resendSimpleRequest(SimpleServerHBReq request) {
        int nodeId = request.getNodeId();
        long heartbeatTime = System.currentTimeMillis();

        EndpointProto endpoint = request.getEndpoint();
        NodeInfo nodeInfo =
                NodeInfo.newBuilder()
                        .setNodeId(NodeID.newBuilder().setRole(request.getRoleType()).setId(nodeId))
                        .setHost(endpoint.getHost())
                        .setPort(endpoint.getPort())
                        .build();
        masterService.updateNodeState(nodeInfo, heartbeatTime, null);
    }

    private class ServerDataService extends ServerDataApiImplBase {

        @Override
        public void heartbeat(ServerHBReq request, StreamObserver<ServerHBResp> responseObserver) {
            try {
                resendRequest(request);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ServerHBResp.Builder builder = ServerHBResp.newBuilder();
            Function<Response, ServerHBResp.Builder> responseBuilderFunction = builder::setResp;

            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        Pair<Boolean, Long> legalResult =
                                serverDataManager.instanceInfo.checkAndUpdateAliveId(
                                        RoleType.EXECUTOR,
                                        request.getId(),
                                        request.getAliveId(),
                                        Endpoint.fromProto(request.getEndpoint()));
                        boolean isLegal = legalResult.getLeft();
                        builder.setIsLegal(isLegal).setAliveId(legalResult.getRight());
                        LOG.info(
                                "serverId is {} isLegal is {} aliveId is {}",
                                request.getId(),
                                isLegal,
                                legalResult.getRight());

                        if (isLegal) {
                            metricCollector.updateMetrics(
                                    request.getId(), request.getInfoProto(), -1);
                            InstanceInfo instanceInfo = serverDataManager.instanceInfo;
                            DataStatus dataStatus = DataStatus.fromProto(request);
                            instanceInfo.onServerHeartBeat(builder, dataStatus);
                        } else {
                            LOG.info(
                                    "worker {} on host {} is illegal.",
                                    request.getId(),
                                    request.getEndpoint().getHost());
                        }

                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void simpleHeartbeat(
                SimpleServerHBReq request,
                StreamObserver<SimpleServerHBResponse> responseObserver) {
            try {
                if (request.getRoleType() == RoleType.FRONTEND) {
                    resendSimpleRequest(request);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            SimpleServerHBResponse.Builder builder = SimpleServerHBResponse.newBuilder();

            Function<Response, SimpleServerHBResponse.Builder> responseBuilderFunction =
                    builder::setResp;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        Pair<Boolean, Long> legalResult =
                                serverDataManager.instanceInfo.checkAndUpdateAliveId(
                                        request.getRoleType(),
                                        request.getId(),
                                        request.getAliveId(),
                                        Endpoint.fromProto(request.getEndpoint()));
                        boolean isLegal = legalResult.getLeft();
                        builder.setIsLegal(isLegal).setAliveId(legalResult.getRight());
                        LOG.info(
                                "serverId is {} isLegal is {} aliveId is {}",
                                request.getId(),
                                isLegal,
                                legalResult.getRight());

                        if (isLegal) {
                            serverDataManager.instanceInfo.onSimpleHeartBeat(
                                    request.getRoleType(),
                                    request.getId(),
                                    Endpoint.fromProto(request.getEndpoint()),
                                    request.getLogDir());
                            metricCollector.updateMetrics(
                                    request.getId(), request.getInfoProto(), -1);
                            if (request.getRoleType() == RoleType.FRONTEND) {
                                WorkerInfoProtos.Builder workerInfoBuilder =
                                        WorkerInfoProtos.newBuilder();
                                serverDataManager.instanceInfo.initSimpleHBResponseForFrontend(
                                        workerInfoBuilder);
                                builder.setWorkerInfoProtos(workerInfoBuilder.build());
                            }
                        } else {
                            LOG.info(
                                    "worker {} on host {} is illegal.",
                                    request.getId(),
                                    request.getEndpoint().getHost());
                        }

                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void getRealTimeMetric(
                MetricInfoRequest request, StreamObserver<MetricInfoResp> responseObserver) {
            MetricInfoResp.Builder builder = MetricInfoResp.newBuilder();
            Function<Response, MetricInfoResp.Builder> responseBuilderFunction = builder::setResp;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        List<ServerMetricValue> latestMetrics =
                                metricCollector.getMetricByName(request.getMetricName());
                        LOG.info("latestMetrics is {}", latestMetrics);
                        builder.addAllValues(latestMetrics);
                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void getAllRealTimeMetrics(
                Request request, StreamObserver<AllMetricsInfoResp> responseObserver) {
            AllMetricsInfoResp.Builder builder = AllMetricsInfoResp.newBuilder();
            Function<Response, AllMetricsInfoResp.Builder> responseBuilderFunction =
                    builder::setResp;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        Map<String, List<ServerMetricValue>> allMetrics =
                                metricCollector.getAllMetrics();
                        for (Map.Entry<String, List<ServerMetricValue>> entry :
                                allMetrics.entrySet()) {
                            AllMetricsInfoProto.Builder infoBuild =
                                    AllMetricsInfoProto.newBuilder();
                            infoBuild.setMetricName(entry.getKey());
                            infoBuild.addAllValues(entry.getValue());
                            builder.addInfo(infoBuild.build());
                        }
                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void getWorkerInfoAndRoutingServerList(
                Request request, StreamObserver<RoutingServerInfoResp> responseObserver) {
            RoutingServerInfoResp.Builder builder = RoutingServerInfoResp.newBuilder();
            Function<Response, RoutingServerInfoResp.Builder> responseBuilderFunction =
                    builder::setResp;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        WorkerInfoProtos.Builder workerInfoBuilder = WorkerInfoProtos.newBuilder();
                        serverDataManager.instanceInfo.getExecutorWorkerInfo(workerInfoBuilder);
                        builder.setWorkerInfoProtos(workerInfoBuilder.build());
                        List<Integer> currentRouting =
                                serverDataManager.instanceInfo.getCurrentRouting();
                        builder.addAllServingServerId(currentRouting);
                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void getInstanceInfo(
                Request request, StreamObserver<InstanceInfoResp> responseObserver) {
            LOG.info("received getInstanceInfo");
            Builder builder = InstanceInfoResp.newBuilder();
            Function<Response, Builder> responseBuilderFunction = builder::setResp;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        InstanceInfo instanceInfo = serverDataManager.instanceInfo;
                        builder.setInstanceInfoProto(instanceInfo.toSimpleProto().build());
                        return Iterators.singletonIterator(builder.build());
                    });
        }

        public void updateRuntimeEnv(
                RuntimeEnv request, StreamObserver<RuntimeEnvList> responseObserver) {
            LOG.info(
                    "received Update RuntimeEnv, id: {}, port: {}",
                    request.getId(),
                    request.getPort());
            List<String> envs =
                    serverDataManager.instanceInfo.updateExecutorRuntimeEnv(
                            request.getId(), request.getIp(), request.getPort());
            RuntimeEnvList.Builder builder = RuntimeEnvList.newBuilder();
            envs.forEach(builder::addEnvs);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        public void resetRuntimeEnv(Empty request, StreamObserver<Empty> responseObserver) {
            LOG.info("received Reset RuntimeEnv...");
            serverDataManager.instanceInfo.resetRuntimeEnv();
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void getRuntimeGroupStatus(
                Empty request, StreamObserver<RuntimeGroupStatusResp> responseObserver) {
            RuntimeGroupStatusResp.Builder builder = RuntimeGroupStatusResp.newBuilder();
            Function<Response, RuntimeGroupStatusResp.Builder> responseBuilderFunction =
                    builder::setResponse;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        Map<Integer, GroupStatus> groupsStatus =
                                serverDataManager.runtimeManager.getGroupsStatus();
                        for (Map.Entry<Integer, GroupStatus> entry : groupsStatus.entrySet()) {
                            GroupStatus singleGroupStatus = entry.getValue();
                            switch (singleGroupStatus) {
                                case RUNNING:
                                    builder.putRuntimeGroupsStstus(
                                            entry.getKey(), RuntimeGroupStatusResp.Status.RUNNING);
                                    break;
                                case READY:
                                    builder.putRuntimeGroupsStstus(
                                            entry.getKey(), RuntimeGroupStatusResp.Status.READY);
                                    break;
                                case RESTORE:
                                    builder.putRuntimeGroupsStstus(
                                            entry.getKey(), RuntimeGroupStatusResp.Status.RESTORE);
                                    break;
                                case STARTING:
                                    builder.putRuntimeGroupsStstus(
                                            entry.getKey(), RuntimeGroupStatusResp.Status.STARTING);
                                    break;
                                default:
                                    builder.putRuntimeGroupsStstus(
                                            entry.getKey(),
                                            RuntimeGroupStatusResp.Status.UNRECOGNIZED);
                                    break;
                            }
                        }
                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void isDataPathInUse(
                ServerIdAliveIdProto request,
                StreamObserver<DataPathStatusResponse> responseObserver) {
            DataPathStatusResponse.Builder builder = DataPathStatusResponse.newBuilder();
            Function<Response, DataPathStatusResponse.Builder> responseBuilderFunction =
                    builder::setResponse;
            RpcUtils.execute(
                    LOG,
                    responseObserver,
                    responseBuilderFunction,
                    () -> {
                        int serverId = request.getServerId();
                        long aliveId = request.getAliveId();
                        builder.setIsInUse(
                                serverDataManager.instanceInfo.isDataPathInUse(serverId, aliveId));

                        return Iterators.singletonIterator(builder.build());
                    });
        }

        @Override
        public void getExecutorAliveId(
                GetExecutorAliveIdRequest request,
                StreamObserver<GetExecutorAliveIdResponse> responseObserver) {
            GetExecutorAliveIdResponse.Builder builder = GetExecutorAliveIdResponse.newBuilder();
            Pair<Boolean, Long> result =
                    serverDataManager.instanceInfo.checkAndUpdateAliveId(
                            RoleType.EXECUTOR,
                            request.getServerId(),
                            0L,
                            new Endpoint(request.getIp(), 1234));
            builder.setAliveId(result.getRight());
            LOG.info(
                    "rpc serverId is {} isLegal is {} aliveId is {}",
                    request.getServerId(),
                    result.getLeft(),
                    result.getRight());
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getPartitionAssignment(
                GetPartitionAssignmentRequest request,
                StreamObserver<GetPartitionAssignmentResponse> responseObserver) {
            Set<Integer> partitions = new HashSet<>();
            try {
                partitions =
                        serverDataManager
                                .partitionManager
                                .getServerAssignment(request.getServerId())
                                .getPartitions();
            } catch (Exception e) {
                e.printStackTrace();
            }
            GetPartitionAssignmentResponse.Builder builder =
                    GetPartitionAssignmentResponse.newBuilder();
            builder.addAllPartitionId(partitions);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }
}
