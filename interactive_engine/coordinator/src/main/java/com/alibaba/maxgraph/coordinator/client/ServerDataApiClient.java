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
package com.alibaba.maxgraph.coordinator.client;

import com.alibaba.maxgraph.common.InstanceStatus;
import com.alibaba.maxgraph.common.client.BaseAutoRefreshClient;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.common.util.RpcUtils;
import com.alibaba.maxgraph.common.zookeeper.ZkUtils;
import com.alibaba.maxgraph.proto.*;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.google.common.collect.Maps;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-13 上午10:41
 */
public class ServerDataApiClient
        extends BaseCoordinatorClient<ServerDataApiGrpc.ServerDataApiBlockingStub> {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDataApiClient.class);

    public ServerDataApiClient(InstanceConfig instanceConfig) {
        super(instanceConfig);
    }

    public ServerDataApiClient(InstanceConfig instanceConfig, ZkUtils zkUtils) {
        super(instanceConfig, zkUtils);
    }

    public void doRefresh(Endpoint endpoint) throws Exception {
        closeChannel();
        channel.set(RpcUtils.createChannel(endpoint.getIp(), endpoint.getPort()));
        serverStub.set(ServerDataApiGrpc.newBlockingStub(channel.get()));
        LOG.info("graph: {}, init server data api client, endpoint: {}", graphName, endpoint);
    }

    @Override
    protected void doRefresh() throws Exception {
        closeChannel();
        Endpoint endpoint = namingProxy.getCoordinatorEndpoint();
        if (endpoint == null) {
            throw new RuntimeException(
                    "graphName: "
                            + graphName
                            + " with zk:"
                            + this.zkUtils.getZkUrl()
                            + " has no "
                            + "valid endpoint");
        }

        channel.set(RpcUtils.createChannel(endpoint.getIp(), endpoint.getPort()));
        serverStub.set(ServerDataApiGrpc.newBlockingStub(channel.get()));
        LOG.info("graph: {}, init server data api client, endpoint: {}", graphName, endpoint);
    }

    public Map<RoleType, Map<Integer, Endpoint>> workerHeartbeat(
            int serverId,
            Endpoint endpoint,
            RoleType roleType,
            String logDir,
            AtomicReference<Long> aliveId,
            int roleId)
            throws Exception {
        return call(
                () -> {
                    SimpleServerHBReq.Builder builder = SimpleServerHBReq.newBuilder();
                    builder.setEndpoint(endpoint.toProto());
                    builder.setId(serverId);
                    builder.setRoleType(roleType);
                    builder.setLogDir(logDir);
                    builder.setAliveId(aliveId.get());
                    builder.setNodeId(roleId);

                    SimpleServerHBResponse serverHBResponse =
                            serverStub.get().simpleHeartbeat(builder.build());
                    if (!serverHBResponse.getIsLegal()) {
                        LOG.error("current worker is outdated");
                        System.exit(0);
                    }
                    aliveId.set(new Long(serverHBResponse.getAliveId()));
                    LOG.debug(
                            "frontend isLegal is {} aliveId is {}",
                            serverHBResponse.getIsLegal(),
                            aliveId.get());

                    BaseAutoRefreshClient.validateResponse(serverHBResponse.getResp());

                    Map<RoleType, Map<Integer, Endpoint>> result = Maps.newHashMap();
                    serverHBResponse
                            .getWorkerInfoProtos()
                            .getInfosList()
                            .forEach(
                                    workerInfoProto -> {
                                        if (!result.containsKey(workerInfoProto.getRoleType())) {
                                            result.put(
                                                    workerInfoProto.getRoleType(),
                                                    Maps.newHashMap());
                                        }

                                        result.get(workerInfoProto.getRoleType())
                                                .put(
                                                        workerInfoProto.getId(),
                                                        Endpoint.fromProto(
                                                                workerInfoProto.getAddress()));
                                    });

                    return result;
                });
    }

    public MetricInfoResp getRealTimeMetric(MetricInfoRequest request) throws Exception {
        return call(
                () -> {
                    MetricInfoResp resp = serverStub.get().getRealTimeMetric(request);
                    return resp;
                });
    }

    public AllMetricsInfoResp getAllRealTimeMetrics(Request request) throws Exception {
        return call(
                () -> {
                    AllMetricsInfoResp resp = serverStub.get().getAllRealTimeMetrics(request);
                    return resp;
                });
    }

    public ServerHBResp storeHb2Coordinator(int serverId, Endpoint serverEndpoint)
            throws Exception {
        return call(
                () -> {
                    ServerHBReq.Builder builder = ServerHBReq.newBuilder();
                    builder.setId(serverId);
                    builder.setEndpoint(serverEndpoint.toProto());

                    ServerHBResp heartbeat = serverStub.get().heartbeat(builder.build());
                    BaseAutoRefreshClient.validateResponse(heartbeat.getResp());
                    return heartbeat;
                });
    }

    public ServerHBResp storeHb2Coordinator(ServerHBReq req) throws Exception {
        return call(
                () -> {
                    ServerHBResp heartbeat = serverStub.get().heartbeat(req);
                    BaseAutoRefreshClient.validateResponse(heartbeat.getResp());
                    return heartbeat;
                });
    }

    public RoutingServerInfoResp getWorkerInfoAndRoutingServerList() throws Exception {
        return call(
                () -> {
                    return serverStub
                            .get()
                            .getWorkerInfoAndRoutingServerList(Request.newBuilder().build());
                });
    }

    public InstanceStatus getServerStatuses() throws Exception {
        return call(
                () -> {
                    Request.Builder builder = Request.newBuilder();
                    InstanceInfoResp instanceInfoResp =
                            serverStub.get().getInstanceInfo(builder.build());
                    BaseAutoRefreshClient.validateResponse(instanceInfoResp.getResp());
                    return new InstanceStatus(instanceInfoResp.getInstanceInfoProto());
                });
    }

    public Map<Integer, RuntimeGroupStatusResp.Status> getRuntimeGroupStatus() throws Exception {
        return call(
                () -> {
                    RuntimeGroupStatusResp runtimeGroupStatusResp =
                            serverStub.get().getRuntimeGroupStatus(Empty.newBuilder().build());
                    validateResponse(runtimeGroupStatusResp.getResponse());
                    return runtimeGroupStatusResp.getRuntimeGroupsStstusMap();
                });
    }

    public boolean isDataInUse(int serverId, long aliveId) throws Exception {
        return call(
                () ->
                        serverStub
                                .get()
                                .isDataPathInUse(
                                        ServerIdAliveIdProto.newBuilder()
                                                .setServerId(serverId)
                                                .setAliveId(aliveId)
                                                .build())
                                .getIsInUse());
    }

    public static void main(String[] args) throws Exception {
        InstanceConfig instanceConfig = CommonUtil.getInstanceConfig(args, 0);
        ServerDataApiClient client = new ServerDataApiClient(instanceConfig);
        client.start();

        try {
            AllMetricsInfoResp allRealTimeMetrics =
                    client.getAllRealTimeMetrics(Request.newBuilder().build());

            System.out.println(allRealTimeMetrics.getInfoList());
        } catch (StatusRuntimeException e) {
            System.out.println(e.getStatus());
        }

        client.close();
    }
}
