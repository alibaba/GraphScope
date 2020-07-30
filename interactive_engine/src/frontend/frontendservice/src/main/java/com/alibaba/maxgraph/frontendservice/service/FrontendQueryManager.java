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
package com.alibaba.maxgraph.frontendservice.service;

import com.alibaba.maxgraph.api.query.QueryCallbackManager;
import com.alibaba.maxgraph.api.query.QueryStatus;
import com.alibaba.maxgraph.common.component.AbstractLifecycleComponent;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.coordinator.client.ServerDataApiClient;
import com.alibaba.maxgraph.frontendservice.ClientManager;
import com.alibaba.maxgraph.frontendservice.server.ExecutorAddressFetcher;
import com.alibaba.maxgraph.proto.RoutingServerInfoResp;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.server.query.PegasusRpcConnector;
import com.alibaba.maxgraph.server.query.RpcConnector;

public class FrontendQueryManager extends AbstractLifecycleComponent implements QueryCallbackManager {
    // manage queries <snapshotId, is_done>
    private BlockingQueue<QueryStatus> queryQueue;
    private ClientManager clientManager;
    private int frontId;
    private ScheduledExecutorService updateExecutor;
    private RpcConnector rpcConnector;
    private final static int QUEUE_SZIE = 1024 * 1024;

    public FrontendQueryManager(InstanceConfig instanceConfig, ClientManager clientManager) {
        super(instanceConfig);
        frontId = settings.getServerId();
        queryQueue = new ArrayBlockingQueue<>(QUEUE_SZIE);
        this.clientManager = clientManager;
        ExecutorAddressFetcher executorAddressFetcher = new ExecutorAddressFetcher(clientManager);
        RpcConfig rpcConfig = new RpcConfig();
        rpcConfig.setQueryTimeoutSec(instanceConfig.getTimelyQueryTimeoutSec());
        this.rpcConnector = new PegasusRpcConnector(executorAddressFetcher, rpcConfig);
    }

    private List<Endpoint> getRoutingServerEndpointList() throws Exception {
        List<Endpoint> endpointList = new ArrayList<>();
        ServerDataApiClient serverDataApiClient = clientManager.getServerDataApiClient();
        // rpc to coordinator to fetch latest routing server workerInfo and serverIdList
        RoutingServerInfoResp routingServerInfoResp = serverDataApiClient.getWorkerInfoAndRoutingServerList();
        // transform workerInfoList to serverId2Endpoint Map
        Map<Integer, Endpoint> serverId2Endpoint = new HashMap<>();
        routingServerInfoResp.getWorkerInfoProtos().getInfosList().forEach(workerInfoProto -> {
            int id = workerInfoProto.getId();
            if (!serverId2Endpoint.containsKey(id)) {
                serverId2Endpoint.put(id, Endpoint.fromProto(workerInfoProto.getAddress()));
            }
        });
        routingServerInfoResp.getServingServerIdList().forEach((serverId) -> {
            endpointList.add(serverId2Endpoint.get(serverId));
        });
        return endpointList;
    }

    @Override
    protected void doStart() {
        // rpc to executor to cancel query by front_id
        while (true) {
            try {
                rpcConnector.cancelDataflowByFront(frontId, getRoutingServerEndpointList());
                break;
            } catch (Exception e) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    logger.error("Interrupt retry sleep fail ", e1);
                }
                logger.error("cancelDataflowByFront by front {} fail. retry again.", frontId, e);
            }
        }
        // cancelDataflowByFront is async, check whether really cancelled
        while (true) {
            try {
                if (!rpcConnector.hasCancelDataFlowByFrontCompleted(frontId, getRoutingServerEndpointList())) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        logger.error("Interrupt retry sleep fail ", e1);
                    }
                    continue;
                } else {
                    break;
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    logger.error("Interrupt retry sleep fail ", e1);
                }
                logger.error("hasCancelDataFlowByFrontCompleted by frontId {} fail. retry again.", frontId, e);
            }
        }
        logger.info("cancel data flow by front {} success", frontId);
        updateExecutor = Executors.newSingleThreadScheduledExecutor(
                CommonUtil.createFactoryWithDefaultExceptionHandler("maxgraph-frontend-query-manager", logger));
        updateExecutor.scheduleWithFixedDelay(new UpdateSnapshot(), 5000, 2000, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() {
        if (updateExecutor != null) {
            updateExecutor.shutdownNow();
            try {
                if (!updateExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    logger.error("updateExecutor await timeout before shutdown");
                }
            } catch (InterruptedException e) {
                logger.error("updateExecutor awaitTermination exception ", e);
            }
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    public QueryStatus beforeExecution(Long snapshotId) {
        QueryStatus query = new QueryStatus(snapshotId, false);
        try {
            queryQueue.put(query);
        } catch (InterruptedException e) {
            logger.error("queryQueue waiting for space be interrupted", e);
        }
        return query;
    }

    public void afterExecution(QueryStatus query) {
        if (query != null) {
            query.isDone = true;
        }
    }

    class UpdateSnapshot implements Runnable {
        @Override
        public void run() {
            while (!queryQueue.isEmpty() && queryQueue.peek().isDone) {
                queryQueue.remove();
            }
        }
    }


}
