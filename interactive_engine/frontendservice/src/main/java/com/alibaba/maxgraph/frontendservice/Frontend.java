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
package com.alibaba.maxgraph.frontendservice;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.compiler.schema.JsonFileSchemaFetcher;
import com.alibaba.maxgraph.coordinator.Constants;
import com.alibaba.maxgraph.frontendservice.monitor.MemoryMonitor;
import com.alibaba.maxgraph.frontendservice.query.ZKStatementStore;
import com.alibaba.maxgraph.frontendservice.server.ExecutorAddressFetcher;
import com.alibaba.maxgraph.frontendservice.server.manager.MaxGraphRecordProcessorManager;
import com.alibaba.maxgraph.frontendservice.service.*;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.server.processor.MixedProcessorLoader;
import com.alibaba.maxgraph.server.GremlinProcessorLoader;
import com.alibaba.maxgraph.server.MaxGraphServer;
import com.alibaba.maxgraph.server.ProcessorLoader;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Frontend {
    private static final Logger LOG = LoggerFactory.getLogger(Frontend.class);
    protected InstanceConfig instanceConfig;
    protected ClientManager clientManager;
    private Endpoint endpoint;
    protected GremlinExecutor gremlinExecutor;
    protected TinkerMaxGraph graph;
    private long currentExecutorVersion = 0L;

    private ScheduledExecutorService hbSchedule;

    private MemoryMonitor memoryMonitor;

    protected MaxGraphServer graphGremlinServer;

    protected RemoteGraph remoteGraph;
    private PreparedQueryManager preparedQueryManager;

    protected int gremlinServerPort;
    private int roleId;

    protected FrontendQueryManager queryManager;

    private AtomicReference<Long> aliveId = new AtomicReference<>(0L);

    public Frontend(InstanceConfig instanceConfig) throws Exception {
        this.instanceConfig = instanceConfig;
        this.roleId = instanceConfig.getRoleOrderId() - 1;
        this.clientManager = new ClientManager(instanceConfig);
        this.memoryMonitor = new MemoryMonitor(instanceConfig.getFrontendServiceMemoryThresholdPercent());
        this.queryManager = new FrontendQueryManager(instanceConfig, clientManager);
        initAndStartGremlinServer();
        // this.metricReporter = new MetricReporter();
        // this.metricReporter.registerMetric(new DiskUtilMetricGetter());
    }

    private void loadPrepareManagerService() {
        Iterator<PreparedQueryManager> managers = ServiceLoader.load(PreparedQueryManager.class,
                ClassLoader.getSystemClassLoader()).iterator();
        while (managers.hasNext()) {
            try {
                this.preparedQueryManager = managers.next();
                this.preparedQueryManager.init(this);
                return;
            } catch (Throwable e) {
                LOG.error("Error to init PrepareQueryManager.", e);
            }
        }

        LOG.error("No PrepareManager service found. ");
        this.preparedQueryManager = null;
    }

    protected void initAndStartGremlinServer() throws Exception {
        SchemaFetcher schemaFetcher;
        String vineyardSchemaPath = this.instanceConfig.getVineyardSchemaPath();

        LOG.info("Read schema from vineyard schema file " + vineyardSchemaPath);
        schemaFetcher = new JsonFileSchemaFetcher(vineyardSchemaPath);

        this.remoteGraph = new RemoteGraph(this, schemaFetcher);
        this.remoteGraph.refresh();

        this.graph = new TinkerMaxGraph(instanceConfig, remoteGraph, new DefaultGraphDfs());
        this.graphGremlinServer = new MaxGraphServer(this.graph);
        int tmpGremlinServerPort = instanceConfig.getGremlinServerPort() > 0 ? instanceConfig.getGremlinServerPort() : 0;

        ProcessorLoader processorLoader;
        switch (instanceConfig.getGremlinServerMode()) {
            case TIMELY:
            case MIXED: {
                processorLoader = MixedProcessorLoader.newProcessorLoader(
                        graph,
                        this.instanceConfig,
                        new ExecutorAddressFetcher(clientManager),
                        schemaFetcher,
                        new ZKStatementStore(instanceConfig),
                        new MaxGraphRecordProcessorManager(this.graph, this),
                        this.getFrontendQueryManager());
                break;
            }
            default: {
                processorLoader = GremlinProcessorLoader.newProcessorLoader();
                break;
            }
        }
        this.graphGremlinServer.start(tmpGremlinServerPort, processorLoader, instanceConfig.getInstanceAuthType() == 1);

        this.gremlinServerPort = tmpGremlinServerPort > 0 ? tmpGremlinServerPort : this.graphGremlinServer.getGremlinServerPort();
        LOG.info("gremlin server port:{}", this.gremlinServerPort);
    }

    private void updateWorkerConnection(Map<RoleType, Map<Integer, Endpoint>> workerInfoMap) throws Exception {

        for (Map.Entry<RoleType, Map<Integer, Endpoint>> entry : workerInfoMap.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            LOG.debug("got workers info from am: role is {}, endpoint list:{}",
                    entry.getKey(), entry.getValue());

            switch (entry.getKey()) {
                case EXECUTOR:
                    entry.getValue().forEach((key, value) -> clientManager.updateExecutorMap(key, value));
                    if (currentExecutorVersion != clientManager.getExecutorUpdateVersion()) {
                        LOG.info("update remote graph proxys");
                        currentExecutorVersion = clientManager.getExecutorUpdateVersion();
                        remoteGraph.refresh();
                        clientManager.updateGroupExecutors();
                    }
                    break;
                case FRONTEND:
                    break;
                default:
                    throw new RuntimeException("illegal role:" + entry.getKey());
            }
        }
    }

    protected void startHBThread() {
        hbSchedule = new ScheduledThreadPoolExecutor(1,
                CommonUtil.createFactoryWithDefaultExceptionHandler("HB_THREAD", LOG));
        hbSchedule.scheduleWithFixedDelay(() -> {
            if (clientManager.isCoordinatorRelatedClientNotReady()) {
                LOG.info("wait am ready.");
            } else {
                try {
                    LOG.info("send hb {} to am", endpoint);
                    Map<RoleType, Map<Integer, Endpoint>> workerInfoMap =
                            clientManager.getServerDataApiClient().workerHeartbeat(instanceConfig.getServerId(),
                                    endpoint, RoleType.FRONTEND, CommonUtil.getYarnLogDir(), this.aliveId, this.roleId);
                    updateWorkerConnection(workerInfoMap);
                } catch (Exception e) {
                    LOG.error("register to am failed:{}", e);
                }
            }
        }, 1, instanceConfig.getFrontendAmHbIntervalSeconds(), TimeUnit.SECONDS);
    }

    public ClientManager getClientManager() {
        return clientManager;
    }

    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    public GremlinExecutor getGremlinExecutor() {
        return this.gremlinExecutor;
    }

    public MemoryMonitor getMemoryMonitor() {
        return memoryMonitor;
    }

    public FrontendQueryManager getFrontendQueryManager() {
        return queryManager;
    }

    public PreparedQueryManager getPreparedQueryManager() throws UnsupportedOperationException {
        if (this.preparedQueryManager == null) {
            throw new UnsupportedOperationException("PREPARE is not support, because no PreparedQueryManager implementation.");
        }

        return this.preparedQueryManager;
    }

    public void start() throws Exception {
        queryManager.start();
        startRpcService();
        startHBThread();
        this.gremlinExecutor = graphGremlinServer.getGremlinExecutor();
    }

    protected void startRpcService() throws Exception {
        String hostName = InetAddress.getLocalHost().getHostAddress();
        int threadCount = this.instanceConfig.getFrontendGrpcThreadCount();
        LOG.info("rpc server thread count: " + threadCount);
        NettyServerBuilder serverBuilder = NettyServerBuilder
                .forAddress(new InetSocketAddress(hostName, 0))
                .executor(CommonUtil.getGrpcExecutor(threadCount))
                .maxInboundMessageSize(Constants.MAXGRAPH_RPC_MAX_MESSAGE_SIZE);

        Server rpcServer = serverBuilder.build().start();

        this.endpoint = new Endpoint(hostName, rpcServer.getPort(), gremlinServerPort);
        LOG.info("frontend host: {}, port: {}, gremlin server port:{}", endpoint.getIp(), endpoint.getPort(), gremlinServerPort);
    }

    public TinkerMaxGraph getGraph() {
        return graph;
    }

}
