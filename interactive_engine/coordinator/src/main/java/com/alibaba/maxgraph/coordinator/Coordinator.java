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
package com.alibaba.maxgraph.coordinator;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.cluster.management.ClusterApplierService;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.common.zookeeper.AbstractRegisterCallBack;
import com.alibaba.maxgraph.common.zookeeper.ZkNamingProxy;
import com.alibaba.maxgraph.common.zookeeper.ZkUtils;
import com.alibaba.maxgraph.coordinator.manager.*;
import com.alibaba.maxgraph.coordinator.service.*;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import static com.alibaba.maxgraph.common.MetricGetter.METRIC_SOURCE_LAST_READ_TIMESTAMP;
import static com.alibaba.maxgraph.common.MetricGetter.METRIC_SOURCE_LAST_WRITE_TIMESTAMP;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-12 上午10:00
 **/

public class Coordinator {

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

    private String graphName;
    private String hostName;
    private int port;
    private ZkUtils zkUtils;
    private ZkNamingProxy namingProxy;
    private Server rpcServer;
    private ServerDataManager serverDataManager;
    private InstanceConfig instanceConfig;
    private WorkerManager workerManager;
    private NettyServerBuilder serverBuilder;

    private ClusterApplierService clusterApplierService;
    private MasterService masterService;
    private MetricCollector metricCollector;

    public static final String EXECUTOR_DISK_UTIL = "disk_util";

    public Coordinator(InstanceConfig instanceConfig) throws Exception {
        this.instanceConfig = instanceConfig;
        this.graphName = instanceConfig.getGraphName();
        this.zkUtils = ZkUtils.getZKUtils(instanceConfig);
        this.namingProxy = new ZkNamingProxy(graphName, zkUtils);
        this.clusterApplierService = new ClusterApplierService(instanceConfig);
        this.serverDataManager = new ServerDataManager(instanceConfig, namingProxy, this.clusterApplierService);
        this.hostName = InetAddress.getLocalHost().getHostAddress();
        int threadCount = instanceConfig.getCoordinatorGrpcThreadCount();
        LOG.info("rpc server thread count: " + threadCount);
        this.serverBuilder = NettyServerBuilder
                .forAddress(new InetSocketAddress(hostName, 0))
                .executor(CommonUtil.getGrpcExecutor(threadCount))
                .maxInboundMessageSize(Constants.MAXGRAPH_RPC_MAX_MESSAGE_SIZE);

        LoggerStore loggerStore = new ZkLoggerStore(instanceConfig, zkUtils);
        this.masterService = new MasterService(instanceConfig, clusterApplierService, loggerStore,
                serverDataManager.partitionManager);
        this.metricCollector = new MetricCollector();
        // executor
        metricCollector.registerMetricProtoParser(EXECUTOR_DISK_UTIL, new DiskUtilMetricProtoParser());
        // ingest node
        metricCollector.registerMetricProtoParser(METRIC_SOURCE_LAST_READ_TIMESTAMP, new CommonMetricProtoParser());
        metricCollector.registerMetricProtoParser(METRIC_SOURCE_LAST_WRITE_TIMESTAMP, new CommonMetricProtoParser());
    }

    public void start() throws Exception {
        initPrepareDir(instanceConfig);
        this.clusterApplierService.start();
        this.serverDataManager.start();
        this.masterService.start();
        startRpcService();
    }

    public void stop() {
        this.namingProxy.deleteAliveIdInfo();
        if (rpcServer != null) {
            rpcServer.shutdown();
        }
        this.masterService.stop();
        this.clusterApplierService.stop();
    }

    public static void initPrepareDir(InstanceConfig config) throws IOException {
        String hdfsPath = config.getYarnHdfsAddress();
        if (hdfsPath != null) {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsPath);
            FileSystem fs = FileSystem.get(conf);
            Path prepareDir = new Path(config.getTimelyPrepareDir());
            if (!fs.exists(prepareDir)) {
                if (fs.mkdirs(prepareDir)) {
                    LOG.info("Directory: {} created . ", prepareDir.getName());
                } else {
                    LOG.error("Failed to create directory : {}", prepareDir.getName());
                }
            }

            Path lockDir = new Path(config.getTimelyPrepareLockDir());
            if (fs.exists(lockDir)) {
                fs.delete(lockDir, true);
            }

            if (fs.mkdirs(lockDir)) {
                LOG.info("Directory: {} created . ", lockDir.getName());
            } else {
                LOG.error("Failed to create directory : {}", lockDir.getName());
            }
        }
    }

    private void startRpcService() throws Exception {
        // server for schema operation
        this.workerManager = new WorkerManager(instanceConfig, serverDataManager);

        // server for DataStatus of Workers
        ServerDataApiServer serverDataApiServer = new ServerDataApiServer(serverDataManager, masterService,
                metricCollector);
        serverDataApiServer.init(serverBuilder);

        CoordinatorRpcServer coordinatorRpcServer = new CoordinatorRpcServer(clusterApplierService, masterService,
                serverDataManager, metricCollector);
        coordinatorRpcServer.init(serverBuilder);

        rpcServer = serverBuilder.build().start();
        port = rpcServer.getPort();
        LOG.info("Controller host: {}, port: {}", hostName, port);

        LOG.info("force delete coordinator zk node");
        namingProxy.deleteCoordinatorInfo();

        // register to naming service
        ControllerRegisterCallBack controllerRegisterCallBack = new ControllerRegisterCallBack(zkUtils);
        controllerRegisterCallBack.register();
    }

    public ServerDataManager getServerDataManager() {
        return serverDataManager;
    }

    private class ControllerRegisterCallBack extends AbstractRegisterCallBack {

        public ControllerRegisterCallBack(ZkUtils zkUtils) {
            super(zkUtils);
        }

        @Override
        public void register() throws Exception {
            namingProxy.registerCoordinator(hostName, rpcServer.getPort());
            LOG.info("register coordinator in zk success");
        }
    }

    public int getPort() {
        return this.port;
    }

    public void shutdown() throws IOException {
        this.stop();
    }

    public void waitShutdown(CountDownLatch shutdownSingle) throws IOException {
        while (true) {
            try {
                shutdownSingle.await();
                shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
    }
}
