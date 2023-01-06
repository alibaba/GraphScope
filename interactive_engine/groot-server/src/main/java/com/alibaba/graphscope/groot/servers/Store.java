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
package com.alibaba.graphscope.groot.servers;

import com.alibaba.graphscope.compiler.api.exception.GrootException;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.discovery.*;
import com.alibaba.graphscope.groot.meta.DefaultMetaService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsCollectService;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.GrootNameResolverFactory;
import com.alibaba.graphscope.groot.rpc.RpcServer;
import com.alibaba.graphscope.groot.store.*;
import com.google.common.annotations.VisibleForTesting;

import io.grpc.NameResolver;

import java.io.IOException;

public class Store extends NodeBase {

    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;
    private StoreService storeService;
    private WriterAgent writerAgent;
    private BackupAgent backupAgent;
    private RpcServer rpcServer;
    private AbstractService executorService;

    public Store(Configs configs) {
        super(configs);
        configs = reConfig(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        DiscoveryFactory discoveryFactory = new DiscoveryFactory(configs);
        this.discovery = discoveryFactory.makeDiscovery(localNodeProvider);

        NameResolver.Factory nameResolverFactory = new GrootNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        this.metaService = new DefaultMetaService(configs);
        MetricsCollector metricsCollector = new MetricsCollector(configs);
        this.storeService = new StoreService(configs, this.metaService, metricsCollector);
        SnapshotCommitter snapshotCommitter = new DefaultSnapshotCommitter(this.channelManager);
        MetricsCollectService metricsCollectService = new MetricsCollectService(metricsCollector);
        this.writerAgent =
                new WriterAgent(
                        configs,
                        this.storeService,
                        this.metaService,
                        snapshotCommitter,
                        metricsCollector);
        StoreWriteService storeWriteService = new StoreWriteService(this.writerAgent);
        this.backupAgent = new BackupAgent(configs, this.storeService);
        StoreBackupService storeBackupService = new StoreBackupService(this.backupAgent);
        StoreSchemaService storeSchemaService = new StoreSchemaService(this.storeService);
        StoreIngestService storeIngestService = new StoreIngestService(this.storeService);
        StoreSnapshotService storeSnapshotService = new StoreSnapshotService(this.storeService);
        this.rpcServer =
                new RpcServer(
                        configs,
                        localNodeProvider,
                        storeWriteService,
                        storeBackupService,
                        storeSchemaService,
                        storeIngestService,
                        storeSnapshotService,
                        metricsCollectService);
        ComputeServiceProducer serviceProducer = ServiceProducerFactory.getProducer(configs);
        this.executorService =
                serviceProducer.makeExecutorService(storeService, metaService, discoveryFactory);
    }

    @Override
    public void start() {
        this.metaService.start();
        try {
            this.storeService.start();
        } catch (IOException e) {
            throw new GrootException(e);
        }
        long availSnapshotId;
        try {
            availSnapshotId = this.storeService.recover();
        } catch (IOException | InterruptedException e) {
            throw new GrootException(e);
        }
        this.writerAgent.init(availSnapshotId);
        this.writerAgent.start();
        this.backupAgent.start();
        try {
            this.rpcServer.start();
        } catch (IOException e) {
            throw new GrootException(e);
        }
        this.discovery.start();
        this.channelManager.start();
        this.executorService.start();
    }

    @Override
    public void close() throws IOException {
        this.executorService.stop();
        this.rpcServer.stop();
        this.backupAgent.stop();
        this.writerAgent.stop();
        this.storeService.stop();
        this.metaService.stop();
        this.channelManager.stop();
        this.discovery.stop();
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Store store = new Store(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(store);
        nodeLauncher.start();
    }

    @VisibleForTesting
    public StoreService getStoreService() {
        return storeService;
    }
}
