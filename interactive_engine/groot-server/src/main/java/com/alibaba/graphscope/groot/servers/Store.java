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

import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.exception.InternalException;
import com.alibaba.graphscope.groot.discovery.*;
import com.alibaba.graphscope.groot.meta.DefaultMetaService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.GrootNameResolverFactory;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.rpc.RpcServer;
import com.alibaba.graphscope.groot.servers.ir.IrServiceProducer;
import com.alibaba.graphscope.groot.store.*;
import com.alibaba.graphscope.groot.store.backup.BackupAgent;
import com.alibaba.graphscope.groot.store.backup.StoreBackupService;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogServiceFactory;

import io.grpc.NameResolver;

import java.io.IOException;

public class Store extends NodeBase {

    private final NodeDiscovery discovery;
    private final ChannelManager channelManager;
    private final MetaService metaService;
    private final StoreService storeService;
    private final WriterAgent writerAgent;
    private final BackupAgent backupAgent;
    private final RpcServer rpcServer;
    private final AbstractService executorService;

    private final KafkaProcessor processor;

    private final PartitionService partitionService;

    public Store(Configs configs) {
        super(configs);
        configs = reConfig(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        DiscoveryFactory discoveryFactory = new DiscoveryFactory(configs);
        this.discovery = discoveryFactory.makeDiscovery(localNodeProvider);
        NameResolver.Factory nameResolverFactory = new GrootNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        this.metaService = new DefaultMetaService(configs);
        this.storeService = new StoreService(configs, this.metaService);
        RoleClients<SnapshotCommitClient> snapshotCommitter =
                new RoleClients<>(channelManager, RoleType.COORDINATOR, SnapshotCommitClient::new);
        LogService logService = LogServiceFactory.makeLogService(configs);

        this.writerAgent =
                new WriterAgent(configs, this.storeService, this.metaService, snapshotCommitter);
        this.backupAgent = new BackupAgent(configs, this.storeService);
        StoreBackupService storeBackupService = new StoreBackupService(this.backupAgent);
        StoreSchemaService storeSchemaService = new StoreSchemaService(this.storeService);
        FrontendStoreService storeIngestService = new FrontendStoreService(this.storeService);
        StoreSnapshotService storeSnapshotService = new StoreSnapshotService(this.storeService);
        this.rpcServer =
                new RpcServer(
                        configs,
                        localNodeProvider,
                        storeBackupService,
                        storeSchemaService,
                        storeIngestService,
                        storeSnapshotService);
        IrServiceProducer serviceProducer = new IrServiceProducer(configs);
        this.executorService =
                serviceProducer.makeExecutorService(storeService, metaService, discoveryFactory);
        this.partitionService = new PartitionService(configs, storeService);
        this.processor = new KafkaProcessor(configs, metaService, writerAgent, logService);
    }

    @Override
    public void start() {
        this.metaService.start();
        try {
            this.storeService.start();
        } catch (IOException e) {
            throw new InternalException(e);
        }
        this.writerAgent.start();
        this.backupAgent.start();
        try {
            this.rpcServer.start();
        } catch (IOException e) {
            throw new InternalException(e);
        }
        this.discovery.start();
        this.channelManager.start();
        this.executorService.start();
        this.processor.start();
        this.partitionService.start();
    }

    @Override
    public void close() throws IOException {
        this.partitionService.stop();
        this.processor.stop();
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
}
