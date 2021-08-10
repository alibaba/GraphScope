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
package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.DefaultMetaService;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.discovery.*;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.MaxGraphNameResolverFactory;
import com.alibaba.maxgraph.v2.common.rpc.RpcServer;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.common.wal.LogService;
import com.alibaba.maxgraph.v2.common.wal.kafka.KafkaLogService;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.frontend.IngestorWriteClient;
import io.grpc.NameResolver;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

public class Coordinator extends NodeBase {

    private CuratorFramework curator;
    private NodeDiscovery discovery;
    private SnapshotManager snapshotManager;
    private MetaService metaService;
    private SchemaManager schemaManager;
    private SnapshotNotifier snapshotNotifier;
    private RpcServer rpcServer;
    private ChannelManager channelManager;
    private LogRecycler logRecycler;
    private GraphInitializer graphInitializer;
    private IdAllocator idAllocator;

    public Coordinator(Configs configs) {
        super(configs);
        configs = reConfig(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        MetaStore metaStore;
        if (CommonConfig.DISCOVERY_MODE.get(configs).equalsIgnoreCase("file")) {
            this.discovery = new FileDiscovery(configs);
            metaStore = new FileMetaStore(configs);
        } else {
            this.curator = CuratorUtils.makeCurator(configs);
            this.discovery = new ZkDiscovery(configs, localNodeProvider, this.curator);
            metaStore = new ZkMetaStore(configs, this.curator);
        }
        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);

        RoleClients<FrontendSnapshotClient> frontendSnapshotClients = new RoleClients<>(this.channelManager,
                RoleType.FRONTEND, FrontendSnapshotClient::new);
        RoleClients<IngestorSnapshotClient> ingestorSnapshotClients = new RoleClients<>(this.channelManager,
                RoleType.INGESTOR, IngestorSnapshotClient::new);
        WriteSnapshotIdNotifier writeSnapshotIdNotifier = new IngestorWriteSnapshotIdNotifier(configs,
                ingestorSnapshotClients);
        LogService logService = new KafkaLogService(configs);
        this.snapshotManager = new SnapshotManager(configs, metaStore, logService, writeSnapshotIdNotifier);
        DdlExecutors ddlExecutors = new DdlExecutors();
        RoleClients<IngestorWriteClient> ingestorWriteClients = new RoleClients<>(this.channelManager,
                RoleType.INGESTOR, IngestorWriteClient::new);
        DdlWriter ddlWriter = new DdlWriter(ingestorWriteClients);
        this.metaService = new DefaultMetaService(configs);
        RoleClients<StoreSchemaClient> storeSchemaClients = new RoleClients<>(this.channelManager,
                RoleType.STORE, StoreSchemaClient::new);
        GraphDefFetcher graphDefFetcher = new GraphDefFetcher(storeSchemaClients);
        this.schemaManager = new SchemaManager(this.snapshotManager, ddlExecutors, ddlWriter, this.metaService,
                graphDefFetcher);
        this.snapshotNotifier = new SnapshotNotifier(this.discovery, this.snapshotManager, this.schemaManager,
                frontendSnapshotClients);
        IngestProgressService ingestProgressService = new IngestProgressService(this.snapshotManager);
        SnapshotCommitService snapshotCommitService = new SnapshotCommitService(this.snapshotManager);
        SchemaService schemaService = new SchemaService(this.schemaManager);
        this.idAllocator = new IdAllocator(metaStore);
        IdAllocateService idAllocateService = new IdAllocateService(this.idAllocator);
        this.rpcServer = new RpcServer(configs, localNodeProvider, ingestProgressService, snapshotCommitService,
                schemaService, idAllocateService);
        this.logRecycler = new LogRecycler(configs, logService, this.snapshotManager);
        this.graphInitializer = new GraphInitializer(configs, this.curator, metaStore, logService);
    }

    @Override
    public void start() {
        if (this.curator != null) {
            this.curator.start();
        }
        this.graphInitializer.initializeIfNeeded();
        this.metaService.start();
        this.idAllocator.start();
        try {
            this.rpcServer.start();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }
        this.discovery.start();
        this.channelManager.start();
        this.snapshotManager.start();
        this.snapshotNotifier.start();
        this.schemaManager.start();
        this.logRecycler.start();
    }

    @Override
    public void close() throws IOException {
        this.logRecycler.stop();
        this.rpcServer.stop();
        this.idAllocator.stop();
        this.snapshotManager.stop();
        this.schemaManager.stop();
        this.metaService.stop();
        this.snapshotNotifier.stop();
        this.channelManager.stop();
        this.discovery.stop();
        if (this.curator != null) {
            this.curator.close();
        }
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Coordinator coordinator = new Coordinator(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(coordinator);
        nodeLauncher.start();
    }
}
