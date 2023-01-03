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

import com.alibaba.graphscope.common.RoleType;
import com.alibaba.graphscope.common.util.CuratorUtils;
import com.alibaba.graphscope.compiler.api.exception.GrootException;
import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.coordinator.*;
import com.alibaba.graphscope.groot.discovery.*;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import com.alibaba.graphscope.groot.meta.DefaultMetaService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.GrootNameResolverFactory;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.rpc.RpcServer;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.kafka.KafkaLogService;

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
    private BackupManager backupManager;

    private GarbageCollectManager garbageCollectManager;

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
        NameResolver.Factory nameResolverFactory = new GrootNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);

        RoleClients<FrontendSnapshotClient> frontendSnapshotClients =
                new RoleClients<>(
                        this.channelManager, RoleType.FRONTEND, FrontendSnapshotClient::new);
        RoleClients<IngestorSnapshotClient> ingestorSnapshotClients =
                new RoleClients<>(
                        this.channelManager, RoleType.INGESTOR, IngestorSnapshotClient::new);
        WriteSnapshotIdNotifier writeSnapshotIdNotifier =
                new IngestorWriteSnapshotIdNotifier(configs, ingestorSnapshotClients);
        LogService logService = new KafkaLogService(configs);
        this.snapshotManager =
                new SnapshotManager(configs, metaStore, logService, writeSnapshotIdNotifier);
        DdlExecutors ddlExecutors = new DdlExecutors();
        RoleClients<IngestorWriteClient> ingestorWriteClients =
                new RoleClients<>(this.channelManager, RoleType.INGESTOR, IngestorWriteClient::new);
        DdlWriter ddlWriter = new DdlWriter(ingestorWriteClients);
        this.metaService = new DefaultMetaService(configs);
        RoleClients<StoreSchemaClient> storeSchemaClients =
                new RoleClients<>(this.channelManager, RoleType.STORE, StoreSchemaClient::new);
        GraphDefFetcher graphDefFetcher = new GraphDefFetcher(storeSchemaClients);
        this.schemaManager =
                new SchemaManager(
                        this.snapshotManager,
                        ddlExecutors,
                        ddlWriter,
                        this.metaService,
                        graphDefFetcher);
        this.snapshotNotifier =
                new SnapshotNotifier(
                        this.discovery,
                        this.snapshotManager,
                        this.schemaManager,
                        frontendSnapshotClients);
        IngestProgressService ingestProgressService =
                new IngestProgressService(this.snapshotManager);
        SnapshotCommitService snapshotCommitService =
                new SnapshotCommitService(this.snapshotManager);
        SchemaService schemaService = new SchemaService(this.schemaManager);
        this.idAllocator = new IdAllocator(metaStore);
        IdAllocateService idAllocateService = new IdAllocateService(this.idAllocator);
        RoleClients<StoreBackupClient> storeBackupClients =
                new RoleClients<>(this.channelManager, RoleType.STORE, StoreBackupClient::new);
        StoreBackupTaskSender storeBackupTaskSender = new StoreBackupTaskSender(storeBackupClients);
        SnapshotCache localSnapshotCache = new SnapshotCache();
        this.backupManager =
                new BackupManager(
                        configs,
                        this.metaService,
                        metaStore,
                        this.snapshotManager,
                        this.schemaManager,
                        localSnapshotCache,
                        storeBackupTaskSender);
        BackupService backupService = new BackupService(this.backupManager);
        RoleClients<CoordinatorSnapshotClient> coordinatorSnapshotClients =
                new RoleClients<>(
                        this.channelManager, RoleType.STORE, CoordinatorSnapshotClient::new);
        this.garbageCollectManager = new GarbageCollectManager(configs, coordinatorSnapshotClients);
        CoordinatorSnapshotService coordinatorSnapshotService =
                new CoordinatorSnapshotService(garbageCollectManager);
        this.rpcServer =
                new RpcServer(
                        configs,
                        localNodeProvider,
                        ingestProgressService,
                        snapshotCommitService,
                        schemaService,
                        idAllocateService,
                        backupService,
                        coordinatorSnapshotService);
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
            throw new GrootException(e);
        }
        this.discovery.start();
        this.channelManager.start();
        this.snapshotManager.start();
        this.snapshotNotifier.start();
        this.schemaManager.start();
        this.logRecycler.start();
        this.backupManager.start();
        this.garbageCollectManager.start();
    }

    @Override
    public void close() throws IOException {
        this.backupManager.stop();
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
