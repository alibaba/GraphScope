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
package com.alibaba.maxgraph.servers;

import com.alibaba.graphscope.groot.discovery.FileDiscovery;
import com.alibaba.graphscope.groot.discovery.LocalNodeProvider;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.discovery.ZkDiscovery;
import com.alibaba.graphscope.groot.frontend.BackupClient;
import com.alibaba.graphscope.groot.frontend.BatchDdlClient;
import com.alibaba.graphscope.groot.frontend.ClientBackupService;
import com.alibaba.graphscope.groot.frontend.ClientDdlService;
import com.alibaba.graphscope.groot.frontend.ClientService;
import com.alibaba.graphscope.groot.frontend.ClientWriteService;
import com.alibaba.graphscope.groot.frontend.FrontendSnapshotService;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import com.alibaba.graphscope.groot.frontend.SchemaClient;
import com.alibaba.graphscope.groot.frontend.SchemaWriter;
import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.frontend.StoreIngestClient;
import com.alibaba.graphscope.groot.frontend.StoreIngestClients;
import com.alibaba.graphscope.groot.frontend.StoreIngestor;
import com.alibaba.graphscope.groot.frontend.WriteSessionGenerator;
import com.alibaba.graphscope.groot.frontend.write.DefaultEdgeIdGenerator;
import com.alibaba.graphscope.groot.frontend.write.EdgeIdGenerator;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.meta.DefaultMetaService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsAggregator;
import com.alibaba.graphscope.groot.metrics.MetricsCollectClient;
import com.alibaba.graphscope.groot.metrics.MetricsCollectService;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.MaxGraphNameResolverFactory;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.rpc.RpcServer;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.util.CuratorUtils;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.NameResolver;
import java.io.IOException;
import org.apache.curator.framework.CuratorFramework;

public class Frontend extends NodeBase {

    private CuratorFramework curator;
    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;
    private RpcServer rpcServer;
    private ClientService clientService;
    private AbstractService graphService;

    public Frontend(Configs configs) {
        super(configs);
        configs = reConfig(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        if (CommonConfig.DISCOVERY_MODE.get(configs).equalsIgnoreCase("file")) {
            this.discovery = new FileDiscovery(configs);
        } else {
            this.curator = CuratorUtils.makeCurator(configs);
            this.discovery = new ZkDiscovery(configs, localNodeProvider, this.curator);
        }
        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        SnapshotCache snapshotCache = new SnapshotCache();
        this.metaService = new DefaultMetaService(configs);
        MetricsCollector metricsCollector = new MetricsCollector(configs);
        RoleClients<IngestorWriteClient> ingestorWriteClients =
                new RoleClients<>(this.channelManager, RoleType.INGESTOR, IngestorWriteClient::new);
        FrontendSnapshotService frontendSnapshotService =
                new FrontendSnapshotService(snapshotCache);
        RoleClients<MetricsCollectClient> frontendMetricsCollectClients =
                new RoleClients<>(
                        this.channelManager, RoleType.FRONTEND, MetricsCollectClient::new);
        RoleClients<MetricsCollectClient> ingestorMetricsCollectClients =
                new RoleClients<>(
                        this.channelManager, RoleType.INGESTOR, MetricsCollectClient::new);
        MetricsAggregator metricsAggregator =
                new MetricsAggregator(
                        configs, frontendMetricsCollectClients, ingestorMetricsCollectClients);
        StoreIngestor storeIngestClients =
                new StoreIngestClients(this.channelManager, RoleType.STORE, StoreIngestClient::new);
        SchemaWriter schemaWriter =
                new SchemaWriter(
                        new RoleClients<>(
                                this.channelManager, RoleType.COORDINATOR, SchemaClient::new));
        DdlExecutors ddlExecutors = new DdlExecutors();
        BatchDdlClient batchDdlClient =
                new BatchDdlClient(ddlExecutors, snapshotCache, schemaWriter);
        this.clientService =
                new ClientService(
                        snapshotCache,
                        metricsAggregator,
                        storeIngestClients,
                        this.metaService,
                        batchDdlClient);
        ClientDdlService clientDdlService = new ClientDdlService(snapshotCache, batchDdlClient);
        MetricsCollectService metricsCollectService = new MetricsCollectService(metricsCollector);
        WriteSessionGenerator writeSessionGenerator = new WriteSessionGenerator(configs);
        EdgeIdGenerator edgeIdGenerator = new DefaultEdgeIdGenerator(configs, this.channelManager);
        GraphWriter graphWriter =
                new GraphWriter(
                        snapshotCache, edgeIdGenerator, this.metaService, ingestorWriteClients);
        ClientWriteService clientWriteService =
                new ClientWriteService(writeSessionGenerator, graphWriter);
        RoleClients<BackupClient> backupClients =
                new RoleClients<>(this.channelManager, RoleType.COORDINATOR, BackupClient::new);
        ClientBackupService clientBackupService = new ClientBackupService(backupClients);
        this.rpcServer =
                new RpcServer(
                        configs,
                        localNodeProvider,
                        frontendSnapshotService,
                        clientService,
                        metricsCollectService,
                        clientDdlService,
                        clientWriteService,
                        clientBackupService);

        WrappedSchemaFetcher wrappedSchemaFetcher =
                new WrappedSchemaFetcher(snapshotCache, metaService);
        ComputeServiceProducer serviceProducer = ServiceProducerFactory.getProducer(configs);
        this.graphService =
                serviceProducer.makeGraphService(
                        wrappedSchemaFetcher,
                        channelManager,
                        discovery,
                        graphWriter,
                        writeSessionGenerator,
                        metaService);
    }

    @Override
    public void start() {
        if (this.curator != null) {
            this.curator.start();
        }
        this.metaService.start();
        try {
            this.rpcServer.start();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }
        this.discovery.start();
        this.channelManager.start();
        this.graphService.start();
    }

    @Override
    public void close() throws IOException {
        this.rpcServer.stop();
        this.metaService.stop();
        this.channelManager.stop();
        this.discovery.stop();
        if (this.curator != null) {
            this.curator.close();
        }
        this.graphService.stop();
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Frontend frontend = new Frontend(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(frontend);
        nodeLauncher.start();
    }

    @VisibleForTesting
    public ClientService getClientService() {
        return this.clientService;
    }
}
