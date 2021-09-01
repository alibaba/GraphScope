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
package com.alibaba.maxgraph.v2.grafting.frontend;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.v2.common.DefaultMetaService;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.*;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.frontend.api.MaxGraphServer;
import com.alibaba.maxgraph.v2.common.metrics.MetricsAggregator;
import com.alibaba.maxgraph.v2.common.metrics.MetricsCollectClient;
import com.alibaba.maxgraph.v2.common.metrics.MetricsCollectService;
import com.alibaba.maxgraph.v2.common.metrics.MetricsCollector;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.MaxGraphNameResolverFactory;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.rpc.RpcServer;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.frontend.*;
import com.alibaba.maxgraph.v2.frontend.write.DefaultEdgeIdGenerator;
import com.alibaba.maxgraph.v2.frontend.write.EdgeIdGenerator;
import com.alibaba.maxgraph.v2.frontend.write.GraphWriter;
import io.grpc.NameResolver;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

public class Frontend extends NodeBase {

    private CuratorFramework curator;
    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;
    private RpcServer rpcServer;
    private MaxGraphServer maxGraphServer;

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
        RoleClients<IngestorWriteClient> ingestorWriteClients = new RoleClients<>(this.channelManager,
                RoleType.INGESTOR, IngestorWriteClient::new);
        FrontendSnapshotService frontendSnapshotService = new FrontendSnapshotService(snapshotCache);
        RoleClients<MetricsCollectClient> frontendMetricsCollectClients = new RoleClients<>(this.channelManager,
                RoleType.FRONTEND, MetricsCollectClient::new);
        RoleClients<MetricsCollectClient> ingestorMetricsCollectClients = new RoleClients<>(this.channelManager,
                RoleType.INGESTOR, MetricsCollectClient::new);
        MetricsAggregator metricsAggregator = new MetricsAggregator(configs, frontendMetricsCollectClients,
                ingestorMetricsCollectClients);
        StoreIngestor storeIngestClients = new StoreIngestClients(this.channelManager, RoleType.STORE,
                StoreIngestClient::new);
        SchemaWriter schemaWriter = new SchemaWriter(new RoleClients<>(this.channelManager,
                RoleType.COORDINATOR, SchemaClient::new));
        DdlExecutors ddlExecutors = new DdlExecutors();
        BatchDdlClient batchDdlClient = new BatchDdlClient(ddlExecutors, snapshotCache, schemaWriter);
        ClientService clientService = new ClientService(snapshotCache, metricsAggregator,
                storeIngestClients, this.metaService, batchDdlClient);
        ClientDdlService clientDdlService = new ClientDdlService(snapshotCache, batchDdlClient);
        MetricsCollectService metricsCollectService = new MetricsCollectService(metricsCollector);
        WriteSessionGenerator writeSessionGenerator = new WriteSessionGenerator(configs);
        EdgeIdGenerator edgeIdGenerator = new DefaultEdgeIdGenerator(configs, this.channelManager);
        GraphWriter graphWriter = new GraphWriter(snapshotCache, edgeIdGenerator, this.metaService,
                ingestorWriteClients);
        ClientWriteService clientWriteService = new ClientWriteService(writeSessionGenerator, graphWriter);
        this.rpcServer = new RpcServer(configs, localNodeProvider, frontendSnapshotService, clientService,
                metricsCollectService, clientDdlService, clientWriteService);
        WrappedSchemaFetcher wrappedSchemaFetcher = new WrappedSchemaFetcher(snapshotCache, metaService);
        MaxGraphImpl maxGraphImpl = new MaxGraphImpl(this.discovery, wrappedSchemaFetcher, graphWriter,
                writeSessionGenerator, metaService);
        TinkerMaxGraph graph = new TinkerMaxGraph(new InstanceConfig(configs.getInnerProperties()), maxGraphImpl,
                new DefaultGraphDfs());
        this.maxGraphServer = new ReadOnlyMaxGraphServer(configs, graph, wrappedSchemaFetcher,
                new DiscoveryAddressFetcher(this.discovery));
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
        this.maxGraphServer.start();
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
        this.maxGraphServer.stop();
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Frontend frontend = new Frontend(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(frontend);
        nodeLauncher.start();
    }
}
