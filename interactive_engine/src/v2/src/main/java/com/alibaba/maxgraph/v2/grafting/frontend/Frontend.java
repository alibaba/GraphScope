package com.alibaba.maxgraph.v2.grafting.frontend;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.compiler.dfs.DefaultGraphDfs;
import com.alibaba.maxgraph.compiler.schema.JsonFileSchemaFetcher;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.v2.common.DefaultMetaService;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.discovery.ZkDiscovery;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.frontend.api.MaxGraphServer;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.frontend.remote.RemoteGraphPartitionManager;
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
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.alibaba.maxgraph.v2.frontend.context.GraphWriterContext;
import io.grpc.NameResolver;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

public class Frontend extends NodeBase {

    private CuratorFramework curator;
    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;
    private RealtimeWriter realtimeWriter;
    private RpcServer rpcServer;
    private MaxGraphServer maxGraphServer;

    private SchemaFetcher oldSchemaFetcher;

    public Frontend(Configs configs) {
        super(configs);
        this.oldSchemaFetcher = new JsonFileSchemaFetcher(FrontendConfig.QUERY_VINEYARD_SCHEMA_PATH.get(configs));
        configs = reConfig(configs);
        this.curator = CuratorUtils.makeCurator(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        this.discovery = new ZkDiscovery(configs, localNodeProvider, this.curator);
        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        SnapshotCache snapshotCache = new SnapshotCache();
        this.metaService = new DefaultMetaService(configs);
        MetricsCollector metricsCollector = new MetricsCollector(configs);
        RoleClients<IngestorWriteClient> ingestorWriteClients = new RoleClients<>(this.channelManager,
                RoleType.INGESTOR, IngestorWriteClient::new);
        this.realtimeWriter = new RealtimeWriter(this.metaService, snapshotCache, ingestorWriteClients,
                metricsCollector);
        FrontendSnapshotService frontendSnapshotService = new FrontendSnapshotService(snapshotCache);

        RoleClients<MetricsCollectClient> frontendMetricsCollectClients = new RoleClients<>(this.channelManager,
                RoleType.FRONTEND, MetricsCollectClient::new);
        RoleClients<MetricsCollectClient> ingestorMetricsCollectClients = new RoleClients<>(this.channelManager,
                RoleType.INGESTOR, MetricsCollectClient::new);
        MetricsAggregator metricsAggregator = new MetricsAggregator(configs, frontendMetricsCollectClients,
                ingestorMetricsCollectClients);
        StoreIngestor storeIngestClients = new StoreIngestClients(this.channelManager, RoleType.STORE,
                StoreIngestClient::new);
        RoleClients<QueryStoreRpcClient> queryStoreClients = new RoleClients<>(this.channelManager,
                RoleType.EXECUTOR_GRAPH, QueryStoreRpcClient::new);
        ClientService clientService = new ClientService(this.realtimeWriter, snapshotCache, metricsAggregator,
                storeIngestClients, this.metaService, queryStoreClients);
        MetricsCollectService metricsCollectService = new MetricsCollectService(metricsCollector);
        this.rpcServer = new RpcServer(configs, localNodeProvider, frontendSnapshotService, clientService,
                metricsCollectService);
        int executorCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        GraphPartitionManager partitionManager = new RemoteGraphPartitionManager(this.metaService);
        SchemaWriter schemaWriter = new SchemaWriter(new RoleClients<>(this.channelManager,
                RoleType.COORDINATOR, SchemaClient::new));

        ReadOnlyGraph readOnlyGraph = new ReadOnlyGraph(this.discovery, oldSchemaFetcher, partitionManager);
        TinkerMaxGraph graph = new TinkerMaxGraph(new InstanceConfig(configs.getInnerProperties()), readOnlyGraph,
                new DefaultGraphDfs());
        GraphWriterContext graphWriterContext = new GraphWriterContext(realtimeWriter, schemaWriter, new DdlExecutors(),
                snapshotCache, true);
        this.maxGraphServer = new ReadOnlyMaxGraphServer(configs, graph, oldSchemaFetcher,
                new DiscoveryAddressFetcher(this.discovery));
    }

    @Override
    public void start() {
        this.curator.start();
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
        this.curator.close();
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
