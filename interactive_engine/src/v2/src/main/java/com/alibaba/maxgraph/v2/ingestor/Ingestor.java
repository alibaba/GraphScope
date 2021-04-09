package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.v2.common.DefaultMetaService;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import com.alibaba.maxgraph.v2.common.metrics.MetricsCollectService;
import com.alibaba.maxgraph.v2.common.metrics.MetricsCollector;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.discovery.ZkDiscovery;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.MaxGraphNameResolverFactory;
import com.alibaba.maxgraph.v2.common.rpc.RpcServer;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.common.wal.LogService;
import com.alibaba.maxgraph.v2.common.wal.kafka.KafkaLogService;
import io.grpc.NameResolver;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

public class Ingestor extends NodeBase {

    private CuratorFramework curator;
    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;

    private IngestService ingestService;
    private RpcServer rpcServer;

    public Ingestor(Configs configs) {
        super(configs);
        configs = reConfig(configs);
        this.curator = CuratorUtils.makeCurator(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        this.discovery = new ZkDiscovery(configs, localNodeProvider, this.curator);
        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        this.metaService = new DefaultMetaService(configs);
        LogService logService = new KafkaLogService(configs);
        IngestProgressFetcher ingestProgressClients = new IngestProgressClients(this.channelManager,
                RoleType.COORDINATOR, IngestProgressClient::new);
        StoreWriter storeWriteClients = new StoreWriteClients(this.channelManager, RoleType.STORE,
                StoreWriteClient::new);
        MetricsCollector metricsCollector = new MetricsCollector(configs);
        this.ingestService = new IngestService(configs, this.discovery, this.metaService, logService,
                ingestProgressClients, storeWriteClients, metricsCollector);
        MetricsCollectService metricsCollectService = new MetricsCollectService(metricsCollector);
        IngestorSnapshotService ingestorSnapshotService = new IngestorSnapshotService(this.ingestService);
        IngestorWriteService ingestorWriteService = new IngestorWriteService(this.ingestService);
        this.rpcServer = new RpcServer(configs, localNodeProvider, ingestorSnapshotService, ingestorWriteService,
                metricsCollectService);
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
        this.ingestService.start();
    }

    @Override
    public void close() throws IOException {
        this.rpcServer.stop();
        this.ingestService.stop();
        this.metaService.stop();
        this.channelManager.stop();
        this.discovery.stop();
        this.curator.close();
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Ingestor ingestor = new Ingestor(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(ingestor);
        nodeLauncher.start();
    }

}
