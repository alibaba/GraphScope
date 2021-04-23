package com.alibaba.maxgraph.v2.store;

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
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.MaxGraphNameResolverFactory;
import com.alibaba.maxgraph.v2.common.rpc.RpcServer;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.store.executor.ExecutorService;
import io.grpc.NameResolver;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

public class Store extends NodeBase {

    private CuratorFramework curator;
    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;
    private StoreService storeService;
    private WriterAgent writerAgent;
    private RpcServer rpcServer;
    private ExecutorService executorService;

    public Store(Configs configs) {
        super(configs);
        configs = reConfig(configs);
        this.curator = CuratorUtils.makeCurator(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        this.discovery = new ZkDiscovery(configs, localNodeProvider, this.curator);
        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        this.metaService = new DefaultMetaService(configs);
        this.storeService = new StoreService(configs, this.metaService);
        SnapshotCommitter snapshotCommitter = new SnapshotCommitClients(this.channelManager, RoleType.COORDINATOR,
                SnapshotCommitClient::new);
        this.writerAgent = new WriterAgent(configs, this.storeService, this.metaService, snapshotCommitter);
        StoreWriteService storeWriteService = new StoreWriteService(this.writerAgent);
        StoreSchemaService storeSchemaService = new StoreSchemaService(this.storeService);
        StoreIngestService storeIngestService = new StoreIngestService(this.storeService);
        this.rpcServer = new RpcServer(configs, localNodeProvider, storeWriteService, storeSchemaService,
                storeIngestService);
        this.executorService = new ExecutorService(configs, storeService, this.curator);
    }

    @Override
    public void start() {
        this.curator.start();
        this.metaService.start();
        try {
            this.storeService.start();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }
        long availSnapshotId;
        try {
            availSnapshotId = this.storeService.recover();
        } catch (IOException | InterruptedException e) {
            throw new MaxGraphException(e);
        }
        this.writerAgent.init(availSnapshotId);
        this.writerAgent.start();
        try {
            this.rpcServer.start();
        } catch (IOException e) {
            throw new MaxGraphException(e);
        }
        this.discovery.start();
        this.channelManager.start();
        this.executorService.start();
    }

    @Override
    public void close() throws IOException {
        this.rpcServer.stop();
        this.writerAgent.stop();
        this.storeService.stop();
        this.metaService.stop();
        this.channelManager.stop();
        this.discovery.stop();
        this.curator.close();
        this.executorService.close();
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Store store = new Store(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(store);
        nodeLauncher.start();
    }
}
