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

import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.meta.DefaultMetaService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.graphscope.groot.discovery.*;
import com.alibaba.graphscope.groot.metrics.MetricsCollectService;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.MaxGraphNameResolverFactory;
import com.alibaba.graphscope.groot.rpc.RpcServer;
import com.alibaba.maxgraph.common.util.CuratorUtils;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.kafka.KafkaLogService;
import com.alibaba.graphscope.groot.ingestor.IngestProgressFetcher;
import com.alibaba.graphscope.groot.ingestor.IngestService;
import com.alibaba.graphscope.groot.ingestor.IngestorSnapshotService;
import com.alibaba.graphscope.groot.ingestor.IngestorWriteService;
import com.alibaba.graphscope.groot.ingestor.RemoteIngestProgressFetcher;
import com.alibaba.graphscope.groot.ingestor.StoreWriteClient;
import com.alibaba.graphscope.groot.ingestor.StoreWriteClients;
import com.alibaba.graphscope.groot.ingestor.StoreWriter;
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
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        if (CommonConfig.DISCOVERY_MODE.get(configs).equalsIgnoreCase("file")) {
            this.discovery = new FileDiscovery(configs);
        } else {
            this.curator = CuratorUtils.makeCurator(configs);
            this.discovery = new ZkDiscovery(configs, localNodeProvider, this.curator);
        }
        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        this.metaService = new DefaultMetaService(configs);
        LogService logService = new KafkaLogService(configs);
        IngestProgressFetcher ingestProgressClients =
                new RemoteIngestProgressFetcher(this.channelManager);
        StoreWriter storeWriteClients =
                new StoreWriteClients(this.channelManager, RoleType.STORE, StoreWriteClient::new);
        MetricsCollector metricsCollector = new MetricsCollector(configs);
        this.ingestService =
                new IngestService(
                        configs,
                        this.discovery,
                        this.metaService,
                        logService,
                        ingestProgressClients,
                        storeWriteClients,
                        metricsCollector);
        MetricsCollectService metricsCollectService = new MetricsCollectService(metricsCollector);
        IngestorSnapshotService ingestorSnapshotService =
                new IngestorSnapshotService(this.ingestService);
        IngestorWriteService ingestorWriteService = new IngestorWriteService(this.ingestService);
        this.rpcServer =
                new RpcServer(
                        configs,
                        localNodeProvider,
                        ingestorSnapshotService,
                        ingestorWriteService,
                        metricsCollectService);
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
        this.ingestService.start();
    }

    @Override
    public void close() throws IOException {
        this.rpcServer.stop();
        this.ingestService.stop();
        this.metaService.stop();
        this.channelManager.stop();
        this.discovery.stop();
        if (this.curator != null) {
            this.curator.close();
        }
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        Ingestor ingestor = new Ingestor(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(ingestor);
        nodeLauncher.start();
    }
}
