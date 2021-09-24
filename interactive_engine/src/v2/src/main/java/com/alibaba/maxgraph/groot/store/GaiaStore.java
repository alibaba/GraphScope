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
package com.alibaba.maxgraph.groot.store;

import com.alibaba.maxgraph.groot.common.DefaultMetaService;
import com.alibaba.maxgraph.groot.common.MetaService;
import com.alibaba.maxgraph.groot.common.NodeBase;
import com.alibaba.maxgraph.groot.common.NodeLauncher;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.groot.common.discovery.DiscoveryFactory;
import com.alibaba.maxgraph.groot.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.groot.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.groot.common.rpc.ChannelManager;
import com.alibaba.maxgraph.groot.common.rpc.MaxGraphNameResolverFactory;
import com.alibaba.maxgraph.groot.common.rpc.RpcServer;
import com.alibaba.maxgraph.groot.store.executor.ExecutorEngine;
import com.alibaba.maxgraph.groot.store.executor.gaia.GaiaEngine;
import com.alibaba.maxgraph.groot.store.executor.gaia.GaiaService;
import io.grpc.NameResolver;

import java.io.IOException;

public class GaiaStore extends NodeBase {

    private NodeDiscovery discovery;
    private ChannelManager channelManager;
    private MetaService metaService;
    private StoreService storeService;
    private WriterAgent writerAgent;
    private RpcServer rpcServer;
    private GaiaService gaiaService;

    public GaiaStore(Configs configs) {
        super(configs, RoleType.STORE);
        configs = reConfig(configs);
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        DiscoveryFactory discoveryFactory = new DiscoveryFactory(configs);
        this.discovery = discoveryFactory.makeDiscovery(localNodeProvider);

        NameResolver.Factory nameResolverFactory = new MaxGraphNameResolverFactory(this.discovery);
        this.channelManager = new ChannelManager(configs, nameResolverFactory);
        this.metaService = new DefaultMetaService(configs);
        this.storeService = new StoreService(configs, this.metaService);
        SnapshotCommitter snapshotCommitter = new DefaultSnapshotCommitter(this.channelManager);
        this.writerAgent = new WriterAgent(configs, this.storeService, this.metaService, snapshotCommitter);
        StoreWriteService storeWriteService = new StoreWriteService(this.writerAgent);
        StoreSchemaService storeSchemaService = new StoreSchemaService(this.storeService);
        StoreIngestService storeIngestService = new StoreIngestService(this.storeService);
        this.rpcServer = new RpcServer(configs, localNodeProvider, storeWriteService, storeSchemaService,
                storeIngestService);
        ExecutorEngine executorEngine = new GaiaEngine(configs, discoveryFactory);
        this.gaiaService = new GaiaService(configs, executorEngine, this.storeService, this.metaService);
    }

    @Override
    public void start() {
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
        this.gaiaService.start();
    }

    @Override
    public void close() throws IOException {
        this.gaiaService.stop();
        this.rpcServer.stop();
        this.writerAgent.stop();
        this.storeService.stop();
        this.metaService.stop();
        this.channelManager.stop();
        this.discovery.stop();
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        GaiaStore store = new GaiaStore(conf);
        NodeLauncher nodeLauncher = new NodeLauncher(store);
        nodeLauncher.start();
    }
}
