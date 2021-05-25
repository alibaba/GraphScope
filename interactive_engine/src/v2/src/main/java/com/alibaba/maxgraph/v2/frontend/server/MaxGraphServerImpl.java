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
package com.alibaba.maxgraph.v2.frontend.server;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.frontend.api.MaxGraphServer;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryExecuteRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryManageRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.alibaba.maxgraph.v2.frontend.context.GraphWriterContext;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.channelizer.MaxGraphWsAndHttpSocketChannelizer;
import com.alibaba.maxgraph.v2.frontend.server.loader.MaxGraphProcessorLoader;
import com.alibaba.maxgraph.v2.frontend.server.loader.ProcessorLoader;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Gremlin server launcher, read the server.yaml config and start gremlin server
 */
public class MaxGraphServerImpl implements MaxGraphServer {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphServerImpl.class);

    private Configs configs;
    private SchemaFetcher schemaFetcher;

    private Settings settings;
    private GremlinServer server;
    private GraphPartitionManager partitionManager;
    private RoleClients<QueryExecuteRpcClient> queryExecuteClients;
    private RoleClients<QueryStoreRpcClient> queryStoreClients;
    private RoleClients<QueryManageRpcClient> queryManageClients;
    private int executorCount;
    private GraphWriterContext graphWriterContext;

    public MaxGraphServerImpl(Configs configs,
                              SchemaFetcher schemaFetcher,
                              GraphPartitionManager partitionManager,
                              RoleClients<QueryExecuteRpcClient> queryExecuteClients,
                              RoleClients<QueryStoreRpcClient> queryStoreClients,
                              RoleClients<QueryManageRpcClient> queryManageClients,
                              int executorCount,
                              GraphWriterContext graphWriterContext) {
        this.configs = configs;
        this.schemaFetcher = schemaFetcher;
        this.partitionManager = partitionManager;
        this.queryExecuteClients = queryExecuteClients;
        this.queryStoreClients = queryStoreClients;
        this.queryManageClients = queryManageClients;
        this.executorCount = executorCount;
        this.graphWriterContext = graphWriterContext;
    }

    /**
     * Load settings of gremlin server from specify file
     */
    private void loadSettings() {
        InputStream input = this.getClass().getClassLoader().getResourceAsStream("conf/server.yaml");
        InputStream groovy = this.getClass().getClassLoader().getResourceAsStream("conf/generate-classic.groovy");
        checkNotNull(input, "cant find conf/server.yaml in path");
        checkNotNull(groovy, "cant find conf/generate-classic.groovy file in path");
        File tmp = new File("/tmp/generate-classic.groovy");
        tmp.deleteOnExit();
        try {
            Files.copy(groovy, Paths.get("/tmp/generate-classic.groovy"));
        } catch (Throwable e) {
            logger.warn("get error : ", e);
        }
        this.settings = Settings.read(input);
    }

    /**
     * Start gremlin server
     */
    @Override
    public void start() {
        this.loadSettings();

        logger.info(GremlinServer.getHeader());
        this.settings.port = FrontendConfig.GREMLIN_PORT.get(this.configs);
        this.settings.host = "0.0.0.0";
        if (settings.gremlinPool == 0) {
            settings.gremlinPool = Runtime.getRuntime().availableProcessors();
        }

        if (StringUtils.equals(settings.channelizer, WsAndHttpChannelizer.class.getName())) {
            settings.channelizer = MaxGraphWsAndHttpSocketChannelizer.class.getName();
        } else {
            throw new IllegalArgumentException("Not support for channelizer=>" + settings.channelizer);
        }
        settings.writeBufferHighWaterMark = FrontendConfig.SERVER_WRITE_BUFFER_HIGH_WATER.get(this.configs);
        settings.writeBufferLowWaterMark = FrontendConfig.SERVER_WRITE_BUFFER_LOW_WATER.get(this.configs);

        this.server = new GremlinServer(settings);
        ProcessorLoader processorLoader = new MaxGraphProcessorLoader(this.configs,
                this.schemaFetcher,
                this.partitionManager,
                this.queryExecuteClients,
                this.queryStoreClients,
                this.queryManageClients,
                this.executorCount,
                this.graphWriterContext);
        processorLoader.loadProcessor(settings);
        try {
            this.server.start().exceptionally(t -> {
                logger.error("Gremlin Server was unable to start and will now begin shutdown: {}", t.getMessage());
                this.server.stop().join();
                return null;
            }).join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger.info("Gremlin Server started....");
    }

    @Override
    public int getGremlinServerPort() throws Exception {
        Field ch = this.server.getClass().getDeclaredField("ch");
        ch.setAccessible(true);
        Channel o = (Channel) ch.get(this.server);
        SocketAddress localAddr = o.localAddress();
        return ((InetSocketAddress) localAddr).getPort();
    }

    @Override
    public void stop() {
        if (this.server != null) {
            this.server.stop();
            this.server = null;
        }
    }
}
