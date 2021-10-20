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
package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.server.MaxGraphWsAndHttpSocketChannelizer;
import com.alibaba.maxgraph.server.ProcessorLoader;
import com.alibaba.maxgraph.servers.AbstractService;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.tinkerpop.Utils;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.common.config.GremlinConfig;
import io.netty.channel.Channel;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ReadOnlyGraphServer implements AbstractService {
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyGraphServer.class);

    private Configs configs;
    private Settings settings;
    private GremlinServer server;
    private TinkerMaxGraph graph;
    private SchemaFetcher schemaFetcher;
    private RpcAddressFetcher rpcAddressFetcher;

    public ReadOnlyGraphServer(
            Configs configs,
            TinkerMaxGraph graph,
            SchemaFetcher schemaFetcher,
            RpcAddressFetcher rpcAddressFetcher) {
        this.configs = configs;
        this.graph = graph;
        this.schemaFetcher = schemaFetcher;
        this.rpcAddressFetcher = rpcAddressFetcher;
    }

    @Override
    public void start() {
        this.loadSettings();

        logger.info(GremlinServer.getHeader());
        this.settings.port = GremlinConfig.GREMLIN_PORT.get(this.configs);
        this.settings.host = "0.0.0.0";
        if (settings.gremlinPool == 0) {
            settings.gremlinPool = Runtime.getRuntime().availableProcessors();
        }
        if (StringUtils.equals(settings.channelizer, WsAndHttpChannelizer.class.getName())) {
            settings.channelizer = MaxGraphWsAndHttpSocketChannelizer.class.getName();
        } else {
            throw new IllegalArgumentException(
                    "Not support for channelizer=>" + settings.channelizer);
        }
        settings.writeBufferHighWaterMark =
                GremlinConfig.SERVER_WRITE_BUFFER_HIGH_WATER.get(this.configs);
        settings.writeBufferLowWaterMark =
                GremlinConfig.SERVER_WRITE_BUFFER_LOW_WATER.get(this.configs);
        this.server = new GremlinServer(settings);

        ServerGremlinExecutor serverGremlinExecutor =
                Utils.getFieldValue(GremlinServer.class, this.server, "serverGremlinExecutor");
        serverGremlinExecutor.getGraphManager().putGraph("graph", graph);
        serverGremlinExecutor.getGraphManager().putTraversalSource("g", graph.traversal());
        GremlinExecutor gremlinExecutor =
                Utils.getFieldValue(
                        ServerGremlinExecutor.class, serverGremlinExecutor, "gremlinExecutor");
        Bindings globalBindings =
                Utils.getFieldValue(GremlinExecutor.class, gremlinExecutor, "globalBindings");
        globalBindings.put("graph", graph);
        globalBindings.put("g", graph.traversal());

        ProcessorLoader processorLoader =
                new ReadOnlyMaxGraphProcessorLoader(
                        this.configs, this.graph, this.schemaFetcher, this.rpcAddressFetcher);
        try {
            processorLoader.loadProcessor(settings);
        } catch (Exception e) {
            throw new MaxGraphException(e);
        }
        try {
            this.server
                    .start()
                    .exceptionally(
                            t -> {
                                logger.error(
                                        "Gremlin Server was unable to start and will now begin shutdown: {}",
                                        t.getMessage());
                                this.server.stop().join();
                                return null;
                            })
                    .join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger.info("Gremlin Server started....");
    }

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

    private void loadSettings() {
        InputStream input =
                com.alibaba.maxgraph.server.MaxGraphServer.class
                        .getClassLoader()
                        .getResourceAsStream("conf/server.yaml");
        InputStream groovy =
                com.alibaba.maxgraph.server.MaxGraphServer.class
                        .getClassLoader()
                        .getResourceAsStream("conf/generate-classic.groovy");
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
        List<String> customSerializerList = null;
        try {
            customSerializerList =
                    IOUtils.readLines(
                            com.alibaba.maxgraph.server.MaxGraphServer.class
                                    .getClassLoader()
                                    .getResourceAsStream("serializer.custom.config"),
                            "utf-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Settings.SerializerSettings serializerSettings : this.settings.serializers) {
            serializerSettings.config.put("custom", customSerializerList);
        }
    }

    public GremlinExecutor getGremlinExecutor() {
        return this.server.getServerGremlinExecutor().getGremlinExecutor();
    }
}
