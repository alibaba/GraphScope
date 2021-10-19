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
package com.alibaba.maxgraph.server;

import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.tinkerpop.Utils;

import io.netty.channel.Channel;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.script.Bindings;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class MaxGraphServer {
    private static Logger LOG = LoggerFactory.getLogger(MaxGraphServer.class);
    private Graph graph;
    private GremlinServer innerServer;
    private Settings settings;

    /**
     * Use default Gremlin Server setting to start server.
     *
     * @param graph
     */
    public MaxGraphServer(@Nonnull final Graph graph) {
        this.graph = graph;
        initSettings();
    }

    /**
     * Supply a well defined Gremlin Server setting to start server.
     *
     * @param graph
     * @param setting : setting should be a {@link InputStream} read from a '*.yaml' file;
     */
    public MaxGraphServer(@Nonnull final TinkerMaxGraph graph, @Nonnull final InputStream setting) {
        this.graph = graph;
        this.settings = Settings.read(setting);
    }

    protected void initSettings() {
        try {
            InputStream input =
                    this.getClass().getClassLoader().getResourceAsStream("conf/server.yaml");
            InputStream groovy =
                    this.getClass()
                            .getClassLoader()
                            .getResourceAsStream("conf/generate-classic.groovy");
            File tmp = new File("/tmp/generate-classic.groovy");
            if (tmp.exists()) {
                tmp.delete();
            }
            Files.copy(groovy, Paths.get("/tmp/generate-classic.groovy"));
            this.settings = Settings.read(input);
            List<String> customSerializerList =
                    IOUtils.readLines(
                            this.getClass()
                                    .getClassLoader()
                                    .getResourceAsStream("serializer.custom.config"),
                            "utf-8");
            for (Settings.SerializerSettings serializerSettings : this.settings.serializers) {
                serializerSettings.config.put("custom", customSerializerList);
            }
            String logString =
                    new BufferedReader(
                                    new InputStreamReader(
                                            this.getClass()
                                                    .getClassLoader()
                                                    .getResourceAsStream("conf/server.yaml")))
                            .lines()
                            .collect(Collectors.joining("\n"));
            LOG.info("hx test " + logString);

        } catch (Throwable e) {
            LOG.error("get error : ", e);
            throw new RuntimeException(e);
        }
    }

    public void start(int port, ProcessorLoader processorLoader, boolean hasAuth) throws Exception {
        LOG.info(GremlinServer.getHeader());
        this.settings.port = port;
        this.settings.host = "0.0.0.0";
        if (settings.gremlinPool == 0) {
            settings.gremlinPool = Runtime.getRuntime().availableProcessors();
        }
        if (StringUtils.equals(settings.channelizer, WebSocketChannelizer.class.getName())) {
            settings.channelizer = MaxGraphWebSocketChannelizer.class.getName();
        } else if (StringUtils.equals(settings.channelizer, WsAndHttpChannelizer.class.getName())) {
            settings.channelizer = MaxGraphWsAndHttpSocketChannelizer.class.getName();
        } else {
            throw new IllegalArgumentException(
                    "Not support for channelizer=>" + settings.channelizer);
        }

        // If auth mode is not set, auth setting need to be re-initialized
        if (!hasAuth) {
            settings.authentication = new Settings.AuthenticationSettings();
        }

        this.innerServer = new GremlinServer(settings);
        ServerGremlinExecutor serverGremlinExecutor =
                Utils.getFieldValue(GremlinServer.class, this.innerServer, "serverGremlinExecutor");
        serverGremlinExecutor.getGraphManager().putGraph("graph", graph);
        serverGremlinExecutor.getGraphManager().putTraversalSource("g", graph.traversal());
        GremlinExecutor gremlinExecutor =
                Utils.getFieldValue(
                        ServerGremlinExecutor.class, serverGremlinExecutor, "gremlinExecutor");
        Bindings globalBindings =
                Utils.getFieldValue(GremlinExecutor.class, gremlinExecutor, "globalBindings");
        globalBindings.put("graph", graph);
        globalBindings.put("g", graph.traversal());

        processorLoader.loadProcessor(settings);
        //        OpLoader.init(settings);
        innerServer
                .start()
                .exceptionally(
                        t -> {
                            LOG.error(
                                    "Gremlin Server was unable to start and will now begin shutdown: {}",
                                    t.getMessage());
                            innerServer.stop().join();
                            return null;
                        })
                .join();
        LOG.info("Gremlin Server started....");
    }

    public GremlinExecutor getGremlinExecutor() {
        return this.innerServer.getServerGremlinExecutor().getGremlinExecutor();
    }

    public int getGremlinServerPort() throws Exception {
        Field ch = this.innerServer.getClass().getDeclaredField("ch");
        ch.setAccessible(true);
        Channel o = (Channel) ch.get(this.innerServer);
        SocketAddress localAddr = o.localAddress();
        return ((InetSocketAddress) localAddr).getPort();
    }
}
