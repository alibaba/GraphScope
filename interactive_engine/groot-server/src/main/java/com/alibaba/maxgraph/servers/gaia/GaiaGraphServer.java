/*
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
package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.graphscope.gaia.TraversalSourceGraph;
import com.alibaba.graphscope.gaia.broadcast.AbstractBroadcastProcessor;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.processor.GaiaGraphOpProcessor;
import com.alibaba.graphscope.gaia.processor.LogicPlanProcessor;
import com.alibaba.graphscope.gaia.processor.TraversalOpProcessor;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.GremlinConfig;
import com.alibaba.maxgraph.servers.AbstractService;
import io.netty.channel.Channel;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class GaiaGraphServer implements AbstractService {
    private static final Logger logger = LoggerFactory.getLogger(GaiaGraphServer.class);
    private Configs configs;
    private GraphStoreService storeService;

    private Settings settings;
    private GremlinServer server;
    private GaiaConfig gaiaConfig;
    private AbstractBroadcastProcessor broadcastProcessor;

    public GaiaGraphServer(
            Configs configs,
            GraphStoreService storeService,
            AbstractBroadcastProcessor broadcastProcessor,
            GaiaConfig gaiaConfig) {
        this.configs = configs;
        this.broadcastProcessor = broadcastProcessor;
        this.storeService = storeService;
        this.gaiaConfig = gaiaConfig;
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
        settings.writeBufferHighWaterMark =
                GremlinConfig.SERVER_WRITE_BUFFER_HIGH_WATER.get(this.configs);
        settings.writeBufferLowWaterMark =
                GremlinConfig.SERVER_WRITE_BUFFER_LOW_WATER.get(this.configs);
        this.server = new GremlinServer(settings);

        loadProcessor(gaiaConfig, broadcastProcessor, storeService);

        // bind g to traversal source
        Graph traversalGraph = TraversalSourceGraph.open(new BaseConfiguration());
        ServerGremlinExecutor serverGremlinExecutor =
                PlanUtils.getServerGremlinExecutor(this.server);
        serverGremlinExecutor.getGraphManager().putGraph("graph", traversalGraph);
        serverGremlinExecutor.getGraphManager().putTraversalSource("g", traversalGraph.traversal());
        Bindings globalBindings =
                PlanUtils.getGlobalBindings(server.getServerGremlinExecutor().getGremlinExecutor());
        globalBindings.put("graph", traversalGraph);
        globalBindings.put("g", traversalGraph.traversal());

        // start gremlin server
        try {
            server.start()
                    .exceptionally(
                            t -> {
                                logger.error(
                                        "Gremlin Server was unable to start and will now begin shutdown {}",
                                        t);
                                server.stop().join();
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
                GaiaGraphServer.class.getClassLoader().getResourceAsStream("conf/server.gaia.yaml");
        this.settings = Settings.read(input);
    }

    private static void loadProcessor(
            GaiaConfig config,
            AbstractBroadcastProcessor broadcastProcessor,
            GraphStoreService storeService) {
        try {
            Map<String, OpProcessor> gaiaProcessors = new HashMap<>();
            gaiaProcessors.put(
                    "", new GaiaGraphOpProcessor(config, storeService, broadcastProcessor));
            gaiaProcessors.put("plan", new LogicPlanProcessor(config, storeService));
            gaiaProcessors.put(
                    "traversal",
                    new TraversalOpProcessor(config, storeService, broadcastProcessor));
            PlanUtils.setFinalStaticField(OpLoader.class, "processors", gaiaProcessors);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
