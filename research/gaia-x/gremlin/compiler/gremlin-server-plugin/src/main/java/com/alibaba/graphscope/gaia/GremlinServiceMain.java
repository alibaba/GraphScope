/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.gaia;

import com.alibaba.graphscope.gaia.config.ExperimentalGaiaConfig;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.processor.GaiaProcessorLoader;
import com.alibaba.graphscope.gaia.store.ExperimentalGraphStore;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;

public class GremlinServiceMain {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServiceMain.class);

    public static void main(String[] args) throws Exception {
        logger.info("start server");
        Settings settings = load();
        GremlinServer server = new GremlinServer(settings);

        // create graph
        GaiaConfig gaiaConfig = new ExperimentalGaiaConfig("conf");
        GraphStoreService storeService = new ExperimentalGraphStore(gaiaConfig);
        GaiaProcessorLoader.load(gaiaConfig, storeService);

        // set global variables
        Graph traversalGraph = server.getServerGremlinExecutor().getGraphManager().getGraph("graph");
        GlobalEngineConf.setGlobalVariables(traversalGraph.variables());

        // bind g to traversal source
        Bindings globalBindings = PlanUtils.getGlobalBindings(server.getServerGremlinExecutor().getGremlinExecutor());
        globalBindings.put("g", traversalGraph.traversal());

        // start gremlin server
        server.start().exceptionally(t -> {
            logger.error("Gremlin Server was unable to start and will now begin shutdown {}", t);
            server.stop().join();
            return null;
        }).join();
    }

    public static Settings load() throws Exception {
        String file = "conf/gremlin-server.yaml";
        return Settings.read(file);
    }
}
