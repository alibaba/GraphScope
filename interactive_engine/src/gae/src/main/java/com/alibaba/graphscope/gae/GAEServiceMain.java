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
package com.alibaba.graphscope.gae;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class GAEServiceMain {
    private static final Logger logger = LoggerFactory.getLogger(GAEServiceMain.class);

    public static void main(String[] args) throws Exception {
        logger.info("start server");
        Settings settings = load();
        GremlinServer server = new GremlinServer(settings);

        Properties properties = new Properties();
        properties.setProperty("graph.name", "test_graph");
        GAEProcessLoader.load(new InstanceConfig(properties));

        // start gremlin server
        server.start().exceptionally(t -> {
            logger.error("Gremlin Server was unable to start and will now begin shutdown {}", t);
            server.stop().join();
            return null;
        }).join();
    }

    public static Settings load() throws Exception {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf/server.gae.yaml");
        return Settings.read(inputStream);
    }
}
