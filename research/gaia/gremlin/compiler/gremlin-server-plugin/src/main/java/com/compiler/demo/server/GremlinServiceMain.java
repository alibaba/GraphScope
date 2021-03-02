/**
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
package com.compiler.demo.server;

import com.compiler.demo.server.plan.PlanUtils;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class GremlinServiceMain {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServiceMain.class);

    public static void main(String[] args) throws Exception {
        logger.info("start server");
        Settings settings = load(args);
        GremlinServer server = new GremlinServer(settings);
        // todo: better way to modify op loader
        PlanUtils.setFinalStaticField(OpLoader.class, "processors", Collections.singletonMap("", new MaxGraphOpProcessor()));
        // logger.info("op loader is {}", OpLoader.getProcessors());
        server.start().exceptionally(t -> {
            logger.error("Gremlin Server was unable to start and will now begin shutdown {}", t);
            server.stop().join();
            return null;
        }).join();
    }

    public static Settings load(String[] args) throws Exception {
        String file = (args.length > 0) ? args[0] : "conf/gremlin-server.yaml";
        return Settings.read(file);
    }
}
