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

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FileLoadType;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.integration.processor.IrTestOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.processor.IrOpLoader;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;

public class GraphServiceMain {
    private static IrCoreLibrary INSTANCE = IrCoreLibrary.INSTANCE;

    public static void main(String[] args) throws Exception {
        Configs configs = new Configs("conf/ir.compiler.properties", FileLoadType.RELATIVE_PATH);

        AbstractOpProcessor standardProcessor = new IrStandardOpProcessor(configs);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor = new IrTestOpProcessor(configs);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);

        // set graph schema
        String schemaFilePath = GraphConfig.GRAPH_SCHEMA.get(configs);
        INSTANCE.setSchema(Utils.readStringFromFile(schemaFilePath));

        Settings settings = loadSettings();
        GremlinServer server = new GremlinServer(settings);
        server.start();
    }

    private static Settings loadSettings() throws Exception {
        return Settings.read("conf/gremlin-server.yaml");
    }
}
