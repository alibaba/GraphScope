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

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.processor.IrTestOpProcessor;
import com.alibaba.graphscope.gremlin.integration.result.GraphProperties;
import com.alibaba.graphscope.gremlin.plugin.processor.IrOpLoader;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;

import java.io.InputStream;

public class IrGremlinServer implements AutoCloseable {
    private GremlinServer gremlinServer;
    private Settings settings;

    public IrGremlinServer() {
        InputStream input =
                getClass().getClassLoader().getResourceAsStream("conf/gremlin-server.yaml");
        settings = Settings.read(input);
    }

    public IrGremlinServer(int gremlinPort) {
        this();
        settings.port = (gremlinPort >= 0) ? gremlinPort : settings.port;
    }

    public void start(
            Configs configs,
            IrMetaFetcher irMetaFetcher,
            RpcChannelFetcher fetcher,
            IrMetaQueryCallback metaQueryCallback,
            GraphProperties testGraph)
            throws Exception {
        AbstractOpProcessor standardProcessor =
                new IrStandardOpProcessor(configs, irMetaFetcher, fetcher, metaQueryCallback);
        IrOpLoader.addProcessor(standardProcessor.getName(), standardProcessor);
        AbstractOpProcessor testProcessor =
                new IrTestOpProcessor(configs, irMetaFetcher, fetcher, metaQueryCallback, testGraph);
        IrOpLoader.addProcessor(testProcessor.getName(), testProcessor);

        this.gremlinServer = new GremlinServer(settings);
        this.gremlinServer.start();
    }

    @Override
    public void close() throws Exception {
        if (this.gremlinServer != null) {
            this.gremlinServer.stop();
        }
    }
}
