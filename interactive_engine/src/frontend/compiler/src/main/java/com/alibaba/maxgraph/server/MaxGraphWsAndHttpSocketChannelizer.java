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
package com.alibaba.maxgraph.server;

import com.alibaba.maxgraph.tinkerpop.Utils;
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsAndHttpChannelizerHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

public class MaxGraphWsAndHttpSocketChannelizer extends WsAndHttpChannelizer {
    @Override
    public void init(ServerGremlinExecutor serverGremlinExecutor) {
        super.init(serverGremlinExecutor);

        WsAndHttpChannelizerHandler handler = new WsAndHttpChannelizerHandler();
        Object processor = MaxGraphOpLoader.getProcessor("").orElseThrow(() -> new IllegalArgumentException("cant get op processor"));
        if (processor instanceof AbstractMixedOpProcessor) {
            AbstractMixedOpProcessor opProcessor = (AbstractMixedOpProcessor) MaxGraphOpLoader.getProcessor("").orElseThrow(() -> new IllegalArgumentException("cant get op processor"));
            handler.init(serverGremlinExecutor, new MaxGraphHttpGremlinEndpointHandler(this.serializers, this.gremlinExecutor, this.graphManager, this.settings, opProcessor));
            MaxGraphOpSelectorHandler maxGraphOpSelectorHandler = new MaxGraphOpSelectorHandler(this.settings, this.graphManager, this.gremlinExecutor, this.scheduledExecutorService, this);
            Utils.setFieldValue(AbstractChannelizer.class, this, "opSelectorHandler", maxGraphOpSelectorHandler);
        } else {
            handler.init(serverGremlinExecutor, new HttpGremlinEndpointHandler(this.serializers, this.gremlinExecutor, this.graphManager, this.settings));
        }
        Utils.setFieldValue(WsAndHttpChannelizer.class, this, "handler", handler);
    }
}
