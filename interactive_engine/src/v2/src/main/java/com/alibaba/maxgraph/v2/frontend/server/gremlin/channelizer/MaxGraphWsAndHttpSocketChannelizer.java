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
package com.alibaba.maxgraph.v2.frontend.server.gremlin.channelizer;

import com.alibaba.maxgraph.v2.frontend.server.gremlin.handler.MaxGraphOpSelectorHandler;
import com.alibaba.maxgraph.v2.frontend.utils.ReflectionUtil;
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsAndHttpChannelizerHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

/**
 * Maxgraph ws and http socket channelizer will load maxgraph processor
 */
public class MaxGraphWsAndHttpSocketChannelizer extends WsAndHttpChannelizer {
    @Override
    public void init(ServerGremlinExecutor serverGremlinExecutor) {
        super.init(serverGremlinExecutor);

        WsAndHttpChannelizerHandler handler = new WsAndHttpChannelizerHandler();
        handler.init(serverGremlinExecutor, new HttpGremlinEndpointHandler(this.serializers, this.gremlinExecutor, this.graphManager, this.settings));
        MaxGraphOpSelectorHandler maxGraphOpSelectorHandler = new MaxGraphOpSelectorHandler(this.settings, this.graphManager, this.gremlinExecutor, this.scheduledExecutorService, this);
        ReflectionUtil.setFieldValue(AbstractChannelizer.class, this, "opSelectorHandler", maxGraphOpSelectorHandler);
        ReflectionUtil.setFieldValue(WsAndHttpChannelizer.class, this, "handler", handler);
    }
}
