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

import com.alibaba.graphscope.gremlin.Utils;

import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsAndHttpChannelizerHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

// config the channelizer in conf/gremlin-server.yaml to set the IrOpSelectorHandler as the default
public class IrWsAndHttpChannelizer extends WsAndHttpChannelizer {
    private WsAndHttpChannelizerHandler handler;

    @Override
    public void init(ServerGremlinExecutor serverGremlinExecutor) {
        super.init(serverGremlinExecutor);
        this.handler = new WsAndHttpChannelizerHandler();
        this.handler.init(
                serverGremlinExecutor,
                new HttpGremlinEndpointHandler(
                        this.serializers, this.gremlinExecutor, this.graphManager, this.settings));
        OpSelectorHandler irOpSelectorHandler =
                new IrOpSelectorHandler(
                        this.settings,
                        this.graphManager,
                        this.gremlinExecutor,
                        this.scheduledExecutorService,
                        this);
        Utils.setFieldValue(
                AbstractChannelizer.class, this, "opSelectorHandler", irOpSelectorHandler);
    }
}
