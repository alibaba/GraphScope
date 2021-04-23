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
