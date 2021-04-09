package com.alibaba.maxgraph.v2.frontend.server.session;

import org.apache.tinkerpop.gremlin.server.Context;

import java.util.UUID;

/**
 * manager request id and session id from gremlin client
 */
public class ClientSessionManager implements SessionManager {
    private Context context;

    public ClientSessionManager(Context context) {
        this.context = context;
    }

    @Override
    public UUID getRequestId() {
        return this.context.getRequestMessage().getRequestId();
    }

    @Override
    public String getSessionId() {
        return this.context.getChannelHandlerContext().channel().id().asLongText();
    }
}
