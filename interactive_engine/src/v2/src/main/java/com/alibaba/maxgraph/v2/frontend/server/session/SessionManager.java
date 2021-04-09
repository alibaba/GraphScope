package com.alibaba.maxgraph.v2.frontend.server.session;

import java.util.UUID;

/**
 * API of session manager
 */
public interface SessionManager {
    UUID getRequestId();

    String getSessionId();
}
