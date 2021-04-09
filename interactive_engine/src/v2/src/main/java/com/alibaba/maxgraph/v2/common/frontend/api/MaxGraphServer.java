package com.alibaba.maxgraph.v2.common.frontend.api;

public interface MaxGraphServer {
    /**
     * Start graph server
     */
    void start();

    /**
     * Get the port of gremlin server
     *
     * @return The port of gremlin server
     */
    int getGremlinServerPort() throws Exception;

    /**
     * Stop graph server
     */
    void stop();
}
