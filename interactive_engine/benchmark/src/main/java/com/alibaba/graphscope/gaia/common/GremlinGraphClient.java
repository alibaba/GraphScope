package com.alibaba.graphscope.gaia.common;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

public class GremlinGraphClient implements GraphClient {
    private final Client client;

    public GremlinGraphClient(Client gremlinClient) {
        this.client = gremlinClient;
    }

    @Override
    public GraphResultSet submit(String query) {
        ResultSet gremlinResultSet = client.submit(query);
        return new GremlinGraphResultSet(gremlinResultSet);
    }

    @Override
    public void close() {
        client.close();
    }
}
    