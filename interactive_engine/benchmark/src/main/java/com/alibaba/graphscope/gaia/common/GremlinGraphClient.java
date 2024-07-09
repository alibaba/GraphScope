package com.alibaba.graphscope.gaia.common;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;

public class GremlinGraphClient implements GraphClient {
    private final Client client;

    public GremlinGraphClient(String endpoint, String username, String password) {
        String[] address = endpoint.split(":");
        Cluster.Builder cluster =
                Cluster.build()
                        .addContactPoint(address[0])
                        .port(Integer.parseInt(address[1]))
                        .serializer(initializeSerialize());
        if (!(username == null || username.isEmpty())
                && !(password == null || password.isEmpty())) {
            cluster.credentials(username, password);
        }
        Client gremlinClient = cluster.create().connect();
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

    private static MessageSerializer initializeSerialize() {
        return new GryoMessageSerializerV1d0();
    }
}
