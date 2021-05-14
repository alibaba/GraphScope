package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

import java.util.concurrent.CompletableFuture;

public class RemoteGremlinConnection implements RemoteConnection {
    private RemoteConnection remoteConnection;
    private Cluster cluster;

    public RemoteGremlinConnection(String endpoint) throws Exception {
        this.cluster = createCluster(endpoint);
        this.remoteConnection = DriverRemoteConnection.using(this.cluster);
    }

    public static Cluster createCluster(String endpoint) throws Exception {
        String[] split = endpoint.split(":");
        MessageSerializer serializer = new GryoMessageSerializerV1d0();
        Cluster cluster = Cluster.build()
                .addContactPoint(split[0])
                .port(Integer.valueOf(split[1]))
                .credentials("admin", "admin")
                .serializer(serializer)
                .create();
        return cluster;
    }

    @Override
    public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(Bytecode bytecode) throws RemoteConnectionException {
        return remoteConnection.submitAsync(bytecode);
    }

    @Override
    public void close() throws Exception {
        this.remoteConnection.close();
        this.cluster.close();
    }
}
