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
