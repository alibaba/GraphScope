/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tests.common.rpc;

import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.rpc.MaxGraphNameResolverFactory;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NodeNameResolverTest {

    @Test
    void testNameResolver() throws IOException, InterruptedException {
        String uri = "node://store/0";

        Server server = NettyServerBuilder.forPort(0).build();
        server.start();
        int port = server.getPort();

        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(uri)
                        .nameResolverFactory(
                                new MaxGraphNameResolverFactory(new MockDiscovery(port)))
                        .usePlaintext()
                        .build();
        CountDownLatch latch = new CountDownLatch(1);
        Runnable r =
                new Runnable() {
                    @Override
                    public void run() {
                        ConnectivityState state = channel.getState(true);
                        if (state == ConnectivityState.READY) {
                            latch.countDown();
                        } else {
                            channel.notifyWhenStateChanged(state, this);
                        }
                    }
                };
        r.run();
        assertTrue(latch.await(5L, TimeUnit.SECONDS));
        channel.shutdownNow();
        server.shutdown().awaitTermination(3000L, TimeUnit.MILLISECONDS);
    }

    class MockDiscovery implements NodeDiscovery {

        private int port;

        public MockDiscovery(int port) {
            this.port = port;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void addListener(Listener listener) {
            listener.nodesJoin(
                    RoleType.STORE,
                    Collections.singletonMap(
                            0, new MaxGraphNode(RoleType.STORE.getName(), 0, "localhost", port)));
        }

        @Override
        public void removeListener(Listener listener) {
            listener.nodesLeft(
                    RoleType.STORE,
                    Collections.singletonMap(
                            0, new MaxGraphNode(RoleType.STORE.getName(), 0, "localhost", port)));
        }

        @Override
        public MaxGraphNode getLocalNode() {
            return null;
        }
    }
}
