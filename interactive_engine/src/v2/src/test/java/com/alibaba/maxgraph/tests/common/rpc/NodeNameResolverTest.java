package com.alibaba.maxgraph.tests.common.rpc;

import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.MaxGraphNameResolverFactory;
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

        ManagedChannel channel = ManagedChannelBuilder.forTarget(uri)
                .nameResolverFactory(new MaxGraphNameResolverFactory(new MockDiscovery(port)))
                .usePlaintext()
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        Runnable r = new Runnable() {
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
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void addListener(Listener listener) {
            listener.nodesJoin(RoleType.STORE, Collections.singletonMap(0,
                    new MaxGraphNode(RoleType.STORE.getName(), 0, "localhost", port)));
        }

        @Override
        public void removeListener(Listener listener) {
            listener.nodesLeft(RoleType.STORE, Collections.singletonMap(0,
                    new MaxGraphNode(RoleType.STORE.getName(), 0, "localhost", port)));
        }

        @Override
        public MaxGraphNode getLocalNode() {
            return null;
        }
    }
}
