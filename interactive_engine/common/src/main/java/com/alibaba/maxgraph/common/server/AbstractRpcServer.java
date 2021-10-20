/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiafei.qiuxf on 16/5/25.
 */
public abstract class AbstractRpcServer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRpcServer.class);
    private static final int BIGGRAPH_RPC_MAX_MESSAGE_SIZE = 1073741824;

    private Server grpcServer;
    private ConcurrentHashMap<K, V> delegates = new ConcurrentHashMap<>();
    private int port = -1;

    public abstract BindableService getService();

    public String getName() {
        return this.getClass().getSimpleName();
    }

    public void init(Endpoint addr, Executor executor) throws IOException {
        NettyServerBuilder builder = NettyServerBuilder
                .forAddress(new InetSocketAddress(addr.getIp(), addr.getPort()))
                .maxMessageSize(BIGGRAPH_RPC_MAX_MESSAGE_SIZE)
                .addService(getService());
        if (executor != null) {
            builder.executor(executor);
        }
        grpcServer = builder.build();
        grpcServer.start();
        port = grpcServer.getPort();
        LOG.info("rpc service [{}] started on: {}:{}", this.getClass().getCanonicalName(), addr.getIp(), port);
    }

    public void init(NettyServerBuilder serverBuilder) throws IOException {
        this.grpcServer = null;
        serverBuilder.addService(getService());
        // won't start the server, the invoker should do this.
    }

    /**
     * Get the actual port this server listens to.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    public void shutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.shutdown();
            grpcServer.awaitTermination();
        }
    }

    public boolean containsDelegate(K k) {
        return delegates.containsKey(k);
    }

    public <X extends Throwable> V getDelegateOrNull(K k) throws X {
        return delegates.get(k);
    }

    public <X extends Throwable> V getDelegate(K k, Function<String, ? extends X> exCtor) throws X {
        V v = delegates.get(k);
        if (v == null) {
            throw exCtor.apply(String.valueOf(k));
        }
        return v;
    }

    /**
     * Add an delegate
     *
     * @param k        delegate key
     * @param delegate delegate object
     * @return the previous delegate or null if no previous one
     */
    public V addDelegate(K k, V delegate) {
        return delegates.put(k, delegate);
    }

    /**
     * Remove an delegate
     *
     * @param k
     * @return
     */
    public V removeDelegate(K k) {
        return delegates.remove(k);
    }

}

