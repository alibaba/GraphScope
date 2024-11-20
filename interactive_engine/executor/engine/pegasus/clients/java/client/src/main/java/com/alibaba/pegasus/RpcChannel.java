/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.pegasus;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RpcChannel {
    private static final Logger logger = LoggerFactory.getLogger(RpcChannel.class);

    private final Channel channel;

    public RpcChannel(Channel channel) {
        this.channel = channel;
    }

    public RpcChannel(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }

    public RpcChannel(String host, int port, OpenTelemetry openTelemetry) {
        GrpcTelemetry grpcTelemetry = GrpcTelemetry.create(openTelemetry);
        this.channel =
                ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .intercept(grpcTelemetry.newClientInterceptor())
                        .build();
    }

    public Channel getChannel() {
        return channel;
    }

    public void shutdown() throws InterruptedException {
        if (this.channel instanceof ManagedChannel) {
            String name = channel.authority();
            ManagedChannel managedChannel = (ManagedChannel) this.channel;
            managedChannel.awaitTermination(5, TimeUnit.SECONDS);
            logger.info("rpc channel {} shutdown successfully", name);
        }
    }

    public String toString() {
        return "RpcChannel: " + channel.authority();
    }
}
