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

package com.alibaba.graphscope.common.client;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.HQPSConfig;
import com.alibaba.graphscope.gaia.proto.Hqps;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class HttpExecutionClient extends ExecutionClient<URI> {
    private static final Logger logger = LoggerFactory.getLogger(HttpExecutionClient.class);
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String TEXT_PLAIN = "text/plain;charset=UTF-8";
    private final HttpClient httpClient;
    public HttpExecutionClient(Configs graphConfig, ChannelFetcher<URI> channelFetcher) {
        super(channelFetcher);
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(HQPSConfig.HQPS_HTTP_TIMEOUT.get(graphConfig))).build();
    }
    @Override
    public void submit(ExecutionRequest request, ExecutionResponseListener listener) throws Exception {
        List<CompletableFuture> responseFutures = Lists.newArrayList();
        for (URI httpURI : channelFetcher.fetch()) {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(httpURI)
                    .headers(CONTENT_TYPE, TEXT_PLAIN)
                    .POST(HttpRequest.BodyPublishers.ofByteArray((byte[]) request.getRequestPhysical().build()))
                    .build();
            // todo: synchronous call will block compiler thread
            CompletableFuture<HttpResponse<byte[]>> responseFuture =
                    httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                            .whenComplete((bytes, exception) -> {
                if (exception != null) {
                    listener.onError(exception);
                }
                try {
                    Hqps.HighQPSResults results = Hqps.HighQPSResults.parseFrom(bytes.body());
                    for (IrResult.Results irResult : results.getResultsList()) {
                        listener.onNext(irResult.getRecord());
                    }
                } catch (InvalidProtocolBufferException e) {
                    listener.onError(e);
                }
            });
            responseFutures.add(responseFuture);
        }
        CompletableFuture<Void> joinFuture = CompletableFuture.runAsync(() -> {
            try {
                CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0])).get();
                listener.onCompleted();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        joinFuture.whenComplete((aVoid, exception) -> {
            if (exception != null) {
                listener.onError(exception);
            }
        });
    }

    @Override
    public void close() throws Exception {
    }
}
