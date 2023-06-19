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
import com.alibaba.graphscope.common.config.HiactorConfig;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
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

/**
 * http client to send request to hqps engine service
 */
public class HttpExecutionClient extends ExecutionClient<URI> {
    private static final Logger logger = LoggerFactory.getLogger(HttpExecutionClient.class);
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String TEXT_PLAIN = "text/plain;charset=UTF-8";
    private static final String INTERACTIVE_QUERY_PATH = "/interactive/query";
    private static final String INTERACTIVE_ADHOC_QUERY_PATH = "/interactive/adhoc_query";
    private final HttpClient httpClient;

    public HttpExecutionClient(Configs graphConfig, ChannelFetcher<URI> channelFetcher) {
        super(channelFetcher);
        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(
                                Duration.ofMillis(HiactorConfig.HIACTOR_TIMEOUT.get(graphConfig)))
                        .build();
    }

    @Override
    public void submit(ExecutionRequest request, ExecutionResponseListener listener)
            throws Exception {
        List<CompletableFuture> responseFutures = Lists.newArrayList();
        for (URI httpURI : channelFetcher.fetch()) {
            HttpRequest httpRequest =
                    HttpRequest.newBuilder()
                            .uri(resolvePath(httpURI, request))
                            .headers(CONTENT_TYPE, TEXT_PLAIN)
                            .POST(
                                    HttpRequest.BodyPublishers.ofByteArray(
                                            (byte[]) request.getRequestPhysical().build()))
                            .build();
            CompletableFuture<HttpResponse<byte[]>> responseFuture =
                    httpClient
                            .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                            .whenComplete(
                                    (bytes, exception) -> {
                                        if (exception != null) {
                                            listener.onError(exception);
                                        }
                                        try {
                                            IrResult.CollectiveResults results =
                                                    IrResult.CollectiveResults.parseFrom(
                                                            bytes.body());
                                            for (IrResult.Results irResult :
                                                    results.getResultsList()) {
                                                listener.onNext(irResult.getRecord());
                                            }
                                        } catch (InvalidProtocolBufferException e) {
                                            listener.onError(e);
                                        }
                                    });
            responseFutures.add(responseFuture);
        }
        CompletableFuture<Void> joinFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                CompletableFuture.allOf(
                                                responseFutures.toArray(new CompletableFuture[0]))
                                        .get();
                                listener.onCompleted();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        joinFuture.whenComplete(
                (aVoid, exception) -> {
                    if (exception != null) {
                        listener.onError(exception);
                    }
                });
    }

    private URI resolvePath(URI original, ExecutionRequest request) {
        LogicalPlan logicalPlan = request.getRequestLogical();
        if (logicalPlan.getRegularQuery() != null) {
            return original.resolve(INTERACTIVE_ADHOC_QUERY_PATH);
        } else if (logicalPlan.getProcedureCall() != null) {
            return original.resolve(INTERACTIVE_QUERY_PATH);
        } else {
            throw new IllegalArgumentException("the request can not be sent to the remote service");
        }
    }

    @Override
    public void close() throws Exception {}
}
