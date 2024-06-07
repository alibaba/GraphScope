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
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.semconv.SemanticAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * http client to send request to hqps engine service
 */
public class HttpExecutionClient extends ExecutionClient<URI> {
    private static final Logger logger = LoggerFactory.getLogger(HttpExecutionClient.class);
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String INTERACTIVE_REQUEST_FORMAT = "X-Interactive-Request-Format";
    private static final String TEXT_PLAIN = "text/plain;charset=UTF-8";
    private static final String PROTOCOL_FORMAT = "proto";
    private static final String INTERACTIVE_QUERY_PATH = "/v1/graph/current/query";
    private static final String INTERACTIVE_ADHOC_QUERY_PATH = "/interactive/adhoc_query";
    private final HttpClient httpClient;

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;

    private final TextMapSetter<HttpRequest.Builder> setter =
            (carrier, key, value) -> carrier.headers(key, value);

    public HttpExecutionClient(Configs graphConfig, ChannelFetcher<URI> channelFetcher) {
        super(channelFetcher);
        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(
                                Duration.ofMillis(HiactorConfig.HIACTOR_TIMEOUT.get(graphConfig)))
                        .build();
        this.openTelemetry = GlobalOpenTelemetry.get();
        this.tracer = openTelemetry.getTracer(HttpExecutionClient.class.getName());
    }

    @Override
    public void submit(
            ExecutionRequest request,
            ExecutionResponseListener listener,
            QueryTimeoutConfig timeoutConfig)
            throws Exception {
        List<CompletableFuture> responseFutures = Lists.newArrayList();
        for (URI httpURI : channelFetcher.fetch()) {
            Span outgoing =
                    tracer.spanBuilder("/submit").setSpanKind(SpanKind.INTERNAL).startSpan();
            try (Scope scope = outgoing.makeCurrent()) {
                URI uri = resolvePath(httpURI, request);
                outgoing.setAttribute(SemanticAttributes.HTTP_REQUEST_METHOD, "POST");
                outgoing.setAttribute(SemanticAttributes.URL_FULL, uri.toString());

                HttpRequest.Builder httpRequest =
                        HttpRequest.newBuilder()
                                .uri(uri)
                                .headers(CONTENT_TYPE, TEXT_PLAIN)
                                .headers(INTERACTIVE_REQUEST_FORMAT, PROTOCOL_FORMAT)
                                .POST(
                                        HttpRequest.BodyPublishers.ofByteArray(
                                                (byte[])
                                                        request.getRequestPhysical().getContent()));
                openTelemetry
                        .getPropagators()
                        .getTextMapPropagator()
                        .inject(Context.current(), httpRequest, setter);

                CompletableFuture<HttpResponse<byte[]>> responseFuture =
                        httpClient
                                .sendAsync(
                                        httpRequest.build(),
                                        HttpResponse.BodyHandlers.ofByteArray())
                                .orTimeout(
                                        timeoutConfig.getChannelTimeoutMS(), TimeUnit.MILLISECONDS)
                                .whenComplete(
                                        (response, exception) -> {
                                            if (exception != null) {
                                                listener.onError(exception);
                                                outgoing.recordException(exception);
                                            }
                                            try {
                                                // if response is not 200
                                                if (response.statusCode() != 200) {
                                                    // parse String from response.body()
                                                    String errorMessage =
                                                            new String(response.body());
                                                    RuntimeException ex =
                                                            new RuntimeException(
                                                                    "Query execution failed:"
                                                                        + " response status code is"
                                                                        + " "
                                                                            + response.statusCode()
                                                                            + ", error message: "
                                                                            + errorMessage);
                                                    outgoing.recordException(ex);
                                                    listener.onError(ex);
                                                } else {
                                                    outgoing.end();
                                                }
                                                IrResult.CollectiveResults results =
                                                        IrResult.CollectiveResults.parseFrom(
                                                                response.body());
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
