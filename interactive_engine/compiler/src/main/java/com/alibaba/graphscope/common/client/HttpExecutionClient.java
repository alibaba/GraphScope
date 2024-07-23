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
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gaia.proto.StoredProcedure;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.impl.DefaultSession;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * http client to send request to hqps engine service
 */
public class HttpExecutionClient extends ExecutionClient<URI> {
    private static final Logger logger = LoggerFactory.getLogger(HttpExecutionClient.class);
    private final Session session;

    public HttpExecutionClient(Configs graphConfig, ChannelFetcher<URI> channelFetcher) {
        super(channelFetcher);
        session =
                DefaultSession.newInstance(
                        HiactorConfig.INTERACTIVE_ADMIN_ENDPOINT.get(graphConfig),
                        HiactorConfig.INTERACTIVE_QUERY_ENDPOINT.get(graphConfig));
    }

    @Override
    public void submit(
            ExecutionRequest request,
            ExecutionResponseListener listener,
            QueryTimeoutConfig timeoutConfig)
            throws Exception {
        List<CompletableFuture> responseFutures = Lists.newArrayList();
        for (URI httpURI : channelFetcher.fetch()) {
            CompletableFuture<Result<IrResult.CollectiveResults>> future;
            if (request.getRequestLogical().getRegularQuery() != null) {
                byte[] bytes = (byte[]) request.getRequestPhysical().getContent();
                future =
                        session.runAdhocQueryAsync(
                                GraphAlgebraPhysical.PhysicalPlan.parseFrom(bytes));
            } else if (request.getRequestLogical().getProcedureCall() != null) {
                byte[] bytes = (byte[]) request.getRequestPhysical().getContent();
                future = session.callProcedureAsync(StoredProcedure.Query.parseFrom(bytes));
            } else {
                throw new IllegalArgumentException(
                        "the request can not be sent to the remote service, expect a regular"
                                + " query or a procedure call");
            }

            CompletableFuture<Result<IrResult.CollectiveResults>> responseFuture =
                    future.whenComplete(
                            (response, exception) -> {
                                if (exception != null) {
                                    listener.onError(exception);
                                }

                                // if response is not 200
                                if (!response.isOk()) {
                                    // parse String from response.body()
                                    String errorMessage = new String(response.getStatusMessage());
                                    RuntimeException ex =
                                            new RuntimeException(
                                                    "Query execution failed:"
                                                            + " response status code is"
                                                            + " "
                                                            + response.getStatusCode()
                                                            + ", error message: "
                                                            + errorMessage);
                                    listener.onError(ex);
                                } else {
                                }
                                IrResult.CollectiveResults results = response.getValue();
                                for (IrResult.Results irResult : results.getResultsList()) {
                                    listener.onNext(irResult.getRecord());
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

    @Override
    public void close() throws Exception {}
}
