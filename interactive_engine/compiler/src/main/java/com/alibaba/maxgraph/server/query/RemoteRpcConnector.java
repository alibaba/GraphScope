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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.common.rpc.RpcAddress;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.proto.GremlinQuery;
import com.alibaba.maxgraph.proto.GremlinServiceGrpc;
import com.alibaba.maxgraph.sdk.exception.ExceptionHolder;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.tinkerpop.gremlin.server.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Remote graph vertex/edge query connector */
public class RemoteRpcConnector implements Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteRpcConnector.class);

    private final RpcAddressFetcher addressFetcher;
    private final RpcConfig rpcConfig;
    private final ExecutorService service = MoreExecutors.newDirectExecutorService();
    private final CacheLoader<RpcAddress, ManagedChannel> channelLoader =
            new CacheLoader<RpcAddress, ManagedChannel>() {
                @Override
                public ManagedChannel load(RpcAddress targetEnvAddress) throws Exception {
                    int flowControlWindow = 10 * 1024 * 1024;
                    ManagedChannel channel =
                            NettyChannelBuilder.forAddress(
                                            targetEnvAddress.getHost(), targetEnvAddress.getPort())
                                    .negotiationType(NegotiationType.PLAINTEXT)
                                    .maxInboundMessageSize(1024 * 1024 * 1024)
                                    .executor(service)
                                    .flowControlWindow(flowControlWindow)
                                    .build();
                    LOG.info("Create channel from " + targetEnvAddress.toString());
                    return channel;
                }
            };
    private LoadingCache<RpcAddress, ManagedChannel> channels;

    private final boolean queryCacheFlag;
    private final RpcProcessorType rpcProcessorType;

    public RemoteRpcConnector(
            RpcAddressFetcher addressFetcher,
            RpcConfig rpcConfig,
            boolean queryCacheFlag,
            RpcProcessorType rpcProcessorType) {
        this.addressFetcher = addressFetcher;
        this.rpcConfig = rpcConfig;
        this.channels =
                CacheBuilder.newBuilder()
                        .removalListener(new RemoteRpcConnector.ChannelRemovalListener())
                        .build(channelLoader);

        this.queryCacheFlag = queryCacheFlag;
        this.rpcProcessorType = rpcProcessorType;
    }

    private RemoteRpcProcessor createRemoteRpcProcessor(
            RpcProcessorType rpcProcessorType, Context context, int batchSize) {
        switch (rpcProcessorType) {
            case NETTY:
                return new NettyVertexRpcProcessor(context, batchSize, queryCacheFlag);
            case TRAVERSER:
                return new NettyTraverserVertexProcessor(context, batchSize, queryCacheFlag);
            default:
                return new DefaultVertexRpcProcessor();
        }
    }

    public void queryVertices(
            Map<Integer, Map<ElementId, Integer>> classified,
            GraphSchema schema,
            TinkerMaxGraph graph,
            Context context,
            int batchSize,
            List<Object> resultList,
            boolean vertexCacheFlag,
            RpcProcessorType rpcProcessorType,
            Map<CompositeId, Map<String, Object>> existPropMap) {
        List<RpcAddress> remoteAddressList = addressFetcher.getAddressList();
        if (classified.size() > remoteAddressList.size()) {
            throw new IllegalArgumentException(
                    "classified=>"
                            + classified.keySet()
                            + " count > remote address list=>"
                            + remoteAddressList.toString());
        }

        int classifiedCount = classified.size();
        CountDownLatch latch = new CountDownLatch(classifiedCount);
        ExceptionHolder exceptionHolder = new ExceptionHolder();
        List<RemoteRpcProcessor> remoteRpcProcessorList =
                Lists.newArrayListWithCapacity(classifiedCount);
        for (Map.Entry<Integer, Map<ElementId, Integer>> entry : classified.entrySet()) {
            if (entry.getKey() < 0) {
                throw new RuntimeException(
                        "Invalid key in " + classified + " when get detail of vertex");
            }
            RpcAddress rpcAddress = remoteAddressList.get(entry.getKey());
            LOG.info("Get rpc address " + rpcAddress + " for store id " + entry.getKey());
            try {
                ManagedChannel managedChannel = channels.get(rpcAddress);
                GremlinServiceGrpc.GremlinServiceStub gremlinServiceStub =
                        GremlinServiceGrpc.newStub(managedChannel);
                GremlinQuery.VertexRequest.Builder reqBuilder =
                        GremlinQuery.VertexRequest.newBuilder();
                Map<ElementId, Integer> vertexCountList = entry.getValue();
                for (ElementId elementId : vertexCountList.keySet()) {
                    reqBuilder.addIds(
                            GremlinQuery.VertexId.newBuilder()
                                    .setId(elementId.id())
                                    .setTypeId(elementId.typeId())
                                    .build());
                }
                RemoteRpcProcessor remoteRpcProcessor =
                        createRemoteRpcProcessor(rpcProcessorType, context, batchSize);
                remoteRpcProcessorList.add(remoteRpcProcessor);
                VertexStreamObserver vertexStreamObserver =
                        new VertexStreamObserver(
                                remoteRpcProcessor,
                                schema,
                                graph,
                                exceptionHolder,
                                latch,
                                vertexCountList,
                                vertexCacheFlag,
                                existPropMap);
                gremlinServiceStub.getVertexs(reqBuilder.build(), vertexStreamObserver);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            if (!latch.await(rpcConfig.getQueryTimeoutSec(), TimeUnit.SECONDS)) {
                throw new RuntimeException("Wait query vertex complete time out");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (exceptionHolder.getException() != null) {
            LOG.error("query vertex from store service fail", exceptionHolder.getException());
            throw new RuntimeException(exceptionHolder.getException());
        }
        if (null != resultList) {
            for (RemoteRpcProcessor remoteRpcProcessor : remoteRpcProcessorList) {
                resultList.addAll(remoteRpcProcessor.getResultList());
            }
        }
    }

    public void queryVertices(
            Map<Integer, Map<ElementId, Integer>> classified,
            GraphSchema schema,
            TinkerMaxGraph graph,
            Context context,
            int batchSize,
            List<Object> resultList,
            boolean vertexCacheFlag,
            Map<CompositeId, Map<String, Object>> existPropMap) {
        queryVertices(
                classified,
                schema,
                graph,
                context,
                batchSize,
                resultList,
                vertexCacheFlag,
                this.rpcProcessorType,
                existPropMap);
    }

    private class ChannelRemovalListener implements RemovalListener<RpcAddress, ManagedChannel> {
        @Override
        public void onRemoval(RemovalNotification<RpcAddress, ManagedChannel> removalNotification) {
            if (null != removalNotification.getValue()) {
                removalNotification.getValue().shutdown();
            }
            LOG.info(
                    "channel for => "
                            + removalNotification.getKey()
                            + " removed, cause:"
                            + removalNotification.getCause().toString());
        }
    }
}
