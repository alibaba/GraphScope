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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.rpc.DefaultRpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.utils.ResultParserUtils;
import com.alibaba.maxgraph.result.ListResult;
import com.alibaba.maxgraph.result.MapValueResult;
import com.alibaba.maxgraph.result.PropertyValueResult;
import com.alibaba.maxgraph.rpc.AsyncMaxGraphServiceGrpc;
import com.alibaba.maxgraph.rpc.GremlinService;
import com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest;
import com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse;
import com.alibaba.maxgraph.rpc.GremlinService.RunningQuery;
import com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest;
import com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse;
import com.alibaba.maxgraph.rpc.MaxGraphCtrlServiceGrpc;
import com.alibaba.maxgraph.rpc.MaxGraphCtrlServiceGrpc.MaxGraphCtrlServiceBlockingStub;
import com.alibaba.maxgraph.rpc.MaxGraphServiceGrpc;
import com.alibaba.maxgraph.rpc.MaxGraphServiceGrpc.MaxGraphServiceBlockingStub;
import com.alibaba.maxgraph.rpc.MaxGraphServiceGrpc.MaxGraphServiceStub;
import com.alibaba.maxgraph.rpc.TimelyResultProcessor;
import com.alibaba.maxgraph.sdk.exception.ExceptionHolder;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Connect to timely service var grpc, submit query and receive the response.
 */
public class TimelyRpcConnector extends RpcConnector {
    private static final Logger LOG = LoggerFactory.getLogger(TimelyRpcConnector.class);

    public TimelyRpcConnector(List<String> hosts, GraphSchema schema) {
        this(hosts, new RpcConfig(), schema);
    }

    public TimelyRpcConnector(List<String> hosts, RpcConfig rpcConfig, GraphSchema schema) {
        super(DefaultRpcAddressFetcher.fromHostList(hosts), rpcConfig);
    }

    public TimelyRpcConnector(RpcAddressFetcher addressFetcher, RpcConfig rpcConfig) {
        super(addressFetcher, rpcConfig);
    }

    @Override
    protected List<Endpoint> getTargetExecutorAddrs() {
        return super.addressFetcher.getServiceAddress().subList(0, 1);
    }
}
