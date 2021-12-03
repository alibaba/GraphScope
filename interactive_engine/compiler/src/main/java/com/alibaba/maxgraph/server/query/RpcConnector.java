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
import com.alibaba.maxgraph.rpc.*;
import com.alibaba.maxgraph.rpc.GremlinService.*;
import com.alibaba.maxgraph.rpc.MaxGraphCtrlServiceGrpc.MaxGraphCtrlServiceBlockingStub;
import com.alibaba.maxgraph.rpc.MaxGraphServiceGrpc.MaxGraphServiceBlockingStub;
import com.alibaba.maxgraph.rpc.MaxGraphServiceGrpc.MaxGraphServiceStub;
import com.alibaba.maxgraph.sdk.exception.ExceptionHolder;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.base.Stopwatch;
import com.google.common.cache.*;
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
public class RpcConnector implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RpcConnector.class);

    protected static class IpPort {
        String ip;
        int port;

        public IpPort(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        public static IpPort fromGremlinEndpoint(Endpoint endpoint) {
            return new IpPort(endpoint.getIp(), endpoint.getPort());
        }

        public static IpPort fromCtrlAndAsyncEndpoint(Endpoint endpoint) {
            return new IpPort(endpoint.getIp(), endpoint.getRuntimeCtrlAndAsyncPort());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IpPort ipPort = (IpPort) o;
            return port == ipPort.port &&
                    Objects.equals(ip, ipPort.ip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, port);
        }

        @Override
        public String toString() {
            return "IpPort{" +
                    "ip='" + ip + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    protected final RpcAddressFetcher addressFetcher;
    private final ExecutorService service = MoreExecutors.newDirectExecutorService();
    private final int channelCount;
    private CacheLoader<IpPort, List<Pair<MaxGraphServiceStub, ManagedChannel>>> channelLoader;
    private LoadingCache<IpPort, List<Pair<MaxGraphServiceStub, ManagedChannel>>> channels;
    private RpcConfig rpcConfig;

    public RpcConnector(List<String> hosts, GraphSchema schema) {
        this(hosts, new RpcConfig(), schema);
    }

    public RpcConnector(List<String> hosts, RpcConfig rpcConfig, GraphSchema schema) {
        this(DefaultRpcAddressFetcher.fromHostList(hosts), rpcConfig);
    }

    public RpcConnector(RpcAddressFetcher addressFetcher, RpcConfig rpcConfig) {
        this.addressFetcher = addressFetcher;
        this.channelCount = rpcConfig.getChannelCount();
        this.channelLoader = new ChannelCacheLoader(service, channelCount);
        this.channels = CacheBuilder.newBuilder().removalListener(new ChannelRemovalListener()).build(channelLoader);

        this.rpcConfig = rpcConfig;
    }

    protected List<Endpoint> getTargetExecutorAddrs() {
        return addressFetcher.getServiceAddress();
    }

    public void query(QueryFlowOuterClass.QueryFlow.Builder queryFlow, TimelyResultProcessor resultProcessor, GraphSchema schema,
                      Graph graph, long timeout, boolean isAsync) throws Exception {
        ExceptionHolder exceptionHolder = new ExceptionHolder();
        List<Endpoint> executorAddrList = getTargetExecutorAddrs();
        int executorCount = executorAddrList.size();

        CountDownLatch latch = new CountDownLatch(executorCount);
        Stopwatch stopwatch = Stopwatch.createStarted();

        LOG.info("Query plan=>" + TextFormat.printToString(queryFlow));
        List<StreamObserver> streamObserverList = new ArrayList<>();
        for (int i = 0; i < executorAddrList.size(); i++) {
            streamObserverList.add(new QueryStreamObserver(resultProcessor, schema, graph, exceptionHolder, latch, queryFlow.getQueryId()));
        }

        long currentTimeOut = queryFlow.getTimeoutMs();
        if (currentTimeOut == 0) {
            queryFlow.setTimeoutMs(timeout);
            currentTimeOut = timeout;
        }

        queryFlow.setStartTimestampMs(System.currentTimeMillis());
        QueryFlowOuterClass.QueryFlow queryFlowPlan = queryFlow.build();

        if (isAsync) {
            for(int i = 0; i < executorAddrList.size(); i++) {
                IpPort address = IpPort.fromCtrlAndAsyncEndpoint(executorAddrList.get(i));
                ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
                ConnectivityState connectivityState = randomChannel.getState(true);
                if (connectivityState != ConnectivityState.IDLE && connectivityState != ConnectivityState.READY) {
                    LOG.warn("refresh connection for " + address + " with connectivity state " + connectivityState);
                    channels.refresh(address);
                    randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
                }
                connectivityState = randomChannel.getState(true);
                LOG.info("Connectivity state from " + address + " " + connectivityState);
                AsyncMaxGraphServiceGrpc.newStub(randomChannel).asyncExecute(queryFlowPlan, streamObserverList.get(i));            }
        } else {
            for (int i = 0; i < executorAddrList.size(); i++) {
                IpPort address = IpPort.fromGremlinEndpoint(executorAddrList.get(i));
                MaxGraphServiceStub gremlinServiceStub = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getLeft();
                gremlinServiceStub.execute(queryFlowPlan, streamObserverList.get(i));
            }
        }
        long sendTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        long awaitStart = System.currentTimeMillis();
        long waitTimeOut = currentTimeOut;
        while (true) {
            try {
                if (!latch.await(waitTimeOut, TimeUnit.MILLISECONDS)) {
                    getTargetExecutorAddrs(); // try to check if executor is exit
                    if (exceptionHolder.getException() != null) {
                        LOG.error("query from timely fail", exceptionHolder.getException());
                        throw new RuntimeException(exceptionHolder.getException());
                    }
                    throw new RuntimeException("Wait query complete time out for " + (currentTimeOut / 1000.0) + " secs");
                }
                break;
            } catch (InterruptedException e) {
                if (exceptionHolder.getException() != null) {
                    LOG.error("query from timely fail", exceptionHolder.getException());
                    throw new RuntimeException(exceptionHolder.getException());
                }

                long currentTime = System.currentTimeMillis();
                waitTimeOut = currentTimeOut - (currentTime - awaitStart);
                if (waitTimeOut <= 0) {
                    break;
                }
                LOG.error("thread interrupted, continue to wait " + waitTimeOut + "ms");
            }
        }

        if (exceptionHolder.getException() != null) {
            LOG.error("query from timely fail", exceptionHolder.getException());
            throw new RuntimeException(exceptionHolder.getException());
        }
        processEmptyResult(resultProcessor, queryFlowPlan);
        long queryTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        resultProcessor.finish();
        long finishTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        LOG.info("finish query " + queryFlow.getQueryId() + " send cost=>" + sendTime + ", query cost=>" + (queryTime - sendTime)
                + "ms, finish cost=>" + (finishTime - queryTime) + "ms, total cost=>"
                + finishTime + "ms, query count=> " + resultProcessor.total());
        stopwatch.stop();
    }

    private void processEmptyResult(TimelyResultProcessor resultProcessor, QueryFlowOuterClass.QueryFlow queryFlowPlan) {
        long totalCount = resultProcessor.total();
        List<Integer> operatorIpdList = queryFlowPlan.getQueryPlan().getOperatorIdListList();
        int lastOperatorId = operatorIpdList.get(operatorIpdList.size() - 1);
        if (totalCount == 0) {
            for (QueryFlowOuterClass.UnaryOperator unaryOperator : queryFlowPlan.getQueryPlan().getUnaryOpList()) {
                if (unaryOperator.getBase().getId() == lastOperatorId) {
                    if (unaryOperator.getBase().getOperatorType() == QueryFlowOuterClass.OperatorType.COUNT) {
                        resultProcessor.process(new PropertyValueResult(0L));
                    } else if (unaryOperator.getBase().getOperatorType() == QueryFlowOuterClass.OperatorType.GROUP_COUNT
                            || unaryOperator.getBase().getOperatorType() == QueryFlowOuterClass.OperatorType.FOLDMAP) {
                        resultProcessor.process(new MapValueResult());
                    }
                }
            }
        }
    }

    public void prepare(QueryFlowOuterClass.QueryFlow queryFlow, boolean isAsync) throws Exception {
        Message.OperationResponse operationResponse;
        if (isAsync) {
            IpPort address = IpPort.fromCtrlAndAsyncEndpoint(addressFetcher.getServiceAddress().get(0));
            ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
            operationResponse = AsyncMaxGraphServiceGrpc.newBlockingStub(randomChannel).asyncPrepare(queryFlow);
        } else {
            IpPort address = IpPort.fromGremlinEndpoint(addressFetcher.getServiceAddress().get(0));
            ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
            MaxGraphServiceBlockingStub gremlinServiceStub = MaxGraphServiceGrpc.newBlockingStub(randomChannel);
            operationResponse = gremlinServiceStub.prepare(queryFlow);
        }
        if (!operationResponse.getSuccess()) {
            LOG.error("PREPARE query flow fail.");
            throw new RuntimeException(operationResponse.getMessage());
        }
    }

    public void executePrepare(QueryFlowOuterClass.Query.Builder query, TimelyResultProcessor resultProcessor, GraphSchema schema, Graph graph, boolean isAsync) throws Exception {
        ExceptionHolder exceptionHolder = new ExceptionHolder();
        CountDownLatch latch = new CountDownLatch(1);
        Stopwatch stopwatch = Stopwatch.createStarted();
        StreamObserver streamObserver = new QueryStreamObserver(resultProcessor, schema, graph, exceptionHolder, latch, query.getQueryId());

        query.setTimeoutSeconds(rpcConfig.getQueryTimeoutSec());

        if (isAsync) {
            IpPort address = IpPort.fromCtrlAndAsyncEndpoint(addressFetcher.getServiceAddress().get(0));
            ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
            AsyncMaxGraphServiceGrpc.newStub(randomChannel).asyncQuery2(query.build(), streamObserver);
        } else {
            IpPort address = IpPort.fromGremlinEndpoint(addressFetcher.getServiceAddress().get(0));
            ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
            MaxGraphServiceStub gremlinServiceStub = MaxGraphServiceGrpc.newStub(randomChannel);
            gremlinServiceStub.query2(query.build(), streamObserver);
        }
        if (!latch.await(rpcConfig.getQueryTimeoutSec(), TimeUnit.SECONDS)) {
            throw new RuntimeException("Wait query complete time out " + rpcConfig.getQueryTimeoutSec() + " secs");
        }
        if (exceptionHolder.getException() != null) {
            LOG.error("query from timely fail", exceptionHolder.getException());
            throw new RuntimeException(exceptionHolder.getException());
        }
        resultProcessor.finish();
        LOG.info("execute prepare query " + query.getQueryId() + " finish cost=>" + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms, count=> " + resultProcessor.total());
    }

    public void showProcessList(TimelyResultProcessor resultProcessor) throws Exception {
        MaxGraphCtrlServiceBlockingStub stub = randomCtrlStub(getTargetExecutorAddrs().get(0));
        ShowProcessListResponse resp = stub.showProcessList(ShowProcessListRequest.newBuilder().build());
        List<QueryResult> processNameList = Lists.newArrayList();
        for (RunningQuery runningQuery : resp.getQueriesList()) {
            processNameList.add(new PropertyValueResult(runningQuery.getQueryId() + "[" + runningQuery.getScript() + "][" + runningQuery.getElapsedNano() / 1000000 + "ms]"));
        }
        ListResult listResult = new ListResult(processNameList);
        resultProcessor.process(listResult);
        resultProcessor.finish();
    }

    public boolean hasCancelDataFlowByFrontCompleted(int frontId, List<Endpoint> serverList) throws Exception {
        for (Endpoint entry : serverList) {
            IpPort address = IpPort.fromCtrlAndAsyncEndpoint(entry);
            ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
            MaxGraphCtrlServiceBlockingStub stub = MaxGraphCtrlServiceGrpc.newBlockingStub(randomChannel);
            ShowProcessListResponse resp = stub.showProcessList(ShowProcessListRequest.newBuilder().build());
            // LOG.info("hasCancelDataFlowByFrontCompleted show processList {}", resp.getQueriesList());
            for (RunningQuery runningQuery : resp.getQueriesList()) {
                if (runningQuery.getFrontId() == frontId) {
                    return false;
                }
            }
        }
        return true;
    }

    public void cancelDataflow(TimelyResultProcessor resultProcessor, String queryId) throws Exception {
        List<Endpoint> executorAddrList = getTargetExecutorAddrs();

        String logMsg = new String("");
        boolean isSuccess = false;
        for(int i = 0; i < executorAddrList.size(); i++) {
            MaxGraphCtrlServiceBlockingStub stub = randomCtrlStub(executorAddrList.get(i));
            CancelDataflowResponse resp = stub.cancelDataflow(CancelDataflowRequest.newBuilder().setQueryId(queryId).build());
            if (resp.getSuccess()) {
                isSuccess = true;
                logMsg = String.format("Cancel %s in worker %d success.", queryId, i);
            } else {
                logMsg = String.format("Cancel %s in worker %d failed.", queryId, i);
            }
            LOG.info(logMsg);
        }

        String resultMsg;
        if (isSuccess) {
            resultMsg = "Cancel success.";
        } else {
            resultMsg = "Cancel failed.";
        }
        resultProcessor.process(new PropertyValueResult(resultMsg));
        resultProcessor.finish();
    }

    public void cancelDataflowByFront(int frontId, List<Endpoint> serverList) throws Exception {
        for (Endpoint entry : serverList) {
            IpPort address = IpPort.fromCtrlAndAsyncEndpoint(entry);
            ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
            MaxGraphCtrlServiceBlockingStub stub = MaxGraphCtrlServiceGrpc.newBlockingStub(randomChannel);
            CancelDataflowResponse resp = stub.cancelDataflowByFront(GremlinService.CancelDataflowByFrontRequest.newBuilder().setFrontId(frontId).build());
            if (!resp.getSuccess()) {
                throw new RuntimeException("request cancelDataflowByFront to server " + address + " by frontend " + frontId + " fail: " + resp.getMessage());
            }
        }
    }

    private MaxGraphCtrlServiceBlockingStub randomCtrlStub(Endpoint endpoint) throws java.util.concurrent.ExecutionException {
        IpPort address = IpPort.fromCtrlAndAsyncEndpoint(endpoint);
        ManagedChannel randomChannel = channels.get(address).get(RandomUtils.nextInt(0, channelCount)).getRight();
        return MaxGraphCtrlServiceGrpc.newBlockingStub(randomChannel);
    }

    @Override
    public void close() throws IOException {
        channels.invalidateAll();
    }

    private class ChannelRemovalListener implements RemovalListener<IpPort, List<Pair<MaxGraphServiceStub, ManagedChannel>>> {
        @Override
        public void onRemoval(
                RemovalNotification<IpPort, List<Pair<MaxGraphServiceStub, ManagedChannel>>> removalNotification) {
            if (null != removalNotification.getValue()) {
                for (Pair<MaxGraphServiceStub, ManagedChannel> channel : removalNotification.getValue()) {
                    channel.getRight().shutdown();
                }
            }
            LOG.info("channel for => " + removalNotification.getKey() + " removed, cause:"
                    + removalNotification.getCause().toString());
        }
    }

    private static class ChannelCacheLoader extends CacheLoader<IpPort, List<Pair<MaxGraphServiceStub, ManagedChannel>>> {
        private final ExecutorService service;
        private final int channelCount;

        public ChannelCacheLoader(ExecutorService service, int channelCount) {
            this.service = service;
            this.channelCount = channelCount;
        }

        @Override
        public List<Pair<MaxGraphServiceStub, ManagedChannel>> load(IpPort address) throws Exception {
            List<Pair<MaxGraphServiceStub, ManagedChannel>> channelList = Lists.newArrayListWithCapacity(channelCount);
            int flowControlWindow = 10 * 1024 * 1024;
            for (int i = 0; i < channelCount; i++) {
                ManagedChannel channel = NettyChannelBuilder
                        .forAddress(address.ip, address.port)
                        .negotiationType(NegotiationType.PLAINTEXT)
                        .maxInboundMessageSize(1024 * 1024 * 1024)
                        .executor(service)
                        .flowControlWindow(flowControlWindow)
                        .build();
                channelList.add(Pair.of(MaxGraphServiceGrpc.newStub(channel), channel));
            }
            LOG.info("Create channel from " + address.toString());
            return channelList;
        }
    }

    private static class QueryStreamObserver implements StreamObserver<Message.QueryResponse> {
        private TimelyResultProcessor resultProcessor;
        private GraphSchema schema;
        private Graph graph;
        private ExceptionHolder exceptionHolder;
        private CountDownLatch latch;
        private String queryId;
        private boolean receiveFlag = false;

        public QueryStreamObserver(TimelyResultProcessor resultProcessor,
                                   GraphSchema schema,
                                   Graph graph,
                                   ExceptionHolder exceptionHolder,
                                   CountDownLatch latch,
                                   String queryId) {
            this.resultProcessor = resultProcessor;
            this.schema = schema;
            this.graph = graph;
            this.exceptionHolder = exceptionHolder;
            this.latch = latch;
            this.queryId = queryId;
        }

        @Override
        public void onNext(Message.QueryResponse queryResponse) {
            if (queryResponse.getErrorCode() != 0) {
                String errorMessage = "errorCode[" + queryResponse.getErrorCode() + "] errorMessage[" + queryResponse.getMessage() + "]";
                Exception exception = new RuntimeException(errorMessage);
                LOG.error("query fail", exception);
                exceptionHolder.hold(exception);
                return;
            }

            List<ByteString> valueList = queryResponse.getValueList();
            for (ByteString bytes : valueList) {
                if (!receiveFlag) {
                    receiveFlag = true;
                    LOG.info("Start to receive and process result for query " + queryId);
                }
                try {
                    resultProcessor.process(ResultParserUtils.parseResponse(bytes, schema, graph));
                } catch (Exception e) {
                    LOG.error("parse value result fail", e);
                    exceptionHolder.hold(e);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            LOG.error("error query count=>" + latch.getCount() + "s", throwable);
            exceptionHolder.hold(throwable);
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }
    }
}
