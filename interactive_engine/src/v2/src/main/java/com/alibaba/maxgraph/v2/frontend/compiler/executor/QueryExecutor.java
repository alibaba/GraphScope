package com.alibaba.maxgraph.v2.frontend.compiler.executor;

import com.alibaba.maxgraph.proto.v2.CancelDataflowResponse;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.QueryFlow;
import com.alibaba.maxgraph.proto.v2.QueryPlan;
import com.alibaba.maxgraph.proto.v2.UnaryOperator;
import com.alibaba.maxgraph.v2.common.CloseableIterator;
import com.alibaba.maxgraph.v2.common.GrpcObserverIterator;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryExecuteRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryManageRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.query.MaxGraphQuery;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphResultProcessor;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.ResultParserUtils;
import com.alibaba.maxgraph.v2.frontend.config.FrontendConfig;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Defines methods to execute queries that are described by an Query object.
 * This interface is intended for implementation by query executors.
 */
public abstract class QueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    protected RoleClients<QueryExecuteRpcClient> queryRpcClients;
    protected RoleClients<QueryManageRpcClient> manageRpcClients;
    protected int executorCount;
    protected int queueSize;

    protected QueryExecutor(Configs configs, RoleClients<QueryExecuteRpcClient> queryRpcClients,
                            RoleClients<QueryManageRpcClient> manageRpcClients, int executorCount) {
        this.queryRpcClients = queryRpcClients;
        this.manageRpcClients = manageRpcClients;
        this.executorCount = executorCount;
        this.queueSize = FrontendConfig.QUERY_RESPONSE_BUFFER_QUEUE_SIZE.get(configs);
    }

    /**
     * Executes the query represented by a specified expression tree.
     *
     * @param maxGraphQuery The maxgraph query
     */
    public void execute(MaxGraphQuery maxGraphQuery, long timeoutHint) {
        QueryFlow.Builder queryFlowBuilder = maxGraphQuery.getQueryFlowManager().getQueryFlow();
        String queryId = maxGraphQuery.getQueryId();
        queryFlowBuilder.setQueryId(queryId);
        long timeout = queryFlowBuilder.getTimeoutMs();
        if (timeout == 0) {
            timeout = timeoutHint;
            queryFlowBuilder.setTimeoutMs(timeoutHint);
        }
        queryFlowBuilder.setStartTimestampMs(System.currentTimeMillis());
        QueryFlow queryFlow = queryFlowBuilder.build();

        GraphSchema schema = maxGraphQuery.getSchema();
        Map<Integer, String> labelIdNameList = maxGraphQuery.getLabelIdNameList();
        MaxGraphResultProcessor resultProcessor = maxGraphQuery.getResultProcessor();

        CloseableIterator<List<ByteString>> resultIterator = query(queryFlow, timeout);
        try {
            while (resultIterator.hasNext()) {
                for (ByteString byteString : resultIterator.next()) {
                    try {
                        List<Object> objects = ResultParserUtils.parseResponse(byteString, schema, labelIdNameList);
                        resultProcessor.process(objects);
                    } catch (InvalidProtocolBufferException e) {
                        throw new MaxGraphException(e);
                    }
                }
            }
        } catch (Exception e){
            if (resultIterator != null) {
                try {
                    resultIterator.close();
                } catch (IOException ioe) {
                    // Ignore
                }
            }
            throw e;
        }

        if (resultProcessor.total() == 0) {
            processEmptyResult(queryFlow, resultProcessor);
        }

        resultProcessor.finish();
    }

    private CloseableIterator<List<ByteString>> query(QueryFlow queryFlow, long timeout) {
        logger.debug("Query plan=>" + queryFlow);
        List<QueryExecuteRpcClient> targetClients = getTargetClients();
        GrpcObserverIterator<List<ByteString>> iterator = new GrpcObserverIterator<>(this.queueSize);
        AtomicInteger counter = new AtomicInteger(targetClients.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (QueryExecuteRpcClient client : targetClients) {
            client.execute(queryFlow, timeout, new QueryExecuteListener() {
                @Override
                public void onReceive(List<ByteString> byteStrings) {
                    if (finished.get()) {
                        return;
                    }
                    try {
                        iterator.putData(byteStrings);
                    } catch (InterruptedException e) {
                        onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    iterator.fail(t);
                }

                @Override
                public void onCompleted() {
                    if (counter.decrementAndGet() == 0) {
                        try {
                            iterator.finish();
                        } catch (InterruptedException e) {
                            onError(e);
                        }
                    }
                }
            });
        }
        return iterator;
    }

    protected abstract List<QueryExecuteRpcClient> getTargetClients();

    private void processEmptyResult(QueryFlow queryFlow, MaxGraphResultProcessor resultProcessor) {
        QueryPlan queryPlan = queryFlow.getQueryPlan();
        List<Integer> operatorIpdList = queryPlan.getOperatorIdListList();
        int lastOperatorId = operatorIpdList.get(operatorIpdList.size() - 1);
        for (UnaryOperator unaryOperator : queryPlan.getUnaryOpList()) {
            if (unaryOperator.getBase().getId() != lastOperatorId) {
                continue;
            }
            if (unaryOperator.getBase().getOperatorType() == OperatorType.COUNT ||
                    unaryOperator.getBase().getOperatorType() == OperatorType.SUM) {
                if (unaryOperator.getBase().getOperatorType() == OperatorType.COUNT) {
                    resultProcessor.process(Lists.newArrayList(0L));
                } else {
                    for (UnaryOperator combinerOperator : queryPlan.getUnaryOpList()) {
                        if (combinerOperator.getBase().getId() == operatorIpdList.get(operatorIpdList.size() - 2)) {
                            if (combinerOperator.getBase().getOperatorType() == OperatorType.COMBINER_COUNT
                                    || combinerOperator.getBase().getOperatorType() == OperatorType.OUT_COUNT
                                    || combinerOperator.getBase().getOperatorType() == OperatorType.IN_COUNT
                                    || combinerOperator.getBase().getOperatorType() == OperatorType.BOTH_COUNT) {
                                resultProcessor.process(Lists.newArrayList(0L));
                            } else {
                                break;
                            }
                        }
                    }
                }
            } else if (unaryOperator.getBase().getOperatorType() == OperatorType.GROUP_COUNT
                    || unaryOperator.getBase().getOperatorType() == OperatorType.FOLDMAP) {
                resultProcessor.process(Lists.newArrayList(new HashMap()));
            }
        }
    }

    /**
     * Query current process list.
     */
    public void showProcessList(MaxGraphResultProcessor resultProcessor) {
        manageRpcClients.getClient(0).showProcessList(resultProcessor);
    }

    /**
     * Cancel a running dataflow.
     */
    public void cancelDataflow(MaxGraphResultProcessor resultProcessor, String queryId, long timeoutMs) {
        boolean isSuccess = true;
        for (int i = 0; i < this.executorCount; i++) {
            CancelDataflowResponse response = manageRpcClients.getClient(i).cancelDataflow(queryId, timeoutMs);
            if (response.getSuccess()) {
                logger.info("Cancel " + queryId + " in worker " + i + " success.");
            } else {
                isSuccess = false;
                logger.info("Cancel " + queryId + " in worker " + i + " failed.");
            }
        }

        String resultMessage;
        if (isSuccess) {
            resultMessage = "Cancel success.";
        } else {
            resultMessage = "Cancel failed.";
        }
        resultProcessor.process(Lists.newArrayList(resultMessage));
        resultProcessor.finish();
    }
}
