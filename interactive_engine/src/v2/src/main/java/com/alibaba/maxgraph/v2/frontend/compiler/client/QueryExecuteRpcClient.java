package com.alibaba.maxgraph.v2.frontend.compiler.client;

import com.alibaba.maxgraph.proto.v2.GraphQueryExecuteServiceGrpc;
import com.alibaba.maxgraph.proto.v2.QueryFlow;
import com.alibaba.maxgraph.proto.v2.QueryResponse;
import com.alibaba.maxgraph.v2.common.exception.QueryExecuteException;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.executor.QueryExecuteListener;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class QueryExecuteRpcClient extends RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecuteRpcClient.class);

    private GraphQueryExecuteServiceGrpc.GraphQueryExecuteServiceStub stub;

    public QueryExecuteRpcClient(ManagedChannel channel) {
        super(channel);
        this.stub = GraphQueryExecuteServiceGrpc.newStub(this.channel);
    }

    public void execute(QueryFlow queryFlow, long timeoutInMillis, QueryExecuteListener listener) {
        this.stub.withDeadlineAfter(timeoutInMillis, TimeUnit.MILLISECONDS).execute(queryFlow,
                new StreamObserver<QueryResponse>() {
            @Override
            public void onNext(QueryResponse response) {
                try {
                    if (response.getErrorCode() != 0) {
                        String errMsg = "errorCode [" + response.getErrorCode() + "] errorMessage [" +
                                response.getMessage() + "]";
                        throw new QueryExecuteException(errMsg);
                    }
                    listener.onReceive(response.getValueList());
                } catch (Exception e) {
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                listener.onError(t);
            }

            @Override
            public void onCompleted() {
                listener.onCompleted();
            }
        });
    }

}
