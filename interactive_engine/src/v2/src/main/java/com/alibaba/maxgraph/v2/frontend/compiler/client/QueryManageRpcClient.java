package com.alibaba.maxgraph.v2.frontend.compiler.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.maxgraph.proto.v2.CancelDataflowRequest;
import com.alibaba.maxgraph.proto.v2.CancelDataflowResponse;
import com.alibaba.maxgraph.proto.v2.GraphQueryManageServiceGrpc;
import com.alibaba.maxgraph.proto.v2.RunningQuery;
import com.alibaba.maxgraph.proto.v2.ShowProcessListRequest;
import com.alibaba.maxgraph.proto.v2.ShowProcessListResponse;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.rpc.MaxGraphResultProcessor;
import com.google.common.collect.Lists;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class QueryManageRpcClient extends RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(QueryManageRpcClient.class);

    public QueryManageRpcClient(ManagedChannel channel) {
        super(channel);
    }

    public void showProcessList(MaxGraphResultProcessor resultProcessor) {
        GraphQueryManageServiceGrpc.GraphQueryManageServiceBlockingStub queryManageStub = GraphQueryManageServiceGrpc.newBlockingStub(this.channel);
        ShowProcessListResponse response = queryManageStub.showProcessList(ShowProcessListRequest.newBuilder().build());
        List<Object> queryProcessList = Lists.newArrayList();
        for (RunningQuery runningQuery : response.getQueriesList()) {
            JSONObject queryJson = new JSONObject();
            queryJson.put("queryId", runningQuery.getQueryId());
            queryJson.put("script", runningQuery.getScript());
            queryJson.put("runningTimeNano", runningQuery.getElapsedNano());
            queryProcessList.add(queryJson.toJSONString());
        }
        logger.info("show process list finish");
        resultProcessor.process(queryProcessList);
        resultProcessor.finish();
    }

    public CancelDataflowResponse cancelDataflow(String queryId, long timeoutMs) {
        GraphQueryManageServiceGrpc.GraphQueryManageServiceBlockingStub queryManageStub = GraphQueryManageServiceGrpc.newBlockingStub(this.channel);
        return queryManageStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .cancelDataflow(CancelDataflowRequest.newBuilder()
                        .setFrontId(0)
                        .setQueryId(queryId)
                        .build());
    }
}
