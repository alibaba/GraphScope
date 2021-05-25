package com.alibaba.maxgraph.v2.store;

import com.alibaba.maxgraph.proto.v2.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.v2.GraphDefPb;
import com.alibaba.maxgraph.proto.v2.StoreSchemaGrpc;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StoreSchemaService extends StoreSchemaGrpc.StoreSchemaImplBase {
    private static final Logger logger = LoggerFactory.getLogger(StoreSchemaService.class);

    private StoreService storeService;

    public StoreSchemaService(StoreService storeService) {
        this.storeService = storeService;
    }

    @Override
    public void fetchSchema(FetchSchemaRequest request, StreamObserver<FetchSchemaResponse> responseObserver) {
        try {
            GraphDefPb graphDefBlob = this.storeService.getGraphDefBlob();
            responseObserver.onNext(FetchSchemaResponse.newBuilder().setGraphDef(graphDefBlob).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            logger.error("fetch schema failed", e);
            responseObserver.onError(e);
        }
    }

}
