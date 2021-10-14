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
package com.alibaba.graphscope.groot.store;

import com.alibaba.maxgraph.proto.groot.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.groot.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.groot.GraphDefPb;
import com.alibaba.maxgraph.proto.groot.StoreSchemaGrpc;
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
    public void fetchSchema(
            FetchSchemaRequest request, StreamObserver<FetchSchemaResponse> responseObserver) {
        try {
            GraphDefPb graphDefBlob = this.storeService.getGraphDefBlob();
            responseObserver.onNext(
                    FetchSchemaResponse.newBuilder().setGraphDef(graphDefBlob).build());
            responseObserver.onCompleted();
        } catch (IOException e) {
            logger.error("fetch schema failed", e);
            responseObserver.onError(e);
        }
    }
}
