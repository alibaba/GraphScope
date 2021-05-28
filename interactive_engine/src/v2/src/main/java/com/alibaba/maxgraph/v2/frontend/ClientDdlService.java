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
package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.graphscope.proto.ddl.*;
import io.grpc.stub.StreamObserver;

public class ClientDdlService extends ClientDdlGrpc.ClientDdlImplBase {

    private SchemaWriter schemaWriter;
    private SnapshotCache snapshotCache;

    public ClientDdlService(SchemaWriter schemaWriter, SnapshotCache snapshotCache) {
        this.schemaWriter = schemaWriter;
        this.snapshotCache = snapshotCache;
    }

    @Override
    public void batchSubmit(BatchSubmitRequest request, StreamObserver<BatchSubmitResponse> responseObserver) {
        request.getl
    }

    @Override
    public void getGraphDef(GetGraphDefRequest request, StreamObserver<GetGraphDefResponse> responseObserver) {
    }

    //
//    @Override
//    public void submitDdl(SubmitDdlRequest request, StreamObserver<SubmitDdlResponse> responseObserver) {
//        DdlRequestBatchPb.Builder builder = DdlRequestBatchPb.newBuilder();
//        for (DdlRequestPb ddlRequestPb : request.getDdlRequestsList()) {
//            builder.addDdlRequests(ddlRequestPb);
//        }
//        long snapshotId = this.schemaWriter.submitBatchDdl(UuidUtils.getBase64UUIDString(), "", builder.build());
//        this.snapshotCache.addListener(snapshotId, () -> {
//            GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
//            responseObserver.onNext(SubmitDdlResponse.newBuilder().setGraphDef(graphDef.toProto()).build());
//            responseObserver.onCompleted();
//        });
//    }
}
