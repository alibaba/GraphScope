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
package com.alibaba.maxgraph.groot.frontend;

import com.alibaba.maxgraph.proto.v2.DdlRequestBatchPb;
import com.alibaba.maxgraph.proto.v2.SchemaGrpc;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlRequest;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlResponse;
import com.alibaba.maxgraph.groot.common.rpc.RpcClient;
import com.alibaba.maxgraph.groot.common.schema.request.DdlException;
import io.grpc.ManagedChannel;

public class SchemaClient extends RpcClient {

    private SchemaGrpc.SchemaBlockingStub stub;

    public SchemaClient(ManagedChannel channel) {
        super(channel);
        this.stub = SchemaGrpc.newBlockingStub(channel);
    }

    public SchemaClient(SchemaGrpc.SchemaBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public long submitBatchDdl(String requestId, String sessionId, DdlRequestBatchPb ddlRequestBatchPb) {
        SubmitBatchDdlRequest request = SubmitBatchDdlRequest.newBuilder()
                .setRequestId(requestId)
                .setSessionId(sessionId)
                .setDdlRequests(ddlRequestBatchPb)
                .build();
        SubmitBatchDdlResponse submitBatchDdlResponse = stub.submitBatchDdl(request);
        if (submitBatchDdlResponse.getSuccess()) {
            long ddlSnapshotId = submitBatchDdlResponse.getDdlSnapshotId();
            return ddlSnapshotId;
        } else {
            throw new DdlException(submitBatchDdlResponse.getMsg());
        }
    }

}
