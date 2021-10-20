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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.maxgraph.proto.groot.DdlRequestBatchPb;
import com.alibaba.maxgraph.proto.groot.SchemaGrpc;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlRequest;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlResponse;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.groot.schema.request.DdlException;
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

    public long submitBatchDdl(
            String requestId, String sessionId, DdlRequestBatchPb ddlRequestBatchPb) {
        SubmitBatchDdlRequest request =
                SubmitBatchDdlRequest.newBuilder()
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
