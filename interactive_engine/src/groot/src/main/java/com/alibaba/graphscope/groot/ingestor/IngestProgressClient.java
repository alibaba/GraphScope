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
package com.alibaba.graphscope.groot.ingestor;

import com.alibaba.maxgraph.proto.groot.GetTailOffsetsRequest;
import com.alibaba.maxgraph.proto.groot.GetTailOffsetsResponse;
import com.alibaba.maxgraph.proto.groot.IngestProgressGrpc;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import io.grpc.ManagedChannel;

import java.util.List;

/** ingestor -> coordinator */
public class IngestProgressClient extends RpcClient {

    private IngestProgressGrpc.IngestProgressBlockingStub stub;

    public IngestProgressClient(ManagedChannel channel) {
        super(channel);
        this.stub = IngestProgressGrpc.newBlockingStub(this.channel);
    }

    public IngestProgressClient(IngestProgressGrpc.IngestProgressBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public List<Long> getTailOffsets(List<Integer> queueIds) {
        GetTailOffsetsRequest req =
                GetTailOffsetsRequest.newBuilder().addAllQueueId(queueIds).build();
        GetTailOffsetsResponse tailOffsetsResponse = stub.getTailOffsets(req);
        return tailOffsetsResponse.getOffsetsList();
    }
}
