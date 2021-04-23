package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.proto.v2.GetTailOffsetsRequest;
import com.alibaba.maxgraph.proto.v2.GetTailOffsetsResponse;
import com.alibaba.maxgraph.proto.v2.IngestProgressGrpc;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;

import java.util.List;

/**
 * ingestor -> coordinator
 */
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
        GetTailOffsetsRequest req = GetTailOffsetsRequest.newBuilder()
                .addAllQueueId(queueIds)
                .build();
        GetTailOffsetsResponse tailOffsetsResponse = stub.getTailOffsets(req);
        return tailOffsetsResponse.getOffsetsList();
    }

}
