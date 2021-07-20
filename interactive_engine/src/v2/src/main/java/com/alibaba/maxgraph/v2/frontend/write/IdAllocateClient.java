package com.alibaba.maxgraph.v2.frontend.write;

import com.alibaba.maxgraph.proto.v2.AllocateIdRequest;
import com.alibaba.maxgraph.proto.v2.AllocateIdResponse;
import com.alibaba.maxgraph.proto.v2.IdAllocateGrpc;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;

public class IdAllocateClient extends RpcClient {

    private IdAllocateGrpc.IdAllocateBlockingStub stub;

    public IdAllocateClient(ManagedChannel channel) {
        super(channel);
        this.stub = IdAllocateGrpc.newBlockingStub(channel);
    }

    public long allocateId(int allocateSize) {
        AllocateIdRequest req = AllocateIdRequest.newBuilder().setAllocateSize(allocateSize).build();
        AllocateIdResponse response = stub.allocateId(req);
        return response.getStartId();
    }
}
