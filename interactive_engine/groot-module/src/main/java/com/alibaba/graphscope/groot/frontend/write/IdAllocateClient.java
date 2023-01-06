package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.AllocateIdRequest;
import com.alibaba.graphscope.proto.groot.AllocateIdResponse;
import com.alibaba.graphscope.proto.groot.IdAllocateGrpc;

import io.grpc.ManagedChannel;

public class IdAllocateClient extends RpcClient {

    private IdAllocateGrpc.IdAllocateBlockingStub stub;

    public IdAllocateClient(ManagedChannel channel) {
        super(channel);
        this.stub = IdAllocateGrpc.newBlockingStub(channel);
    }

    public long allocateId(int allocateSize) {
        AllocateIdRequest req =
                AllocateIdRequest.newBuilder().setAllocateSize(allocateSize).build();
        AllocateIdResponse response = stub.allocateId(req);
        return response.getStartId();
    }
}
