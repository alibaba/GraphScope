package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.rpc.RpcChannel;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.AllocateIdRequest;
import com.alibaba.graphscope.proto.groot.AllocateIdResponse;
import com.alibaba.graphscope.proto.groot.IdAllocateGrpc;

public class IdAllocateClient extends RpcClient {
    public IdAllocateClient(RpcChannel channel) {
        super(channel);
    }

    private IdAllocateGrpc.IdAllocateBlockingStub getStub() {
        return IdAllocateGrpc.newBlockingStub(rpcChannel.getChannel());
    }

    public long allocateId(int allocateSize) {
        AllocateIdRequest req =
                AllocateIdRequest.newBuilder().setAllocateSize(allocateSize).build();
        AllocateIdResponse response = getStub().allocateId(req);
        return response.getStartId();
    }
}
