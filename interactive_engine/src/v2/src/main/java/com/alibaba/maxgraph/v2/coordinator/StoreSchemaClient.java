package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.proto.v2.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.v2.StoreSchemaGrpc;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import io.grpc.ManagedChannel;

public class StoreSchemaClient extends RpcClient {

    private StoreSchemaGrpc.StoreSchemaBlockingStub stub;

    public StoreSchemaClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreSchemaGrpc.newBlockingStub(channel);
    }

    public StoreSchemaClient(StoreSchemaGrpc.StoreSchemaBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public GraphDef fetchSchema() {
        FetchSchemaResponse response = this.stub.fetchSchema(FetchSchemaRequest.newBuilder().build());
        GraphDef graphDef = GraphDef.parseProto(response.getGraphDef());
        return graphDef;
    }
}
