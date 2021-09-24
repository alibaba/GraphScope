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
package com.alibaba.maxgraph.groot.coordinator;

import com.alibaba.maxgraph.proto.v2.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.v2.StoreSchemaGrpc;
import com.alibaba.maxgraph.groot.common.rpc.RpcClient;
import com.alibaba.maxgraph.groot.common.schema.GraphDef;
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
