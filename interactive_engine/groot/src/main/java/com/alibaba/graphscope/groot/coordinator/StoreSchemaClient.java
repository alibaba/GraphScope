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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.maxgraph.proto.groot.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.groot.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.groot.StoreSchemaGrpc;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.groot.schema.GraphDef;
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
        FetchSchemaResponse response =
                this.stub.fetchSchema(FetchSchemaRequest.newBuilder().build());
        GraphDef graphDef = GraphDef.parseProto(response.getGraphDef());
        return graphDef;
    }
}
