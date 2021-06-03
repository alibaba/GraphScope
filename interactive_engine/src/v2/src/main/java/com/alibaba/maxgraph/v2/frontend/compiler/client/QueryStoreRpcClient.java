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
package com.alibaba.maxgraph.v2.frontend.compiler.client;

import com.alibaba.maxgraph.proto.v2.ScanStoreRequest;
import com.alibaba.maxgraph.proto.v2.StoreEdgesResponse;
import com.alibaba.maxgraph.proto.v2.StoreReadGrpc;
import com.alibaba.maxgraph.proto.v2.StoreTargetEdgesResponse;
import com.alibaba.maxgraph.proto.v2.StoreVertexIdsResponse;
import com.alibaba.maxgraph.proto.v2.StoreVerticesResponse;
import com.alibaba.maxgraph.proto.v2.VertexLabelIdsRequest;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class QueryStoreRpcClient extends RpcClient {

    public QueryStoreRpcClient(ManagedChannel channel) {
        super(channel);
    }

    public void getVertexes(VertexLabelIdsRequest request,
                            StreamObserver<StoreVerticesResponse> vertexStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.queryVertices(request, vertexStreamObserver);
    }

    public void getOutVertexes(VertexLabelIdsRequest request,
                               StreamObserver<StoreVertexIdsResponse> vertexStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.getOutVertices(request, vertexStreamObserver);
    }

    public void getOutEdges(VertexLabelIdsRequest request,
                            StreamObserver<StoreTargetEdgesResponse> edgesResponseStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.getOutEdges(request, edgesResponseStreamObserver);
    }

    public void getInVertexes(VertexLabelIdsRequest request,
                              StreamObserver<StoreVertexIdsResponse> vertexStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.getInVertices(request, vertexStreamObserver);
    }

    public void getInEdges(VertexLabelIdsRequest request,
                           StreamObserver<StoreTargetEdgesResponse> edgesResponseStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.getInEdges(request, edgesResponseStreamObserver);
    }

    public void scanVertexes(ScanStoreRequest request,
                             StreamObserver<StoreVerticesResponse> vertexStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.scanVertices(request, vertexStreamObserver);
    }

    public void scanEdges(ScanStoreRequest request,
                          StreamObserver<StoreEdgesResponse> vertexStreamObserver) {
        StoreReadGrpc.StoreReadStub readStub = StoreReadGrpc.newStub(this.channel);
        readStub.scanEdges(request, vertexStreamObserver);
    }
}
