/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.maxgraph.proto.groot.CoordinatorSnapshotServiceGrpc;
import com.alibaba.maxgraph.proto.groot.UpdateMinQuerySnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.UpdateMinQuerySnapshotIdResponse;

import io.grpc.ManagedChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// send rpc to Coordinator Service,  to report minimum snapshot current used
public class SnapshotUpdateClient extends RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotUpdateClient.class);

    private CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceBlockingStub stub;

    public SnapshotUpdateClient(ManagedChannel channel) {
        super(channel);
        this.stub = CoordinatorSnapshotServiceGrpc.newBlockingStub(channel);
    }

    public SnapshotUpdateClient(
            CoordinatorSnapshotServiceGrpc.CoordinatorSnapshotServiceBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void updateSnapshot(int frontendId, long snapshotId) throws RuntimeException {
        UpdateMinQuerySnapshotIdRequest req =
                UpdateMinQuerySnapshotIdRequest.newBuilder()
                        .setFrontendId(frontendId)
                        .setSnapshotId(snapshotId)
                        .build();
        UpdateMinQuerySnapshotIdResponse res = stub.updateMinQuerySnapshotId(req);
        if (!res.getSuccess()) {
            throw new RuntimeException("update snapshot fail {} " + res.getErrMsg());
        }
    }
}
